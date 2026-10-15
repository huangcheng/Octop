"""Daemon-side executor for ``fs.*`` frames (spec §8/§8.2).

The executor owns every filesystem operation the hub requests over the
runtime session. It is pure ``infra`` code: stdlib + the shared protocol
module only — never ``api``/``cli``.

Reply convention (agreed with the hub backend):
- Success replies ride ``fs.write`` frames echoing the request id: raw file
  bytes for ``fs.read``; a compact JSON document for everything else
  (``ls``/``stat``/``mkdir``/``write`` ack/``edit``/``delete``/``rename``/
  ``glob``/``grep``), chunked with ``chunk_index``/``total`` so the hub
  reassembles via :class:`~octop.infra.runtime.protocol.ChunkBuffer`.
- Failures are ``error`` frames carrying the request id and an
  :class:`~octop.infra.runtime.protocol.ErrorKind`.
- Credit: ``fs.read`` replies consume hub-granted bytes (initial grant
  mirrored from ``rt.welcome``, replenished by inbound ``rt.credit``);
  inbound ``fs.write`` payloads consume runtime-granted bytes (initial grant
  mirrored from ``rt.ready``, replenished via outbound ``rt.credit``).

Containment (spec §8.2):
- every path resolves under the CURRENT roots allowlist, re-read per
  operation (not hello-time);
- POSIX: per-component ``openat`` chain with ``O_NOFOLLOW`` on every
  component; Linux prefers ``openat2(RESOLVE_BENEATH)``;
- ``..`` components, absolute escapes, and symlink escapes are rejected with
  ``path_escape``;
- read/write/edit operate on regular files only (``S_ISREG``).

Documented residuals (spec-accepted):
- append writes in-place per chunk batch with fsync per batch; an
  interrupted append leaves already-written batches in the file (torn
  residual). Overwrites are atomic (temp + fsync + renameat) — an
  interrupted overwrite discards its temp and leaves the target untouched.
- macOS has no ``openat2``: a final-component swap racing the last open
  step is detected only by the etag check inside ``fs.edit`` (dedicated
  fixture). Hardlink escapes share an inode with an in-root file and are
  therefore not distinguishable — documented.
- Windows (when ``dir_fd`` syscalls are unavailable) falls back to
  textual prefix containment: best-effort, consent-visible per spec.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import ctypes
import errno
import json
import logging
import os
import re
import shutil
import stat as stat_module
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

from octop.infra.runtime.protocol import (
    CHUNK_BYTES,
    INITIAL_CREDIT_BYTES,
    CreditLedger,
    ErrorFrame,
    ErrorKind,
    FrameType,
    FsDelete,
    FsEdit,
    FsMkdir,
    FsPath,
    FsPattern,
    FsRead,
    FsRename,
    FsWrite,
    RtCredit,
    chunk_bytes,
)
from octop.infra.utils.ulid import new_ulid

logger = logging.getLogger("octop.runtime.executor")

#: Sends one encoded frame; wired to :meth:`RuntimeClient._send_frame`.
Sender = Callable[[FrameType, Any, "str | None"], Awaitable[None]]
#: Roots allowlist: a static sequence or a zero-arg callable re-read per op.
RootsProvider = Callable[[], Sequence[str]]

# --- Tunables (constructor-injectable for tests) ------------------------------

MAX_INFLIGHT_FS_OPS = 16
LIST_CAP = 10_000
#: Hard cap on files inspected by one glob/grep walk before truncation.
SCAN_CAP_FILES = 50_000
SINGLE_WRITE_CAP_BYTES = 256 * 1024 * 1024
TEMP_TOTAL_CAP_BYTES = 512 * 1024 * 1024
READ_RATE_BYTES_PER_MINUTE = 50 * 1024 * 1024
READ_RATE_WINDOW_SECONDS = 60.0
FS_CHUNK_TIMEOUT_SECONDS = 120.0
SWEEP_INTERVAL_SECONDS = 3600.0
#: Documented guards keeping one-shot JSON replies bounded.
DOC_MAX_BYTES = 16 * 1024 * 1024
GREP_TEXT_MAX_BYTES = 512
GREP_FILE_MAX_BYTES = 64 * 1024 * 1024
BINARY_SNIFF_BYTES = 8192
#: Bounded memory for completed-request duplicate detection (ids only).
CLOSED_ID_CACHE = 1024

TMP_PREFIX = ".octop-tmp-"

_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)
_HAS_DIR_FD = os.open in os.supports_dir_fd and bool(_NOFOLLOW)


# --- Errors -------------------------------------------------------------------


class PathEscapeError(Exception):
    """A requested path left the roots allowlist (maps to ``path_escape``)."""


class FsError(Exception):
    """Operation failure carrying its wire error kind."""

    def __init__(self, code: ErrorKind, detail: str = "") -> None:
        super().__init__(detail or code.value)
        self.code = code
        self.detail = detail or code.value


def _errno_detail(exc: OSError) -> str:
    if exc.errno is None:
        return str(exc)
    return f"{os.strerror(exc.errno)} (errno {exc.errno})"


def _to_fs_error(exc: BaseException) -> FsError:
    if isinstance(exc, FsError):
        return exc
    if isinstance(exc, PathEscapeError):
        return FsError(ErrorKind.PATH_ESCAPE, str(exc))
    if isinstance(exc, OSError):
        return FsError(ErrorKind.INTERNAL, _errno_detail(exc))
    return FsError(ErrorKind.INTERNAL, str(exc) or exc.__class__.__name__)


# --- Containment: path resolution (sync; runs in worker threads) ---------------


def _path_parts(path: str) -> list[str]:
    if not path.startswith(os.sep):
        raise PathEscapeError("path must be absolute")
    parts = [p for p in path.split(os.sep) if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise PathEscapeError("'..' components are not allowed")
    return parts


def _match_root(path: str, roots: Sequence[str]) -> tuple[str, str]:
    """Longest root containing *path* -> ``(root, relative remainder)``."""
    norm = os.path.normpath(path)
    for root in sorted((os.path.normpath(r) for r in roots), key=len, reverse=True):
        if norm == root:
            return root, ""
        if norm.startswith(root + os.sep):
            return root, norm[len(root) + 1 :]
    raise PathEscapeError("path is outside the roots allowlist")


def _open_root_dir(root: str) -> int:
    """Open a root directory fd. Roots are owner-configured trust anchors."""
    return os.open(root, os.O_RDONLY | os.O_DIRECTORY)


def _open_dir_beneath(root_fd: int, parts: Sequence[str]) -> int:
    """fd of the directory formed by *parts*, each opened with O_NOFOLLOW."""
    fd = os.dup(root_fd)
    try:
        for part in parts:
            nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = nxt
        return fd
    except OSError as exc:
        try:
            if exc.errno in (errno.ELOOP, errno.EMLINK):
                raise PathEscapeError(f"symlink in path component: {exc}") from exc
            if exc.errno == errno.ENOTDIR:
                # macOS reports ENOTDIR (not ELOOP) for O_NOFOLLOW|O_DIRECTORY
                # on a symlinked component; disambiguate from a plain missing
                # path with lstat.
                try:
                    mode = os.stat(part, dir_fd=fd, follow_symlinks=False).st_mode
                except OSError:
                    mode = 0
                if stat_module.S_ISLNK(mode):
                    raise PathEscapeError(f"symlink in path component: {exc}") from exc
        finally:
            os.close(fd)
        raise
    except BaseException:
        os.close(fd)
        raise


def _map_loop(exc: OSError) -> BaseException:
    if exc.errno in (errno.ELOOP, errno.EMLINK):
        return PathEscapeError(f"symlink escape rejected: {_errno_detail(exc)}")
    return exc


# Linux openat2(RESOLVE_BENEATH) fast path -------------------------------------

_SYS_openat2 = 437  # x86_64 and aarch64 share the number
_RESOLVE_BENEATH = 0x02
_openat2_available: bool | None = None


class _OpenHow(ctypes.Structure):
    _fields_ = [
        ("flags", ctypes.c_ulonglong),
        ("mode", ctypes.c_ulonglong),
        ("resolve", ctypes.c_ulonglong),
    ]


def _openat2_beneath(root_fd: int, rel: str, flags: int) -> int | None:
    """Single-shot ``openat2(RESOLVE_BENEATH)``; ``None`` when unsupported."""
    global _openat2_available
    if _openat2_available is False or not sys.platform.startswith("linux"):
        return None
    libc = ctypes.CDLL(None, use_errno=True)
    how = _OpenHow(flags=flags & ~os.O_CREAT, mode=0, resolve=_RESOLVE_BENEATH)
    result = libc.syscall(
        ctypes.c_long(_SYS_openat2),
        ctypes.c_int(root_fd),
        ctypes.c_char_p(rel.encode()),
        ctypes.byref(how),
        ctypes.c_size_t(ctypes.sizeof(how)),
    )
    if result >= 0:
        _openat2_available = True
        return int(result)
    code = ctypes.get_errno()
    if code in (errno.ENOSYS, errno.EINVAL, errno.EPERM, errno.EOPNOTSUPP):
        _openat2_available = False
        return None
    raise OSError(code, os.strerror(code), rel)


def _open_final(root_fd: int, parts: Sequence[str], flags: int) -> int:
    """Open the final component of *parts* relative to *root_fd*."""
    rel = "/".join(parts)
    if _HAS_DIR_FD:
        fd = _openat2_beneath(root_fd, rel, flags)
        if fd is not None:
            return fd
        parent = _open_dir_beneath(root_fd, parts[:-1])
        try:
            try:
                return os.open(parts[-1], flags, dir_fd=parent)
            except OSError as exc:
                raise _map_loop(exc) from exc
        finally:
            os.close(parent)
    # Windows best-effort: textual containment already proven by _match_root.
    return os.open(rel, flags)


def open_regular_file(path: str, roots: Sequence[str], flags: int) -> tuple[int, os.stat_result]:
    """Open a contained regular file; returns ``(fd, fstat)``.

    ``O_NOFOLLOW`` + ``O_NONBLOCK`` on every open: symlinks are rejected
    (ELOOP -> ``path_escape``) and FIFOs/devices fail the ``S_ISREG`` check
    instead of blocking the worker thread.
    """
    parts = _path_parts(path)
    if not parts:
        raise FsError(ErrorKind.INTERNAL, "root itself is not a regular file")
    root, rel = _match_root(path, roots)
    if not rel:
        raise FsError(ErrorKind.INTERNAL, "root itself is not a regular file")
    root_fd = _open_root_dir(root)
    try:
        try:
            fd = _open_final(root_fd, rel.split("/"), flags | _NOFOLLOW | _NONBLOCK)
        except OSError as exc:
            raise _map_loop(exc) from exc
    finally:
        os.close(root_fd)
    try:
        st = os.fstat(fd)
        if not stat_module.S_ISREG(st.st_mode):
            raise FsError(ErrorKind.INTERNAL, "not a regular file")
    except BaseException:
        os.close(fd)
        raise
    return fd, st


def resolve_parent(path: str, roots: Sequence[str]) -> tuple[str, int, str]:
    """Resolve to ``(root, parent-dir fd, final name)``; caller closes the fd.

    The final component is deliberately NOT opened or followed — callers
    operate on the bare name through ``dir_fd`` syscalls (mkdir/unlink/
    rename/temp-create), which never traverse symlinks.
    """
    parts = _path_parts(path)
    if not parts:
        raise PathEscapeError("cannot operate on a root itself")
    root, rel = _match_root(path, roots)
    rel_parts = rel.split("/") if rel else []
    if not rel_parts:
        raise PathEscapeError("cannot operate on a root itself")
    root_fd = _open_root_dir(root)
    try:
        if _HAS_DIR_FD:
            return root, _open_dir_beneath(root_fd, rel_parts[:-1]), rel_parts[-1]
        # Windows best-effort: parent opened by full path.
        parent = os.path.join(root, *(rel_parts[:-1]))
        return root, os.open(parent or root, os.O_RDONLY | os.O_DIRECTORY), rel_parts[-1]
    except OSError as exc:
        os.close(root_fd)
        raise _map_loop(exc) from exc
    finally:
        os.close(root_fd)


def weak_etag(st: os.stat_result) -> str:
    """Weak etag: mtime_ns + size + inode (spec §8 documented collision window)."""
    return f"{st.st_mtime_ns}:{st.st_size}:{st.st_ino}"


def lstat_final(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def rmtree_fd(fd: int) -> None:
    """Depth-first contained delete of the directory open at *fd*."""
    for name in os.listdir(fd):
        st = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat_module.S_ISDIR(st.st_mode):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW, dir_fd=fd)
            try:
                rmtree_fd(child)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=fd)
        else:
            os.unlink(name, dir_fd=fd)


def iter_tree_fd(fd: int, rel: str, scanned: list[int], scan_cap: int) -> Any:
    """Yield ``(rel_path, lstat)`` under the directory open at *fd*.

    Symlinks are yielded once and never descended (``symlink-escape
    skipped``). ``scanned`` counts entries against *scan_cap*.
    """
    for name in sorted(os.listdir(fd)):
        scanned[0] += 1
        if scanned[0] > scan_cap:
            return
        st = os.stat(name, dir_fd=fd, follow_symlinks=False)
        child_rel = f"{rel}/{name}" if rel else name
        yield child_rel, st
        if stat_module.S_ISDIR(st.st_mode):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW, dir_fd=fd)
            try:
                yield from iter_tree_fd(child, child_rel, scanned, scan_cap)
            finally:
                os.close(child)


def glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Harness glob dialect: ``**`` crosses directories, ``*`` does not."""
    out: list[str] = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "*":
            if pattern[i : i + 3] == "**/":
                out.append("(?:.*/)?")
                i += 3
                continue
            if pattern[i : i + 2] == "**":
                out.append(".*")
                i += 2
                continue
            out.append("[^/]*")
            i += 1
        elif ch == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(ch))
            i += 1
    return re.compile("(?s:" + "".join(out) + r")\Z")


# --- Per-path async locks ------------------------------------------------------


class _LockEntry:
    __slots__ = ("lock", "usage")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.usage = 0


# --- Inbound fs.write streams --------------------------------------------------


class _WriteStream:
    """One assembling ``fs.write`` transfer (request id keyed)."""

    def __init__(self, request_id: str, path: str, append: bool, total: int) -> None:
        self.request_id = request_id
        self.path = path
        self.append = append
        self.total = max(1, total)
        self.seen = 0
        self.received = 0
        self.lock = asyncio.Lock()
        self.done = False
        self.cleaned = False
        # Filled by _open_write_target:
        self.root = ""
        self.parent_fd: int | None = None
        self.final_name = ""
        self.tmp_name: str | None = None
        self.tmp_fd: int | None = None
        self.tmp_abs: str | None = None
        self.temp_counted = 0


# --- Executor -------------------------------------------------------------------


class FsExecutor:
    """Serves ``fs.*`` frames against a re-read-per-op roots allowlist."""

    def __init__(
        self,
        roots: Sequence[str] | RootsProvider,
        *,
        sender: Sender | None = None,
        initial_credit_bytes: int = INITIAL_CREDIT_BYTES,
        chunk_timeout_seconds: float = FS_CHUNK_TIMEOUT_SECONDS,
        max_inflight_ops: int = MAX_INFLIGHT_FS_OPS,
        list_cap: int = LIST_CAP,
        scan_cap: int = SCAN_CAP_FILES,
        single_write_cap_bytes: int = SINGLE_WRITE_CAP_BYTES,
        temp_total_cap_bytes: int = TEMP_TOTAL_CAP_BYTES,
        read_rate_bytes_per_minute: int = READ_RATE_BYTES_PER_MINUTE,
        sweep_interval_seconds: float = SWEEP_INTERVAL_SECONDS,
    ) -> None:
        self._roots: RootsProvider = roots if callable(roots) else (lambda: roots)
        self._sender = sender
        self._initial_credit = initial_credit_bytes
        self._chunk_timeout = chunk_timeout_seconds
        self._max_inflight = max_inflight_ops
        self._list_cap = list_cap
        self._scan_cap = scan_cap
        self._single_write_cap = single_write_cap_bytes
        self._temp_cap = temp_total_cap_bytes
        self._read_rate = read_rate_bytes_per_minute
        self._sweep_interval = sweep_interval_seconds

        self._out_credit = CreditLedger()
        self._credit_events: dict[str, asyncio.Event] = {}
        self._locks: dict[str, _LockEntry] = {}
        self._streams: dict[str, _WriteStream] = {}
        self._read_tasks: dict[str, asyncio.Task[None]] = {}
        self._inbound_granted: dict[str, int] = {}
        self._active: set[str] = set()
        self._closed_ids: collections.deque[str] = collections.deque(maxlen=CLOSED_ID_CACHE)
        self._inflight = 0
        self._temp_bytes = 0
        self._live_temps: set[str] = set()
        self._read_window: collections.deque[tuple[float, int]] = collections.deque()
        self._sweep_task: asyncio.Task[None] | None = None
        self._closed = False

    # -- wiring ---------------------------------------------------------------

    def bind(self, sender: Sender) -> None:
        self._sender = sender

    def start_sweeper(self) -> None:
        """Startup sweep of orphaned temps, then hourly (spec §8)."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return  # no session loop yet; a later bind/start call retries
        if self._sweep_task is None or self._sweep_task.done():
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def close(self) -> None:
        """Abort every stream, stop the sweeper, wake credit waiters."""
        self._closed = True
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            self._sweep_task = None
        for stream in list(self._streams.values()):
            with contextlib.suppress(Exception):
                await self._teardown_stream(stream)
        for task in list(self._read_tasks.values()):
            task.cancel()
        for event in self._credit_events.values():
            event.set()

    # -- admission / bookkeeping ------------------------------------------------

    def _load_roots(self) -> list[str]:
        return [r for r in (str(x) for x in self._roots()) if r]

    def _admit(self, request_id: str) -> None:
        if request_id in self._active:
            raise FsError(ErrorKind.DUPLICATE_REQUEST, "request already in flight")
        if request_id in self._closed_ids:
            raise FsError(ErrorKind.DUPLICATE_REQUEST, "request already completed")
        if self._inflight >= self._max_inflight:
            raise FsError(ErrorKind.QUOTA_EXCEEDED, "in-flight fs ops cap reached")
        self._active.add(request_id)
        self._inflight += 1

    def _release(self, request_id: str) -> None:
        if request_id in self._active:
            self._active.discard(request_id)
            self._inflight = max(0, self._inflight - 1)
        self._closed_ids.append(request_id)

    def _admit_read_rate(self, nbytes: int) -> None:
        """Admission-only read rate (already-open streams may finish)."""
        now = time.monotonic()
        while self._read_window and now - self._read_window[0][0] > READ_RATE_WINDOW_SECONDS:
            self._read_window.popleft()
        used = sum(n for _, n in self._read_window)
        if used + nbytes > self._read_rate:
            raise FsError(
                ErrorKind.QUOTA_EXCEEDED, f"read rate exceeded ({used + nbytes} bytes/min cap)"
            )
        self._read_window.append((now, nbytes))

    # -- locks -------------------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _lock_paths(self, paths: Sequence[str]) -> AsyncIterator[None]:
        """Acquire per-path locks in sorted order (deadlock-free)."""
        entries: list[tuple[str, _LockEntry]] = []
        for path in sorted(set(paths)):
            entry = self._locks.get(path)
            if entry is None:
                entry = _LockEntry()
                self._locks[path] = entry
            entry.usage += 1
            entries.append((path, entry))
        try:
            for _, entry in entries:
                await entry.lock.acquire()
            yield
        finally:
            for path, entry in reversed(entries):
                entry.lock.release()
                entry.usage -= 1
                if entry.usage <= 0 and self._locks.get(path) is entry:
                    del self._locks[path]

    # -- send helpers --------------------------------------------------------------

    async def _send(self, frame_type: FrameType, payload: Any, request_id: str | None) -> None:
        if self._sender is None:
            raise FsError(ErrorKind.INTERNAL, "executor is not bound to a sender")
        await self._sender(frame_type, payload, request_id)

    async def _send_error(self, request_id: str | None, code: ErrorKind, detail: str) -> None:
        try:
            await self._send(
                FrameType.ERROR,
                ErrorFrame(request_id=request_id, code=code, detail=detail),
                request_id,
            )
        except Exception:
            logger.debug("error frame send failed for %s", request_id, exc_info=True)

    async def _send_doc(self, request_id: str, doc: dict[str, Any], path_echo: str = "") -> None:
        """Chunked one-shot JSON reply (ChunkBuffer-compatible framing)."""
        data = json.dumps(doc, separators=(",", ":")).encode()
        if len(data) > DOC_MAX_BYTES:
            raise FsError(ErrorKind.QUOTA_EXCEEDED, "reply document too large")
        chunks = chunk_bytes(data, CHUNK_BYTES)
        for index, chunk in enumerate(chunks):
            await self._send(
                FrameType.FS_WRITE,
                FsWrite(path=path_echo, data=chunk, chunk_index=index, total=len(chunks)),
                request_id,
            )

    async def _grant_inbound_credit(self, stream_id: str, nbytes: int) -> None:
        try:
            await self._send(FrameType.RT_CREDIT, RtCredit(stream_id=stream_id, bytes=nbytes), None)
        except Exception:
            logger.debug("rt.credit send failed for %s", stream_id, exc_info=True)

    # -- frame handlers -----------------------------------------------------------

    async def handle_fs_ls(self, frame: Any) -> None:
        payload: FsPath = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                doc = await asyncio.to_thread(self._sync_ls, payload.path)
                await self._send_doc(rid, doc, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_stat(self, frame: Any) -> None:
        payload: FsPath = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                doc = await asyncio.to_thread(self._sync_stat, payload.path)
                await self._send_doc(rid, doc, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_mkdir(self, frame: Any) -> None:
        payload: FsMkdir = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                await asyncio.to_thread(self._sync_mkdir, payload.path, payload.recursive)
                await self._send_doc(rid, {}, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_read(self, frame: Any) -> None:
        payload: FsRead = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
        except FsError as exc:
            await self._reply_error(rid, exc)
            return
        self._read_tasks[rid] = asyncio.current_task()  # type: ignore[assignment]
        # The hub's initial grant applies to every outbound stream.
        self._out_credit.grant(rid, self._initial_credit)
        try:
            await self._read_stream(rid, payload)
        except asyncio.CancelledError:
            raise
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)
        finally:
            self._read_tasks.pop(rid, None)
            self._out_credit.release(rid)
            event = self._credit_events.pop(rid, None)
            if event is not None:
                event.set()
            self._release(rid)

    async def handle_fs_write(self, frame: Any) -> None:
        payload: FsWrite = frame.payload
        rid = frame.request_id
        assert rid is not None
        stream = self._streams.get(rid)
        if stream is None:
            if payload.chunk_index != 0:
                logger.debug("dropping chunk %s for unknown fs.write stream", rid)
                return
            try:
                self._admit(rid)
            except FsError as exc:
                await self._reply_error(rid, exc)
                return
            stream = _WriteStream(rid, payload.path, payload.append, payload.total)
            self._streams[rid] = stream
            self._inbound_granted[rid] = self._initial_credit
            try:
                await asyncio.to_thread(self._open_write_target, stream)
            except (FsError, PathEscapeError, OSError) as exc:
                await self._teardown_stream(stream)
                await self._reply_error(rid, exc)
                return
        if stream.done:
            logger.debug("dropping chunk for completed fs.write stream %s", rid)
            return
        async with stream.lock:
            if stream.done:
                return
            try:
                await self._write_chunk(stream, payload)
            except asyncio.CancelledError:
                await self._teardown_stream(stream)
                raise
            except BaseException as exc:
                await self._teardown_stream(stream)
                await self._reply_error(rid, exc)

    async def handle_fs_edit(self, frame: Any) -> None:
        payload: FsEdit = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                async with self._lock_paths((os.path.normpath(payload.path),)):
                    doc = await asyncio.to_thread(self._sync_edit, payload)
                await self._send_doc(rid, doc, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_delete(self, frame: Any) -> None:
        payload: FsDelete = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                await asyncio.to_thread(self._sync_delete, payload.path, payload.recursive)
                await self._send_doc(rid, {}, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_rename(self, frame: Any) -> None:
        payload: FsRename = frame.payload
        rid = frame.request_id
        assert rid is not None
        old = os.path.normpath(payload.old)
        new = os.path.normpath(payload.new)
        try:
            self._admit(rid)
            try:
                # Both path locks, alphabetical (spec §8; deadlock-free).
                async with self._lock_paths((old, new)):
                    await asyncio.to_thread(self._sync_rename, payload)
                await self._send_doc(rid, {}, path_echo=new)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_glob(self, frame: Any) -> None:
        payload: FsPattern = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                doc = await asyncio.to_thread(self._sync_glob, payload.pattern, payload.path)
                await self._send_doc(rid, doc, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_grep(self, frame: Any) -> None:
        payload: FsPattern = frame.payload
        rid = frame.request_id
        assert rid is not None
        try:
            self._admit(rid)
            try:
                doc = await asyncio.to_thread(self._sync_grep, payload.pattern, payload.path)
                await self._send_doc(rid, doc, path_echo=payload.path)
            finally:
                self._release(rid)
        except (FsError, PathEscapeError, OSError) as exc:
            await self._reply_error(rid, exc)

    async def handle_fs_cancel(self, frame: Any) -> None:
        payload = frame.payload.request_id
        task = self._read_tasks.get(payload)
        if task is not None:
            task.cancel()
            return
        stream = self._streams.get(payload)
        if stream is not None:
            async with stream.lock:
                await self._teardown_stream(stream)
        # Unknown ids: frames for closed streams drop silently (spec §5).

    async def handle_rt_credit(self, frame: Any) -> None:
        payload: RtCredit = frame.payload
        event = self._credit_events.get(payload.stream_id)
        if event is not None:
            self._out_credit.grant(payload.stream_id, payload.bytes)
            event.set()
        # Unknown stream ids (e.g. exec streams) are not ours yet.

    async def _reply_error(self, request_id: str, exc: BaseException) -> None:
        error = _to_fs_error(exc)
        logger.debug("fs op %s failed: %s %s", request_id, error.code.value, error.detail)
        await self._send_error(request_id, error.code, error.detail)

    # -- fs.read stream -------------------------------------------------------------

    async def _read_stream(self, rid: str, payload: FsRead) -> None:
        fd, st = await asyncio.to_thread(
            open_regular_file, payload.path, self._load_roots(), os.O_RDONLY
        )
        try:
            size = st.st_size
            start = max(0, min(payload.offset, size))
            remaining = payload.length if payload.length is not None else size - start
            remaining = max(0, min(remaining, size - start))
            self._admit_read_rate(remaining)
            total = max(1, -(-remaining // CHUNK_BYTES))
            index = 0
            sent = 0
            while sent < remaining:
                want = min(CHUNK_BYTES, remaining - sent)
                chunk = await self._pread_full(fd, want, start + sent)
                if not chunk:
                    # File shrank mid-read: complete early with a marker.
                    await self._send_chunk(rid, payload.path, b"", index, total, truncated=True)
                    return
                await self._send_chunk(rid, payload.path, chunk, index, total)
                sent += len(chunk)
                index += 1
            if remaining == 0:
                # EOF / empty range: a single empty chunk completes the stream.
                await self._send_chunk(rid, payload.path, b"", 0, 1)
        finally:
            await asyncio.to_thread(os.close, fd)

    async def _pread_full(self, fd: int, want: int, offset: int) -> bytes:
        buf = bytearray()
        while len(buf) < want:
            got = await asyncio.to_thread(os.pread, fd, want - len(buf), offset + len(buf))
            if not got:
                break
            buf += got
        return bytes(buf)

    async def _send_chunk(
        self, rid: str, path: str, data: bytes, index: int, total: int, *, truncated: bool = False
    ) -> None:
        event = self._credit_events.setdefault(rid, asyncio.Event())
        while not self._out_credit.may_send(rid, len(data)):
            if self._closed:
                raise FsError(ErrorKind.CANCELLED, "executor closed")
            try:
                await asyncio.wait_for(event.wait(), timeout=self._chunk_timeout)
            except TimeoutError:
                raise FsError(
                    ErrorKind.TIMEOUT, f"credit wait exceeded {self._chunk_timeout}s"
                ) from None
            event.clear()
        self._out_credit.record_sent(rid, len(data))
        await self._send(
            FrameType.FS_WRITE,
            FsWrite(path=path, data=data, chunk_index=index, total=total, truncated=truncated),
            rid,
        )

    # -- fs.write streams -------------------------------------------------------------

    def _open_write_target(self, stream: _WriteStream) -> None:
        roots = self._load_roots()
        stream.root, stream.parent_fd, stream.final_name = resolve_parent(stream.path, roots)
        existing = lstat_final(stream.parent_fd, stream.final_name)
        if existing is not None and stat_module.S_ISLNK(existing.st_mode):
            raise PathEscapeError("refusing to write through a symlink")
        if existing is not None and not stat_module.S_ISREG(existing.st_mode):
            raise FsError(ErrorKind.INTERNAL, "not a regular file")
        if stream.append:
            return  # opened per batch with O_APPEND|O_CREAT
        stream.tmp_name = f"{TMP_PREFIX}{new_ulid().lower()}"
        stream.tmp_fd = os.open(
            stream.tmp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW,
            0o644,
            dir_fd=stream.parent_fd,
        )
        stream.tmp_abs = os.path.join(os.path.dirname(stream.path), stream.tmp_name)
        self._live_temps.add(stream.tmp_abs)

    async def _write_chunk(self, stream: _WriteStream, payload: FsWrite) -> None:
        rid = stream.request_id
        if payload.chunk_index != stream.seen:
            raise FsError(ErrorKind.INTERNAL, "out-of-order fs.write chunk")
        new_received = stream.received + len(payload.data)
        granted = self._inbound_granted.get(rid, self._initial_credit)
        if new_received > granted:
            raise FsError(
                ErrorKind.INTERNAL,
                f"credit oversend: {new_received} bytes against {granted} granted",
            )
        if new_received > self._single_write_cap:
            raise FsError(ErrorKind.QUOTA_EXCEEDED, "single transfer exceeds write cap")
        await self._write_batch(stream, payload.data, new_received)
        stream.received = new_received
        stream.seen += 1
        if not stream.append:
            self._temp_bytes += len(payload.data)
            stream.temp_counted += len(payload.data)
        await self._maybe_replenish(rid, stream.received)
        if stream.seen >= stream.total:
            await self._finish_write(stream)

    async def _write_batch(self, stream: _WriteStream, data: bytes, received_after: int) -> None:
        def work() -> None:
            free = shutil.disk_usage(stream.root).free
            if free < 2 * received_after:
                raise FsError(ErrorKind.QUOTA_EXCEEDED, "insufficient free space (2x pre-check)")
            if not stream.append:
                if self._temp_bytes + len(data) > self._temp_cap:
                    raise FsError(ErrorKind.QUOTA_EXCEEDED, "in-flight temp cap reached")
                tmp_fd = stream.tmp_fd
                assert tmp_fd is not None  # set by _open_write_target
                os.write(tmp_fd, data)
                return
            try:
                fd = os.open(
                    stream.final_name,
                    os.O_WRONLY | os.O_APPEND | os.O_CREAT | _NOFOLLOW | _NONBLOCK,
                    0o644,
                    dir_fd=stream.parent_fd,
                )
            except OSError as exc:
                raise _map_loop(exc) from exc
            try:
                os.write(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)

        if stream.append:
            async with self._lock_paths((os.path.normpath(stream.path),)):
                await asyncio.to_thread(work)
        else:
            await asyncio.to_thread(work)

    async def _maybe_replenish(self, rid: str, received: int) -> None:
        granted = self._inbound_granted.get(rid, 0)
        if granted - received < self._initial_credit // 2:
            self._inbound_granted[rid] = granted + self._initial_credit
            await self._grant_inbound_credit(rid, self._initial_credit)

    async def _finish_write(self, stream: _WriteStream) -> None:
        if stream.append:
            parent_fd = stream.parent_fd
            assert parent_fd is not None  # set by _open_write_target
            st = await asyncio.to_thread(lstat_final, parent_fd, stream.final_name)
            etag = weak_etag(st) if st is not None else None
        else:

            def commit() -> str:
                tmp_fd = stream.tmp_fd
                tmp_name = stream.tmp_name
                parent_fd = stream.parent_fd
                assert tmp_fd is not None and tmp_name is not None  # _open_write_target
                assert parent_fd is not None
                os.fsync(tmp_fd)
                os.rename(
                    tmp_name,
                    stream.final_name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
                return weak_etag(os.stat(stream.final_name, dir_fd=parent_fd))

            async with self._lock_paths((os.path.normpath(stream.path),)):
                etag = await asyncio.to_thread(commit)
        stream.done = True
        rid = stream.request_id
        await self._teardown_stream(stream)
        await self._send_doc(rid, {"etag": etag}, path_echo=stream.path)

    async def _teardown_stream(self, stream: _WriteStream) -> None:
        """Idempotent cleanup: close fds, discard temp, release admission."""
        if stream.cleaned:
            return
        stream.cleaned = True
        stream.done = True
        rid = stream.request_id
        self._streams.pop(rid, None)
        self._inbound_granted.pop(rid, None)
        if stream.tmp_abs is not None:
            self._live_temps.discard(stream.tmp_abs)
        if stream.temp_counted:
            self._temp_bytes = max(0, self._temp_bytes - stream.temp_counted)
            stream.temp_counted = 0

        def cleanup() -> None:
            if stream.tmp_fd is not None:
                os.close(stream.tmp_fd)
            if stream.tmp_name is not None and stream.parent_fd is not None:
                with contextlib.suppress(OSError):
                    os.unlink(stream.tmp_name, dir_fd=stream.parent_fd)
            if stream.parent_fd is not None:
                os.close(stream.parent_fd)

        try:
            await asyncio.to_thread(cleanup)
        except OSError:
            logger.debug("write stream cleanup failed for %s", rid, exc_info=True)
        self._release(rid)

    # -- sync operation bodies (worker threads) -----------------------------------------

    def _sync_stat(self, path: str) -> dict[str, Any]:
        roots = self._load_roots()
        norm = os.path.normpath(path)
        root, rel = _match_root(norm, roots)
        if not rel:  # the root itself
            st = os.stat(root)
        else:
            rel_parts = rel.split("/")
            root_fd = _open_root_dir(root)
            try:
                parent = _open_dir_beneath(root_fd, rel_parts[:-1])
            finally:
                os.close(root_fd)
            try:
                st = os.stat(rel_parts[-1], dir_fd=parent, follow_symlinks=False)
            finally:
                os.close(parent)
            if stat_module.S_ISLNK(st.st_mode):
                raise PathEscapeError("stat through a symlink rejected")
        return {
            "size": st.st_size,
            "is_dir": stat_module.S_ISDIR(st.st_mode),
            "etag": weak_etag(st),
            "mtime_ns": st.st_mtime_ns,
        }

    def _sync_ls(self, path: str) -> dict[str, Any]:
        roots = self._load_roots()
        norm = os.path.normpath(path)
        root, rel = _match_root(norm, roots)
        root_fd = _open_root_dir(root)
        try:
            fd = _open_dir_beneath(root_fd, rel.split("/") if rel else [])
        except BaseException:
            os.close(root_fd)
            raise
        os.close(root_fd)
        try:
            names = sorted(os.listdir(fd))
            truncated = len(names) > self._list_cap
            entries = []
            for name in names[: self._list_cap]:
                st = os.stat(name, dir_fd=fd, follow_symlinks=False)
                entries.append(
                    {
                        "path": os.path.join(norm, name),
                        "is_dir": stat_module.S_ISDIR(st.st_mode),
                        "size": st.st_size,
                    }
                )
            return {"entries": entries, "truncated": truncated}
        finally:
            os.close(fd)

    def _sync_mkdir(self, path: str, recursive: bool) -> None:
        _path_parts(path)  # reject '..' / non-absolute before touching the fs
        root, rel = _match_root(os.path.normpath(path), self._load_roots())
        rel_parts = rel.split("/") if rel else []
        if not rel_parts:
            return  # the root itself: nothing to create
        root_fd = _open_root_dir(root)
        try:
            if not recursive and len(rel_parts) > 1:
                raise FsError(ErrorKind.INTERNAL, "parent missing (recursive=false)")
            if not _HAS_DIR_FD:
                os.makedirs(os.path.join(root, *rel_parts), exist_ok=recursive)
                return
            fd = os.dup(root_fd)
            try:
                for part in rel_parts:
                    try:
                        os.mkdir(part, 0o777, dir_fd=fd)
                    except FileExistsError:
                        if not recursive:
                            raise
                    nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW, dir_fd=fd)
                    os.close(fd)
                    fd = nxt
            finally:
                os.close(fd)
        finally:
            os.close(root_fd)

    def _sync_delete(self, path: str, recursive: bool) -> None:
        root, parent_fd, name = resolve_parent(path, self._load_roots())
        try:
            st = lstat_final(parent_fd, name)
            if st is None:
                return
            if stat_module.S_ISDIR(st.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW, dir_fd=parent_fd)
                try:
                    if not recursive:
                        entries = os.listdir(child)
                        if entries:
                            raise FsError(
                                ErrorKind.INTERNAL,
                                f"directory not empty ({len(entries)} entries)",
                            )
                        os.rmdir(name, dir_fd=parent_fd)
                        return
                    rmtree_fd(child)
                finally:
                    os.close(child)
                os.rmdir(name, dir_fd=parent_fd)
            else:
                os.unlink(name, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)

    def _sync_rename(self, payload: FsRename) -> None:
        roots = self._load_roots()
        old_root, old_fd, old_name = resolve_parent(payload.old, roots)
        try:
            new_root, new_fd, new_name = resolve_parent(payload.new, roots)
        except BaseException:
            os.close(old_fd)
            raise
        try:
            if old_root != new_root:
                raise PathEscapeError("cross-root rename rejected")
            old_st = lstat_final(old_fd, old_name)
            if old_st is None:
                raise FileNotFoundError(errno.ENOENT, "no such file or directory", payload.old)
            if stat_module.S_ISDIR(old_st.st_mode):
                raise FsError(ErrorKind.CAPABILITY_UNSUPPORTED, "directory rename is not supported")
            if not stat_module.S_ISREG(old_st.st_mode):
                raise FsError(ErrorKind.INTERNAL, "only regular files can be renamed")
            try:
                os.rename(
                    old_name,
                    new_name,
                    src_dir_fd=old_fd,
                    dst_dir_fd=new_fd,
                )
            except OSError as exc:
                raise _map_loop(exc) from exc
        finally:
            os.close(old_fd)
            os.close(new_fd)

    def _sync_edit(self, payload: FsEdit) -> dict[str, Any]:
        fd, st = open_regular_file(payload.path, self._load_roots(), os.O_RDWR)
        try:
            # Etag verified INSIDE the path lock (caller holds it).
            current = weak_etag(st)
            if payload.etag is not None and payload.etag != current:
                raise FsError(ErrorKind.CONFLICT, f"etag mismatch: {payload.etag} != {current}")
            size = st.st_size
            content = b""
            while len(content) < size:
                got = os.pread(fd, size - len(content), len(content))
                if not got:
                    break
                content += got
            try:
                text = content.decode()
                find = payload.find
                replace = payload.replace
            except UnicodeDecodeError as exc:
                raise FsError(ErrorKind.INTERNAL, "file is not valid UTF-8") from exc
            occurrences = text.count(find)
            if not find:
                replacements = 0
                new_text = text
            elif payload.all:
                replacements = occurrences
                new_text = text.replace(find, replace)
            elif occurrences:
                replacements = 1
                new_text = text.replace(find, replace, 1)
            else:
                replacements = 0
                new_text = text
            # RMW through a temp in the same directory, then renameat —
            # atomic under the already-held path lock.
            root, parent_fd, name = resolve_parent(payload.path, self._load_roots())
            try:
                tmp = f"{TMP_PREFIX}{new_ulid().lower()}"
                tmp_fd = os.open(
                    tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW, 0o644, dir_fd=parent_fd
                )
                try:
                    os.write(tmp_fd, new_text.encode())
                    os.fsync(tmp_fd)
                    os.rename(tmp, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
                    new_st = os.stat(name, dir_fd=parent_fd)
                finally:
                    os.close(tmp_fd)
                    with contextlib.suppress(OSError):
                        os.unlink(tmp, dir_fd=parent_fd)
            finally:
                os.close(parent_fd)
            return {"occurrences": replacements, "etag": weak_etag(new_st)}
        finally:
            os.close(fd)

    def _walk_base(self, base: str) -> tuple[str, str, int]:
        """Resolve a glob/grep base to ``(root, rel, dir_fd)``."""
        roots = self._load_roots()
        if base:
            norm = os.path.normpath(base)
            root, rel = _match_root(norm, roots)
            root_fd = _open_root_dir(root)
            try:
                return root, rel, _open_dir_beneath(root_fd, rel.split("/") if rel else [])
            except BaseException:
                os.close(root_fd)
                raise
        # No base: walk every root (single synthetic walk when one root).
        first = os.path.normpath(roots[0])
        return first, "", _open_root_dir(first)

    def _sync_glob(self, pattern: str, base: str) -> dict[str, Any]:
        regex = glob_to_regex(pattern)
        absolute = pattern.startswith("/")
        scanned = [0]
        matches: list[str] = []
        roots = self._load_roots()
        bases = [base] if base else list(roots)
        truncated = False
        for one_base in bases:
            root, rel_base, fd = self._walk_base(one_base)
            try:
                for rel, st in iter_tree_fd(fd, rel_base, scanned, self._scan_cap):
                    if stat_module.S_ISLNK(st.st_mode):
                        continue  # symlink-escape skipped
                    candidate = os.path.join(root, rel)
                    if regex.match(candidate if absolute else rel):
                        matches.append(candidate)
                        if len(matches) >= self._list_cap:
                            truncated = True
                            break
            finally:
                os.close(fd)
            if truncated:
                break
        if scanned[0] > self._scan_cap:
            truncated = True
        return {"matches": matches, "truncated": truncated}

    def _sync_grep(self, pattern: str, base: str) -> dict[str, Any]:
        try:
            regex = re.compile(pattern)
        except re.error as exc:
            raise FsError(ErrorKind.INTERNAL, f"invalid regex: {exc}") from exc
        scanned = [0]
        matches: list[dict[str, Any]] = []
        truncated = False
        roots = self._load_roots()
        bases = [base] if base else list(roots)
        for one_base in bases:
            root, rel_base, fd = self._walk_base(one_base)
            try:
                for rel, st in iter_tree_fd(fd, rel_base, scanned, self._scan_cap):
                    if truncated:
                        break
                    if stat_module.S_ISLNK(st.st_mode) or not stat_module.S_ISREG(st.st_mode):
                        continue
                    if st.st_size > GREP_FILE_MAX_BYTES:
                        continue
                    found = self._grep_file(os.path.join(root, rel), regex)
                    for line_no, text in found:
                        matches.append(
                            {
                                "path": os.path.join(root, rel),
                                "line": line_no,
                                "text": text[:GREP_TEXT_MAX_BYTES],
                            }
                        )
                        if len(matches) >= self._list_cap:
                            truncated = True
                            break
                if truncated:
                    break
            finally:
                os.close(fd)
        if scanned[0] > self._scan_cap:
            truncated = True
        return {"matches": matches, "truncated": truncated}

    def _grep_file(self, path: str, regex: re.Pattern[str]) -> list[tuple[int, str]]:
        fd, _ = open_regular_file(path, self._load_roots(), os.O_RDONLY)
        try:
            head = os.pread(fd, BINARY_SNIFF_BYTES, 0)
            if b"\x00" in head:
                return []  # binary files skipped
            chunks = [head]
            pos = len(head)
            while True:
                got = os.pread(fd, CHUNK_BYTES, pos)
                if not got:
                    break
                chunks.append(got)
                pos += len(got)
            out: list[tuple[int, str]] = []
            for line_no, raw in enumerate(b"".join(chunks).splitlines(), start=1):
                text = raw.decode(errors="replace")
                if regex.search(text):
                    out.append((line_no, text))
            return out
        finally:
            os.close(fd)

    # -- temp sweeper ---------------------------------------------------------------

    async def _sweep_loop(self) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await self.sweep()
            while True:
                await asyncio.sleep(self._sweep_interval)
                await self.sweep()

    async def sweep(self) -> int:
        """Unlink orphaned ``.octop-tmp-*`` files (startup + hourly)."""
        try:
            return await asyncio.to_thread(self._sync_sweep)
        except OSError:
            logger.debug("temp sweep failed", exc_info=True)
            return 0

    def _sync_sweep(self) -> int:
        removed = 0
        for root in self._load_roots():
            if not os.path.isdir(root):
                continue
            for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
                for name in filenames:
                    if not name.startswith(TMP_PREFIX):
                        continue
                    if os.path.join(dirpath, name) in self._live_temps:
                        continue
                    try:
                        os.unlink(os.path.join(dirpath, name))
                        removed += 1
                    except OSError:
                        pass
        return removed
