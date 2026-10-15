"""Hub-side ``remote_runtime`` agent backend (spec §7.2/§8/§8.1).

:class:`RemoteRuntimeBackend` is the stable proxy agents hold: it is bound to
an ``enrollment_id`` (not to one WebSocket) and swaps the underlying
:class:`~octop.infra.runtime.registry.RuntimeSession` across generations.  It
implements the deepagents ``SandboxBackendProtocol`` so harness
``resolve_backend`` accepts it as a prebuilt instance and
``deepagents.supports_execution`` detects its execution capability (spike
outcome A).

Frame contract with the daemon executor (``infra/runtime/executor.py``)
---------------------------------------------------------------
The shared codec has no generic "result" frame, so both sides pin this
convention (shared surface = protocol frames only):

* Successful ``fs.*`` data replies are **``fs.write`` frames** — the only fs
  payload that carries ``data`` plus ``chunk_index``/``total``/``truncated``
  (``ChunkBuffer``-compatible) — echoing the request's ``request_id``:

  - ``fs.read`` replies carry **raw file bytes** (path echo, POSIX
    short-read/EOF; an empty/EOF range is one empty chunk with ``total=1``).
  - every other fs reply (``ls``/``stat``/``edit``/``mkdir``/``delete``/
    ``rename``/``write`` ack/``glob``/``grep``) carries a compact JSON doc:
    ``stat`` ``{"size","is_dir","etag","mtime_ns"}``,
    ``ls`` ``{"entries":[{"path","is_dir","size"}],"truncated"}``,
    ``glob`` ``{"matches":[str],"truncated"}``,
    ``grep`` ``{"matches":[{"path","line","text"}],"truncated"}``,
    ``edit`` ``{"occurrences",...}``, plain ``{}`` for simple acks.
* Failures are ``error`` frames carrying the originating ``request_id``.
* ``exec`` uses ``exec.output`` frames (stdout/stderr data, ``activity``
  heartbeats, ``exit`` with ``exit_code``/``reason``/request_id echo).
* Credit (bytes): the hub grants ``rt.credit`` for rt→hub streams
  (``fs.read`` replies, ``exec.output``) replenishing the window whenever the
  remaining credit drops below half the initial grant; for hub→rt data
  (``fs.write`` chunks, ``exec.input``) the hub consumes grants received from
  the runtime (initial = ``INITIAL_CREDIT_BYTES``, mirroring ``rt.ready``)
  before every chunk.  Control frames bypass credit entirely.

Design notes
------------
* **Line-based read.**  The harness ``read()`` contract is line-based while
  the wire protocol is byte-based.  The proxy fetches the file bytes **once
  per read call**, bounded by ``read_fetch_max_bytes`` (default 2 MiB), and
  slices lines hub-side; ``total_lines`` is only reported when the whole file
  was inside the fetch window.  Requests beyond the window fail with a clear
  error instead of lying about totals.
* **System files stay hub-local.**  ``BackendWorkspace`` routes system paths
  (``{workspace}/.octop``) through the backend itself — the split is not
  expressible at that layer, so the proxy enforces it with a path filter:
  virtual paths whose first segment is ``.octop`` are rejected with a clear
  error (spec §1: a ``remote_runtime`` backend carries content files only).
* **One-shot execute.**  ``execute()`` maps to ``exec.open`` + draining
  ``exec.output`` until the exit frame, merging stdout/stderr in arrival
  order (drain-before-resolve is the runtime's contract).  Hub-side hard
  wall-clock and idle timers (spec §8.1: idle = no frames of any kind;
  ``activity`` counts as activity) send ``exec.close`` and allow a 5 s grace
  for the exit frame before resolving locally with exit code 124 (matching
  the harness local-shell convention).
* **Swap semantics.**  Every op captures its session at dispatch.  When a
  newer generation supersedes it, in-flight ops die with ``RUNTIME_OFFLINE``
  (``RUNTIME_REVOKED`` when the close code is 4002); agent retries are always
  fresh requests with fresh ``request_id``s.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import fnmatch
import json
import logging
import posixpath
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, TypeVar, cast

from deepagents.backends.protocol import (
    DeleteResult,
    EditResult,
    ExecuteResponse,
    FileData,
    FileDownloadResponse,
    FileInfo,
    FileUploadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
    SandboxBackendProtocol,
    WriteResult,
)

from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.protocol import (
    CHUNK_BYTES,
    EXEC_QUEUE_BYTES,
    FS_STREAM_QUEUE_CHUNKS,
    INITIAL_CREDIT_BYTES,
    ChunkBuffer,
    CreditLedger,
    ErrorFrame,
    ErrorKind,
    ExecClose,
    ExecOpen,
    ExecOutput,
    ExecStream,
    FrameType,
    FsCancel,
    FsDelete,
    FsEdit,
    FsPath,
    FsPattern,
    FsRead,
    FsWrite,
    RtCredit,
    chunk_bytes,
    encode_frame,
    fits_frame_cap,
)
from octop.infra.utils.ulid import new_ulid

if TYPE_CHECKING:
    from octop.infra.runtime.protocol import FrameEnvelope
    from octop.infra.runtime.registry import RuntimeSession

logger = logging.getLogger("octop.runtime.backend")

#: Hub-side codec minor version (one-minor window; subprotocol MAJOR rules).
HUB_MINOR_VERSION = 1

# --- Timers & caps (spec §8.1) -----------------------------------------------

#: fs: max wait for any single reply chunk.
FS_CHUNK_TIMEOUT_S = 120.0
#: fs: max wall clock for one op → error + ``fs.cancel``.
FS_TOTAL_TIMEOUT_S = 30 * 60.0
#: exec: no frames of any kind for this long → ``exec.close`` (activity counts).
EXEC_IDLE_TIMEOUT_S = 300.0
#: exec: grace for the exit frame after the hub sends ``exec.close``.
EXEC_CLOSE_GRACE_S = 5.0
#: exec: default and maximum ``timeout_s`` (hard cap column, spec §8.1).
EXEC_DEFAULT_TIMEOUT_S = 600
EXEC_MAX_TIMEOUT_S = 3600
#: One fs.write payload cap (quota table) and the per-read fetch window.
SINGLE_WRITE_MAX_BYTES = 256 * 1024 * 1024
READ_FETCH_MAX_BYTES = 2 * 1024 * 1024
#: Defensive inbound queue bound per stream (credit is the real gate).
_STREAM_QUEUE_MAX = FS_STREAM_QUEUE_CHUNKS * 4

#: Hub close codes (mirror of the endpoint's; kept local to avoid api imports).
_CLOSE_REVOKED = 4002

#: TypeVar for the sync→async bridge return type.
_T = TypeVar("_T")


class RemoteOpError(Exception):
    """One mapped runtime-side error (spec §8 ``error`` frame / timer cap)."""

    def __init__(self, kind: ErrorKind, detail: str) -> None:
        self.kind = kind
        self.detail = detail or kind.value
        super().__init__(self.detail)

    @property
    def message(self) -> str:
        return f"remote runtime error ({self.kind.value}): {self.detail}"


def _offline_error(enrollment_id: str, detail: str = "") -> OctopError:
    return OctopError(
        ErrorCode.RUNTIME_OFFLINE,
        f"runtime machine {enrollment_id!r} is offline" + (f": {detail}" if detail else ""),
    )


def _session_error(session: RuntimeSession, detail: str) -> OctopError:
    """Map a session close to its OctopError per the §10 close-code map."""
    if session.close_code == _CLOSE_REVOKED:
        return OctopError(ErrorCode.RUNTIME_REVOKED, f"runtime enrollment was revoked: {detail}")
    return _offline_error(session.enrollment_id, detail or session.close_reason or "session closed")


def resolve_content_root(workspace_base: str, agent_root: str, agent_id: str | None) -> str:
    """Runtime-host directory that backs this agent's workspace content.

    ``workspace_base`` is the enrollment/workspace root on the runtime
    machine.  ``agent_root`` selects the layout below it (spec §7.1):
    ``"auto"`` (default) → ``{base}/agents/{agent_id}`` per-agent root,
    ``"shared"`` → the base itself (shared, last-writer-wins); any other
    non-empty value is used as a relative directory under the base.
    """
    base = (workspace_base or "").strip().rstrip("/")
    mode = (agent_root or "auto").strip().rstrip("/")
    if not base:
        return ""
    if mode in ("", "shared"):
        return base
    if mode == "auto":
        if not agent_id:
            return base
        return posixpath.join(base, "agents", agent_id.strip("/"))
    return posixpath.join(base, mode.lstrip("/"))


@dataclasses.dataclass(frozen=True)
class SessionLink:
    """One live hub-side session plus its transport send hook.

    ``loop`` is the event loop that owns the WebSocket (and therefore the
    session's ``closed`` event); sync ``SandboxBackendProtocol`` methods hop
    onto it with ``asyncio.run_coroutine_threadsafe``.
    """

    session: RuntimeSession
    send: Callable[[str], Awaitable[None]]
    loop: asyncio.AbstractEventLoop | None = None


#: Current live link for an enrollment, or ``None`` when offline.  Must be a
#: cheap, thread-safe sync callable (e.g. a closure over the session registry).
SessionProvider = Callable[[], SessionLink | None]


class _IncomingCredit:
    """Hub→runtime regrant state for one rt→hub data stream.

    The hub starts each incoming stream with ``initial`` bytes granted and
    tops the window back up by half the initial grant whenever the remaining
    credit dips below half (hysteresis, spec §8).
    """

    __slots__ = ("granted", "received", "_step")

    def __init__(self, initial: int) -> None:
        self.granted = max(1, int(initial))
        self.received = 0
        self._step = max(1, self.granted // 2)

    def on_bytes(self, n: int) -> int:
        """Record ``n`` received bytes; return a regrant amount (0 = none)."""
        self.received += max(0, n)
        if self.granted - self.received < self._step:
            self.granted += self._step
            return self._step
        return 0


class _OpTarget:
    """Shared wait surface for one in-flight fs request or exec run."""

    __slots__ = ("request_id", "session", "queue", "aborted", "aborted_error")

    def __init__(self, request_id: str, session: RuntimeSession) -> None:
        self.request_id = request_id
        self.session = session
        self.queue: asyncio.Queue[FrameEnvelope] = asyncio.Queue()
        self.aborted = asyncio.Event()
        self.aborted_error: BaseException | None = None

    def abort(self, error: BaseException) -> None:
        if self.aborted.is_set():
            return
        self.aborted_error = error
        self.aborted.set()


class _ExecRun(_OpTarget):
    """One in-flight ``exec.open`` drain (spec §8 exec.* semantics)."""

    __slots__ = ("exec_id", "chunks", "buffered_bytes", "truncated", "last_activity")

    def __init__(self, exec_id: str, request_id: str, session: RuntimeSession) -> None:
        super().__init__(request_id, session)
        self.exec_id = exec_id
        #: (is_stderr, data) in arrival order — the merge order.
        self.chunks: list[tuple[bool, bytes]] = []
        self.buffered_bytes = 0
        self.truncated = False
        self.last_activity = 0.0

    def merged_output(self) -> str:
        return b"".join(data for _is_err, data in self.chunks).decode("utf-8", errors="replace")


class RemoteRuntimeBackend(SandboxBackendProtocol):
    """Stable ``SandboxBackendProtocol`` proxy over live runtime sessions.

    Args:
        enrollment_id: The enrollment this proxy is bound to (identity).
        session_provider: Sync callable returning the current
            :class:`SessionLink` (``None`` = offline).
        allow_exec: Whether ``execute`` may open exec streams (resolved from
            the enrollment row at adapter time; ``4007``/capability off →
            one-shot error responses).
        workspace_base: Runtime-machine workspace root (spec §7.1).
        agent_root: ``"auto"`` | ``"shared"`` | explicit relative dir.
        agent_id: Agent ULID for the ``auto`` per-agent root layout.
        shell_argv: Argv prefix wrapping one-shot ``execute`` commands.
        default_timeout_s: Default exec wall-clock budget.
        initial_credit_bytes: Initial per-stream credit (rt.ready mirror).
        fs_chunk_timeout_s / fs_total_timeout_s: fs timers (§8.1).
        exec_idle_timeout_s / exec_close_grace_s: exec timers (§8.1).
        read_fetch_max_bytes: Byte window fetched per line-based read call.
    """

    def __init__(
        self,
        enrollment_id: str,
        *,
        session_provider: SessionProvider,
        allow_exec: bool = True,
        workspace_base: str = "",
        agent_root: str = "auto",
        agent_id: str | None = None,
        shell_argv: Sequence[str] = ("/bin/sh", "-c"),
        default_timeout_s: int = EXEC_DEFAULT_TIMEOUT_S,
        initial_credit_bytes: int = INITIAL_CREDIT_BYTES,
        fs_chunk_timeout_s: float = FS_CHUNK_TIMEOUT_S,
        fs_total_timeout_s: float = FS_TOTAL_TIMEOUT_S,
        exec_idle_timeout_s: float = EXEC_IDLE_TIMEOUT_S,
        exec_close_grace_s: float = EXEC_CLOSE_GRACE_S,
        read_fetch_max_bytes: int = READ_FETCH_MAX_BYTES,
    ) -> None:
        self._enrollment_id = str(enrollment_id)
        self._provider = session_provider
        self._allow_exec = bool(allow_exec)
        self._content_root = resolve_content_root(workspace_base, agent_root, agent_id)
        self._shell_argv = tuple(shell_argv)
        self._default_timeout_s = int(default_timeout_s)
        self._initial_credit_bytes = max(1, int(initial_credit_bytes))
        self._fs_chunk_timeout_s = float(fs_chunk_timeout_s)
        self._fs_total_timeout_s = float(fs_total_timeout_s)
        self._exec_idle_timeout_s = float(exec_idle_timeout_s)
        self._exec_close_grace_s = float(exec_close_grace_s)
        self._read_fetch_max_bytes = max(1, int(read_fetch_max_bytes))
        #: Attached link (swaps across generations).
        self._link: SessionLink | None = None
        #: request_id → waiting op target (fs pending or exec run).
        self._rid_owner: dict[str, _OpTarget] = {}
        #: exec_id → run.
        self._execs: dict[str, _ExecRun] = {}
        #: Hub→rt byte credit (fs.write / exec.input chunks).
        self._out_ledger = CreditLedger()
        self._credit_waiters: dict[str, asyncio.Event] = {}
        #: rt→hu regrant state keyed by stream id (request_id / exec_id).
        self._in_credit: dict[str, _IncomingCredit] = {}

    @property
    def id(self) -> str:  # noqa: A003 - SandboxBackendProtocol contract
        return f"remote_runtime:{self._enrollment_id}"

    @property
    def content_root(self) -> str:
        """Runtime-host directory backing this agent's content files."""
        return self._content_root

    @property
    def allow_exec(self) -> bool:
        return self._allow_exec

    # ------------------------------------------------------------------
    # Sync → async bridge
    # ------------------------------------------------------------------

    def _run_sync(self, make: Callable[[], Coroutine[Any, Any, _T]]) -> _T:
        """Run one op coroutine on the session loop from a worker thread."""
        link = self._provider()
        if link is None or not link.session.is_live:
            raise _offline_error(self._enrollment_id)
        loop = link.loop
        if loop is None:
            raise OctopError(
                ErrorCode.RUNTIME_UNSUPPORTED,
                "SessionLink.loop is not wired; cannot bridge sync backend ops",
            )
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:  # would deadlock: fut.result() blocks the loop
            raise OctopError(
                ErrorCode.RUNTIME_UNSUPPORTED,
                "sync remote_runtime op called on the session loop; use the async variant",
            )
        future: concurrent.futures.Future[_T] = asyncio.run_coroutine_threadsafe(make(), loop)
        return future.result()

    # ------------------------------------------------------------------
    # Session attach / frame routing
    # ------------------------------------------------------------------

    async def _attach(self) -> SessionLink:
        """Bind to the current live session, failing in-flight ops on swap."""
        link = self._provider()
        if link is None or not link.session.is_live:
            current = self._link
            if current is not None:
                self._fail_session(current.session, _offline_error(self._enrollment_id))
            self._link = None
            raise _offline_error(self._enrollment_id)
        current = self._link
        if current is not None and current.session is link.session:
            return link
        if current is not None:
            # Spec §7.2: in-flight ops die at swap; retries are fresh requests.
            self._fail_session(
                current.session,
                _session_error(
                    current.session, "superseded by a newer runtime session (generation swap)"
                ),
            )
        link.session.set_frame_handler(self._on_frame)
        self._link = link
        return link

    def _fail_session(self, session: RuntimeSession, error: BaseException) -> None:
        for target in list(self._rid_owner.values()):
            if target.session is session:
                target.abort(error)
        for run in list(self._execs.values()):
            if run.session is session:
                run.abort(error)

    async def _on_frame(self, frame: FrameEnvelope) -> None:
        """Route one runtime frame to its waiting op (plugged per session)."""
        if frame.frame_type is FrameType.RT_CREDIT:
            payload = frame.payload
            if isinstance(payload, RtCredit) and payload.bytes > 0:
                self._out_ledger.grant(payload.stream_id, payload.bytes)
                waiter = self._credit_waiters.get(payload.stream_id)
                if waiter is not None:
                    waiter.set()
            return
        target = self._route_target(frame)
        if target is None:
            logger.debug(
                "dropping unmatched frame %s (rid=%s)", frame.frame_type.value, frame.request_id
            )
            return
        if target.queue.qsize() >= _STREAM_QUEUE_MAX:
            # Credit is the real flow-control gate; overflowing anyway means
            # the peer misbehaved → per-stream error, never session death.
            target.abort(RemoteOpError(ErrorKind.INTERNAL, "inbound stream queue overflow"))
            return
        target.queue.put_nowait(frame)

    def _route_target(self, frame: FrameEnvelope) -> _OpTarget | None:
        rid = frame.request_id
        if rid is not None:
            target = self._rid_owner.get(rid)
            if target is not None:
                return target
        exec_id = getattr(frame.payload, "exec_id", None)
        if exec_id is not None:
            return self._execs.get(str(exec_id))
        return None

    # ------------------------------------------------------------------
    # Frame plumbing
    # ------------------------------------------------------------------

    async def _send(
        self,
        link: SessionLink,
        frame_type: FrameType,
        payload: Any,
        *,
        request_id: str | None = None,
    ) -> None:
        text = encode_frame(
            link.session.generation, HUB_MINOR_VERSION, frame_type, payload, request_id=request_id
        )
        if not fits_frame_cap(text):
            raise RemoteOpError(ErrorKind.INTERNAL, "outgoing frame exceeds FRAME_CAP_BYTES")
        await link.send(text)

    async def _next_frame(self, target: _OpTarget, *, timeout: float) -> FrameEnvelope:
        """Await the next routed frame; raises on abort/close/timeout."""
        get_task = asyncio.ensure_future(target.queue.get())
        closed_task = asyncio.ensure_future(target.session.closed.wait())
        aborted_task = asyncio.ensure_future(target.aborted.wait())
        try:
            _done, _pending = await asyncio.wait(
                {get_task, closed_task, aborted_task},
                timeout=max(0.0, timeout),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if get_task.done() and not get_task.cancelled():
                return get_task.result()
            if aborted_task.done():
                raise target.aborted_error or _offline_error(self._enrollment_id)
            if closed_task.done():
                raise _session_error(target.session, "session closed mid-op")
            raise RemoteOpError(ErrorKind.TIMEOUT, "timed out waiting for the runtime")
        finally:
            for task in (get_task, closed_task, aborted_task):
                if not task.done():
                    task.cancel()

    async def _await_out_credit(self, link: SessionLink, stream_id: str, need: int) -> None:
        """Wait until the runtime granted enough credit for ``need`` bytes."""
        if self._out_ledger.granted(stream_id) == 0:
            self._out_ledger.grant(stream_id, self._initial_credit_bytes)
        while not self._out_ledger.may_send(stream_id, need):
            event = self._credit_waiters.get(stream_id)
            if event is None:
                event = asyncio.Event()
                self._credit_waiters[stream_id] = event
            try:
                await asyncio.wait_for(event.wait(), timeout=self._fs_chunk_timeout_s)
            except TimeoutError:
                raise RemoteOpError(ErrorKind.TIMEOUT, "runtime credit window stalled") from None
            event.clear()

    def _regrant_incoming(
        self, link: SessionLink, incoming: _IncomingCredit, stream_id: str, n: int
    ) -> None:
        amount = incoming.on_bytes(n)
        if amount > 0:
            asyncio.ensure_future(
                self._send(link, FrameType.RT_CREDIT, RtCredit(stream_id=stream_id, bytes=amount))
            )

    async def _fs_roundtrip(
        self,
        frame_type: FrameType,
        payload: Any,
        *,
        sender: Callable[[SessionLink, str], Awaitable[None]] | None = None,
    ) -> bytes:
        """One fs request → chunked reply bytes (raw or JSON doc).

        ``sender`` overrides the initial frame emission (``write`` sends its
        whole chunk series and then waits for the ack).
        """
        link = await self._attach()
        request_id = new_ulid()
        pending = _OpTarget(request_id, link.session)
        self._rid_owner[request_id] = pending
        incoming = _IncomingCredit(self._initial_credit_bytes)
        self._in_credit[request_id] = incoming
        buffer = ChunkBuffer()
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            if sender is not None:
                await sender(link, request_id)
            else:
                await self._send(link, frame_type, payload, request_id=request_id)
            while True:
                elapsed = loop.time() - started
                remaining_total = self._fs_total_timeout_s - elapsed
                if remaining_total <= 0:
                    raise RemoteOpError(ErrorKind.TIMEOUT, "fs op exceeded the 30-minute cap")
                frame = await self._next_frame(
                    pending, timeout=min(self._fs_chunk_timeout_s, remaining_total)
                )
                if frame.frame_type is FrameType.ERROR and isinstance(frame.payload, ErrorFrame):
                    raise RemoteOpError(frame.payload.code, frame.payload.detail)
                if frame.frame_type is FrameType.FS_WRITE and isinstance(frame.payload, FsWrite):
                    chunk = frame.payload
                    self._regrant_incoming(link, incoming, request_id, len(chunk.data))
                    assembled = buffer.add(
                        chunk.chunk_index, chunk.total, chunk.data, chunk.truncated
                    )
                    if assembled is not None:
                        if buffer.truncated:
                            raise RemoteOpError(
                                ErrorKind.QUOTA_EXCEEDED,
                                "reply truncated by the runtime before completion",
                            )
                        return assembled
                # Other frame types (late credit, unrelated echoes) are ignored.
        except RemoteOpError as exc:
            if exc.kind is ErrorKind.TIMEOUT:
                try:
                    await self._send(
                        link,
                        FrameType.FS_CANCEL,
                        FsCancel(request_id=request_id),
                    )
                except Exception:  # noqa: BLE001 - best-effort cancel
                    logger.debug("fs.cancel send failed", exc_info=True)
            raise
        finally:
            self._rid_owner.pop(request_id, None)
            self._in_credit.pop(request_id, None)
            self._out_ledger.release(request_id)
            self._credit_waiters.pop(request_id, None)

    # ------------------------------------------------------------------
    # Path handling
    # ------------------------------------------------------------------

    def _remote_path(self, virtual: str, *, allow_root: bool = False) -> str:
        """Map a harness virtual path to a runtime-host path.

        Rejects ``..`` escapes and the hub-local system tree (first segment
        ``.octop``) — spec §1: the runtime carries content files only.
        """
        parts = [
            segment
            for segment in str(virtual).replace("\\", "/").split("/")
            if segment not in ("", ".")
        ]
        if any(segment == ".." for segment in parts):
            raise RemoteOpError(
                ErrorKind.PATH_ESCAPE, f"path {virtual!r} escapes the runtime roots ('..' segment)"
            )
        if parts and parts[0] == ".octop":
            raise RemoteOpError(
                ErrorKind.PATH_ESCAPE,
                "system files (.octop/) stay on the hub; the remote runtime "
                "backend carries content files only",
            )
        if not parts:
            if allow_root:
                return self._content_root
            raise RemoteOpError(
                ErrorKind.PATH_ESCAPE, f"path {virtual!r} resolves to the workspace root"
            )
        relative = "/".join(parts)
        if not self._content_root:
            return relative
        return posixpath.join(self._content_root, relative)

    def _to_virtual(self, remote_path: str) -> str:
        """Map a runtime-host path from a reply back to a virtual path."""
        text = str(remote_path).replace("\\", "/")
        root = self._content_root
        if root and (text == root or text.startswith(root.rstrip("/") + "/")):
            text = text[len(root.rstrip("/")) :]
        return "/" + text.lstrip("/")

    @staticmethod
    def _fs_pattern_payload(pattern: str, path: str) -> FsPattern:
        """FsPattern with path scoping ("" = all roots; executor contract)."""
        return FsPattern(pattern=pattern, path=path)

    # ------------------------------------------------------------------
    # File operations (SandboxBackendProtocol)
    # ------------------------------------------------------------------

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return self._run_sync(lambda: self._read_async(file_path, offset, limit))

    async def _read_async(self, file_path: str, offset: int, limit: int) -> ReadResult:
        if limit is not None and limit <= 0:
            return ReadResult(no_lines_requested=True)
        line_offset = max(0, int(offset))
        try:
            remote = self._remote_path(file_path)
            raw = await self._fs_roundtrip(
                FrameType.FS_READ,
                FsRead(path=remote, offset=0, length=self._read_fetch_max_bytes),
            )
        except RemoteOpError as exc:
            return ReadResult(error=exc.message)
        # The wire protocol is byte-based; harness read() is line-based.  We
        # fetch once per call within the fetch window and slice lines here.
        complete = len(raw) < self._read_fetch_max_bytes
        lines = raw.decode("utf-8", errors="replace").splitlines(keepends=True)
        if not complete and line_offset >= len(lines):
            return ReadResult(
                error=(
                    f"file {file_path} exceeds the remote read window "
                    f"({self._read_fetch_max_bytes} bytes); narrow the read or "
                    "fetch byte ranges"
                )
            )
        window = lines[line_offset : line_offset + max(0, int(limit))]
        file_data: FileData = {"content": "".join(window), "encoding": "utf-8"}
        if not window:
            return ReadResult(file_data=file_data)
        end_line = line_offset + len(window)
        return ReadResult(
            file_data=file_data,
            start_line=line_offset + 1,
            end_line=end_line,
            next_offset=end_line,
            total_lines=len(lines) if complete else None,
        )

    def write(self, file_path: str, content: str) -> WriteResult:
        return self._run_sync(lambda: self._write_bytes_async(file_path, content.encode("utf-8")))

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        out: list[FileUploadResponse] = []
        for path, content in files:
            try:
                result = self._run_sync(partial(self._write_bytes_async, path, bytes(content)))
                out.append(FileUploadResponse(path=path, error=result.error))
            except OctopError as exc:
                out.append(FileUploadResponse(path=path, error=exc.message))
        return out

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        out: list[FileDownloadResponse] = []
        for path in paths:
            try:
                result = self._run_sync(partial(self._download_async, path))
                out.append(FileDownloadResponse(path=path, content=result, error=None))
            except (OctopError, RemoteOpError) as exc:
                out.append(FileDownloadResponse(path=path, content=None, error=str(exc)))
        return out

    async def _download_async(self, file_path: str) -> bytes:
        remote = self._remote_path(file_path)
        return await self._fs_roundtrip(
            FrameType.FS_READ, FsRead(path=remote, offset=0, length=self._read_fetch_max_bytes)
        )

    async def _write_bytes_async(self, file_path: str, data: bytes) -> WriteResult:
        try:
            remote = self._remote_path(file_path)
        except RemoteOpError as exc:
            return WriteResult(error=exc.message)
        if len(data) > SINGLE_WRITE_MAX_BYTES:
            return WriteResult(
                error=f"payload {len(data)} bytes exceeds the 256 MiB single-write cap"
            )
        chunks = chunk_bytes(data, CHUNK_BYTES)

        async def sender(link: SessionLink, request_id: str) -> None:
            for index, chunk in enumerate(chunks):
                await self._await_out_credit(link, request_id, len(chunk))
                self._out_ledger.record_sent(request_id, len(chunk))
                await self._send(
                    link,
                    FrameType.FS_WRITE,
                    FsWrite(
                        path=remote,
                        data=chunk,
                        append=False,
                        chunk_index=index,
                        total=len(chunks),
                    ),
                    request_id=request_id,
                )

        try:
            await self._fs_roundtrip(FrameType.FS_WRITE, None, sender=sender)
        except RemoteOpError as exc:
            return WriteResult(error=exc.message)
        return WriteResult(path=file_path)

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,  # noqa: FBT001, FBT002
    ) -> EditResult:
        return self._run_sync(
            lambda: self._edit_async(file_path, old_string, new_string, replace_all)
        )

    async def _edit_async(
        self, file_path: str, old_string: str, new_string: str, replace_all: bool
    ) -> EditResult:
        if old_string == new_string:
            return EditResult(error="old_string and new_string must be different")
        try:
            remote = self._remote_path(file_path)
            stat_doc = self._decode_doc(
                await self._fs_roundtrip(FrameType.FS_STAT, FsPath(path=remote))
            )
            if stat_doc.get("is_dir"):
                return EditResult(error=f"{file_path} is a directory")
            etag = stat_doc.get("etag")
            raw = await self._fs_roundtrip(
                FrameType.FS_READ,
                FsRead(path=remote, offset=0, length=self._read_fetch_max_bytes),
            )
            if len(raw) >= self._read_fetch_max_bytes:
                return EditResult(
                    error=(
                        f"file {file_path} exceeds the remote read window "
                        f"({self._read_fetch_max_bytes} bytes); edit refused"
                    )
                )
            occurrences = raw.decode("utf-8", errors="replace").count(old_string)
            if occurrences == 0:
                return EditResult(error=f"old_string not found in {file_path}")
            if occurrences > 1 and not replace_all:
                return EditResult(
                    error=(
                        f"old_string is not unique in {file_path} "
                        f"({occurrences} occurrences); use replace_all"
                    )
                )
            doc = self._decode_doc(
                await self._fs_roundtrip(
                    FrameType.FS_EDIT,
                    FsEdit(
                        path=remote,
                        find=old_string,
                        replace=new_string,
                        all=bool(replace_all),
                        etag=str(etag) if etag is not None else None,
                    ),
                )
            )
        except RemoteOpError as exc:
            if exc.kind is ErrorKind.CONFLICT:
                return EditResult(
                    error=(
                        f"edit conflict: {file_path} changed since it was read "
                        "(etag mismatch); re-read and retry once"
                    )
                )
            return EditResult(error=exc.message)
        try:
            count = int(doc.get("occurrences", 1))
        except (TypeError, ValueError):
            count = 1
        return EditResult(path=file_path, occurrences=count)

    def ls(self, path: str) -> LsResult:
        return self._run_sync(lambda: self._ls_async(path))

    async def _ls_async(self, path: str) -> LsResult:
        try:
            remote = self._remote_path(path or "/", allow_root=True)
            doc = self._decode_doc(await self._fs_roundtrip(FrameType.FS_LS, FsPath(path=remote)))
        except RemoteOpError as exc:
            return LsResult(error=exc.message)
        entries: list[FileInfo] = []
        for entry in doc.get("entries", ()):
            if not isinstance(entry, dict) or "path" not in entry:
                continue
            info: FileInfo = {"path": self._to_virtual(str(entry["path"]))}
            if "is_dir" in entry:
                info["is_dir"] = bool(entry["is_dir"])
            if "size" in entry:
                info["size"] = int(entry["size"])
            if "modified_at" in entry:
                info["modified_at"] = str(entry["modified_at"])
            entries.append(info)
        return LsResult(entries=entries)

    def delete(self, file_path: str) -> DeleteResult:
        return self._run_sync(lambda: self._delete_async(file_path))

    async def _delete_async(self, file_path: str) -> DeleteResult:
        try:
            remote = self._remote_path(file_path)
            await self._fs_roundtrip(FrameType.FS_DELETE, FsDelete(path=remote, recursive=True))
        except RemoteOpError as exc:
            return DeleteResult(error=exc.message)
        return DeleteResult(path=file_path)

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        return self._run_sync(lambda: self._glob_async(pattern, path))

    async def _glob_async(self, pattern: str, path: str | None) -> GlobResult:
        try:
            base = self._remote_path(path, allow_root=True) if path else ""
            doc = self._decode_doc(
                await self._fs_roundtrip(
                    FrameType.FS_GLOB, self._fs_pattern_payload(str(pattern), base)
                )
            )
        except RemoteOpError as exc:
            return GlobResult(error=exc.message)
        matches: list[FileInfo] = [
            {"path": self._to_virtual(str(item))} for item in doc.get("matches", ())
        ]
        if path:
            prefix = "/" + str(path).strip("/")
            matches = [m for m in matches if str(m["path"]).startswith(prefix)]
        truncated = bool(doc.get("truncated"))
        return GlobResult(
            matches=matches,
            truncated=truncated,
            truncation_reason="transport" if truncated else None,
        )

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        return self._run_sync(lambda: self._grep_async(pattern, path, glob, max_count))

    async def _grep_async(
        self, pattern: str, path: str | None, glob: str | None, max_count: int | None
    ) -> GrepResult:
        try:
            base = self._remote_path(path, allow_root=True) if path else ""
            doc = self._decode_doc(
                await self._fs_roundtrip(
                    FrameType.FS_GREP, self._fs_pattern_payload(str(pattern), base)
                )
            )
        except RemoteOpError as exc:
            return GrepResult(error=exc.message)
        matches: list[GrepMatch] = []
        for item in doc.get("matches", ()):
            if not isinstance(item, dict) or "path" not in item:
                continue
            matches.append(
                GrepMatch(
                    path=self._to_virtual(str(item["path"])),
                    line=int(item.get("line", 0)),
                    text=str(item.get("text", "")),
                )
            )
        # The wire carries only the pattern; harness-side path scoping and the
        # filename glob filter are applied to the returned matches.
        if path:
            prefix = "/" + str(path).strip("/")
            matches = [m for m in matches if str(m["path"]).startswith(prefix)]
        if glob:
            matches = [
                m
                for m in matches
                if fnmatch.fnmatch(str(m["path"]), str(glob))
                or fnmatch.fnmatch(posixpath.basename(str(m["path"])), str(glob))
            ]
        truncated = bool(doc.get("truncated"))
        if max_count is not None and len(matches) > max_count:
            matches = matches[:max_count]
            truncated = True
        return GrepResult(matches=matches, truncated=truncated)

    # ------------------------------------------------------------------
    # Execution (SandboxBackendProtocol)
    # ------------------------------------------------------------------

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        if not self._allow_exec:
            return ExecuteResponse(
                output="Error: execution is disabled for this runtime (allow_exec is off)",
                exit_code=None,
                truncated=False,
            )
        return self._run_sync(lambda: self._execute_async(command, timeout))

    def _clamp_timeout(self, timeout: int | None) -> int:
        value = self._default_timeout_s if timeout is None else int(timeout)
        return max(1, min(value, EXEC_MAX_TIMEOUT_S))

    async def _execute_async(self, command: str, timeout: int | None) -> ExecuteResponse:
        timeout_s = self._clamp_timeout(timeout)
        link = await self._attach()
        exec_id = new_ulid()
        request_id = new_ulid()
        run = _ExecRun(exec_id, request_id, link.session)
        self._execs[exec_id] = run
        self._rid_owner[request_id] = run
        incoming = _IncomingCredit(self._initial_credit_bytes)
        self._in_credit[exec_id] = incoming
        try:
            await self._send(
                link,
                FrameType.EXEC_OPEN,
                ExecOpen(
                    request_id=request_id,
                    exec_id=exec_id,
                    cmd=(*self._shell_argv, command),
                    work_dir=self._content_root or "",
                    timeout_s=timeout_s,
                ),
                request_id=request_id,
            )
            return await self._drain_exec(link, run, incoming, timeout_s)
        finally:
            self._execs.pop(exec_id, None)
            self._rid_owner.pop(request_id, None)
            self._in_credit.pop(exec_id, None)
            self._out_ledger.release(exec_id)

    async def _drain_exec(
        self, link: SessionLink, run: _ExecRun, incoming: _IncomingCredit, timeout_s: int
    ) -> ExecuteResponse:
        loop = asyncio.get_running_loop()
        started = loop.time()
        run.last_activity = started
        close_sent = False
        while True:
            now = loop.time()
            hard_left = timeout_s - (now - started)
            idle_left = self._exec_idle_timeout_s - (now - run.last_activity)
            if close_sent:
                budget = self._exec_close_grace_s
            elif hard_left <= 0 or idle_left <= 0:
                await self._send(link, FrameType.EXEC_CLOSE, ExecClose(exec_id=run.exec_id))
                close_sent = True
                continue
            else:
                budget = min(hard_left, idle_left)
            try:
                frame = await self._next_frame(run, timeout=budget)
            except RemoteOpError:
                if close_sent:
                    # Spec §8.1: hub timeout → exec.close + grace, then local
                    # resolution.  Exit code 124 mirrors the harness convention.
                    return ExecuteResponse(
                        output=run.merged_output(), exit_code=124, truncated=run.truncated
                    )
                await self._send(link, FrameType.EXEC_CLOSE, ExecClose(exec_id=run.exec_id))
                close_sent = True
                continue
            # Any frame of any kind proves executor liveness (spec §8.1).
            run.last_activity = loop.time()
            payload = frame.payload
            if frame.frame_type is FrameType.ERROR and isinstance(payload, ErrorFrame):
                return ExecuteResponse(
                    output=f"Error: {payload.detail or payload.code.value}",
                    exit_code=None,
                    truncated=False,
                )
            if frame.frame_type is not FrameType.EXEC_OUTPUT or not isinstance(payload, ExecOutput):
                continue
            if payload.stream is ExecStream.EXIT:
                return ExecuteResponse(
                    output=run.merged_output(),
                    exit_code=payload.exit_code,
                    truncated=run.truncated or bool(payload.truncated),
                )
            if payload.stream in (ExecStream.STDOUT, ExecStream.STDERR):
                size = len(payload.data)
                self._regrant_incoming(link, incoming, run.exec_id, size)
                if payload.truncated:
                    run.truncated = True
                if run.buffered_bytes + size > EXEC_QUEUE_BYTES:
                    # Bounded per-exec queue: drain-discard past the cap.
                    run.truncated = True
                    continue
                run.chunks.append((payload.stream is ExecStream.STDERR, payload.data))
                run.buffered_bytes += size
            # ExecStream.ACTIVITY: liveness only, nothing to buffer.

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _decode_doc(raw: bytes) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RemoteOpError(ErrorKind.INTERNAL, f"malformed JSON reply: {exc}") from exc
        if not isinstance(doc, dict):
            raise RemoteOpError(ErrorKind.INTERNAL, "JSON reply must be an object")
        return doc


class ExecTargetCompositeBackend(SandboxBackendProtocol):
    """Composite routing file ops by path prefix, ``execute`` to a sandbox.

    deepagents ``CompositeBackend.execute`` always delegates to its *default*
    sub-backend, and ``supports_execution`` only inspects that default — so an
    Octop composite that declares ``exec_target`` (execute on the remote
    runtime while files stay on the default/route backends) cannot be
    expressed as a harness spec alone.  The resolver keeps ``exec_target``
    as an Octop-reserved spec key (:func:`octop.infra.backend.resolver`
    strips it before the harness sees the spec) and the agent wiring wraps
    the harness-resolved composite in this class.

    File operations delegate to *files_backend* (the resolved composite);
    ``execute``/``aexecute`` delegate to *exec_backend* (any
    ``SandboxBackendProtocol``, typically a :class:`RemoteRuntimeBackend`).
    """

    def __init__(self, files_backend: Any, exec_backend: SandboxBackendProtocol) -> None:
        self._files = files_backend
        self._exec = exec_backend

    @property
    def id(self) -> str:  # noqa: A003 - SandboxBackendProtocol contract
        files_id = getattr(self._files, "id", None) or type(self._files).__name__
        return f"exec_target:{files_id}->{self._exec.id}"

    @property
    def files_backend(self) -> Any:
        return self._files

    @property
    def exec_backend(self) -> SandboxBackendProtocol:
        return self._exec

    # -- file ops: straight delegation -----------------------------------

    def ls(self, path: str) -> LsResult:
        return cast(LsResult, self._files.ls(path))

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return cast(ReadResult, self._files.read(file_path, offset, limit))

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        return cast(GrepResult, self._files.grep(pattern, path, glob, max_count=max_count))

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        return cast(GlobResult, self._files.glob(pattern, path))

    def write(self, file_path: str, content: str) -> WriteResult:
        return cast(WriteResult, self._files.write(file_path, content))

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,  # noqa: FBT001, FBT002
    ) -> EditResult:
        return cast(EditResult, self._files.edit(file_path, old_string, new_string, replace_all))

    def delete(self, file_path: str) -> DeleteResult:
        deleter = getattr(self._files, "delete", None)
        if deleter is None:
            return DeleteResult(error=f"delete is not supported by {type(self._files).__name__}")
        return cast(DeleteResult, deleter(file_path))

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        uploader = getattr(self._files, "upload_files", None)
        if uploader is None:
            return [FileUploadResponse(path=p, error="upload_files unsupported") for p, _ in files]
        return cast(list[FileUploadResponse], uploader(files))

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        downloader = getattr(self._files, "download_files", None)
        if downloader is None:
            return [FileDownloadResponse(path=p, error="download_files unsupported") for p in paths]
        return cast(list[FileDownloadResponse], downloader(paths))

    # -- execution: the exec target ---------------------------------------

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        return self._exec.execute(command, timeout=timeout)
