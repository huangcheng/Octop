"""Daemon-side executor for ``exec.*`` frames (spec §8/§8.1/§8.2/§8.3).

The executor runs unsandboxed user processes on the runtime machine
(``allow_exec`` consent is a hub-side gate — the daemon only sees frames the
hub admitted). It is pure ``infra`` code: stdlib + the shared protocol module
+ the fs executor's containment helpers — never ``api``/``cli``.

Reply convention (agreed with the hub backend):
- ``exec.open`` success has no ack frame; the exec streams ``exec.output``
  frames keyed by the hub-provided ``exec_id`` (``stdout``/``stderr`` are DATA
  class and consume hub-granted byte credit per the fs.read conventions:
  initial grant mirrored from the ``rt.ready``-time negotiation, replenished
  by inbound ``rt.credit``).
- **drain-before-exit:** every queued output chunk is flushed BEFORE the
  single terminal frame (``stream=exit``, ``exit_code``, ``reason`` ∈
  ``exited|killed|timeout``, ``request_id`` echo, truncation marker).
- ``activity`` frames (CONTROL, zero data credit) prove executor liveness
  every ``activity_interval_seconds`` of output silence.
- Failures of ``exec.open`` itself are ``error`` frames carrying the request
  id (``path_escape`` work_dir, ``quota_exceeded``, ``duplicate_request``,
  ``capability_unsupported`` pty, ``internal`` spawn errno).

Process lifecycle (spec §8.2):
- POSIX: ``start_new_session=True`` puts the child in its own process group;
  every kill path escalates group TERM → grace → group KILL and reaps.
  Documented residual: a grandchild reparented between the TERM and KILL
  windows can escape the group (``PR_SET_PDEATHSIG``-style hardening is a
  Linux phase-2 concern).
- Windows: pipes only, ``terminate()``/``kill()`` best-effort (consent-visible
  per spec); ``pty=True`` fails closed with ``capability_unsupported``.
- PTY (Linux + macOS): ``pty.openpty()``; the child gets the slave as
  stdin/stdout/stderr, the master is read non-blocking via the event loop.

Timers & quotas (spec §8.1): hard ``timeout_s`` (default 600, clamped 1-3600)
is enforced daemon-side independently of the hub (kill + ``reason=timeout``);
idle detection is hub-side — the daemon keeps streaming activity. Concurrent
execs default 4 (cap 16). Captured output is capped (16 MiB default) — past
the cap data is dropped while pipes keep draining, and the exit frame carries
``truncated=True``; the process itself keeps running.

Duplicate rules (spec §8): an in-flight ``exec.open`` request id with an
identical payload fingerprint (hash of cmd+work_dir+env) re-attaches to the
same exec — answered with an ``activity`` frame carrying the same ``exec_id``
(no second process, never someone else's); a fingerprint mismatch (or any
completed request id) is ``duplicate_request``.

Environment (spec §8): a clean minimal per-OS floor ({PATH,HOME,TMPDIR,LANG}
on POSIX / {PATH,TEMP,TMP,SystemRoot,ComSpec} on Windows) is the default;
``clean_env`` opts into a caller-managed env (floor skipped); explicit
``env`` pairs apply on top; ``unset_env`` removes last. Wholesale daemon-env
inheritance is not expressible in the committed payload — pass explicit
``env`` pairs instead (hub-side concern).

Documented residuals:
- pty EOF is delivered as a ``^D`` byte on the master (a pty has no
  half-close).
- A credit-starved stream (no ``rt.credit`` for
  ``credit_wait_timeout_seconds``) stops forwarding and drain-discards further
  output (``truncated=True``).
- After a natural exit, readers get EOF once every pipe writer dies; lingering
  grandchildren holding the pipes are cut off after ``drain_grace_seconds``.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import errno
import hashlib
import inspect
import json
import logging
import os
import signal
import time
from collections.abc import Callable, Sequence
from typing import Any

from octop.infra.runtime.executor import (
    _HAS_DIR_FD,
    CLOSED_ID_CACHE,
    PathEscapeError,
    RootsProvider,
    Sender,
    _errno_detail,
    _match_root,
    _open_dir_beneath,
    _open_root_dir,
    _path_parts,
)
from octop.infra.runtime.protocol import (
    CHUNK_BYTES,
    EXEC_QUEUE_BYTES,
    INITIAL_CREDIT_BYTES,
    CreditLedger,
    ErrorFrame,
    ErrorKind,
    ExecClose,
    ExecInput,
    ExecOpen,
    ExecOutput,
    ExecStream,
    ExitReason,
    FrameType,
    RtCredit,
    chunk_bytes,
)
from octop.infra.utils import posix_compat

logger = logging.getLogger("octop.runtime.exec_executor")

# --- Tunables (constructor-injectable for tests) ------------------------------

DEFAULT_TIMEOUT_S = 600
MIN_TIMEOUT_S = 1
MAX_TIMEOUT_S = 3600
DEFAULT_CONCURRENT_EXECS = 4
CONCURRENT_EXECS_CAP = 16
OUTPUT_CAP_BYTES = 16 * 1024 * 1024
ACTIVITY_INTERVAL_SECONDS = 30.0
KILL_GRACE_SECONDS = 5.0
DRAIN_GRACE_SECONDS = 5.0
CREDIT_WAIT_TIMEOUT_SECONDS = 120.0
#: Bounded output queue per exec, in chunks (spec: exec queue 4 MiB).
EXEC_QUEUE_CHUNKS = max(1, EXEC_QUEUE_BYTES // CHUNK_BYTES)

#: Sentinel enqueued when a stream reaches EOF; ``_finish`` backfills misses.
_EOF = object()

_IS_POSIX = os.name == "posix"


# --- Errors --------------------------------------------------------------------


class ExecError(Exception):
    """Operation failure carrying its wire error kind."""

    def __init__(self, code: ErrorKind, detail: str = "") -> None:
        super().__init__(detail or code.value)
        self.code = code
        self.detail = detail or code.value


def _map_error(exc: BaseException) -> tuple[ErrorKind, str]:
    if isinstance(exc, ExecError):
        return exc.code, exc.detail
    if isinstance(exc, PathEscapeError):
        return ErrorKind.PATH_ESCAPE, str(exc)
    if isinstance(exc, OSError):
        return ErrorKind.INTERNAL, _errno_detail(exc)
    return ErrorKind.INTERNAL, str(exc) or exc.__class__.__name__


# --- Pure helpers --------------------------------------------------------------


def clean_env_floor() -> dict[str, str]:
    """Minimal per-OS environment floor (spec §8) — the default exec env."""
    if _IS_POSIX:
        floor = {
            "PATH": os.environ.get("PATH") or "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": os.environ.get("HOME") or os.path.expanduser("~"),
            "TMPDIR": os.environ.get("TMPDIR") or "/tmp",
            "LANG": os.environ.get("LANG") or "C.UTF-8",
        }
    else:
        floor = {
            "PATH": os.environ.get("PATH", ""),
            "TEMP": os.environ.get("TEMP", ""),
            "TMP": os.environ.get("TMP", ""),
            "SystemRoot": os.environ.get("SystemRoot", ""),  # noqa: SIM112 - real var case
            "ComSpec": os.environ.get("ComSpec", ""),  # noqa: SIM112 - real var case
        }
    return {key: value for key, value in floor.items() if value}


def build_spawn_env(payload: ExecOpen) -> dict[str, str]:
    """Floor (unless ``clean_env``) + explicit pairs; ``unset_env`` last."""
    env: dict[str, str] = {} if payload.clean_env else clean_env_floor()
    for key, value in payload.env:
        env[key] = value
    for key in payload.unset_env:
        env.pop(key, None)
    return env


def exec_fingerprint(payload: ExecOpen) -> str:
    """Payload fingerprint for the in-flight dedupe carve-out (spec §8)."""
    doc = json.dumps(
        [list(payload.cmd), payload.work_dir, sorted(payload.env)],
        separators=(",", ":"),
    )
    return hashlib.sha256(doc.encode("utf-8")).hexdigest()


def clamp_timeout_s(value: int | float) -> int:
    """Hard wall-clock clamp 1..3600 (spec §8.1)."""
    return max(MIN_TIMEOUT_S, min(int(value), MAX_TIMEOUT_S))


@dataclasses.dataclass(frozen=True)
class ExecStart:
    """Facts handed to the injectable ``on_exec_start`` callback (audit hook)."""

    exec_id: str
    request_id: str
    cmd: tuple[str, ...]
    work_dir: str
    pty: bool
    timeout_s: int


# --- Per-exec state --------------------------------------------------------------


class _ExecRun:
    """State for one live exec (event-loop confined)."""

    __slots__ = (
        "activity_task",
        "captured",
        "cmd",
        "control",
        "deadline",
        "eofs_enqueued",
        "exec_id",
        "expected_eofs",
        "fingerprint",
        "finishing",
        "inbound_granted",
        "inbound_received",
        "kill_lock",
        "killed",
        "last_output",
        "master_fd",
        "proc",
        "pty",
        "queue",
        "reader_tasks",
        "request_id",
        "sender_task",
        "stdin_closed",
        "stdin_lock",
        "stream_blocked",
        "timeout_s",
        "truncated",
        "winsize",
        "work_dir",
    )

    def __init__(self, payload: ExecOpen) -> None:
        self.exec_id = payload.exec_id
        self.request_id = payload.request_id
        self.fingerprint = exec_fingerprint(payload)
        self.cmd = tuple(payload.cmd)
        self.work_dir = payload.work_dir
        self.pty = bool(payload.pty)
        self.winsize = payload.winsize
        self.timeout_s = DEFAULT_TIMEOUT_S
        self.proc: asyncio.subprocess.Process | None = None
        self.master_fd: int | None = None
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=EXEC_QUEUE_CHUNKS)
        self.control = asyncio.Event()
        self.stdin_lock = asyncio.Lock()
        self.kill_lock = asyncio.Lock()
        self.killed = False
        self.finishing = False
        self.stdin_closed = False
        self.truncated = False
        self.stream_blocked = False
        self.captured = 0
        self.expected_eofs = 0
        self.eofs_enqueued = 0
        self.last_output = 0.0
        self.deadline = 0.0
        self.inbound_granted = 0
        self.inbound_received = 0
        self.reader_tasks: list[asyncio.Task[None]] = []
        self.sender_task: asyncio.Task[None] | None = None
        self.activity_task: asyncio.Task[None] | None = None


# --- Executor -------------------------------------------------------------------


def _make_ready_callback(fut: asyncio.Future[None]) -> Callable[[], None]:
    """Resolve *fut* once its fd watcher fires (closures bind fut eagerly)."""

    def _ready() -> None:
        if not fut.done():
            fut.set_result(None)

    return _ready


class ExecExecutor:
    """Serves ``exec.*`` frames against a re-read-per-op roots allowlist."""

    def __init__(
        self,
        roots: Sequence[str] | RootsProvider,
        *,
        sender: Sender | None = None,
        on_exec_start: Callable[[ExecStart], Any] | None = None,
        initial_credit_bytes: int = INITIAL_CREDIT_BYTES,
        max_concurrent_execs: int = DEFAULT_CONCURRENT_EXECS,
        output_cap_bytes: int = OUTPUT_CAP_BYTES,
        activity_interval_seconds: float = ACTIVITY_INTERVAL_SECONDS,
        kill_grace_seconds: float = KILL_GRACE_SECONDS,
        drain_grace_seconds: float = DRAIN_GRACE_SECONDS,
        credit_wait_timeout_seconds: float = CREDIT_WAIT_TIMEOUT_SECONDS,
    ) -> None:
        self._roots: RootsProvider = roots if callable(roots) else (lambda: roots)
        self._sender = sender
        self._on_exec_start = on_exec_start
        self._initial_credit = initial_credit_bytes
        self._max_concurrent = max(1, min(int(max_concurrent_execs), CONCURRENT_EXECS_CAP))
        self._output_cap = output_cap_bytes
        self._activity_interval = activity_interval_seconds
        self._kill_grace = kill_grace_seconds
        self._drain_grace = drain_grace_seconds
        self._credit_wait = credit_wait_timeout_seconds

        self._out_credit = CreditLedger()
        self._credit_events: dict[str, asyncio.Event] = {}
        self._execs: dict[str, _ExecRun] = {}
        self._requests: dict[str, str] = {}
        self._closed_ids: collections.deque[str] = collections.deque(maxlen=CLOSED_ID_CACHE)
        self._closed = False

    # -- wiring ---------------------------------------------------------------

    def bind(self, sender: Sender) -> None:
        self._sender = sender

    async def close(self) -> None:
        """Kill every live exec of the session (teardown), wake credit waiters."""
        self._closed = True
        for run in list(self._execs.values()):
            run.control.set()
            try:
                await self._kill_tree(run)
            except Exception:
                logger.debug("close kill failed for %s", run.exec_id, exc_info=True)
        for event in list(self._credit_events.values()):
            event.set()

    # -- frame handlers ---------------------------------------------------------

    async def handle_exec_open(self, frame: Any) -> None:
        payload: ExecOpen = frame.payload
        rid = payload.request_id
        run: _ExecRun | None = None
        try:
            inflight = self._dedupe(payload)
            if inflight is not None:
                # Spec §8 carve-out: identical in-flight request re-attaches
                # to the same exec; an activity frame returns the same exec_id.
                await self._send_output(inflight, ExecStream.ACTIVITY, b"")
                return
            if not payload.exec_id:
                raise ExecError(ErrorKind.INTERNAL, "exec_id is required")
            if not payload.cmd or not all(isinstance(c, str) and c for c in payload.cmd):
                raise ExecError(ErrorKind.INTERNAL, "cmd must be a non-empty argv")
            if len(self._execs) >= self._max_concurrent:
                raise ExecError(
                    ErrorKind.QUOTA_EXCEEDED,
                    f"concurrent exec cap ({self._max_concurrent}) reached",
                )
            if payload.exec_id in self._execs:
                raise ExecError(ErrorKind.CONFLICT, "exec_id already in use")
            if payload.pty and not _IS_POSIX:
                raise ExecError(ErrorKind.CAPABILITY_UNSUPPORTED, "pty requires a POSIX runtime")
            run = _ExecRun(payload)
            run.timeout_s = clamp_timeout_s(payload.timeout_s)
            # Reserve before the async prep so a racing duplicate re-attaches
            # instead of double-spawning.
            self._execs[run.exec_id] = run
            self._requests[rid] = run.exec_id
            # Seed the outbound stream now (not at supervise time) so an
            # rt.credit racing the spawn is never dropped.
            self._out_credit.grant(run.exec_id, self._initial_credit)
            self._credit_events.setdefault(run.exec_id, asyncio.Event())
            work_dir, env = await asyncio.to_thread(self._prepare_spawn, payload)
        except (ExecError, PathEscapeError, OSError) as exc:
            if run is not None:  # reserved then failed: release the slot
                await self._cleanup(run)
            await self._reply_error(rid, exc)
            return

        try:
            await self._spawn(run, env, work_dir)
        except (ExecError, OSError) as exc:
            await self._cleanup(run)
            await self._reply_error(rid, exc)
            return
        await self._notify_start(run)
        await self._supervise(run)

    async def handle_exec_input(self, frame: Any) -> None:
        payload: ExecInput = frame.payload
        run = self._execs.get(payload.exec_id)
        if run is None:
            logger.debug("exec.input for unknown exec %s dropped", payload.exec_id)
            return
        if payload.resize is not None:
            self._apply_resize(run, payload.resize)
        if payload.data:
            if run.stdin_closed:
                logger.debug("exec.input data for %s after stdin close ignored", run.exec_id)
            else:
                async with run.stdin_lock:
                    await self._write_stdin(run, payload.data)
                run.inbound_received += len(payload.data)
                await self._maybe_replenish(run)
        if payload.eof:
            if run.stdin_closed:
                logger.debug("exec.input eof for %s after stdin close ignored", run.exec_id)
            else:
                run.stdin_closed = True
                async with run.stdin_lock:
                    await self._close_stdin(run)

    async def handle_exec_close(self, frame: Any) -> None:
        payload: ExecClose = frame.payload
        run = self._execs.get(payload.exec_id)
        if run is None:
            # Frames for closed/cancelled streams drop silently (spec §5).
            logger.debug("exec.close for unknown exec %s dropped", payload.exec_id)
            return
        run.control.set()

    async def handle_rt_credit(self, frame: Any) -> None:
        payload: RtCredit = frame.payload
        event = self._credit_events.get(payload.stream_id)
        if event is not None:
            self._out_credit.grant(payload.stream_id, payload.bytes)
            event.set()
        # fs stream ids are not ours.

    # -- admission ----------------------------------------------------------------

    def _load_roots(self) -> list[str]:
        return [r for r in (str(x) for x in self._roots()) if r]

    def _dedupe(self, payload: ExecOpen) -> _ExecRun | None:
        """In-flight request-id carve-out; raises on mismatch/completed ids."""
        rid = payload.request_id
        exec_id = self._requests.get(rid)
        if exec_id is not None:
            run = self._execs.get(exec_id)
            if run is not None:
                if run.fingerprint == exec_fingerprint(payload):
                    return run
                raise ExecError(
                    ErrorKind.DUPLICATE_REQUEST, "request in flight with a different payload"
                )
        if rid in self._closed_ids:
            raise ExecError(ErrorKind.DUPLICATE_REQUEST, "request already completed")
        return None

    # -- spawn prep (worker thread) -------------------------------------------------

    def _prepare_spawn(self, payload: ExecOpen) -> tuple[str, dict[str, str]]:
        roots = self._load_roots()
        work_dir = payload.work_dir or (roots[0] if roots else "")
        work_dir = self._resolve_work_dir(work_dir, roots)
        return work_dir, build_spawn_env(payload)

    def _resolve_work_dir(self, work_dir: str, roots: Sequence[str]) -> str:
        """Containment: absolute, no ``..``, beneath the CURRENT roots, a real
        directory, no symlinked components (fs executor helpers, spec §8.2)."""
        _path_parts(work_dir)
        root, rel = _match_root(work_dir, roots)
        if _HAS_DIR_FD:
            root_fd = _open_root_dir(root)
            try:
                fd = _open_dir_beneath(root_fd, rel.split("/") if rel else [])
            finally:
                os.close(root_fd)
            os.close(fd)
        elif not os.path.isdir(work_dir):
            # Windows best-effort: textual containment only (documented).
            raise ExecError(ErrorKind.INTERNAL, "work_dir is not a directory")
        return work_dir

    # -- spawn -----------------------------------------------------------------------

    async def _spawn(self, run: _ExecRun, env: dict[str, str], work_dir: str) -> None:
        kwargs: dict[str, Any] = {
            "cwd": work_dir,
            "env": env,
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
        }
        if _IS_POSIX:
            kwargs["start_new_session"] = True  # own process group for killpg
        master: int | None = None
        slave: int | None = None
        # PTY setup lives inside the try so a failing fcntl/ioctl cannot leak
        # both descriptors — the handler closes master/slave on any error.
        try:
            if run.pty:
                master, slave = posix_compat.openpty()
                posix_compat.set_nonblock(master)
                if run.winsize is not None:
                    cols, rows = run.winsize
                    posix_compat.set_winsize(master, int(cols), int(rows))
            if master is not None and slave is not None:
                kwargs.update({"stdin": slave, "stdout": slave, "stderr": slave})
            run.proc = await asyncio.create_subprocess_exec(*run.cmd, **kwargs)
        except BaseException:
            for fd in (master, slave):
                if fd is not None:
                    with contextlib.suppress(OSError):
                        os.close(fd)
            raise
        if master is not None and slave is not None:
            await asyncio.to_thread(os.close, slave)
            run.master_fd = master

    # -- supervision -------------------------------------------------------------------

    async def _supervise(self, run: _ExecRun) -> None:
        run.inbound_granted = self._initial_credit
        now = time.monotonic()
        run.last_output = now
        run.deadline = now + run.timeout_s
        if run.master_fd is not None:
            run.expected_eofs = 1
            run.reader_tasks.append(asyncio.create_task(self._read_pty(run)))
        else:
            proc = run.proc
            assert proc is not None  # set by _spawn
            if proc.stdout is not None:
                run.reader_tasks.append(
                    asyncio.create_task(self._read_pipe(run, ExecStream.STDOUT, proc.stdout))
                )
            if proc.stderr is not None:
                run.reader_tasks.append(
                    asyncio.create_task(self._read_pipe(run, ExecStream.STDERR, proc.stderr))
                )
            run.expected_eofs = len(run.reader_tasks)
        run.sender_task = asyncio.create_task(self._send_loop(run))
        run.activity_task = asyncio.create_task(self._activity_loop(run))
        exit_wait = asyncio.create_task(self._wait_proc(run))
        control_wait = asyncio.create_task(run.control.wait())
        reason = ExitReason.EXITED
        try:
            while True:
                left = run.deadline - time.monotonic()
                if left <= 0:
                    await self._kill_tree(run)
                    reason = ExitReason.TIMEOUT
                    break
                done, _ = await asyncio.wait(
                    {exit_wait, control_wait}, timeout=left, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    await self._kill_tree(run)
                    reason = ExitReason.TIMEOUT
                    break
                if exit_wait in done:
                    break  # natural exit wins over a racing close
                await self._kill_tree(run)
                reason = ExitReason.KILLED
                break
        except asyncio.CancelledError:
            await self._abort(run)
            raise
        finally:
            control_wait.cancel()
            exit_wait.cancel()
        exit_code = run.proc.returncode if run.proc is not None else None
        await self._finish(run, reason, exit_code)

    async def _wait_proc(self, run: _ExecRun) -> None:
        proc = run.proc
        if proc is not None:
            await proc.wait()

    async def _finish(self, run: _ExecRun, reason: ExitReason, exit_code: int | None) -> None:
        run.finishing = True
        if run.activity_task is not None:
            run.activity_task.cancel()
        live = [task for task in run.reader_tasks if not task.done()]
        if live:
            # EOF arrives once every pipe writer dies; lingering grandchildren
            # holding the pipes are cut off after the drain grace.
            await asyncio.wait(set(live), timeout=self._drain_grace)
            for task in live:
                task.cancel()
        await asyncio.gather(*run.reader_tasks, return_exceptions=True)
        while run.eofs_enqueued < run.expected_eofs:
            try:
                run.queue.put_nowait(_EOF)
                run.eofs_enqueued += 1
            except asyncio.QueueFull:
                # The sender always consumes (credit waits are bounded), so
                # this only yields until queue space frees up.
                await asyncio.sleep(0.01)
        if run.sender_task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(run.sender_task), timeout=self._credit_wait + 30.0
                )
            except TimeoutError:
                pass
            except Exception:
                # A dead sender must never skip cleanup below (orphaned child,
                # leaked PTY, stuck registry entries).
                logger.debug("sender task failed for %s", run.exec_id, exc_info=True)
            run.sender_task.cancel()
            await asyncio.gather(run.sender_task, return_exceptions=True)
        try:
            await self._send_exit(run, reason, exit_code)
        except Exception:
            logger.debug("exit frame send failed for %s", run.exec_id, exc_info=True)
        await self._cleanup(run)

    async def _abort(self, run: _ExecRun) -> None:
        """Session drop / unexpected failure: kill, best-effort exit frame."""
        run.finishing = True
        try:
            await self._kill_tree(run)
        except Exception:
            logger.debug("abort kill failed for %s", run.exec_id, exc_info=True)
        tasks: list[asyncio.Task[None]] = [*run.reader_tasks]
        if run.sender_task is not None:
            tasks.append(run.sender_task)
        if run.activity_task is not None:
            tasks.append(run.activity_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        exit_code = run.proc.returncode if run.proc is not None else None
        try:
            await self._send_exit(run, ExitReason.KILLED, exit_code)
        except Exception:
            logger.debug("abort exit frame send failed for %s", run.exec_id, exc_info=True)
        await self._cleanup(run)

    # -- kill paths ---------------------------------------------------------------------

    async def _kill_tree(self, run: _ExecRun) -> None:
        """Group TERM → grace → group KILL → reap (idempotent)."""
        proc = run.proc
        if proc is None or proc.returncode is not None:
            return
        async with run.kill_lock:
            if proc.returncode is not None:
                return
            run.killed = True
            self._signal(proc, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=self._kill_grace)
                return
            except TimeoutError:
                pass
            if _IS_POSIX:
                self._signal(proc, posix_compat.sigkill())
            else:
                with contextlib.suppress(Exception):
                    proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()

    def _signal(self, proc: asyncio.subprocess.Process, sig: int) -> None:
        try:
            if _IS_POSIX:
                # start_new_session: the child's pgid == its pid.
                posix_compat.killpg(proc.pid, sig)
            elif sig == signal.SIGTERM:
                proc.terminate()  # Windows best-effort (documented)
            else:
                proc.kill()
        except (ProcessLookupError, OSError):
            pass  # already gone

    # -- streaming ----------------------------------------------------------------------

    def _enqueue(self, run: _ExecRun, stream: ExecStream, data: bytes) -> None:
        try:
            run.queue.put_nowait((stream, data))
        except asyncio.QueueFull:
            run.truncated = True  # bounded queue: drop, keep draining (spec §5)

    def _enqueue_eof(self, run: _ExecRun) -> None:
        try:
            run.queue.put_nowait(_EOF)
            run.eofs_enqueued += 1
        except asyncio.QueueFull:
            pass  # backfilled by _finish

    async def _read_pipe(
        self, run: _ExecRun, stream: ExecStream, pipe: asyncio.StreamReader
    ) -> None:
        try:
            while True:
                data = await pipe.read(CHUNK_BYTES)
                if not data:
                    break
                self._enqueue(run, stream, data)
        finally:
            self._enqueue_eof(run)

    async def _read_pty(self, run: _ExecRun) -> None:
        try:
            while run.master_fd is not None:
                chunk = await self._read_master_chunk(run.master_fd)
                if chunk is None:
                    break
                if chunk:
                    self._enqueue(run, ExecStream.STDOUT, chunk)
        finally:
            self._enqueue_eof(run)

    async def _read_master_chunk(self, fd: int) -> bytes | None:
        """Wait readable, then one non-blocking read. ``None`` = pty EOF."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        loop.add_reader(fd, _make_ready_callback(fut))
        try:
            await fut
        except asyncio.CancelledError:
            return None
        finally:
            loop.remove_reader(fd)
        try:
            return os.read(fd, CHUNK_BYTES)  # non-blocking fd: never blocks
        except BlockingIOError:
            return b""  # spurious wakeup
        except OSError as exc:
            if exc.errno in (errno.EIO, errno.EBADF, errno.ENXIO):
                return None  # all slave fds closed / master closed
            raise

    async def _send_loop(self, run: _ExecRun) -> None:
        seen = 0
        while seen < run.expected_eofs:
            item = await run.queue.get()
            if item is _EOF:
                seen += 1
                continue
            stream, data = item
            if not data:
                continue
            if run.stream_blocked or run.captured >= self._output_cap:
                run.truncated = True  # cap reached: drop, keep draining
                continue
            if run.captured + len(data) > self._output_cap:
                data = data[: self._output_cap - run.captured]
                run.truncated = True
            for chunk in chunk_bytes(data):
                if not await self._send_with_credit(run, stream, chunk):
                    run.stream_blocked = True
                    run.truncated = True
                    break

    async def _send_with_credit(self, run: _ExecRun, stream: ExecStream, chunk: bytes) -> bool:
        event = self._credit_events.setdefault(run.exec_id, asyncio.Event())
        while not self._out_credit.may_send(run.exec_id, len(chunk)):
            if self._closed:
                return False
            try:
                await asyncio.wait_for(event.wait(), timeout=self._credit_wait)
            except TimeoutError:
                return False
            event.clear()
        self._out_credit.record_sent(run.exec_id, len(chunk))
        run.captured += len(chunk)
        run.last_output = time.monotonic()
        await self._send_output(run, stream, chunk)
        return True

    async def _activity_loop(self, run: _ExecRun) -> None:
        try:
            while not run.finishing:
                await asyncio.sleep(self._activity_interval)
                if run.finishing:
                    return
                if time.monotonic() - run.last_output >= self._activity_interval:
                    await self._send_output(run, ExecStream.ACTIVITY, b"")
                    run.last_output = time.monotonic()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.debug("activity loop stopped for %s", run.exec_id, exc_info=True)

    # -- stdin ---------------------------------------------------------------------------

    def _apply_resize(self, run: _ExecRun, resize: tuple[int, int]) -> None:
        if run.master_fd is None:
            logger.debug("resize ignored for non-pty exec %s", run.exec_id)
            return
        cols, rows = resize
        try:
            posix_compat.set_winsize(run.master_fd, int(cols), int(rows))
        except Exception:
            logger.debug("resize failed for %s", run.exec_id, exc_info=True)

    async def _write_stdin(self, run: _ExecRun, data: bytes) -> None:
        try:
            if run.master_fd is not None:
                await self._pty_write_all(run.master_fd, data)
                return
            proc = run.proc
            if proc is None or proc.stdin is None:
                return
            proc.stdin.write(data)
            await proc.stdin.drain()
        except OSError as exc:
            run.stdin_closed = True
            # EIO on a PTY master is the EPIPE equivalent (slave side gone).
            logger.debug("stdin write failed (process gone) for %s: %s", run.exec_id, exc)

    async def _close_stdin(self, run: _ExecRun) -> None:
        try:
            if run.master_fd is not None:
                # A pty has no half-close; ^D is the terminal EOF convention.
                await self._pty_write_all(run.master_fd, b"\x04")
                return
            proc = run.proc
            if proc is not None and proc.stdin is not None:
                proc.stdin.close()
        except (BrokenPipeError, ConnectionResetError, OSError):
            logger.debug("stdin close failed for %s", run.exec_id, exc_info=True)

    async def _pty_write_all(self, fd: int, data: bytes) -> None:
        loop = asyncio.get_running_loop()
        view = memoryview(data)
        while view:
            try:
                written = os.write(fd, view)  # non-blocking fd
            except BlockingIOError:
                fut: asyncio.Future[None] = loop.create_future()
                loop.add_writer(fd, _make_ready_callback(fut))
                try:
                    await fut
                finally:
                    loop.remove_writer(fd)
                continue
            view = view[written:]

    async def _maybe_replenish(self, run: _ExecRun) -> None:
        """Mirror the fs inbound-credit hysteresis (<50% → re-grant)."""
        if run.inbound_granted - run.inbound_received >= self._initial_credit // 2:
            return
        run.inbound_granted += self._initial_credit
        try:
            await self._send(
                FrameType.RT_CREDIT,
                RtCredit(stream_id=run.exec_id, bytes=self._initial_credit),
                None,
            )
        except Exception:
            logger.debug("rt.credit send failed for %s", run.exec_id, exc_info=True)

    # -- send helpers ----------------------------------------------------------------------

    async def _send(self, frame_type: FrameType, payload: Any, request_id: str | None) -> None:
        if self._sender is None:
            raise ExecError(ErrorKind.INTERNAL, "executor is not bound to a sender")
        await self._sender(frame_type, payload, request_id)

    async def _send_output(self, run: _ExecRun, stream: ExecStream, data: bytes) -> None:
        await self._send(
            FrameType.EXEC_OUTPUT,
            ExecOutput(exec_id=run.exec_id, stream=stream, data=data),
            None,
        )

    async def _send_exit(self, run: _ExecRun, reason: ExitReason, exit_code: int | None) -> None:
        await self._send(
            FrameType.EXEC_OUTPUT,
            ExecOutput(
                exec_id=run.exec_id,
                stream=ExecStream.EXIT,
                exit_code=exit_code,
                reason=reason,
                request_id=run.request_id,
                truncated=run.truncated,
            ),
            None,
        )

    async def _reply_error(self, request_id: str, exc: BaseException) -> None:
        code, detail = _map_error(exc)
        logger.debug("exec op %s failed: %s %s", request_id, code.value, detail)
        try:
            await self._send(
                FrameType.ERROR,
                ErrorFrame(request_id=request_id, code=code, detail=detail),
                request_id,
            )
        except Exception:
            logger.debug("error frame send failed for %s", request_id, exc_info=True)

    # -- bookkeeping -------------------------------------------------------------------------

    async def _notify_start(self, run: _ExecRun) -> None:
        if self._on_exec_start is None:
            return
        try:
            result = self._on_exec_start(
                ExecStart(
                    exec_id=run.exec_id,
                    request_id=run.request_id,
                    cmd=run.cmd,
                    work_dir=run.work_dir,
                    pty=run.pty,
                    timeout_s=run.timeout_s,
                )
            )
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.debug("on_exec_start callback failed", exc_info=True)

    async def _cleanup(self, run: _ExecRun) -> None:
        self._execs.pop(run.exec_id, None)
        if self._requests.get(run.request_id) == run.exec_id:
            self._requests.pop(run.request_id, None)
        self._closed_ids.append(run.request_id)
        self._out_credit.release(run.exec_id)
        event = self._credit_events.pop(run.exec_id, None)
        if event is not None:
            event.set()
        fd, run.master_fd = run.master_fd, None
        if fd is not None:
            try:
                await asyncio.to_thread(os.close, fd)
            except OSError:
                logger.debug("master fd close failed for %s", run.exec_id, exc_info=True)
