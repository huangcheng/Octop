"""Outbound WebSocket client for the remote runtime daemon (spec §6).

The runtime dials the hub — never the reverse — at ``wss://<hub>/api/runtime/ws``
with subprotocol ``octop.runtime.v1`` and the enrolled ``rk_`` key as a Bearer
token. Lifecycle per spec:

- handshake: ``rt.hello`` -> ``rt.welcome`` (10 s, one resend, else 4004-class
  retry that does NOT count toward the protocol-hold cap) -> persist the
  generation -> ``rt.ready`` with the runtime-side initial credits;
- terminal close codes (4002 revoked / 4003 rotated / 4006 version_mismatch)
  stop the client and persist the mapped state;
- reconnectable codes: 4001 (one key re-read, then hold), 4004 (capped:
  more than 3 per 10 min -> protocol_hold), 4005 (60 s hold, then redial),
  4007 (redial); network loss and ``rt.bye`` redial with backoff;
- backoff 1 s -> 60 s, +/-20 % jitter, reset after 30 s of stable session;
- ping/pong liveness 20 s interval / 30 s timeout via the transport.

Only the shared protocol module, the enrollment store, the stdlib, and the
``websockets`` dependency are imported — never ``api``.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import dataclasses
import hashlib
import inspect
import logging
import platform
import shutil
import socket
import ssl
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version
from pathlib import Path
from typing import Any, Protocol

from octop.infra.runtime.enrollment_store import (
    STATE_PROTOCOL_HOLD,
    STATE_REVOKED,
    STATE_ROTATED,
    STATE_VERSION_MISMATCH,
    EnrollmentStore,
    RuntimeEnrollment,
)
from octop.infra.runtime.exec_executor import ExecExecutor
from octop.infra.runtime.executor import FsExecutor
from octop.infra.runtime.protocol import (
    FRAME_CAP_BYTES,
    INITIAL_CREDIT_BYTES,
    FrameEnvelope,
    FrameType,
    ProtocolFormatError,
    RtHello,
    RtPing,
    RtPong,
    RtReady,
    RtWelcome,
    decode_frame,
    encode_frame,
)
from octop.infra.utils.json_file import JsonFileCorruptError

try:
    CLIENT_VERSION = _dist_version("octop")
except PackageNotFoundError:  # running from a source tree without install
    CLIENT_VERSION = "dev"

WS_SUBPROTOCOL = "octop.runtime.v1"
WS_PATH = "/api/runtime/ws"
#: Codec minor version carried in the envelope; the subprotocol MAJOR is the
#: authority (one-minor window, spec §6).
MINOR_VERSION = 1

#: Close codes assigned by the hub (spec §6).
CLOSE_AUTH_FAILED = 4001
CLOSE_REVOKED = 4002
CLOSE_ROTATED = 4003
CLOSE_PROTOCOL_ERROR = 4004
CLOSE_SUPERSEDED = 4005
CLOSE_VERSION_MISMATCH = 4006
CLOSE_EXEC_DISABLED = 4007

#: Terminal close code -> persisted enrollment state.
_TERMINAL_STATES: dict[int, str] = {
    CLOSE_REVOKED: STATE_REVOKED,
    CLOSE_ROTATED: STATE_ROTATED,
    CLOSE_VERSION_MISMATCH: STATE_VERSION_MISMATCH,
}

WELCOME_TIMEOUT_SECONDS = 10.0
BACKOFF_BASE_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
BACKOFF_STABLE_RESET_SECONDS = 30.0
BACKOFF_JITTER = 0.2
PROTOCOL_ERROR_CAP = 3
PROTOCOL_ERROR_WINDOW_SECONDS = 600.0
SUPERSEDED_HOLD_SECONDS = 60.0
PIN_RECHECK_SECONDS = 60.0
#: WS-level oversize (1009) maps to the 4004 protocol-error class.
_CLOSE_MESSAGE_TOO_BIG = 1009

PING_INTERVAL_SECONDS = 20.0
PING_TIMEOUT_SECONDS = 30.0
OPEN_TIMEOUT_SECONDS = 15.0

#: Frame types the dispatch loop consumes itself before handler lookup.
_SELF_CONSUMED = (FrameType.RT_WELCOME,)

logger = logging.getLogger("octop.runtime.client")


class Clock(Protocol):
    """Injectable time source (tests fast-forward holds without sleeping)."""

    def monotonic(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@dataclasses.dataclass(frozen=True)
class ClientStatus:
    """Snapshot pushed to the status callback after every transition."""

    state: str
    hub_url: str
    generation: int
    last_close_code: int | None
    detail: str = ""


@dataclasses.dataclass(frozen=True)
class _SessionOutcome:
    kind: str  # terminal | hold | pin_mismatch | key_reread | superseded | stopped | network
    close_code: int | None = None


def compute_backoff(attempt: int, rand: float) -> float:
    """Backoff delay for ``attempt`` (1-based), +/-20 % jitter via ``rand`` in [0, 1)."""
    base = min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * (1 << max(0, attempt - 1)))
    factor = 1.0 + BACKOFF_JITTER * (2.0 * rand - 1.0)
    return base * factor


def hub_ws_url(hub_url: str) -> str:
    """Map an enrolled hub URL to its runtime WebSocket URL."""
    if hub_url.startswith("https://"):
        return "wss://" + hub_url[len("https://") :].rstrip("/") + WS_PATH
    if hub_url.startswith("http://"):
        return "ws://" + hub_url[len("http://") :].rstrip("/") + WS_PATH
    raise ValueError(f"unsupported hub_url scheme: {hub_url!r}")


def _close_code(exc: Any) -> int | None:
    """Close code from a ConnectionClosed without the deprecated ``.code``."""
    rcvd = getattr(exc, "rcvd", None)
    if rcvd is not None:
        code = getattr(rcvd, "code", None)
        return int(code) if isinstance(code, int) else None
    fallback = getattr(exc, "code", None)  # websockets < 13.1 fallback
    return fallback if isinstance(fallback, int) else None


#: Sentinel distinguishing "leave the field unchanged" from "set to None".
_UNSET: Any = object()


def spki_pin_from_der(der_cert: bytes) -> str:
    """TOFU pin of a leaf certificate: ``sha256/<urlsafe-base64-no-pad>``."""
    digest = hashlib.sha256(der_cert).digest()
    return "sha256/" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


async def _maybe_await(result: Any) -> None:
    if inspect.isawaitable(result):
        await result


class RuntimeClient:
    """Dial-and-serve loop for one enrolled runtime.

    ``handlers`` maps :class:`FrameType` to async callables receiving the
    decoded :class:`FrameEnvelope`; unknown/absent frame types are logged and
    ignored. ``on_rotation_pending`` fires when a welcome/rotate frame carries
    a pending rotation (the new key is NEVER in-band, spec §9). ``on_status``
    fires after every lifecycle transition.
    """

    def __init__(
        self,
        store: EnrollmentStore,
        *,
        handlers: Mapping[FrameType, Callable[[FrameEnvelope], Awaitable[None]]] | None = None,
        capabilities: Sequence[str] = ("fs",),
        workspace_base: str | None = None,
        on_rotation_pending: Callable[[], Any] | None = None,
        on_status: Callable[[ClientStatus], Any] | None = None,
        initial_credit_bytes: int = INITIAL_CREDIT_BYTES,
        welcome_timeout: float = WELCOME_TIMEOUT_SECONDS,
        clock: Clock | None = None,
        rand: Callable[[], float] | None = None,
        fs_executor: FsExecutor | None = None,
        exec_executor: ExecExecutor | None = None,
    ) -> None:
        self._store = store
        self._handlers = dict(handlers or {})
        self._capabilities = tuple(capabilities)
        self._workspace_base = workspace_base
        self._on_rotation_pending = on_rotation_pending
        self._on_status = on_status
        self._initial_credit_bytes = initial_credit_bytes
        self._welcome_timeout = welcome_timeout
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._rand = rand if rand is not None else _default_rand
        self._stopped = False
        self._conn: Any = None
        self._generation = 0
        self._tasks: set[asyncio.Task[None]] = set()
        self._fs_executor: FsExecutor | None = None
        self._exec_executor: ExecExecutor | None = None
        if fs_executor is not None:
            self.set_fs_executor(fs_executor)
        if exec_executor is not None:
            self.set_exec_executor(exec_executor)

    @property
    def generation(self) -> int:
        return self._generation

    def set_fs_executor(self, executor: FsExecutor) -> None:
        """Route ``fs.*`` frames (and ``rt.credit`` for fs streams) to *executor*.
        Replies, errors, and credit grants ride :meth:`_send_frame`. Additive:
        clients without an executor behave exactly as before.
        """
        self._fs_executor = executor
        executor.bind(self._send_frame)
        self._handlers.update(
            {
                FrameType.FS_LS: executor.handle_fs_ls,
                FrameType.FS_STAT: executor.handle_fs_stat,
                FrameType.FS_MKDIR: executor.handle_fs_mkdir,
                FrameType.FS_READ: executor.handle_fs_read,
                FrameType.FS_WRITE: executor.handle_fs_write,
                FrameType.FS_EDIT: executor.handle_fs_edit,
                FrameType.FS_DELETE: executor.handle_fs_delete,
                FrameType.FS_RENAME: executor.handle_fs_rename,
                FrameType.FS_GLOB: executor.handle_fs_glob,
                FrameType.FS_GREP: executor.handle_fs_grep,
                FrameType.FS_CANCEL: executor.handle_fs_cancel,
                # With an exec executor bound, rt.credit fans out to both
                # (each side ignores foreign stream ids).
                FrameType.RT_CREDIT: (
                    self._route_credit
                    if self._exec_executor is not None
                    else executor.handle_rt_credit
                ),
            }
        )
        executor.start_sweeper()

    def set_exec_executor(self, executor: ExecExecutor) -> None:
        """Route ``exec.*`` frames (and ``rt.credit`` for exec streams) to
        *executor*. Replies ride :meth:`_send_frame`; teardown kills every
        live exec (session drop = kill all, spec §8.2). Additive: clients
        without an executor behave exactly as before.
        """
        self._exec_executor = executor
        executor.bind(self._send_frame)
        self._handlers.update(
            {
                FrameType.EXEC_OPEN: executor.handle_exec_open,
                FrameType.EXEC_INPUT: executor.handle_exec_input,
                FrameType.EXEC_CLOSE: executor.handle_exec_close,
                FrameType.RT_CREDIT: self._route_credit,
            }
        )

    async def _route_credit(self, frame: FrameEnvelope) -> None:
        """rt.credit fan-out: each executor ignores foreign stream ids."""
        if self._fs_executor is not None:
            await self._fs_executor.handle_rt_credit(frame)
        if self._exec_executor is not None:
            await self._exec_executor.handle_rt_credit(frame)

    async def stop(self) -> None:
        """Graceful shutdown: send ``rt.bye`` when connected, then close."""
        self._stopped = True
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                await conn.send(encode_frame(self._generation, MINOR_VERSION, FrameType.RT_BYE))
            except Exception:
                logger.debug("rt.bye send failed", exc_info=True)
            try:
                await conn.close()
            except Exception:
                logger.debug("ws close failed", exc_info=True)
        for task in self._tasks:
            task.cancel()
        if self._fs_executor is not None:
            await self._fs_executor.close()
        if self._exec_executor is not None:
            await self._exec_executor.close()

    async def run(self) -> None:
        """Dial/serve until a terminal close code, a hold, or :meth:`stop`."""
        attempts = 0
        protocol_errors: collections.deque[float] = collections.deque()
        while not self._stopped:
            enrollment = self._store.load()
            if enrollment is None:
                await self._emit_status("missing_enrollment", None)
                return
            if enrollment.pending_pin:
                await self._emit_status("pin_mismatch", enrollment)
                await self._clock.sleep(PIN_RECHECK_SECONDS)
                continue
            session_start = self._clock.monotonic()
            outcome = await self._dial_and_serve(enrollment)
            if self._stopped or outcome.kind == "stopped":
                return
            stable = self._clock.monotonic() - session_start >= BACKOFF_STABLE_RESET_SECONDS
            attempts = 0 if stable else attempts + 1
            if outcome.kind == "terminal":
                return
            if outcome.kind == "hold":
                return
            if outcome.kind == "pin_mismatch":
                # Pin recorded as pending by _dial_and_serve; loop re-checks.
                continue
            if outcome.kind == "key_reread":
                try:
                    reloaded = self._store.load_required()
                except Exception:
                    return  # unreadable store: hold
                if reloaded.key != enrollment.key:
                    attempts = 0  # fresh key delivered out-of-band: redial now
                    continue
                if self._record_close(enrollment, CLOSE_AUTH_FAILED) is None:
                    return
                await self._emit_status("auth_failed", enrollment)
                return
            if outcome.kind == "superseded":
                if self._record_close(enrollment, CLOSE_SUPERSEDED) is None:
                    return
                await self._clock.sleep(SUPERSEDED_HOLD_SECONDS)
                continue
            if outcome.kind == "protocol_error":
                now = self._clock.monotonic()
                protocol_errors.append(now)
                while protocol_errors and now - protocol_errors[0] > PROTOCOL_ERROR_WINDOW_SECONDS:
                    protocol_errors.popleft()
                if len(protocol_errors) > PROTOCOL_ERROR_CAP:
                    if not self._persist(
                        enrollment, state=STATE_PROTOCOL_HOLD, close_code=CLOSE_PROTOCOL_ERROR
                    ):
                        return
                    await self._emit_status("protocol_hold", enrollment)
                    return
            if (
                outcome.close_code is not None
                and self._record_close(enrollment, outcome.close_code) is None
            ):
                return
            await self._emit_status("reconnecting", enrollment, close_code=outcome.close_code)
            await self._clock.sleep(compute_backoff(attempts, self._rand()))
        return

    # -- internals -----------------------------------------------------------

    async def _dial_and_serve(self, enrollment: RuntimeEnrollment) -> _SessionOutcome:
        from websockets.exceptions import (
            ConnectionClosed,
            InvalidHandshake,
            InvalidStatus,
            NegotiationError,
            WebSocketException,
        )

        ws_url = hub_ws_url(enrollment.hub_url)
        headers = {"Authorization": f"Bearer {enrollment.key}"}
        ssl_context = self._ssl_context(enrollment)
        try:
            from websockets.asyncio.client import connect as ws_connect
            from websockets.typing import Subprotocol

            conn = await ws_connect(
                ws_url,
                subprotocols=[Subprotocol(WS_SUBPROTOCOL)],
                additional_headers=headers,
                # websockets >= 14 reads HTTPS_PROXY / ALL_PROXY from the
                # environment when proxy is True (its default); an enrolled
                # --proxy overrides the environment.
                proxy=enrollment.proxy if enrollment.proxy else True,
                ping_interval=PING_INTERVAL_SECONDS,
                ping_timeout=PING_TIMEOUT_SECONDS,
                open_timeout=OPEN_TIMEOUT_SECONDS,
                max_size=FRAME_CAP_BYTES,
                ssl=ssl_context,
            )
        except NegotiationError as exc:
            # 101 but wrong/absent subprotocol: permanent, action-required.
            logger.error("hub rejected subprotocol %s: %s", WS_SUBPROTOCOL, exc)
            await self._emit_status("subprotocol_rejected", enrollment)
            return _SessionOutcome("hold")
        except InvalidStatus as exc:
            status = getattr(exc, "response", None)
            code = getattr(status, "status_code", None)
            if code is not None and (code in (404, 408, 425, 429) or code >= 500):
                return _SessionOutcome("network", close_code=code)
            logger.error("hub refused the dial with HTTP %s", code)
            if code is not None and self._record_close(enrollment, code) is None:
                return _SessionOutcome("hold")
            await self._emit_status("dial_refused", enrollment)
            return _SessionOutcome("hold")
        except (InvalidHandshake, WebSocketException, OSError, TimeoutError) as exc:
            logger.warning("dial to %s failed: %s", ws_url, exc)
            return _SessionOutcome("network")

        self._conn = conn
        try:
            pin_outcome = self._check_pin(conn, enrollment)
            if pin_outcome is not None:
                return pin_outcome
            outcome = await self._handshake(conn, enrollment)
            if outcome is not None:
                return outcome
            self._spawn(self._keepalive_loop(conn))
            return await self._dispatch_loop(conn)
        except ConnectionClosed as exc:
            if self._stopped:
                return _SessionOutcome("stopped")
            code = _close_code(exc) or 1006
            logger.info("ws closed with code %s", code)
            if code == _CLOSE_MESSAGE_TOO_BIG:
                return _SessionOutcome("protocol_error", CLOSE_PROTOCOL_ERROR)
            if code in _TERMINAL_STATES:
                if not self._persist(enrollment, state=_TERMINAL_STATES[code], close_code=code):
                    return _SessionOutcome("hold")
                await self._emit_status(_TERMINAL_STATES[code], enrollment, close_code=code)
                return _SessionOutcome("terminal", code)
            if code == CLOSE_AUTH_FAILED:
                return _SessionOutcome("key_reread", code)
            if code == CLOSE_SUPERSEDED:
                return _SessionOutcome("superseded", code)
            # 4004 / 4007 / rt.bye / clean hub restarts: reconnectable.
            return _SessionOutcome("network", code)
        finally:
            self._conn = None
            for task in list(self._tasks):
                task.cancel()
            self._tasks.clear()
            with contextlib.suppress(Exception):
                await conn.close()

    async def _handshake(self, conn: Any, enrollment: RuntimeEnrollment) -> _SessionOutcome | None:
        """hello -> welcome -> persist generation -> ready. None on success."""
        hello = self._build_hello(enrollment)
        # The header generation is 0: the session generation is created by
        # this handshake, so nothing is stale against it yet.
        await conn.send(encode_frame(0, MINOR_VERSION, FrameType.RT_HELLO, hello))
        welcome = await self._await_welcome(conn)
        if welcome is None:
            # One resend per spec §6 before treating it as 4004-class.
            await conn.send(encode_frame(0, MINOR_VERSION, FrameType.RT_HELLO, hello))
            welcome = await self._await_welcome(conn)
        if not isinstance(welcome, RtWelcome):
            # Second timeout (None) or peer close/malformed frame (outcome):
            # a 4004-class retry that does NOT count toward protocol-hold.
            logger.warning("rt.welcome handshake failed: %r", welcome)
            if isinstance(welcome, _SessionOutcome):
                return welcome
            return _SessionOutcome("network", CLOSE_PROTOCOL_ERROR)

        self._generation = welcome.generation
        if not self._persist(
            enrollment,
            generation=welcome.generation,
            close_code=None,
        ):
            return _SessionOutcome("hold")
        if welcome.rotation_pending and self._on_rotation_pending is not None:
            await _maybe_await(self._on_rotation_pending())
        await conn.send(
            encode_frame(
                welcome.generation,
                MINOR_VERSION,
                FrameType.RT_READY,
                RtReady(
                    generation=welcome.generation, initial_credit_bytes=self._initial_credit_bytes
                ),
            )
        )
        return None

    async def _await_welcome(self, conn: Any) -> RtWelcome | _SessionOutcome | None:
        """Wait one welcome-timeout window for ``rt.welcome``.

        Returns the payload, ``None`` on timeout, or an outcome when the peer
        closes or sends a structurally malformed frame (4004-class).
        """
        from websockets.exceptions import ConnectionClosed

        try:
            raw = await asyncio.wait_for(conn.recv(), timeout=self._welcome_timeout)
        except TimeoutError:
            return None
        except ConnectionClosed as exc:
            return _SessionOutcome("network", _close_code(exc) or 1006)
        try:
            frame = decode_frame(raw)
        except ProtocolFormatError:
            logger.warning("malformed frame during handshake")
            return _SessionOutcome("protocol_error", CLOSE_PROTOCOL_ERROR)
        if frame.frame_type != FrameType.RT_WELCOME or not isinstance(frame.payload, RtWelcome):
            logger.warning("expected rt.welcome, got %s", frame.frame_type.value)
            return _SessionOutcome("protocol_error", CLOSE_PROTOCOL_ERROR)
        return frame.payload

    async def _dispatch_loop(self, conn: Any) -> _SessionOutcome:
        from websockets.exceptions import ConnectionClosed

        while True:
            try:
                raw = await conn.recv()
            except ConnectionClosed:
                raise  # handled by _dial_and_serve
            try:
                frame = decode_frame(raw)
            except ProtocolFormatError:
                logger.warning("dropping malformed frame (4004-class)")
                return _SessionOutcome("protocol_error", CLOSE_PROTOCOL_ERROR)
            if frame.frame_type in _SELF_CONSUMED:
                logger.debug("ignoring duplicate %s", frame.frame_type.value)
                continue
            if frame.frame_type == FrameType.RT_PING:
                with contextlib.suppress(Exception):
                    await self._send_frame(FrameType.RT_PONG, RtPong())
                continue
            if frame.frame_type == FrameType.RT_PONG:
                continue
            if frame.frame_type == FrameType.RT_BYE:
                logger.info("hub sent rt.bye; reconnecting")
                return _SessionOutcome("network")
            if frame.frame_type == FrameType.RT_ROTATE:
                if self._on_rotation_pending is not None:
                    await _maybe_await(self._on_rotation_pending())
                continue
            handler = self._handlers.get(frame.frame_type)
            if handler is None:
                logger.debug("no handler for %s; ignoring", frame.frame_type.value)
                continue
            task = asyncio.create_task(self._run_handler(handler, frame))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _run_handler(self, handler: Any, frame: FrameEnvelope) -> None:
        try:
            await handler(frame)
        except Exception:
            # Per-op failures are never session death (spec §5/§10).
            logger.exception("handler for %s failed", frame.frame_type.value)

    async def _send_frame(
        self, frame_type: FrameType, payload: Any = None, request_id: str | None = None
    ) -> None:
        """Send one frame on the live session (executor replies and credits)."""
        conn = self._conn
        if conn is None:
            raise ConnectionError("runtime client is not connected")
        await conn.send(
            encode_frame(self._generation, MINOR_VERSION, frame_type, payload, request_id)
        )

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _keepalive_loop(self, conn: Any) -> None:
        """Application-level keepalive: Starlette hides WS-layer pings from the
        hub, so the hub's idle timeout needs rt.ping frames from the daemon."""
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL_SECONDS)
                try:
                    await conn.send(
                        encode_frame(self._generation, MINOR_VERSION, FrameType.RT_PING, RtPing())
                    )
                except Exception:
                    logger.debug("keepalive send failed; stopping", exc_info=True)
                    return
        except asyncio.CancelledError:
            return

    def _build_hello(self, enrollment: RuntimeEnrollment) -> RtHello:
        roots = enrollment.roots or (str(Path.home()),)
        base = self._workspace_base or roots[0]
        try:
            free_bytes = shutil.disk_usage(base).free
        except OSError:
            free_bytes = 0
        return RtHello(
            hostname=socket.gethostname(),
            os=platform.system().lower(),
            arch=platform.machine(),
            client_version=CLIENT_VERSION,
            workspace_base=base,
            roots=tuple(roots),
            free_bytes=free_bytes,
            # Placeholder until a platform probe lands; 1 ns means "exact".
            mtime_granularity_ns=1,
            capabilities=self._capabilities,
        )

    def _ssl_context(self, enrollment: RuntimeEnrollment) -> ssl.SSLContext | None:
        if enrollment.ca_bundle:
            return ssl.create_default_context(cafile=enrollment.ca_bundle)
        return None

    def _check_pin(self, conn: Any, enrollment: RuntimeEnrollment) -> _SessionOutcome | None:
        """TOFU hub TLS pin: first contact pins, mismatch holds for re-pin.

        Leaf-SPKI pin. A fuller chain-set pin (surviving LE-style leaf
        renewals, spec §6) needs peer-chain access the stdlib TLS API does
        not expose before Python 3.13; the re-pin runbook covers renewal.
        Plaintext ``ws://`` (localhost dev) has nothing to pin.
        """
        ssl_obj = conn.transport.get_extra_info("ssl_object")
        if ssl_obj is None:
            return None
        try:
            der = ssl_obj.getpeercert(binary_form=True)
        except (ssl.SSLError, ValueError) as exc:
            logger.warning("could not read hub certificate for pinning: %s", exc)
            return None
        if not der:
            return None
        observed = spki_pin_from_der(der)
        if enrollment.hub_pin is None:
            if not self._persist(enrollment, hub_pin=observed):
                return _SessionOutcome("hold")
            return None
        if observed == enrollment.hub_pin:
            return None
        logger.error("hub TLS pin mismatch (possible MITM or cert renewal); holding for re-pin")
        if not self._persist(enrollment, pending_pin=observed):
            return _SessionOutcome("hold")
        return _SessionOutcome("pin_mismatch")

    def _persist(
        self,
        enrollment: RuntimeEnrollment,
        *,
        state: str | None = None,
        generation: int | None = None,
        close_code: int | None | Any = _UNSET,
        hub_pin: str | None = None,
        pending_pin: str | None = None,
    ) -> bool:
        """Best-effort persistence of session bookkeeping; False = hold.

        A persist failure means the caller would dial on unpersisted state,
        which the spec forbids — so the caller holds instead.
        """

        def mutate(e: RuntimeEnrollment) -> None:
            if state is not None:
                e.state = state
            if generation is not None:
                e.last_generation = generation
            if close_code is not _UNSET:
                e.last_close_code = close_code
            if hub_pin is not None:
                e.hub_pin = hub_pin
            if pending_pin is not None:
                e.pending_pin = pending_pin

        try:
            self._store.update(mutate)
        except (OSError, JsonFileCorruptError):
            logger.error("failed to persist enrollment state; holding", exc_info=True)
            return False
        return True

    def _record_close(
        self, enrollment: RuntimeEnrollment, code: int | None
    ) -> RuntimeEnrollment | None:
        try:
            return self._store.update(lambda e: setattr(e, "last_close_code", code))
        except (OSError, JsonFileCorruptError):
            logger.error("failed to persist last_close_code; holding", exc_info=True)
            return None

    async def _emit_status(
        self,
        detail: str,
        enrollment: RuntimeEnrollment | None,
        *,
        close_code: int | None = None,
    ) -> None:
        if self._on_status is None:
            return
        status = ClientStatus(
            state=(enrollment.state if enrollment is not None else "unenrolled"),
            hub_url=(enrollment.hub_url if enrollment is not None else ""),
            generation=self._generation,
            last_close_code=close_code
            if close_code is not None
            else (enrollment.last_close_code if enrollment else None),
            detail=detail,
        )
        await _maybe_await(self._on_status(status))


def _default_rand() -> float:
    import random

    return random.random()
