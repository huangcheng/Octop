"""Outbound-dial runtime WebSocket endpoint.

A cloud Octop ("hub") accepts reverse connections from enrolled local machines
("runtimes") here. The runtime dials ``/api/runtime/ws`` with its per-machine
``rk_`` credential and negotiates exactly the ``octop.runtime.v1``
subprotocol. Authentication is key-based (SHA-256 of the credential), so this
endpoint bypasses JWT but stays gated on setup completion.

The endpoint is a thin adapter: handshake choreography, rate limiting and
session registration live here; frame codec and session state live in
``octop.infra.runtime``.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from typing import Any, cast

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from octop.api.common.client_ip import UNKNOWN_CLIENT
from octop.infra.db.repos.runtime_enrollments import RuntimeEnrollmentRepo
from octop.infra.runtime import protocol
from octop.infra.runtime.protocol import (
    ErrorFrame,
    ErrorKind,
    FrameType,
    ProtocolFormatError,
    RtPong,
    RtWelcome,
    decode_frame,
    encode_frame,
    is_stale,
)
from octop.infra.runtime.rate_limit import RuntimeRateLimiter
from octop.infra.runtime.registry import (
    CLOSE_SUPERSEDED,
    DEFAULT_QUOTAS,
    HUB_CAPABILITIES,
    RuntimeSession,
    RuntimeSessionRegistry,
)

logger = logging.getLogger(__name__)

router = APIRouter()

WS_SUBPROTOCOL = "octop.runtime.v1"
PROTOCOL_MINOR_VERSION = 0
HELLO_TIMEOUT_SECONDS = 10.0
READY_TIMEOUT_SECONDS = 10.0
# Application-level keepalive: the runtime sends rt.ping every 20s; a silent
# peer for three intervals is treated as dead (Starlette hides WS pings).
SESSION_IDLE_TIMEOUT_SECONDS = 65.0

# Wire close codes (spec §6): 4001 auth, 4002 revoked, 4003 rotated,
# 4004 protocol, 4005 superseded, 4006 version, 4007 exec_disabled.
CLOSE_AUTH_FAILED = 4001
CLOSE_REVOKED = 4002
CLOSE_ROTATED = 4003
CLOSE_PROTOCOL_ERROR = 4004
CLOSE_BLOCKED = 1008  # credential-failure block (policy violation)
CLOSE_TRY_AGAIN_LATER = 1013  # dial rate limit / setup incomplete


def _runtime_parts(
    server: Any,
) -> tuple[RuntimeSessionRegistry, RuntimeRateLimiter, RuntimeEnrollmentRepo] | None:
    rt = getattr(server, "app_runtime", None)
    if rt is None:
        return None
    registry = cast(RuntimeSessionRegistry | None, getattr(rt, "runtime_registry", None))
    limiter = cast(RuntimeRateLimiter | None, getattr(rt, "runtime_rate_limiter", None))
    if registry is None or limiter is None or getattr(server, "services", None) is None:
        return None
    return registry, limiter, cast(RuntimeEnrollmentRepo, server.services.runtime_enrollment_repo)


def _client_ip(websocket: WebSocket) -> str:
    # X-Forwarded-For is ignored: no trusted-proxy list is configured, and a
    # caller must not be able to pick its own rate-limit bucket.
    client = websocket.client
    host = client.host if client is not None else ""
    return host.strip() or UNKNOWN_CLIENT


def _bearer_key(websocket: WebSocket) -> str | None:
    header = websocket.headers.get("authorization") or ""
    if not header.lower().startswith("bearer "):
        return None
    token = header[7:].strip()
    return token if token.startswith("rk_") else None


async def _close_quietly(websocket: WebSocket, code: int, reason: str = "") -> None:
    """Best-effort close; the peer may already be gone."""
    with contextlib.suppress(Exception):
        await websocket.close(code=code, reason=reason)


async def _recv_frame(websocket: WebSocket, timeout: float) -> protocol.FrameEnvelope:
    raw = await asyncio.wait_for(websocket.receive_text(), timeout)
    return decode_frame(raw)


def _machine_metadata(hello: protocol.RtHello) -> str:
    doc = {
        "schema_version": 1,
        "hostname": hello.hostname,
        "os": hello.os,
        "arch": hello.arch,
        "client_version": hello.client_version,
        "workspace_base": hello.workspace_base,
        "roots": list(hello.roots),
        "free_bytes": hello.free_bytes,
        "mtime_granularity_ns": hello.mtime_granularity_ns,
        "capabilities": list(hello.capabilities),
    }
    return json.dumps(doc, separators=(",", ":"))


async def _session_loop(websocket: WebSocket, session: RuntimeSession) -> None:
    """Dispatch frames until the session closes (bye, eviction, disconnect).

    No hub DATA frames flow before rt.ready — this loop starts only after the
    handshake completed, so every send here is legal.
    """
    while True:
        recv_task = asyncio.create_task(websocket.receive_text())
        closed_task = asyncio.create_task(session.closed.wait())
        try:
            done, pending = await asyncio.wait(
                {recv_task, closed_task},
                return_when=asyncio.FIRST_COMPLETED,
                timeout=SESSION_IDLE_TIMEOUT_SECONDS,
            )
            if not done:
                # Silent peer: Starlette never surfaces WS-layer pings, so an
                # application-level keepalive gap is the only liveness signal.
                await session.close_with(CLOSE_PROTOCOL_ERROR, "keepalive timeout")
                return
            for task in pending:
                task.cancel()
            if recv_task not in done:
                # Evicted (close_with raced us) — closing the transport is the
                # evictor's job; just leave.
                return
            try:
                raw = recv_task.result()
            except (WebSocketDisconnect, RuntimeError) as exc:
                # RuntimeError: the transport was closed while receive_text
                # was in flight (eviction). close_with may have already
                # recorded a code — never overwrite it.
                if session.close_code is None:
                    session.close_code = getattr(exc, "code", None) or 1005
                session.close_reason = session.close_reason or "disconnected"
                session.closed.set()
                return
        finally:
            for task in (recv_task, closed_task):
                if task.done():
                    continue
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        try:
            frame = decode_frame(raw)
        except ProtocolFormatError:
            await session.close_with(CLOSE_PROTOCOL_ERROR, "malformed frame")
            return

        if is_stale(frame.generation, session.generation):
            continue
        if frame.frame_type == FrameType.RT_PING:
            with contextlib.suppress(Exception):
                await websocket.send_text(
                    encode_frame(
                        session.generation,
                        PROTOCOL_MINOR_VERSION,
                        FrameType.RT_PONG,
                        RtPong(),
                    )
                )
            continue
        if frame.frame_type == FrameType.RT_PONG:
            continue  # liveness is implicit in having received any frame
        if frame.frame_type == FrameType.RT_BYE:
            await session.close_with(1000, "bye")
            return
        if frame.frame_type == FrameType.ERROR:
            logger.info(
                "runtime %s reported error for request %s: %s",
                session.enrollment_id,
                frame.request_id,
                getattr(frame.payload, "detail", ""),
            )
            # The plugged handler (RemoteRuntimeBackend) must see error frames
            # too: they resolve the waiting op future. Logging alone leaves the
            # hub-side future hanging until the session dies.
            handler = session.frame_handler
            if handler is not None:
                try:
                    await handler(frame)
                except Exception:
                    logger.exception(
                        "runtime frame handler failed on ERROR frame (%s)",
                        frame.request_id,
                    )
            continue

        handler = session.frame_handler
        if handler is not None:
            try:
                await handler(frame)
            except Exception:
                logger.exception("runtime frame handler failed (%s)", frame.frame_type.value)
            continue
        if protocol.is_control(frame):
            # Known control frame without a plugged handler yet (credit
            # accounting lands in a later wave) — ignore, never error.
            continue

        # Unknown/unhandled application frame: per-request tool error,
        # never session-terminal.
        error = ErrorFrame(
            request_id=frame.request_id,
            code=ErrorKind.UNSUPPORTED_FRAME,
            detail=f"unsupported frame {frame.frame_type.value}",
        )
        with contextlib.suppress(Exception):
            await websocket.send_text(
                encode_frame(
                    session.generation,
                    PROTOCOL_MINOR_VERSION,
                    FrameType.ERROR,
                    error,
                    request_id=frame.request_id,
                )
            )


@router.websocket("/ws")
async def runtime_ws(websocket: WebSocket) -> None:
    """Runtime daemon dial-in: rk_ Bearer auth, hello/welcome/ready fencing."""
    server = websocket.app.state.octop_server

    # The registry and limiter are per-process; multi-worker hubs cannot serve
    # runtime sessions. Deny the handshake with HTTP 404 (REST stays up).
    if getattr(server, "uvicorn_workers", 1) > 1:
        raise HTTPException(status_code=404, detail="runtime ws unavailable on multi-worker hubs")

    # Negotiate exactly our subprotocol or reject the handshake outright.
    if WS_SUBPROTOCOL not in websocket.scope.get("subprotocols", []):
        await _close_quietly(websocket, 1002, "subprotocol required")
        return
    await websocket.accept(subprotocol=WS_SUBPROTOCOL)

    parts = _runtime_parts(server)
    if parts is None:
        await _close_quietly(websocket, 1011, "runtime services unavailable")
        return
    registry, limiter, repo = parts
    client_ip = _client_ip(websocket)

    # Setup gate: same predicate the lockdown middleware uses, applied here
    # because plain-HTTP middleware never sees websocket scopes.
    user_manager = getattr(server, "user_manager", None)
    if user_manager is None or user_manager.count() == 0:
        await _close_quietly(websocket, CLOSE_TRY_AGAIN_LATER, "setup_required")
        return

    # Blocked keys are rejected before any auth work.
    if limiter.is_blocked(client_ip, None):
        await _close_quietly(websocket, CLOSE_BLOCKED, "blocked")
        return

    key = _bearer_key(websocket)
    resolved = None
    if key is not None:
        key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        resolved = repo.get_by_key_hash(key_hash)

    enrollment_id: str | None = resolved[0].enrollment_id if resolved else None
    if resolved is not None and limiter.is_blocked(client_ip, enrollment_id):
        await _close_quietly(websocket, CLOSE_BLOCKED, "blocked")
        return

    if not limiter.consume_dial(client_ip, enrollment_id):
        await _close_quietly(websocket, CLOSE_TRY_AGAIN_LATER, "rate_limited")
        return

    if resolved is None:
        limiter.record_credential_failure(client_ip, None)
        await _close_quietly(websocket, CLOSE_AUTH_FAILED, "auth_failed")
        return

    row, key_slot = resolved
    if row.status != "active":
        await _close_quietly(websocket, CLOSE_REVOKED, "revoked")
        return

    now = int(time.time())
    if key_slot == "previous":
        expires = row.rotation_expires_at
        if expires is None or now >= expires:
            await _close_quietly(websocket, CLOSE_ROTATED, "rotated")
            return

    rotation_pending = (
        row.previous_key_hash is not None
        and row.rotation_expires_at is not None
        and now < row.rotation_expires_at
    )

    async def close_impl(code: int, reason: str) -> None:
        if websocket.application_state == WebSocketState.CONNECTED:
            with contextlib.suppress(Exception):
                await websocket.close(code=code, reason=reason)

    # Handshake: rt.hello -> bump generation -> rt.welcome -> rt.ready.
    try:
        hello_frame = await _recv_frame(websocket, HELLO_TIMEOUT_SECONDS)
        if hello_frame.frame_type != FrameType.RT_HELLO:
            raise ProtocolFormatError("expected rt.hello")
        hello = cast(protocol.RtHello, hello_frame.payload)
    except (TimeoutError, WebSocketDisconnect, ProtocolFormatError):
        await _close_quietly(websocket, CLOSE_PROTOCOL_ERROR, "handshake failed")
        return
    except Exception:
        logger.exception("runtime handshake receive failed")
        await _close_quietly(websocket, CLOSE_PROTOCOL_ERROR, "handshake failed")
        return

    if not set(hello.capabilities).issubset(HUB_CAPABILITIES):
        await _close_quietly(websocket, CLOSE_PROTOCOL_ERROR, "unknown capability")
        return

    generation = repo.bump_generation(row.enrollment_id)
    if generation is None:
        await _close_quietly(websocket, 1011, "internal")
        return

    capabilities = tuple(c for c in HUB_CAPABILITIES if c in hello.capabilities)
    welcome = RtWelcome(
        generation=generation,
        capabilities=capabilities,
        quotas=dict(DEFAULT_QUOTAS),
        initial_credit_bytes=protocol.INITIAL_CREDIT_BYTES,
        rotation_pending=rotation_pending,
    )
    with contextlib.suppress(Exception):
        await websocket.send_text(
            encode_frame(generation, PROTOCOL_MINOR_VERSION, FrameType.RT_WELCOME, welcome)
        )

    try:
        ready_frame = await _recv_frame(websocket, READY_TIMEOUT_SECONDS)
        if ready_frame.frame_type != FrameType.RT_READY:
            raise ProtocolFormatError("expected rt.ready")
        ready = cast(protocol.RtReady, ready_frame.payload)
        if ready.generation != generation:
            raise ProtocolFormatError("rt.ready generation mismatch")
    except (TimeoutError, WebSocketDisconnect, ProtocolFormatError):
        await _close_quietly(websocket, CLOSE_PROTOCOL_ERROR, "handshake failed")
        return
    except Exception:
        logger.exception("runtime handshake receive failed")
        await _close_quietly(websocket, CLOSE_PROTOCOL_ERROR, "handshake failed")
        return

    session = RuntimeSession(
        enrollment_id=row.enrollment_id,
        generation=generation,
        key_slot=key_slot,
        capabilities=capabilities,
        client_ip=client_ip,
        close_impl=close_impl,
    )
    # Supersession happens AFTER the handshake so eviction observes the final
    # registry state: a current-key dial supersedes every live session of the
    # enrollment (4005). Previous-key dials never evict — both coexist during
    # the rotation overlap.
    if key_slot == "current":
        for old in registry.sessions(row.enrollment_id):
            with contextlib.suppress(Exception):
                await old.close_with(CLOSE_SUPERSEDED, "superseded")

    registry.register(session)
    # The session loop owns the socket from here on: bind the transport send
    # hook (plus the owning loop) so hub-side backends can push frames.
    session.set_sender(websocket.send_text)
    limiter.refund_dial(client_ip, enrollment_id)  # successful handshakes are exempt
    with contextlib.suppress(Exception):
        repo.mark_seen(row.enrollment_id, machine_metadata=_machine_metadata(hello))

    logger.info(
        "runtime session established: %s (generation %s, slot %s)",
        row.enrollment_id,
        generation,
        key_slot,
    )
    try:
        await _session_loop(websocket, session)
    except Exception:
        logger.exception("runtime session loop crashed: %s", row.enrollment_id)
    finally:
        registry.remove(session)
        session.set_sender(None)
        await session.close_with(session.close_code or 1005, session.close_reason or "closed")
        if session.close_code == CLOSE_SUPERSEDED:
            # Supersession telemetry: which live connection lost the race.
            with contextlib.suppress(Exception):
                repo.record_conflict(row.enrollment_id, ip=session.client_ip)
        with contextlib.suppress(Exception):
            repo.mark_seen(row.enrollment_id)
        logger.info(
            "runtime session closed: %s (code %s)",
            row.enrollment_id,
            session.close_code,
        )
