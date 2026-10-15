"""WebSocket handshake and session lifecycle tests for /api/runtime/ws."""

from __future__ import annotations

import asyncio
import hashlib
import secrets
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.websockets import WebSocketDisconnect
from tests.support.app import ensure_control_plane_bound, octop_client
from tests.support.auth import bearer, bootstrap_admin, login
from tests.support.http import HandshakeDenied, ws_connect

from octop.api.app import build_app
from octop.infra.runtime import protocol
from octop.infra.runtime.protocol import (
    INITIAL_CREDIT_BYTES,
    FrameType,
    RtHello,
    RtReady,
    encode_frame,
)
from octop.infra.runtime.rate_limit import RuntimeRateLimiter
from octop.infra.server import OctopServer
from octop.infra.utils.ulid import new_ulid

WS_PATH = "/api/runtime/ws"
SUBPROTOCOL = "octop.runtime.v1"
# ASGI test scope client host; the endpoint rate-limits on this address.
TESTCLIENT_IP = "testclient"


def _fresh_key() -> str:
    return "rk_" + secrets.token_hex(32)


def _key_hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _enroll(srv: OctopServer, key: str, *, owner_user_id: int, status: str = "active") -> str:
    assert srv.services is not None
    enrollment_id = new_ulid()
    srv.services.runtime_enrollment_repo.create(
        enrollment_id=enrollment_id,
        owner_user_id=owner_user_id,
        display_name="machine-1",
        key_hash=_key_hash(key),
        status=status,
    )
    return enrollment_id


def _admin_id(srv: OctopServer) -> int:
    assert srv.user_manager is not None
    admin = srv.user_manager.get("admin")
    assert admin is not None
    return admin.id


def _hello_frame(
    capabilities: tuple[str, ...] = ("fs", "exec", "pty"),
    *,
    generation: int = 0,
) -> str:
    return encode_frame(
        generation,
        0,
        FrameType.RT_HELLO,
        RtHello(
            hostname="machine-1",
            os="linux",
            arch="arm64",
            client_version="0.1.0",
            workspace_base="/home/u/ws",
            roots=("/home/u/ws",),
            free_bytes=1 << 30,
            mtime_granularity_ns=1,
            capabilities=capabilities,
        ),
    )


def _ready_frame(generation: int) -> str:
    return encode_frame(
        generation,
        0,
        FrameType.RT_READY,
        RtReady(generation=generation, initial_credit_bytes=INITIAL_CREDIT_BYTES),
    )


def _dial(c: Any, key: str, *, subprotocols: list[str] | None = None) -> Any:
    return ws_connect(
        c._octop_app,
        WS_PATH,
        headers={"Authorization": f"Bearer {key}"},
        subprotocols=[SUBPROTOCOL] if subprotocols is None else subprotocols,
    )


@pytest.fixture
async def env(tmp_octop_home: Path) -> AsyncIterator[tuple[Any, OctopServer]]:
    async with octop_client(tmp_octop_home) as (c, srv):
        await bootstrap_admin(c, tmp_octop_home)
        yield c, srv


async def _handshake(
    ws: Any,
    expected_generation: int = 1,
    *,
    expect_registered: tuple[Any, str] | None = None,
) -> dict[str, Any]:
    """Drive hello -> welcome -> ready and return the welcome payload."""
    await ws.send_text(_hello_frame())
    welcome = await ws.receive_json()
    assert welcome["t"] == "rt.welcome"
    assert welcome["v"] == [expected_generation, 0]
    await ws.send_text(_ready_frame(welcome["d"]["generation"]))
    if expect_registered is not None:
        registry, eid = expect_registered
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not registry.online(eid):
            await asyncio.sleep(0.01)
        assert registry.online(eid) is True
    return welcome


async def test_handshake_happy_path_dispatch_and_bye(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    eid = _enroll(srv, key, owner_user_id=_admin_id(srv))

    registry = srv.app_runtime.runtime_registry  # type: ignore[union-attr]
    async with _dial(c, key) as ws:
        welcome = await _handshake(ws, expect_registered=(registry, eid))
        d = welcome["d"]
        assert d["generation"] == 1
        assert d["capabilities"] == ["fs", "exec", "pty"]
        assert d["rotation_pending"] is False
        assert d["initial_credit_bytes"] == INITIAL_CREDIT_BYTES
        assert d["quotas"]["frame_cap_bytes"] == protocol.FRAME_CAP_BYTES
        assert d["quotas"]["exec_queue_bytes"] == protocol.EXEC_QUEUE_BYTES

        assert srv.services is not None
        row = srv.services.runtime_enrollment_repo.get_by_enrollment_id(eid)
        assert row is not None
        assert row.generation == 1
        assert row.last_seen_at is not None
        assert row.machine_metadata is not None
        assert '"hostname":"machine-1"' in row.machine_metadata
        assert registry.online(eid) is True

        assert srv.services is not None
        row = srv.services.runtime_enrollment_repo.get_by_enrollment_id(eid)
        assert row is not None
        assert row.generation == 1
        assert row.last_seen_at is not None
        assert row.machine_metadata is not None
        assert '"hostname":"machine-1"' in row.machine_metadata

        # No handler plugged yet: an application frame gets a per-request
        # unsupported_frame error, never a session close.
        await ws.send_text(
            encode_frame(1, 0, FrameType.FS_STAT, protocol.FsPath(path="/x"), request_id="req-1")
        )
        error = await ws.receive_json()
        assert error["t"] == "error"
        assert error["r"] == "req-1"
        assert error["d"]["code"] == "unsupported_frame"

        # Graceful bye closes with 1000.
        await ws.send_text(encode_frame(1, 0, FrameType.RT_BYE, protocol.RtBye()))
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 1000

    assert registry.online(eid) is False


async def test_bad_key_closes_4001(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    async with _dial(c, _fresh_key()) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 4001
        assert exc_info.value.reason == "auth_failed"

    limiter = srv.app_runtime.runtime_rate_limiter  # type: ignore[union-attr]
    assert limiter.is_blocked(TESTCLIENT_IP, None) is False  # one failure != block


async def test_revoked_enrollment_closes_4002(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    _enroll(srv, key, owner_user_id=_admin_id(srv), status="revoked")
    async with _dial(c, key) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 4002


@pytest.mark.parametrize("subprotocols", [[], ["octop.chat.v9"]])
async def test_wrong_subprotocol_rejects_handshake(
    env: tuple[Any, OctopServer],
    subprotocols: list[str],
) -> None:
    c, srv = env
    key = _fresh_key()
    _enroll(srv, key, owner_user_id=_admin_id(srv))
    with pytest.raises(WebSocketDisconnect) as exc_info:
        async with _dial(c, key, subprotocols=subprotocols):
            pass  # pragma: no cover - connect raises first
    assert exc_info.value.code == 1002


async def test_stale_generation_frame_is_dropped(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    eid = _enroll(srv, key, owner_user_id=_admin_id(srv))

    registry = srv.app_runtime.runtime_registry  # type: ignore[union-attr]
    async with _dial(c, key) as ws:
        await _handshake(ws, expect_registered=(registry, eid))

        # Stale-generation application frame: silently dropped, no error frame.
        stale = encode_frame(
            999, 0, FrameType.FS_STAT, protocol.FsPath(path="/x"), request_id="stale-1"
        )
        await ws.send_text(stale)
        with pytest.raises(TimeoutError):
            await ws.receive_json(timeout=1.0)

        # Current-generation frame still gets its per-request error.
        await ws.send_text(
            encode_frame(1, 0, FrameType.FS_STAT, protocol.FsPath(path="/x"), request_id="fresh-1")
        )
        error = await ws.receive_json()
        assert error["r"] == "fresh-1"

        # Session is unaffected throughout.
        assert registry.online(eid) is True


async def test_current_key_dial_supersedes_live_session(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    eid = _enroll(srv, key, owner_user_id=_admin_id(srv))
    registry = srv.app_runtime.runtime_registry  # type: ignore[union-attr]

    async with _dial(c, key) as first:
        await _handshake(first, expect_registered=(registry, eid))
        assert registry.online(eid) is True

        async with _dial(c, key) as second:
            # The new dial completes its own fenced handshake first — only a
            # fully established session supersedes (post-handshake eviction).
            welcome = await _handshake(second, expected_generation=2)
            assert welcome["d"]["generation"] == 2
            # ... then the older session is evicted with 4005.
            with pytest.raises(WebSocketDisconnect) as exc_info:
                await first.receive_json()
            assert exc_info.value.code == 4005
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and len(registry.sessions(eid)) != 1:
                await asyncio.sleep(0.01)
            assert len(registry.sessions(eid)) == 1

    assert registry.online(eid) is False
    assert srv.services is not None
    row = srv.services.runtime_enrollment_repo.get_by_enrollment_id(eid)
    assert row is not None
    assert row.last_conflict_at is not None
    assert row.last_conflict_ip == TESTCLIENT_IP


async def test_old_key_dial_during_overlap_never_evicts(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key_old = _fresh_key()
    eid = _enroll(srv, key_old, owner_user_id=_admin_id(srv))
    registry = srv.app_runtime.runtime_registry  # type: ignore[union-attr]
    assert registry is not None
    assert srv.services is not None

    async with _dial(c, key_old) as current_session:
        welcome = await _handshake(current_session, expect_registered=(registry, eid))
        assert welcome["d"]["rotation_pending"] is False

        # Owner rotates the credential; the old key stays valid in its window.
        key_new = _fresh_key()
        srv.services.runtime_enrollment_repo.rotate(eid, new_key_hash=_key_hash(key_new))

        # Old-key dial: no eviction, rotation_pending announced, coexistence.
        async with _dial(c, key_old) as overlap_session:
            overlap_welcome = await _handshake(overlap_session, expected_generation=2)
            assert overlap_welcome["d"]["rotation_pending"] is True
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and len(registry.sessions(eid)) != 2:
                await asyncio.sleep(0.01)
            assert len(registry.sessions(eid)) == 2
            assert not any(s.closed.is_set() for s in registry.sessions(eid))

            # A current-key dial outranks both and supersedes them.
            async with _dial(c, key_new) as winner:
                await _handshake(winner, expected_generation=3)
                with pytest.raises(WebSocketDisconnect) as first_exc:
                    await current_session.receive_json()
                assert first_exc.value.code == 4005
                with pytest.raises(WebSocketDisconnect) as overlap_exc:
                    await overlap_session.receive_json()
                assert overlap_exc.value.code == 4005
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline and len(registry.sessions(eid)) != 1:
                    await asyncio.sleep(0.01)
                assert len(registry.sessions(eid)) == 1


async def test_expired_previous_key_closes_4003(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key_old = _fresh_key()
    eid = _enroll(srv, key_old, owner_user_id=_admin_id(srv))
    assert srv.services is not None
    repo = srv.services.runtime_enrollment_repo
    key_new = _fresh_key()
    repo.rotate(eid, new_key_hash=_key_hash(key_new))

    # Force-expire the rotation window.
    row = repo.get_by_enrollment_id(eid)
    assert row is not None and row.rotation_expires_at is not None
    with repo._db.transaction() as conn:  # noqa: SLF001 - no repo expiry setter
        conn.execute(
            "UPDATE runtime_enrollments SET rotation_expires_at = ? WHERE enrollment_id = ?",
            (row.rotation_expires_at - 25 * 60 * 60, eid),
        )

    async with _dial(c, key_old) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 4003


async def test_unknown_capability_closes_4004(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    _enroll(srv, key, owner_user_id=_admin_id(srv))
    async with _dial(c, key) as ws:
        await ws.send_text(_hello_frame(capabilities=("fs", "webcam")))
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 4004


async def test_wrong_first_frame_closes_4004(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    _enroll(srv, key, owner_user_id=_admin_id(srv))
    async with _dial(c, key) as ws:
        await ws.send_text(_ready_frame(0))
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 4004


async def test_credential_failures_block_then_close_1008(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    key = _fresh_key()
    _enroll(srv, key, owner_user_id=_admin_id(srv))
    limiter = srv.app_runtime.runtime_rate_limiter  # type: ignore[union-attr]

    for _ in range(RuntimeRateLimiter.CREDENTIAL_FAILURE_THRESHOLD):
        limiter.record_credential_failure(TESTCLIENT_IP, None)

    async with _dial(c, key) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 1008


async def test_dial_limit_closes_1013(env: tuple[Any, OctopServer]) -> None:
    c, srv = env
    limiter = srv.app_runtime.runtime_rate_limiter  # type: ignore[union-attr]
    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial(TESTCLIENT_IP, None) is True

    async with _dial(c, _fresh_key()) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            await ws.receive_json()
        assert exc_info.value.code == 1013
        assert exc_info.value.reason == "rate_limited"


async def test_successful_handshakes_are_exempt_from_dial_limit(
    env: tuple[Any, OctopServer],
) -> None:
    c, srv = env
    key = _fresh_key()
    eid = _enroll(srv, key, owner_user_id=_admin_id(srv))
    limiter = srv.app_runtime.runtime_rate_limiter  # type: ignore[union-attr]

    # Leave exactly one token in the (ip, enrollment) bucket.
    for _ in range(RuntimeRateLimiter.DIAL_BURST - 1):
        assert limiter.consume_dial(TESTCLIENT_IP, eid) is True

    async with _dial(c, key) as ws:
        await _handshake(ws)  # consumes the last token, then refunds it

    # The 11th dial would fail without the refund; with it, the handshake
    # succeeds and supersedes the previous session.
    async with _dial(c, key) as ws2:
        await _handshake(ws2, expected_generation=2)


async def test_setup_incomplete_rejects_dial(tmp_octop_home: Path) -> None:
    async with octop_client(tmp_octop_home) as (c, srv):
        # Database bound but no users yet — the lockdown predicate.
        assert srv.user_manager is not None
        assert srv.user_manager.count() == 0
        async with _dial(c, _fresh_key()) as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                await ws.receive_json()
            assert exc_info.value.code == 1013
            assert exc_info.value.reason == "setup_required"


async def test_multi_worker_hub_denies_handshake_with_404(tmp_octop_home: Path) -> None:
    srv = OctopServer(home=tmp_octop_home)
    await srv.start()
    try:
        await ensure_control_plane_bound(srv)
        if srv.app_runtime is not None:
            await srv.app_runtime.proactive_scheduler.shutdown()
            srv.app_runtime.proactive_scheduler.suspend()
        srv.uvicorn_workers = 2
        app = build_app(srv)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as c:
            await bootstrap_admin(c, tmp_octop_home)

        with pytest.raises(HandshakeDenied) as exc_info:
            async with ws_connect(
                app,
                WS_PATH,
                headers={"Authorization": f"Bearer {_fresh_key()}"},
                subprotocols=[SUBPROTOCOL],
            ):
                pass  # pragma: no cover - connect raises first
        assert exc_info.value.status_code == 404
    finally:
        await srv.stop()


async def test_status_endpoint_reports_online_from_registry(
    env: tuple[Any, OctopServer],
) -> None:
    c, srv = env
    key = _fresh_key()
    eid = _enroll(srv, key, owner_user_id=_admin_id(srv))
    token = await login(c)

    offline = await c.get(f"/api/runtimes/{eid}/status", headers=bearer(token))
    assert offline.status_code == 200
    assert offline.json()["online"] is False

    async with _dial(c, key) as ws:
        await _handshake(ws)
        online = await c.get(f"/api/runtimes/{eid}/status", headers=bearer(token))
        assert online.status_code == 200
        body = online.json()
        assert body["online"] is True
        assert body["enrollment_id"] == eid
        assert body["generation"] == 1
        assert body["last_seen_at"] is not None

    offline_again = await c.get(f"/api/runtimes/{eid}/status", headers=bearer(token))
    assert offline_again.json()["online"] is False
