"""Device presence WebSocket — phone registration and tool-call round trip."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from starlette.websockets import WebSocketDisconnect

from octop.infra.devices.hub import DEVICE_HUB
from tests.support.app import octop_client
from tests.support.auth import auth_header, bootstrap_admin, ensure_users
from tests.support.http import ws_connect, ws_token


@pytest.fixture
async def env(tmp_octop_home: Path) -> AsyncIterator[Any]:
    async with octop_client(tmp_octop_home) as (c, srv):
        await bootstrap_admin(c, tmp_octop_home)
        admin_auth = await auth_header(c)
        users = await ensure_users(c, admin_auth, "alice")
        yield c, srv, users["alice"]


def _devices_ws(c: Any, auth: dict[str, str] | None = None) -> Any:
    query = f"?token={ws_token(auth)}" if auth else ""
    return ws_connect(c._octop_app, f"/api/devices/ws{query}")


def _spec(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "description": f"{name} on the phone",
        "input_schema": {"type": "object", "properties": {}},
        "requires_confirmation": False,
    }


async def _wait_until(predicate: Any) -> None:
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached in time")


async def test_devices_ws_ping_pong(env: Any) -> None:
    c, _srv, alice_auth = env
    async with _devices_ws(c, alice_auth) as ws:
        await ws.send_json({"type": "ping"})
        assert await ws.receive_json() == {"type": "pong"}


async def test_devices_ws_missing_token_rejected(env: Any) -> None:
    c, _srv, _alice_auth = env
    with pytest.raises(WebSocketDisconnect):
        async with _devices_ws(c):
            pass


async def test_devices_ws_register_call_round_trip(env: Any) -> None:
    c, srv, alice_auth = env
    user = srv.user_manager.get("alice")
    assert user is not None

    async with _devices_ws(c, alice_auth) as ws:
        await ws.send_json(
            {"type": "device_tools.register", "tools": [_spec("calendar.list_events")]}
        )
        await _wait_until(lambda: DEVICE_HUB.has_tools(user.id))

        task = asyncio.create_task(
            DEVICE_HUB.call(user.id, "calendar.list_events", {"days": 7}, thread_id="thr_1")
        )
        frame = await ws.receive_json()
        assert frame["type"] == "device_tool_call"
        assert frame["name"] == "calendar.list_events"
        assert frame["input"] == {"days": 7}
        assert frame["thread_id"] == "thr_1"

        await ws.send_json(
            {
                "type": "device_tool_result",
                "call_id": frame["call_id"],
                "output": [{"id": "e1", "title": "Standup"}],
            }
        )
        assert await task == {"ok": True, "output": [{"id": "e1", "title": "Standup"}]}

    await _wait_until(lambda: not DEVICE_HUB.has_tools(user.id))


async def test_devices_ws_error_result_and_bad_register(env: Any) -> None:
    c, srv, alice_auth = env
    user = srv.user_manager.get("alice")
    assert user is not None

    async with _devices_ws(c, alice_auth) as ws:
        await ws.send_json({"type": "device_tools.register", "tools": "not-a-list"})
        await ws.send_json({"type": "ping"})
        assert await ws.receive_json() == {"type": "pong"}
        assert not DEVICE_HUB.has_tools(user.id)

        await ws.send_json({"type": "device_tools.register", "tools": [_spec("sms.send")]})
        await _wait_until(lambda: DEVICE_HUB.has_tools(user.id))

        task = asyncio.create_task(DEVICE_HUB.call(user.id, "sms.send", {}))
        frame = await ws.receive_json()
        assert frame["type"] == "device_tool_call"
        await ws.send_json(
            {"type": "device_tool_result", "call_id": frame["call_id"], "error": "cancelled"}
        )
        assert await task == {"ok": False, "error": "cancelled"}

    await _wait_until(lambda: not DEVICE_HUB.has_tools(user.id))
