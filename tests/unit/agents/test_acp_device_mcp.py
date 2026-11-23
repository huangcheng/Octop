"""Loopback device MCP — live tool list from the phone, calls bridged to it."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from octop.infra.agents.acp.device_mcp import (
    DeviceScope,
    device_scope,
    handle_device_mcp,
    retain_device_scope,
)
from octop.infra.agents.acp.turn import attach_managed_turn
from octop.infra.devices.hub import DEVICE_HUB, DevicePresenceHub


def _spec(name: str, *, confirmation: bool = False) -> dict[str, Any]:
    return {
        "name": name,
        "description": f"{name} on the phone",
        "input_schema": {"type": "object", "properties": {}},
        "requires_confirmation": confirmation,
    }


def test_device_token_is_stable_per_expert_and_thread() -> None:
    scope = DeviceScope(user_id=7, agent_id="GWN", thread_id="thr_1")
    assert retain_device_scope(scope) == retain_device_scope(scope)
    other = DeviceScope(user_id=7, agent_id="GWN", thread_id="thr_2")
    assert retain_device_scope(other) != retain_device_scope(scope)
    token = retain_device_scope(scope)
    resolved = device_scope(token)
    assert resolved is not None
    assert resolved.thread_id == "thr_1"
    assert device_scope("bogus") is None


async def test_initialize_and_tools_list_follow_the_phone() -> None:
    hub = DevicePresenceHub()

    async def send(frame: dict[str, Any]) -> None:
        return None

    hub.register("c1", 7, send)
    scope = DeviceScope(user_id=7, agent_id="GWN", thread_id="thr_1")

    init = await handle_device_mcp(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize"}, scope=scope, hub=hub
    )
    assert init["result"]["serverInfo"]["name"] == "octop-device"

    empty = await handle_device_mcp(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, scope=scope, hub=hub
    )
    assert empty["result"]["tools"] == []

    hub.set_tools("c1", [_spec("calendar.list_events"), _spec("sms.send", confirmation=True)])
    listed = await handle_device_mcp(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}, scope=scope, hub=hub
    )
    cards = listed["result"]["tools"]
    assert {card["name"] for card in cards} == {"calendar.list_events", "sms.send"}
    assert all(card["inputSchema"]["type"] == "object" for card in cards)


async def test_tools_call_bridges_to_the_phone() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("calendar.list_events")])
    scope = DeviceScope(user_id=7, agent_id="GWN", thread_id="thr_9")

    body = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {"name": "calendar.list_events", "arguments": {"days": 3}},
    }
    task = asyncio.create_task(handle_device_mcp(body, scope=scope, hub=hub))
    await asyncio.sleep(0.01)
    assert sent[0]["name"] == "calendar.list_events"
    assert sent[0]["input"] == {"days": 3}
    assert sent[0]["thread_id"] == "thr_9"
    hub.resolve(str(sent[0]["call_id"]), output=[{"id": "e1", "title": "Standup"}])
    resp = await task
    assert resp["result"]["isError"] is False
    assert "Standup" in resp["result"]["content"][0]["text"]


async def test_tools_call_cancelled_is_a_failed_result() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("sms.send", confirmation=True)])
    scope = DeviceScope(user_id=7, agent_id="GWN", thread_id="")

    body = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {"name": "sms.send", "arguments": {"number": "10086", "body": "hi"}},
    }
    task = asyncio.create_task(handle_device_mcp(body, scope=scope, hub=hub))
    await asyncio.sleep(0.01)
    assert "thread_id" not in sent[0]
    hub.resolve(str(sent[0]["call_id"]), error="cancelled")
    resp = await task
    assert resp["result"]["isError"] is True
    assert resp["result"]["content"][0]["text"] == "cancelled"


async def test_tools_call_unknown_tool() -> None:
    hub = DevicePresenceHub()
    scope = DeviceScope(user_id=7, agent_id="GWN", thread_id="")
    body = {
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {"name": "sms.send", "arguments": {}},
    }
    resp = await handle_device_mcp(body, scope=scope, hub=hub)
    assert resp["result"]["isError"] is True
    assert "Unknown tool" in resp["result"]["content"][0]["text"]


class _Manager:
    _config = SimpleNamespace(
        bind_host="127.0.0.1",
        port=8000,
        tls=SimpleNamespace(enabled=False, cert_file="", key_file=""),
    )

    def get_row(self, agent_id: str) -> SimpleNamespace:
        return SimpleNamespace(kind="managed")


async def test_managed_turn_attaches_device_mcp_when_a_phone_is_online() -> None:
    async def send(frame: dict[str, Any]) -> None:
        return None

    DEVICE_HUB.register("test-phone", 7, send)
    DEVICE_HUB.set_tools("test-phone", [_spec("calendar.list_events")])
    try:
        request: dict[str, object] = {
            "user": "7",
            "configurable": {"locale": "en", "thread_id": "thr_1"},
        }
        await attach_managed_turn(_Manager(), "GWN", request)
        configurable = request["configurable"]
        assert isinstance(configurable, dict)
        specs = configurable["acp_mcp_servers"]
        assert isinstance(specs, dict)
        assert "octop-device" in specs
        url = specs["octop-device"]["url"]
        assert "/api/internal/mcp/device?token=" in url
        scope = device_scope(url.split("token=", 1)[1])
        assert scope is not None
        assert scope.user_id == 7
        assert scope.thread_id == "thr_1"
        note = configurable["acp_prompt_note"]
        assert isinstance(note, str)
        assert "approval card" in note
    finally:
        DEVICE_HUB.unregister("test-phone")


async def test_managed_turn_without_a_phone_has_no_device_mcp() -> None:
    request: dict[str, object] = {"user": "7", "configurable": {"locale": "en"}}
    await attach_managed_turn(_Manager(), "GWN", request)
    configurable = request["configurable"]
    assert isinstance(configurable, dict)
    specs = configurable.get("acp_mcp_servers")
    assert isinstance(specs, dict)
    assert "octop-device" not in specs
