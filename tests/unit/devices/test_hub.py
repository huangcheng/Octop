"""Device presence hub — routing calls to the phone and collecting results."""

from __future__ import annotations

import asyncio
from typing import Any

from octop.infra.devices.hub import DevicePresenceHub


def _spec(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "description": "",
        "input_schema": {"type": "object", "properties": {}},
        "requires_confirmation": False,
    }


async def test_call_round_trip() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("calendar.list_events")])
    assert hub.has_tools(7)
    assert not hub.has_tools(8)

    task = asyncio.create_task(hub.call(7, "calendar.list_events", {"days": 7}, thread_id="thr_1"))
    await asyncio.sleep(0.01)
    assert len(sent) == 1
    frame = sent[0]
    assert frame["type"] == "device_tool_call"
    assert frame["name"] == "calendar.list_events"
    assert frame["input"] == {"days": 7}
    assert frame["thread_id"] == "thr_1"

    hub.resolve(str(frame["call_id"]), output=[{"id": "e1"}])
    assert await task == {"ok": True, "output": [{"id": "e1"}]}


async def test_call_error_result() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("sms.send")])
    task = asyncio.create_task(hub.call(7, "sms.send", {}))
    await asyncio.sleep(0.01)
    hub.resolve(str(sent[0]["call_id"]), error="cancelled")
    assert await task == {"ok": False, "error": "cancelled"}


async def test_call_without_device_or_tool() -> None:
    hub = DevicePresenceHub()
    assert (await hub.call(7, "sms.send", {}))["ok"] is False

    async def send(frame: dict[str, Any]) -> None:
        raise AssertionError("must not send")

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("calendar.list_events")])
    result = await hub.call(7, "sms.send", {})
    assert result["ok"] is False
    assert "no connected device" in str(result["error"])


async def test_unregister_fails_pending_calls() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("phone.call")])
    task = asyncio.create_task(hub.call(7, "phone.call", {}))
    await asyncio.sleep(0.01)
    hub.unregister("c1")
    result = await task
    assert result["ok"] is False
    assert "disconnected" in str(result["error"])
    assert not hub.has_tools(7)


async def test_call_timeout() -> None:
    hub = DevicePresenceHub()

    async def send(frame: dict[str, Any]) -> None:
        return None

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("calendar.create_event")])
    result = await hub.call(7, "calendar.create_event", {}, timeout=0.01)
    assert result["ok"] is False
    assert "did not answer" in str(result["error"])


async def test_send_failure_is_an_error_result() -> None:
    hub = DevicePresenceHub()

    async def send(frame: dict[str, Any]) -> None:
        raise RuntimeError("socket closed")

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("contacts.search")])
    result = await hub.call(7, "contacts.search", {})
    assert result["ok"] is False
    assert "failed to reach" in str(result["error"])


async def test_tools_merge_first_device_wins() -> None:
    hub = DevicePresenceHub()

    async def send(frame: dict[str, Any]) -> None:
        return None

    first = _spec("calendar.list_events")
    first["description"] = "phone one"
    hub.register("c1", 7, send)
    hub.set_tools("c1", [first])
    hub.register("c2", 7, send)
    hub.set_tools("c2", [_spec("calendar.list_events"), _spec("sms.read")])
    hub.register("c3", 8, send)
    hub.set_tools("c3", [_spec("mail.compose")])

    tools = hub.tools_for_user(7)
    assert {str(t["name"]) for t in tools} == {"calendar.list_events", "sms.read"}
    listed = next(t for t in tools if t["name"] == "calendar.list_events")
    assert listed["description"] == "phone one"
    assert {str(t["name"]) for t in hub.tools_for_user(8)} == {"mail.compose"}
