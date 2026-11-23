"""Native-expert device tools — sanitized names and the hub bridge."""

from __future__ import annotations

import asyncio
from typing import Any

from octop.infra.devices.hub import DevicePresenceHub
from octop.infra.devices.langchain import build_device_langchain_tools


def _spec(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "description": f"{name} on the phone",
        "input_schema": {
            "type": "object",
            "properties": {"days": {"type": "number"}},
        },
        "requires_confirmation": False,
    }


async def test_names_are_sanitized_for_llm_apis() -> None:
    hub = DevicePresenceHub()

    async def send(frame: dict[str, Any]) -> None:
        return None

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("calendar.list_events"), _spec("sms.send")])
    tools = build_device_langchain_tools(7, hub=hub)
    assert [tool.name for tool in tools] == ["device_calendar_list_events", "device_sms_send"]
    assert all(tool.args_schema is not None for tool in tools)
    assert build_device_langchain_tools(8, hub=hub) == []


async def test_tool_call_bridges_to_the_phone() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("calendar.list_events")])
    tool = build_device_langchain_tools(7, hub=hub)[0]

    task = asyncio.create_task(tool.ainvoke({"days": 3}))
    await asyncio.sleep(0.01)
    assert sent[0]["name"] == "calendar.list_events"
    assert sent[0]["input"] == {"days": 3}
    hub.resolve(str(sent[0]["call_id"]), output=[{"id": "e1", "title": "Standup"}])
    text = await task
    assert "Standup" in str(text)


async def test_tool_error_becomes_text() -> None:
    hub = DevicePresenceHub()
    sent: list[dict[str, Any]] = []

    async def send(frame: dict[str, Any]) -> None:
        sent.append(frame)

    hub.register("c1", 7, send)
    hub.set_tools("c1", [_spec("sms.send")])
    tool = build_device_langchain_tools(7, hub=hub)[0]

    task = asyncio.create_task(tool.ainvoke({}))
    await asyncio.sleep(0.01)
    hub.resolve(str(sent[0]["call_id"]), error="cancelled")
    assert await task == "cancelled"
