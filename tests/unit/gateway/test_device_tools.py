"""Unit tests for the per-user device-tools presence registry + middleware."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest
from langchain.agents.middleware import ModelRequest
from langchain_core.tools import StructuredTool
from langgraph.config import var_child_runnable_config

from octop.infra.gateway.device_tools import (
    DeviceToolRegistry,
    DeviceToolSession,
    validate_device_tool_specs,
)
from octop.infra.gateway.device_tools_middleware import DeviceToolsMiddleware


@contextmanager
def _configurable(**kwargs: object):
    token = var_child_runnable_config.set({"configurable": kwargs})
    try:
        yield
    finally:
        var_child_runnable_config.reset(token)


def _echo_spec(name: str = "device.echo") -> dict[str, Any]:
    return {
        "name": name,
        "description": "Echo text back from the device.",
        "input_schema": {
            "type": "object",
            "properties": {"text": {"type": "string", "description": "Text to echo"}},
            "required": ["text"],
        },
        "requires_confirmation": False,
    }


def _session(
    token: str,
    sent: list[dict[str, Any]],
    specs: list[dict[str, Any]] | None = None,
) -> DeviceToolSession:
    async def send_frame(frame: dict[str, Any]) -> None:
        sent.append(frame)

    return DeviceToolSession(
        connection_token=token,
        send_frame=send_frame,
        device_id="phone-1",
        platform="android",
        specs=specs if specs is not None else validate_device_tool_specs([_echo_spec()]),
    )


def test_validate_specs_rejects_bad_shapes() -> None:
    with pytest.raises(ValueError):
        validate_device_tool_specs("not a list")
    with pytest.raises(ValueError):
        validate_device_tool_specs([{"description": "no name"}])
    with pytest.raises(ValueError):
        validate_device_tool_specs([_echo_spec(), _echo_spec()])
    with pytest.raises(ValueError):
        validate_device_tool_specs([{**_echo_spec(), "input_schema": "nope"}])


def test_validate_specs_normalizes_optional_fields() -> None:
    specs = validate_device_tool_specs([{"name": " device.ping "}])
    assert specs == [
        {
            "name": "device.ping",
            "description": "",
            "input_schema": {},
            "requires_confirmation": False,
        }
    ]


async def test_call_tool_round_trip() -> None:
    registry = DeviceToolRegistry()
    sent: list[dict[str, Any]] = []
    registry.register(7, _session("tok-a", sent))

    async def device_answers() -> None:
        while not sent:
            await asyncio.sleep(0)
        frame = sent[0]
        assert frame["type"] == "device_tool_call"
        assert frame["name"] == "device.echo"
        assert frame["input"] == {"text": "hi"}
        assert frame["thread_id"] == "thread-1"
        assert registry.resolve(frame["call_id"], output="echo:hi")

    answer_task = asyncio.create_task(device_answers())
    result = await registry.call_tool(7, "device.echo", {"text": "hi"}, "thread-1")
    await answer_task
    assert result == "echo:hi"
    assert registry.specs_for(7) == validate_device_tool_specs([_echo_spec()])


async def test_call_tool_error_result_is_text_not_raise() -> None:
    registry = DeviceToolRegistry()
    sent: list[dict[str, Any]] = []
    registry.register(7, _session("tok-a", sent))

    async def device_fails() -> None:
        while not sent:
            await asyncio.sleep(0)
        registry.resolve(sent[0]["call_id"], error="permission denied")

    answer_task = asyncio.create_task(device_fails())
    result = await registry.call_tool(7, "device.echo", {"text": "hi"}, "t")
    await answer_task
    assert result == "Device tool error: permission denied"


async def test_call_tool_without_session_returns_error_text() -> None:
    registry = DeviceToolRegistry()
    result = await registry.call_tool(7, "device.echo", {}, "t")
    assert "unavailable" in result


async def test_call_tool_unknown_name_returns_error_text() -> None:
    registry = DeviceToolRegistry()
    registry.register(7, _session("tok-a", []))
    result = await registry.call_tool(7, "device.nope", {}, "t")
    assert "Unknown device tool" in result


async def test_call_tool_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    import octop.infra.gateway.device_tools as mod

    monkeypatch.setattr(mod, "DEVICE_TOOL_CALL_TIMEOUT_SECONDS", 0.05)
    registry = DeviceToolRegistry()
    sent: list[dict[str, Any]] = []
    registry.register(7, _session("tok-a", sent))
    result = await registry.call_tool(7, "device.echo", {"text": "hi"}, "t")
    assert "timed out" in result
    assert sent  # the call frame was delivered; only the answer never came


async def test_latest_session_wins_and_fails_replaced_pending() -> None:
    registry = DeviceToolRegistry()
    sent_old: list[dict[str, Any]] = []
    sent_new: list[dict[str, Any]] = []
    registry.register(7, _session("tok-old", sent_old))

    async def never_answers() -> None:
        while not sent_old:
            await asyncio.sleep(0)

    wait_task = asyncio.create_task(never_answers())
    call = asyncio.create_task(registry.call_tool(7, "device.echo", {"text": "hi"}, "t"))
    await wait_task
    registry.register(7, _session("tok-new", sent_new))
    result = await call
    assert "newer device session" in result

    # The old socket disconnecting later must not drop the new session.
    registry.unregister(7, "tok-old")
    assert registry.specs_for(7)
    registry.unregister(7, "tok-new")
    assert registry.specs_for(7) == []


async def test_unregister_fails_pending() -> None:
    registry = DeviceToolRegistry()
    sent: list[dict[str, Any]] = []
    registry.register(7, _session("tok-a", sent))

    async def wait_for_frame() -> None:
        while not sent:
            await asyncio.sleep(0)

    wait_task = asyncio.create_task(wait_for_frame())
    call = asyncio.create_task(registry.call_tool(7, "device.echo", {"text": "hi"}, "t"))
    await wait_task
    registry.unregister(7, "tok-a")
    result = await call
    assert "disconnected" in result


def _request_with_tools() -> ModelRequest:
    base = StructuredTool.from_function(func=lambda: "x", name="base_tool", description="base")
    return ModelRequest(model=MagicMock(), messages=[], tools=[base])


def test_middleware_injects_tools_for_registered_user() -> None:
    registry = DeviceToolRegistry()
    sent: list[dict[str, Any]] = []
    registry.register(7, _session("tok-a", sent))
    captured: list[ModelRequest] = []

    def handler(req: ModelRequest) -> object:
        captured.append(req)
        return object()

    # Dashboard turns stamp the user id as a string (ChatRequest.user).
    with _configurable(user="7", thread_id="thread-1"):
        DeviceToolsMiddleware(registry).wrap_model_call(_request_with_tools(), handler)

    names = [t.name for t in captured[0].tools or []]
    assert names == ["base_tool", "device_echo"]
    device_tool = captured[0].tools[1]
    assert "device.echo" in device_tool.description
    assert set(device_tool.args_schema.model_fields) == {"text"}


def test_middleware_noop_without_session_or_user() -> None:
    registry = DeviceToolRegistry()
    captured: list[ModelRequest] = []

    def handler(req: ModelRequest) -> object:
        captured.append(req)
        return object()

    request = _request_with_tools()
    with _configurable(user="7"):
        DeviceToolsMiddleware(registry).wrap_model_call(request, handler)
    assert captured[0] is request

    registry.register(7, _session("tok-a", []))
    with _configurable():
        DeviceToolsMiddleware(registry).wrap_model_call(request, handler)
    assert captured[1] is request


async def test_injected_tool_routes_call_to_presence_session() -> None:
    registry = DeviceToolRegistry()
    sent: list[dict[str, Any]] = []
    registry.register(7, _session("tok-a", sent))
    captured: list[ModelRequest] = []

    def handler(req: ModelRequest) -> object:
        captured.append(req)
        return object()

    with _configurable(user="7", thread_id="thread-9"):
        DeviceToolsMiddleware(registry).wrap_model_call(_request_with_tools(), handler)
    device_tool = captured[0].tools[1]

    async def device_answers() -> None:
        while not sent:
            await asyncio.sleep(0)
        frame = sent[0]
        assert frame["thread_id"] == "thread-9"
        registry.resolve(frame["call_id"], output="echo:hi")

    answer_task = asyncio.create_task(device_answers())
    with _configurable(user="7", thread_id="thread-9"):
        result = await device_tool.ainvoke({"text": "hi"})
    await answer_task
    assert result == "echo:hi"
