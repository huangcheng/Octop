"""In-process registry of the user's connected phones (device presence).

A phone advertises the device tools its user consented to over the session
socket (``/api/notifications/ws`` — the same connection that receives pushes,
so the app keeps one background socket). Experts reach those tools through the
loopback device MCP or the in-process bridge; this hub routes one call to the
phone that advertises the tool and awaits the ``device_tool_result`` frame.
Writes/actions may park on the phone's inline approval card, so the wait
budget is minutes, not seconds.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

SendFn = Callable[[dict[str, Any]], Awaitable[None]]

CALL_TIMEOUT_SECONDS = 300.0


def valid_tools(raw: Any) -> list[dict[str, Any]]:
    """Normalise a ``device_tools.register`` payload; malformed entries drop."""
    if not isinstance(raw, list):
        return []
    tools: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        schema = item.get("input_schema")
        tools.append(
            {
                "name": name,
                "description": str(item.get("description") or ""),
                "input_schema": schema
                if isinstance(schema, dict)
                else {"type": "object", "properties": {}},
                "requires_confirmation": bool(item.get("requires_confirmation")),
            }
        )
    return tools


@dataclass
class _Device:
    user_id: int
    send_fn: SendFn
    tools: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class _PendingCall:
    connection_id: str
    future: asyncio.Future[dict[str, Any]]


class DevicePresenceHub:
    """Maps connection ids to phones; routes tool calls and collects results."""

    def __init__(self) -> None:
        self._devices: dict[str, _Device] = {}
        self._pending: dict[str, _PendingCall] = {}

    def register(self, connection_id: str, user_id: int, send_fn: SendFn) -> None:
        self._devices[connection_id] = _Device(user_id=user_id, send_fn=send_fn)

    def unregister(self, connection_id: str) -> None:
        self._devices.pop(connection_id, None)
        for call_id, pending in list(self._pending.items()):
            if pending.connection_id != connection_id:
                continue
            self._pending.pop(call_id, None)
            if not pending.future.done():
                pending.future.set_result({"ok": False, "error": "device disconnected"})

    def set_tools(self, connection_id: str, tools: list[dict[str, Any]]) -> None:
        device = self._devices.get(connection_id)
        if device is not None:
            device.tools = tools

    def tools_for_user(self, user_id: int) -> list[dict[str, Any]]:
        """Consented tool specs across the user's phones; first device wins a name."""
        merged: dict[str, dict[str, Any]] = {}
        for device in self._devices.values():
            if device.user_id != user_id:
                continue
            for tool in device.tools:
                name = str(tool.get("name") or "")
                if name and name not in merged:
                    merged[name] = tool
        return list(merged.values())

    def has_tools(self, user_id: int) -> bool:
        return any(d.user_id == user_id and d.tools for d in self._devices.values())

    def connection_has_tools(self, connection_id: str) -> bool:
        """True when this particular connection is advertising tools."""
        device = self._devices.get(connection_id)
        return device is not None and bool(device.tools)

    async def call(
        self,
        user_id: int,
        name: str,
        tool_input: dict[str, Any],
        *,
        thread_id: str | None = None,
        timeout: float = CALL_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        """Send one ``device_tool_call`` and await the phone's result."""
        target = next(
            (
                cid
                for cid, d in self._devices.items()
                if d.user_id == user_id and any(t.get("name") == name for t in d.tools)
            ),
            None,
        )
        if target is None:
            return {"ok": False, "error": "no connected device advertises this tool"}
        device = self._devices[target]
        call_id = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[call_id] = _PendingCall(connection_id=target, future=future)
        frame: dict[str, Any] = {
            "type": "device_tool_call",
            "call_id": call_id,
            "name": name,
            "input": tool_input,
        }
        if thread_id:
            frame["thread_id"] = thread_id
        try:
            await device.send_fn(frame)
        except Exception:
            self._pending.pop(call_id, None)
            logger.exception("device hub: send failed for %s", target)
            return {"ok": False, "error": "failed to reach the device"}
        try:
            return await asyncio.wait_for(future, timeout)
        except TimeoutError:
            return {
                "ok": False,
                "error": "device did not answer in time; the approval card may have been missed",
            }
        finally:
            self._pending.pop(call_id, None)

    def resolve(self, call_id: str, *, output: Any = None, error: str | None = None) -> None:
        pending = self._pending.pop(call_id, None)
        if pending is None or pending.future.done():
            return
        if error:
            pending.future.set_result({"ok": False, "error": error})
        else:
            pending.future.set_result({"ok": True, "output": output})


DEVICE_HUB = DevicePresenceHub()

__all__ = [
    "CALL_TIMEOUT_SECONDS",
    "DEVICE_HUB",
    "DevicePresenceHub",
    "SendFn",
    "valid_tools",
]
