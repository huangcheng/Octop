"""Per-user registry for client-device tools (calendar / contacts / phone).

A device (mobile app first, desktop later) holds one *presence* WebSocket at
``/api/devices/ws`` and registers its tool specs there. The registry is keyed
by **user id**, not by connection: any turn by that user — web dashboard,
desktop, or mobile — gets the tools injected by
:class:`~octop.infra.gateway.device_tools_middleware.DeviceToolsMiddleware`,
and each call travels to the presence socket as a ``device_tool_call`` frame.
The device answers with ``device_tool_result``.

One active device session per user: the latest presence connection wins and
the replaced session's pending calls fail with an explanatory text.

Privacy: device-derived data is turn-scoped. Nothing in this module writes
tool specs or results into knowledge bases, memory, or any other durable
store — results only flow through the normal turn context.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

SendFrame = Callable[[dict[str, Any]], Awaitable[None]]

#: How long a device call may wait for the client before the model gets an
#: error text it can react to (the device may be locked / offline). Writes
#: need a human approval tap, which can legitimately take minutes.
DEVICE_TOOL_CALL_TIMEOUT_SECONDS = 300.0

_MAX_TOOLS_PER_SESSION = 64
_MAX_NAME_CHARS = 128
_MAX_DESCRIPTION_CHARS = 2000


@dataclass
class DeviceToolSession:
    """One user's active device presence connection and its in-flight calls."""

    connection_token: str
    send_frame: SendFrame
    device_id: str
    platform: str
    specs: list[dict[str, Any]]
    pending: dict[str, asyncio.Future[str]] = field(default_factory=dict)


def validate_device_tool_specs(tools: Any) -> list[dict[str, Any]]:
    """Normalize the client's ``tools`` list; raise ValueError on bad shape."""
    if not isinstance(tools, list):
        raise ValueError("device_tools.register requires a tools list")
    if len(tools) > _MAX_TOOLS_PER_SESSION:
        raise ValueError(f"too many device tools (max {_MAX_TOOLS_PER_SESSION})")
    specs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in tools:
        if not isinstance(item, Mapping):
            raise ValueError("each device tool must be an object")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("each device tool requires a non-empty name")
        name = name.strip()
        if len(name) > _MAX_NAME_CHARS:
            raise ValueError(f"device tool name too long (max {_MAX_NAME_CHARS})")
        if name in seen:
            raise ValueError(f"duplicate device tool name: {name!r}")
        seen.add(name)
        description = item.get("description")
        description = description.strip() if isinstance(description, str) else ""
        if len(description) > _MAX_DESCRIPTION_CHARS:
            description = description[:_MAX_DESCRIPTION_CHARS]
        input_schema = item.get("input_schema")
        if input_schema is None:
            input_schema = {}
        if not isinstance(input_schema, Mapping):
            raise ValueError(f"device tool {name!r}: input_schema must be an object")
        specs.append(
            {
                "name": name,
                "description": description,
                "input_schema": dict(input_schema),
                "requires_confirmation": bool(item.get("requires_confirmation", False)),
            }
        )
    return specs


def _result_text(output: Any) -> str:
    if isinstance(output, str):
        return output
    if output is None:
        return ""
    try:
        return json.dumps(output, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(output)


class DeviceToolRegistry:
    """Maps user ids to device presence sessions; routes calls and results.

    All methods run on the server event loop. ``call_tool`` never raises tool
    failures into the harness — it returns error text so the model can react
    (retry, apologize, or pick another path). Cancellation propagates: it is
    turn control flow, not a tool result.
    """

    def __init__(self) -> None:
        self._sessions: dict[int, DeviceToolSession] = {}

    def register(self, user_id: int, session: DeviceToolSession) -> None:
        """Register (or replace) the active device session for one user.

        Latest connection wins: the replaced session's pending calls fail
        with an explanatory text instead of hanging until timeout.
        """
        previous = self._sessions.get(user_id)
        if previous is not None and previous.connection_token != session.connection_token:
            self._fail_pending(
                previous, "Device tool call aborted: a newer device session took over."
            )
        self._sessions[user_id] = session

    def unregister(self, user_id: int, connection_token: str) -> None:
        """Drop the session only if it is still the one this connection owns.

        A replaced (older) socket disconnecting later must not unregister the
        newer session that already took over the user.
        """
        session = self._sessions.get(user_id)
        if session is None or session.connection_token != connection_token:
            return
        del self._sessions[user_id]
        self._fail_pending(session, "Device tool call aborted: the device disconnected.")

    def specs_for(self, user_id: int) -> list[dict[str, Any]]:
        session = self._sessions.get(user_id)
        return list(session.specs) if session is not None else []

    def all_specs(self) -> list[dict[str, Any]]:
        """Deduped tool specs across live sessions.

        Tool definitions are protocol constants, not user data; only routing
        and per-turn visibility are user-scoped.
        """
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for session in self._sessions.values():
            for spec in session.specs:
                name = str(spec.get("name") or "")
                if name and name not in seen:
                    seen.add(name)
                    out.append(spec)
        return out

    def resolve(self, call_id: str, *, output: Any = None, error: Any = None) -> bool:
        """Deliver a ``device_tool_result`` to the waiting call, if any."""
        for session in self._sessions.values():
            future = session.pending.get(call_id)
            if future is None or future.done():
                continue
            if error is not None and str(error).strip():
                future.set_result(f"Device tool error: {error}")
            else:
                future.set_result(_result_text(output))
            return True
        return False

    async def call_tool(
        self,
        user_id: int,
        name: str,
        tool_input: Mapping[str, Any],
        thread_id: str,
    ) -> str:
        """Ask the user's device to run one tool; return output or error text."""
        session = self._sessions.get(user_id)
        if session is None:
            return (
                "Device tools are unavailable: no device is currently connected for "
                "this user. Tell the user and continue without this tool."
            )
        if not any(spec["name"] == name for spec in session.specs):
            return f"Unknown device tool {name!r} on the connected device."

        call_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        session.pending[call_id] = future
        try:
            await session.send_frame(
                {
                    "type": "device_tool_call",
                    "call_id": call_id,
                    "name": name,
                    "input": dict(tool_input),
                    "thread_id": thread_id,
                }
            )
        except Exception:
            logger.exception("device_tool_call send failed user=%s", user_id)
            session.pending.pop(call_id, None)
            return f"Device tool {name!r} could not be delivered to the device."
        try:
            return await asyncio.wait_for(future, timeout=DEVICE_TOOL_CALL_TIMEOUT_SECONDS)
        except TimeoutError:
            return (
                f"Device tool {name!r} timed out after "
                f"{int(DEVICE_TOOL_CALL_TIMEOUT_SECONDS)}s waiting for the device."
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("device tool call failed user=%s", user_id)
            return f"Device tool {name!r} failed: {exc}"
        finally:
            session.pending.pop(call_id, None)

    @staticmethod
    def _fail_pending(session: DeviceToolSession, reason: str) -> None:
        for future in session.pending.values():
            if not future.done():
                future.set_result(reason)
        session.pending.clear()


#: Process-wide registry shared by the presence WS router and the harness
#: middleware (same pattern as ``octop.infra.metrics.METRICS``).
DEVICE_TOOLS = DeviceToolRegistry()

__all__ = [
    "DEVICE_TOOLS",
    "DEVICE_TOOL_CALL_TIMEOUT_SECONDS",
    "DeviceToolRegistry",
    "DeviceToolSession",
    "validate_device_tool_specs",
]
