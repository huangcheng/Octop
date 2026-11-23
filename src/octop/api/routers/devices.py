"""Device presence WebSocket — per-user channel for client-device tools.

A connected device (mobile app first, desktop later) registers its local tool
specs here; the expert harness then calls them from ANY of the user's clients
(web dashboard, desktop, mobile) via
:class:`~octop.infra.gateway.device_tools_middleware.DeviceToolsMiddleware`.
Execution stays on the device — the server only delegates.

Wire protocol (JSON text frames):

- client → server ``{"type": "device_tools.register", "device_id", "platform",
  "tools": [{name, description, input_schema, requires_confirmation}]}``
- server → client ``{"type": "device_tools.registered", "device_id",
  "platform", "tool_count"}``
- server → client ``{"type": "device_tool_call", "call_id", "name", "input",
  "thread_id"}``
- client → server ``{"type": "device_tool_result", "call_id", "output"?,
  "error"?}``
- ``ping`` / ``pong`` keepalive, same as the dashboard chat WS.

One active session per user: a newer presence connection replaces the older
one (its pending calls fail with an explanatory text). Disconnect unregisters
the session and fails anything still pending.

Privacy: nothing received here is persisted to knowledge bases or memory;
specs live in memory only and results flow through the normal turn context.
"""

from __future__ import annotations

import contextlib
import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from octop.api.deps import resolve_user_from_token
from octop.api.routers.chat.sse import json_chunk_default
from octop.infra.errors import OctopError
from octop.infra.gateway.device_tools import (
    DEVICE_TOOLS,
    DeviceToolSession,
    validate_device_tool_specs,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_MAX_LABEL_CHARS = 128


def _label(raw: Any) -> str:
    text = raw.strip() if isinstance(raw, str) else ""
    return text[:_MAX_LABEL_CHARS]


def _rebuild_device_graphs(server: Any) -> None:
    """Rebuild agent graphs so the ToolNode learns the device tool set.

    langchain builds the ToolNode once per agent graph from middleware.tools;
    a cached graph would keep executing (or rejecting) a stale tool set, so
    registration changes invalidate the graphs. Connect/disconnect is rare;
    the rebuild cost is paid then, not per turn.
    """
    runtime = getattr(server, "app_runtime", None)
    manager = getattr(runtime, "agent_registry", None)
    harness = getattr(manager, "_harness_manager", None)
    if harness is not None:
        harness.rebuild_all_agents()


@router.websocket("/devices/ws")
async def device_presence_ws(
    websocket: WebSocket,
    token: str | None = Query(default=None),
) -> None:
    """Per-user device presence channel (auth via ``?token=``, like chat WS)."""
    server = websocket.app.state.octop_server
    if not token:
        await websocket.close(code=4001, reason="missing token")
        return

    try:
        user = resolve_user_from_token(server, token)
    except OctopError as exc:
        await websocket.close(code=4001, reason=f"auth: {exc.code.value}")
        return

    connection_token = uuid.uuid4().hex
    await websocket.accept()

    async def send_frame(frame: dict[str, Any]) -> None:
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        await websocket.send_text(
            json.dumps(frame, ensure_ascii=False, default=json_chunk_default),
        )

    try:
        while websocket.application_state == WebSocketState.CONNECTED:
            raw = await websocket.receive_text()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                await send_frame({"type": "error", "message": "invalid JSON frame"})
                continue
            if not isinstance(payload, dict):
                continue

            msg_type = str(payload.get("type") or "")
            if msg_type == "ping":
                await send_frame({"type": "pong"})
                continue

            if msg_type == "device_tools.register":
                try:
                    specs = validate_device_tool_specs(payload.get("tools"))
                except ValueError as exc:
                    await send_frame({"type": "error", "message": str(exc)})
                    continue
                device_id = _label(payload.get("device_id"))
                platform = _label(payload.get("platform"))
                DEVICE_TOOLS.register(
                    user.id,
                    DeviceToolSession(
                        connection_token=connection_token,
                        send_frame=send_frame,
                        device_id=device_id,
                        platform=platform,
                        specs=specs,
                    ),
                )
                _rebuild_device_graphs(server)
                await send_frame(
                    {
                        "type": "device_tools.registered",
                        "device_id": device_id,
                        "platform": platform,
                        "tool_count": len(specs),
                    }
                )
                continue

            if msg_type == "device_tool_result":
                call_id = str(payload.get("call_id") or "").strip()
                if not call_id:
                    await send_frame(
                        {"type": "error", "message": "device_tool_result requires call_id"}
                    )
                    continue
                resolved = DEVICE_TOOLS.resolve(
                    call_id,
                    output=payload.get("output"),
                    error=payload.get("error"),
                )
                if not resolved:
                    await send_frame(
                        {
                            "type": "error",
                            "message": f"no pending device tool call for call_id {call_id!r}",
                        }
                    )
                continue

            await send_frame({"type": "error", "message": f"unknown message type: {msg_type}"})

    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("device presence ws error user=%s", user.id)
        if websocket.application_state == WebSocketState.CONNECTED:
            with contextlib.suppress(Exception):
                await send_frame({"type": "error", "message": "internal error"})
    finally:
        DEVICE_TOOLS.unregister(user.id, connection_token)
        _rebuild_device_graphs(server)
        if websocket.application_state == WebSocketState.CONNECTED:
            with contextlib.suppress(Exception):
                await websocket.close()
