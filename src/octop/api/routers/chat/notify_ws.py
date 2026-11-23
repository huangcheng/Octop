"""Session WebSocket — the app's one always-on connection while signed in.

Carries everything that must outlive a screen: server pushes (cron reminders,
proactive care, reply previews) and the phone's device presence (consented
tool registration, routed tool calls and their results). Frames are
multiplexed by ``type``; the per-thread chat stream keeps its own socket
because it lives and dies with the thread screen.
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
from octop.infra.devices.hub import DEVICE_HUB, valid_tools
from octop.infra.errors import OctopError

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/notifications/ws")
async def session_ws(
    websocket: WebSocket,
    token: str | None = Query(default=None),
) -> None:
    """Push ``dashboard_push`` frames to the user's clients and relay device-tool traffic."""
    server = websocket.app.state.octop_server
    if not token:
        await websocket.close(code=4001, reason="missing token")
        return

    try:
        user = resolve_user_from_token(server, token)
    except OctopError as exc:
        await websocket.close(code=4001, reason=f"auth: {exc.code.value}")
        return

    if server.app_runtime is None:
        await websocket.close(code=1011, reason="server not ready")
        return
    runtime = server.app_runtime
    hub = runtime.gateway.ws_hub

    connection_id = uuid.uuid4().hex
    await websocket.accept()

    async def send_frame(frame: dict[str, Any]) -> None:
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        await websocket.send_text(
            json.dumps(frame, ensure_ascii=False, default=json_chunk_default),
        )

    # One connection, two roles: a push target for the user, and — once it
    # registers tools — a device node experts can delegate to.
    hub.register(connection_id, send_frame, user_id=user.id)
    DEVICE_HUB.register(connection_id, user.id, send_frame)

    try:
        while websocket.application_state == WebSocketState.CONNECTED:
            raw = await websocket.receive_text()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            kind = str(payload.get("type") or "")
            if kind == "ping":
                await send_frame({"type": "pong"})
            elif kind == "device_tools.register":
                raw_tools = payload.get("tools")
                if isinstance(raw_tools, list):
                    # Malformed payloads are ignored rather than clearing the
                    # phone's advertised set.
                    DEVICE_HUB.set_tools(connection_id, valid_tools(raw_tools))
                    runtime.agent_registry.refresh_device_tools(user.id)
            elif kind == "device_tool_result":
                call_id = str(payload.get("call_id") or "")
                if call_id:
                    error = payload.get("error")
                    DEVICE_HUB.resolve(
                        call_id,
                        output=payload.get("output"),
                        error=str(error) if error else None,
                    )
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("session ws error user=%s", user.id)
    finally:
        hub.unregister(connection_id)
        # Dashboard clients without tools close without touching any agent.
        had_tools = DEVICE_HUB.connection_has_tools(connection_id)
        DEVICE_HUB.unregister(connection_id)
        if had_tools:
            runtime.agent_registry.refresh_device_tools(user.id)
        if websocket.application_state == WebSocketState.CONNECTED:
            with contextlib.suppress(Exception):
                await websocket.close()
