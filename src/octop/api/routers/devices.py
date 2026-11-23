"""Device presence WebSocket — the phone advertises its consented device tools.

One authenticated socket per phone. Client frames: ``device_tools.register``
(the consented tool specs) and ``device_tool_result`` (answers to calls the
server routes here). Server frames: ``device_tool_call`` and ``pong``.
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
from octop.infra.devices.hub import DEVICE_HUB
from octop.infra.errors import OctopError

logger = logging.getLogger(__name__)

router = APIRouter()


def _valid_tools(raw: Any) -> list[dict[str, Any]]:
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


@router.websocket("/devices/ws")
async def device_presence_ws(
    websocket: WebSocket,
    token: str | None = Query(default=None),
) -> None:
    """Hold the phone's presence socket and relay device-tool traffic."""
    server = websocket.app.state.octop_server
    if not token:
        await websocket.close(code=4001, reason="missing token")
        return

    try:
        user = resolve_user_from_token(server, token)
    except OctopError as exc:
        await websocket.close(code=4001, reason=f"auth: {exc.code.value}")
        return

    connection_id = uuid.uuid4().hex
    await websocket.accept()

    async def send_frame(frame: dict[str, Any]) -> None:
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        await websocket.send_text(json.dumps(frame, ensure_ascii=False))

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
                DEVICE_HUB.set_tools(connection_id, _valid_tools(payload.get("tools")))
                runtime = server.app_runtime
                if runtime is not None:
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
        logger.exception("device presence ws error user=%s", user.id)
    finally:
        DEVICE_HUB.unregister(connection_id)
        runtime = server.app_runtime
        if runtime is not None:
            runtime.agent_registry.refresh_device_tools(user.id)
        if websocket.application_state == WebSocketState.CONNECTED:
            with contextlib.suppress(Exception):
                await websocket.close()
