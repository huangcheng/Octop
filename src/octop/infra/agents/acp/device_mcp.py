"""Loopback MCP bridging an expert's tool calls to the user's phone.

``tools/list`` is the live set the phone advertised over its presence socket;
``tools/call`` routes through the device hub and waits for the phone's result
(writes/actions may park on the phone's inline approval card first).
"""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from typing import Any

from octop.infra.agents.acp.mcp_tools import arguments, err, ok, text_result
from octop.infra.devices.hub import DEVICE_HUB, DevicePresenceHub

_SCOPES: dict[str, DeviceScope] = {}
_TOKENS: dict[tuple[int, str, str], str] = {}


@dataclass(frozen=True)
class DeviceScope:
    user_id: int
    agent_id: str
    thread_id: str


def device_scope(token: str) -> DeviceScope | None:
    return _SCOPES.get(token)


def retain_device_scope(scope: DeviceScope) -> str:
    """Reuse one token per expert and conversation."""
    key = (scope.user_id, scope.agent_id, scope.thread_id)
    token = _TOKENS.get(key)
    if token is None:
        token = secrets.token_urlsafe(32)
        _TOKENS[key] = token
    _SCOPES[token] = scope
    return token


def _tool_card(tool: dict[str, Any]) -> dict[str, Any]:
    schema = tool.get("input_schema")
    return {
        "name": str(tool.get("name") or ""),
        "description": str(tool.get("description") or ""),
        "inputSchema": schema if isinstance(schema, dict) else {"type": "object", "properties": {}},
    }


async def handle_device_mcp(
    body: dict[str, Any],
    *,
    scope: DeviceScope,
    hub: DevicePresenceHub | None = None,
) -> dict[str, Any]:
    device_hub = hub if hub is not None else DEVICE_HUB
    method = str(body.get("method") or "")
    req_id = body.get("id")
    if method == "initialize":
        return ok(
            req_id,
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "octop-device", "version": "0.1.0"},
            },
        )
    if method == "notifications/initialized":
        return {}
    if method == "ping":
        return ok(req_id, {})
    tools = device_hub.tools_for_user(scope.user_id)
    if method == "tools/list":
        return ok(req_id, {"tools": [_tool_card(tool) for tool in tools]})
    if method != "tools/call":
        return err(req_id, -32601, f"Method not found: {method}")

    params = body.get("params") or {}
    name = str(params.get("name") or "") if isinstance(params, dict) else ""
    if not any(str(tool.get("name") or "") == name for tool in tools):
        return text_result(req_id, f"Unknown tool: {name}", failed=True)
    result = await device_hub.call(
        scope.user_id,
        name,
        arguments(params),
        thread_id=scope.thread_id or None,
    )
    if not result.get("ok"):
        return text_result(req_id, str(result.get("error") or "device call failed"), failed=True)
    output = result.get("output")
    text = (
        output if isinstance(output, str) else json.dumps(output, ensure_ascii=False, default=str)
    )
    return text_result(req_id, text)
