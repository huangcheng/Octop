"""Per-turn injection of the user's client-device tools into the expert.

Mirrors :class:`~octop.infra.knowledge.hint.KnowledgeSearchHintMiddleware`:
the turn's ``configurable["user"]`` (the same key the knowledge tools read)
selects the per-user presence session in
:data:`~octop.infra.gateway.device_tools.DEVICE_TOOLS`, and each registered
spec becomes one ``StructuredTool`` whose coroutine round-trips the call to
the device's presence socket. No session / no specs → the request is untouched.

langchain executes tools from the ToolNode, which is built from
``create_agent(tools=...)`` plus ``middleware.tools`` — overriding
``request.tools`` alone only changes what the model SEES and the runtime then
rejects the call as an unknown tool. So the executable tools are declared on
``middleware.tools`` (resolved per agent build from the live sessions), while
``wrap_model_call`` gates visibility per turn: a user without a connected
device never sees the tools at all.

Privacy: device tool results only flow through the normal turn context; this
middleware never persists them into knowledge bases or memory.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.tools import StructuredTool
from langchain_core.tools.base import BaseTool
from langgraph.config import get_config
from pydantic import BaseModel, Field, create_model

from octop.infra.agents.plugins.plugin_tool_names import sanitize_plugin_tool_name
from octop.infra.gateway.device_tools import DEVICE_TOOLS, DeviceToolRegistry
from octop.infra.utils.browser_media import parse_octop_user_id

# Keep device schemas simple: anything else degrades to an untyped value.
_JSON_TYPE_MAP: dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def _args_schema(tool_name: str, schema: Mapping[str, Any]) -> type[BaseModel]:
    """Build a pydantic args model from a JSON schema (tolerant of gaps)."""
    properties = schema.get("properties")
    required_raw = schema.get("required")
    required = {str(item) for item in required_raw} if isinstance(required_raw, list) else set()
    fields: dict[str, Any] = {}
    if isinstance(properties, Mapping):
        for field_name, raw in properties.items():
            if not isinstance(field_name, str) or not field_name.isidentifier():
                continue
            prop = raw if isinstance(raw, Mapping) else {}
            py_type = _JSON_TYPE_MAP.get(str(prop.get("type") or "").lower(), Any)
            description = str(prop.get("description") or "")
            if field_name in required:
                fields[field_name] = (py_type, Field(description=description))
            else:
                fields[field_name] = (
                    py_type | None,
                    Field(default=None, description=description),
                )
    model_name = re.sub(r"\W|^(?=\d)", "_", tool_name) + "_Args"
    try:
        return create_model(model_name, **fields)
    except Exception:
        # A hostile / exotic schema must not break the turn: fall back to a
        # no-argument model and let the device validate the raw input.
        return create_model(model_name)


def _build_device_tool(
    registry: DeviceToolRegistry,
    spec: Mapping[str, Any],
    used_names: set[str],
) -> StructuredTool | None:
    original_name = str(spec.get("name") or "").strip()
    if not original_name:
        return None
    # Strict LLM tool-name APIs reject e.g. dots / CJK; the wire call keeps
    # the original registered name (same approach as plugin tool names).
    exposed_name = sanitize_plugin_tool_name(original_name, used=used_names)
    description = str(spec.get("description") or "").strip()
    if exposed_name != original_name:
        description = f"[device tool: {original_name}] {description}".strip()
    if not description:
        description = f"Run the {original_name} tool on the user's device."
    schema = spec.get("input_schema")
    args_schema = _args_schema(exposed_name, schema if isinstance(schema, Mapping) else {})

    async def _run_device_tool(**kwargs: Any) -> str:
        # Resolve the calling user at execution time: one executable tool
        # instance serves every user's session (ToolNode is built per agent,
        # visibility is gated per turn in wrap_model_call).
        cfg = get_config().get("configurable") or {}
        user_id = parse_octop_user_id(cfg.get("user"))
        if user_id is None:
            return "Device tool error: no user context on this turn."
        thread_id = str(cfg.get("thread_id") or "")
        return await registry.call_tool(user_id, original_name, kwargs, thread_id)

    return StructuredTool.from_function(
        coroutine=_run_device_tool,
        name=exposed_name,
        description=description,
        args_schema=args_schema,
    )


def build_executable_device_tools(registry: DeviceToolRegistry) -> list[StructuredTool]:
    """One executable tool per distinct registered name, across live sessions."""
    tools: list[StructuredTool] = []
    used: set[str] = set()
    seen: set[str] = set()
    for spec in registry.all_specs():
        name = str(spec.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        tool = _build_device_tool(registry, spec, used)
        if tool is not None:
            used.add(tool.name)
            tools.append(tool)
    return tools


class DeviceToolsMiddleware(AgentMiddleware[Any, Any]):
    """Append this turn's registered device tools to the model request."""

    def __init__(self, registry: DeviceToolRegistry | None = None) -> None:
        super().__init__()
        self._registry = registry if registry is not None else DEVICE_TOOLS
        # langchain builds the ToolNode from middleware.tools; without this
        # the model sees the tools but execution rejects them as unknown.
        # Agent graphs are rebuilt whenever a device session registers or
        # disconnects, so capturing the live set here stays fresh.
        self.tools = build_executable_device_tools(self._registry)

    def _with_device_tools(self, request: ModelRequest[Any]) -> ModelRequest[Any]:
        cfg = get_config().get("configurable") or {}
        user_id = parse_octop_user_id(cfg.get("user"))
        if user_id is None:
            return request
        specs = self._registry.specs_for(user_id)
        if not specs:
            return request
        tools_in = list(request.tools or [])
        present = {str(tool.name) for tool in tools_in if isinstance(tool, BaseTool)}
        added: list[StructuredTool] = []
        used = set(present)
        for spec in specs:
            tool = _build_device_tool(self._registry, spec, used)
            if tool is not None and tool.name not in present:
                used.add(tool.name)
                added.append(tool)
        if not added:
            return request
        return request.override(tools=[*tools_in, *added])

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], ModelResponse[Any]],
    ) -> ModelResponse[Any]:
        return handler(self._with_device_tools(request))

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[ModelResponse[Any]]],
    ) -> ModelResponse[Any]:
        return await handler(self._with_device_tools(request))


__all__ = ["DeviceToolsMiddleware", "build_executable_device_tools"]
