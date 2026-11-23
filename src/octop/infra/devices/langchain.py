"""In-process LangChain tools bridging native experts to the user's phone.

Managed ACP runners reach the phone through the loopback device MCP; native
harness experts get the same bridge as injected tools. Names are sanitized
for strict LLM tool-name APIs (``device_calendar_list_events``); routing back
to the phone keeps the original dotted tool name.
"""

from __future__ import annotations

import json
from typing import Any

from octop_harness.mcp import mcp_args_model

from octop.infra.agents.plugins.plugin_tool_names import sanitize_plugin_tool_name
from octop.infra.devices.hub import DEVICE_HUB, DevicePresenceHub

DEVICE_TOOL_PREFIX = "device_"


def _thread_id_from_config() -> str | None:
    from langchain_core.runnables.config import var_child_runnable_config

    cfg = var_child_runnable_config.get() or {}
    configurable = cfg.get("configurable") or {}
    tid = configurable.get("thread_id")
    text = str(tid).strip() if tid else ""
    return text or None


def build_device_langchain_tools(
    user_id: int,
    hub: DevicePresenceHub | None = None,
) -> list[Any]:
    """One StructuredTool per consented tool the user's phone advertises."""
    from langchain_core.tools import StructuredTool

    device_hub = hub if hub is not None else DEVICE_HUB
    out: list[Any] = []
    used: set[str] = set()
    for spec in device_hub.tools_for_user(user_id):
        name = str(spec.get("name") or "").strip()
        if not name:
            continue
        schema = spec.get("input_schema")
        if not isinstance(schema, dict):
            schema = {"type": "object", "properties": {}}
        lc_name = sanitize_plugin_tool_name(f"{DEVICE_TOOL_PREFIX}{name}", used=used)

        def _coroutine(tool_name: str) -> Any:
            async def _run(**kwargs: Any) -> str:
                cleaned = {k: v for k, v in kwargs.items() if v is not None}
                result = await device_hub.call(
                    user_id,
                    tool_name,
                    cleaned,
                    thread_id=_thread_id_from_config(),
                )
                if not result.get("ok"):
                    return str(result.get("error") or "device call failed")
                output = result.get("output")
                if isinstance(output, str):
                    return output
                return json.dumps(output, ensure_ascii=False, default=str)

            return _run

        out.append(
            StructuredTool.from_function(
                coroutine=_coroutine(name),
                name=lc_name,
                description=str(spec.get("description") or name),
                args_schema=mcp_args_model(lc_name, schema),
            )
        )
    return out
