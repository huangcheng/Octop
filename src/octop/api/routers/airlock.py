"""Airlock integration: HTTP task dispatch (feature/airlock-integration).

Airlock is an event warehouse: it admits events, deduplicates by
fingerprint, and fans out to registered subscribers. This router is the
octop end of that pipeline. The airlock bridge (host process) receives
signed wakes and calls these endpoints instead of driving the dashboard
WebSocket — a UI-only protocol with stream-resume semantics that a
headless delivery worker should not depend on.

POST /api/airlock/tasks
    Dispatch a prompt to an agent as a dashboard turn, reusing the exact
    path the chat UI uses (prepare_dashboard_turn ->
    build_dashboard_inbound -> channel_manager.enqueue), so the task
    lands in the agent's thread history like a typed message and is
    visible in the dashboard.

GET /api/airlock/tasks/{thread_id}
    Turn liveness for the bridge's completion signal: "running" while
    the gateway streams, "pending" between enqueue and pickup,
    "done" once an assistant reply exists.

Auth is the standard user JWT (Authorization: Bearer <token>) — the
bridge mints one for its dedicated airlock service account.
"""

from __future__ import annotations

import contextlib
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from octop.api.common.agent import assert_agent_access
from octop.api.deps import current_user, get_server
from octop.api.routers.chat.models import ChatTurnBody
from octop.api.routers.chat.turn import (
    build_dashboard_inbound,
    prepare_dashboard_turn,
)

router = APIRouter()


# Single shared sink connection for headless dispatches. The hub keys
# connections by id and drops frames when the send fn is missing, so one
# reusable id keeps the registry from growing per task.
_HEADLESS_CONN_ID = "airlock-headless"


async def _noop_async() -> None:
    """Chunk sink: headless dispatches have no browser to stream to."""
    return None


class AirlockTaskIn(BaseModel):
    """POST /api/airlock/tasks body."""

    agent_id: str = Field(description="Target agent (expert).")
    prompt: str = Field(min_length=1, description="Task prompt for the agent.")
    thread_id: str | None = Field(
        default=None,
        description="Reuse an existing thread; omit to mint a new one "
        "(one thread per airlock task by default).",
    )
    title: str | None = Field(
        default=None,
        description="Optional thread title (shown in the dashboard sidebar).",
    )
    fingerprint: str | None = Field(
        default=None,
        description="Airlock event fingerprint — echoed back as task_id "
        "so the bridge can correlate completions.",
    )


class AirlockTaskOut(BaseModel):
    task_id: str
    thread_id: str
    agent_id: str
    status: str = "dispatched"


@router.post(
    "/airlock/tasks",
    response_model=AirlockTaskOut,
    status_code=202,
    summary="Dispatch an airlock task to an agent",
)
async def dispatch_airlock_task(
    body: AirlockTaskIn,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> AirlockTaskOut:
    """Enqueue the task prompt as a dashboard turn (fire-and-forget).

    The gateway picks the turn up asynchronously; poll
    GET /api/airlock/tasks/{thread_id} for completion. 202 (Accepted),
    not 200: dispatch acknowledges the enqueue, not the outcome.
    """
    assert_agent_access(server, body.agent_id, user)
    # One thread per airlock task. ThreadRegistry.get_or_create_by_key
    # derives the thread's channel subject from session_key.split(":")[2]
    # (a 2-part key silently falls back to the user id, collapsing every
    # task onto the human's dashboard thread), so the key must be
    # <source>:<agent>:<task>: the per-fingerprint subject mints a fresh
    # thread per event and never rebinds the human's own session.
    session_key = f"airlock:{body.agent_id}:task-{body.fingerprint or uuid4().hex}"
    turn = ChatTurnBody.from_ws_payload(
        {
            "text": body.prompt,
            "session_key": session_key,
            **({"thread_id": body.thread_id} if body.thread_id else {}),
        }
    )
    prepared = await prepare_dashboard_turn(server, agent_id=body.agent_id, user=user, turn=turn)
    if body.title:
        # Best-effort: a titled thread is much easier for the human to
        # find in the dashboard; failures must not block dispatch.
        with contextlib.suppress(Exception):
            server.app_runtime.gateway.thread_registry.set_title_if_null(
                prepared.thread_id, body.title
            )
    inbound = build_dashboard_inbound(
        agent_id=body.agent_id,
        user_id=user.id,
        prepared=prepared,
        turn=turn,
        # The WS channel drops inbounds whose metadata lacks a
        # ws_connection_id, and pushes chunks to that connection. A
        # headless dispatch has no browser, so register a sink
        # connection: the turn still runs and persists to thread
        # history, and chunk pushes become no-ops.
        ws_connection_id=_HEADLESS_CONN_ID,
        user_is_admin=bool(getattr(user, "is_admin", False)),
    )
    hub = server.app_runtime.gateway.ws_hub
    hub.register(
        _HEADLESS_CONN_ID,
        lambda _frame: _noop_async(),
        user_id=user.id,
    )
    server.app_runtime.gateway.channel_manager.enqueue(inbound.channel_id, inbound)
    return AirlockTaskOut(
        task_id=body.fingerprint or prepared.thread_id,
        thread_id=prepared.thread_id,
        agent_id=body.agent_id,
    )


class AirlockTaskStatusOut(BaseModel):
    thread_id: str
    status: str = Field(description="pending | running | done")
    last_reply: str | None = None


@router.get(
    "/airlock/tasks/{thread_id}",
    response_model=AirlockTaskStatusOut,
    summary="Airlock task turn status",
)
async def airlock_task_status(
    thread_id: str,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> AirlockTaskStatusOut:
    """Turn liveness for completion signalling back to airlock.

    status: "running" while the gateway streams this thread, "done"
    once an assistant reply is persisted, "pending" in the enqueue ->
    pickup window (the agent may simply not have started yet).
    """
    from octop.infra.errors import ErrorCode, OctopError  # noqa: PLC0415

    gateway = server.app_runtime.gateway
    row = gateway.thread_registry.get_thread(thread_id)
    if row is None or row.user_id != user.id:
        raise OctopError(ErrorCode.NOT_FOUND, f"thread {thread_id!r} not found")
    if gateway.ws_hub.is_turn_active(thread_id):
        return AirlockTaskStatusOut(thread_id=thread_id, status="running")
    last_reply = await _last_assistant_reply(server, row.agent_id, thread_id)
    return AirlockTaskStatusOut(
        thread_id=thread_id,
        status="done" if last_reply is not None else "pending",
        last_reply=last_reply,
    )


async def _last_assistant_reply(server: Any, agent_id: str, thread_id: str) -> str | None:
    """Newest assistant text in the thread, or None. Reads the thread
    message projection (the same store the dashboard history endpoint
    serves) rather than the agent checkpoint archive, which lags until
    a projection backfill runs."""
    import json as _json  # noqa: PLC0415

    from langchain_core.messages import messages_from_dict  # noqa: PLC0415

    rows, _ = server.services.thread_message_repo.page(thread_id, limit=20, offset=0)
    for row in reversed(rows):
        try:
            msg = messages_from_dict([_json.loads(row.message_json)])[0]
        except Exception:  # noqa: BLE001 - corrupt row: skip, don't fail status
            continue
        if msg.type != "ai":
            continue
        content = msg.content
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            text = "".join(
                b.get("text", "")
                for b in content
                if isinstance(b, dict) and b.get("type") == "text"
            )
            if text.strip():
                return text
    return None
