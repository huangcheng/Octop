"""A reply that finishes with nobody subscribed to the thread is offered as a push."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from octop_gateway.models import ChannelSubject, InboundMessage, TextContent

from octop.infra.gateway.ws import WS_CHANNEL_ID, WebSocketChannel, WebSocketHub
from octop.infra.gateway.ws.ws_channel import _reply_preview


def _message(thread_id: str = "thread-1") -> InboundMessage:
    return InboundMessage(
        channel_id=WS_CHANNEL_ID,
        channel_type="dashboard",
        tenant_id="agent-1",
        channel_subject=ChannelSubject(subject_id="7"),
        content=[TextContent(text="hi")],
        metadata={
            "ws_connection_id": "conn-1",
            "session_key": "sk",
            "thread_id": thread_id,
        },
    )


def _processor(*chunks: dict[str, Any]) -> Any:
    class _FakeProcessor:
        async def iter_turn_chunks(self, msg: InboundMessage) -> AsyncIterator[dict[str, Any]]:
            for chunk in chunks:
                yield chunk

    return _FakeProcessor()


def _notifier() -> tuple[list[tuple[int, str, str, str]], Any]:
    calls: list[tuple[int, str, str, str]] = []

    async def notify(user_id: int, agent_id: str, thread_id: str, preview: str) -> None:
        calls.append((user_id, agent_id, thread_id, preview))

    return calls, notify


@pytest.mark.asyncio
async def test_turn_without_subscribers_pushes_the_final_answer_preview() -> None:
    hub = WebSocketHub()
    calls, notify = _notifier()
    channel = WebSocketChannel(
        _processor(
            {"type": "token", "content": "Let me check.\n"},
            {"type": "tool_call_chunk", "content": "lookup"},
            {"type": "tool_result", "content": "ok"},
            {"type": "token", "content": "## Result\n\nThe **answer** is `42`."},
            {"type": "done"},
        ),
        hub=hub,
        on_turn_complete=notify,
    )  # type: ignore[arg-type]

    await channel.handle_inbound(_message())

    # Text before the tool boundary is dropped; only the final answer is offered.
    assert calls == [(7, "agent-1", "thread-1", "Result The answer is 42.")]


@pytest.mark.asyncio
async def test_turn_with_a_subscriber_is_not_pushed() -> None:
    hub = WebSocketHub()
    frames: list[dict[str, Any]] = []

    async def capture(frame: dict[str, Any]) -> None:
        frames.append(frame)

    hub.register("conn-1", capture)
    hub.subscribe("thread-1", "conn-1")
    calls, notify = _notifier()
    channel = WebSocketChannel(
        _processor({"type": "token", "content": "hi"}, {"type": "done"}),
        hub=hub,
        on_turn_complete=notify,
    )  # type: ignore[arg-type]

    await channel.handle_inbound(_message())

    assert calls == []
    assert frames[-1]["type"] == "turn.ended"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chunks",
    [
        ({"type": "error", "message": "boom"},),
        ({"type": "hitl_required", "pending_id": "p1"},),
    ],
    ids=["failed", "hitl-paused"],
)
async def test_failed_or_parked_turns_are_not_pushed(chunks: tuple[dict[str, Any], ...]) -> None:
    hub = WebSocketHub()
    calls, notify = _notifier()
    channel = WebSocketChannel(_processor(*chunks), hub=hub, on_turn_complete=notify)  # type: ignore[arg-type]

    await channel.handle_inbound(_message())

    assert calls == []


@pytest.mark.asyncio
async def test_notifier_failure_never_fails_the_turn() -> None:
    hub = WebSocketHub()
    frames: list[dict[str, Any]] = []

    async def capture(frame: dict[str, Any]) -> None:
        frames.append(frame)

    async def explode(user_id: int, agent_id: str, thread_id: str, preview: str) -> None:
        raise RuntimeError("push channel down")

    hub.register("conn-1", capture)
    channel = WebSocketChannel(
        _processor({"type": "token", "content": "hi"}, {"type": "done"}),
        hub=hub,
        on_turn_complete=explode,
    )  # type: ignore[arg-type]

    await channel.handle_inbound(_message())

    assert not any(frame["type"] == "error" for frame in frames)
    assert frames[-1]["type"] == "turn.ended"
    assert frames[-1].get("failed") is None


def test_reply_preview_strips_markdown_and_caps() -> None:
    text = (
        "# Title\n\n> quote\n\n- item one\n- item two\n\n"
        "See [docs](https://x.y/z) and ![pic](https://x.y/p.png).\n\n"
        "```python\nprint('nope')\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |"
    )
    assert _reply_preview(text) == "Title quote item one item two See docs and . a b 1 2"
    assert len(_reply_preview("x" * 400)) == 160
    assert _reply_preview("x" * 400).endswith("…")
    assert _reply_preview("```only code```") == ""
