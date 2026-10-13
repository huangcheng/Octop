"""IM busy-path soft steer admission / debounce."""

from __future__ import annotations

from octop_gateway.models import ChannelSubject, ImageContent, InboundMessage, TextContent

from octop.infra.gateway.steer.coordinator import SteerCoordinator
from octop.infra.gateway.steer.im_busy import (
    ImBusyOutcome,
    ImSteerAckDebouncer,
    format_discard_ack,
    is_pure_text_inbound,
    sender_id_from_message,
    try_admit_im_steer,
)
from octop.infra.gateway.steer.types import SKIP_AGENT_TURN_META, SteerAdmitIdle


def _text_msg(text: str, *, meta: dict | None = None) -> InboundMessage:
    return InboundMessage(
        channel_id="ch1",
        channel_type="weixin",
        tenant_id="agent1",
        channel_subject=ChannelSubject(subject_id="u1"),
        content=[TextContent(text=text)],
        metadata=meta or {},
    )


def test_admit_or_idle_returns_idle_when_no_turn() -> None:
    c = SteerCoordinator()
    assert isinstance(
        c.admit_or_idle(
            "t1",
            text="hi",
            client_pending_id="c1",
            expected_turn_id=None,
        ),
        SteerAdmitIdle,
    )


def test_admit_or_idle_queues_when_busy() -> None:
    c = SteerCoordinator()
    turn_id, _ = c.begin_turn("t1")
    r = c.admit_or_idle(
        "t1",
        text="fix it",
        client_pending_id="c1",
        expected_turn_id=turn_id,
    )
    assert getattr(r, "status", None) == "queued"


def test_pure_text_rejects_image() -> None:
    msg = InboundMessage(
        channel_id="ch1",
        channel_type="weixin",
        tenant_id="agent1",
        channel_subject=ChannelSubject(subject_id="u1"),
        content=[TextContent(text="see"), ImageContent(url="https://x/y.png")],
        metadata={},
    )
    assert not is_pure_text_inbound(msg)


def test_busy_text_admits_and_skips_agent_turn() -> None:
    c = SteerCoordinator()
    turn_id, _ = c.begin_turn("t1")
    msg = _text_msg("please also check outlook", meta={"msg_id": "m1"})
    debouncer = ImSteerAckDebouncer()
    result = try_admit_im_steer(
        steer=c,
        message=msg,
        thread_id="t1",
        session_key="sess",
        locale="en",
        debouncer=debouncer,
    )
    assert result.outcome == ImBusyOutcome.BUSY_HANDLED
    assert result.ack_text is not None
    assert "Steered" in result.ack_text
    assert msg.metadata.get(SKIP_AGENT_TURN_META) is True
    assert c.has_pending("t1")
    # live turn id still the same
    assert c.get_state("t1") is not None
    assert c.get_state("t1").turn_id == turn_id  # type: ignore[union-attr]


def test_ack_debounce_per_sender() -> None:
    c = SteerCoordinator()
    turn_id, _ = c.begin_turn("t1")
    debouncer = ImSteerAckDebouncer(steered_window=30.0)
    msg1 = _text_msg("one", meta={"msg_id": "m1"})
    r1 = try_admit_im_steer(
        steer=c,
        message=msg1,
        thread_id="t1",
        session_key="sess",
        locale="zh",
        debouncer=debouncer,
    )
    assert r1.ack_text is not None
    msg2 = _text_msg("two", meta={"msg_id": "m2"})
    r2 = try_admit_im_steer(
        steer=c,
        message=msg2,
        thread_id="t1",
        session_key="sess",
        locale="zh",
        debouncer=debouncer,
    )
    assert r2.outcome == ImBusyOutcome.BUSY_HANDLED
    assert r2.ack_text is None  # debounced
    assert c.get_state("t1") is not None
    assert len(c.get_state("t1").queue) == 2  # type: ignore[union-attr]
    _ = turn_id


def test_other_slash_proceeds_normal() -> None:
    c = SteerCoordinator()
    c.begin_turn("t1")
    msg = _text_msg("/status")
    result = try_admit_im_steer(
        steer=c,
        message=msg,
        thread_id="t1",
        session_key="sess",
        locale="en",
        debouncer=ImSteerAckDebouncer(),
    )
    assert result.outcome == ImBusyOutcome.PROCEED_NORMAL
    assert not c.has_pending("t1")


def test_control_slash_proceeds_without_skip() -> None:
    c = SteerCoordinator()
    c.begin_turn("t1")
    msg = _text_msg("/stop")
    result = try_admit_im_steer(
        steer=c,
        message=msg,
        thread_id="t1",
        session_key="sess",
        locale="en",
        debouncer=ImSteerAckDebouncer(),
    )
    assert result.outcome == ImBusyOutcome.PROCEED_NORMAL
    assert not msg.metadata.get(SKIP_AGENT_TURN_META)


def test_hitl_paused_proceeds_for_plain_text_answer() -> None:
    c = SteerCoordinator()
    c.begin_turn("t1")
    c.set_phase("t1", "hitl_paused")
    msg = _text_msg("A")
    result = try_admit_im_steer(
        steer=c,
        message=msg,
        thread_id="t1",
        session_key="sess",
        locale="en",
        debouncer=ImSteerAckDebouncer(),
    )
    assert result.outcome == ImBusyOutcome.PROCEED_NORMAL
    assert not msg.metadata.get(SKIP_AGENT_TURN_META)


def test_sender_id_prefers_member_metadata_in_group() -> None:
    msg = InboundMessage(
        channel_id="ch1",
        channel_type="weixin",
        tenant_id="agent1",
        channel_subject=ChannelSubject(subject_id="group-openid", chat_type="group"),
        content=[TextContent(text="hi")],
        metadata={"sender_id": "member-openid"},
    )
    assert sender_id_from_message(msg) == "member-openid"


def test_im_hitl_pause_discards_pending_instead_of_parking() -> None:
    c = SteerCoordinator()
    turn_id, _ = c.begin_turn("t1")
    c.admit(
        "t1",
        text="fix later",
        client_pending_id="c1",
        expected_turn_id=turn_id,
    )
    assert c.has_pending("t1")
    # Mirror processor finally: IM must not park (no take_hitl_pending).
    discarded = c.discard("t1", reason="hitl_paused")
    assert discarded is not None
    c.end_turn("t1", failed=False)
    assert not c.is_active("t1")
    assert c.get_state("t1") is None
    assert "reply" in format_discard_ack("hitl_paused", "en").lower()


def test_stop_clears_hitl_park_with_queued_steers() -> None:
    """IM /stop must end_turn after discard so a parked HITL queue cannot wedge."""
    c = SteerCoordinator()
    turn_id, _ = c.begin_turn("t1")
    c.admit(
        "t1",
        text="fix later",
        client_pending_id="c1",
        expected_turn_id=turn_id,
    )
    c.set_phase("t1", "hitl_paused")
    assert c.end_turn("t1") is None  # park
    assert c.is_active("t1")
    assert c.has_pending("t1")

    c.mark_cancelling("t1")
    discarded = c.discard("t1", reason="turn_cancelled")
    assert discarded is not None
    # Without end_turn the phase stays cancelling and is_active stays true.
    c.end_turn("t1", failed=False)
    assert not c.is_active("t1")
    assert c.get_state("t1") is None
    assert "discarded" in format_discard_ack("turn_cancelled", "en").lower()


def test_attachment_rejects_while_busy() -> None:
    c = SteerCoordinator()
    c.begin_turn("t1")
    msg = InboundMessage(
        channel_id="ch1",
        channel_type="weixin",
        tenant_id="agent1",
        channel_subject=ChannelSubject(subject_id="u1"),
        content=[ImageContent(url="https://x/y.png")],
        metadata={},
    )
    result = try_admit_im_steer(
        steer=c,
        message=msg,
        thread_id="t1",
        session_key="sess",
        locale="en",
        debouncer=ImSteerAckDebouncer(),
    )
    assert result.outcome == ImBusyOutcome.BUSY_HANDLED
    assert result.ack_text is not None
    assert "Attachments" in result.ack_text
    assert not c.has_pending("t1")
