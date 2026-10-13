"""IM busy-path soft steer: admit before session lock, ack with per-sender debounce.

Concrete seam: ``ChannelManager.set_pre_lock_handler`` (message already dequeued,
before session lock). Successful admit sets ``SKIP_AGENT_TURN_META`` so
``handle_inbound`` / ``GlobalProcessor`` does not start a second harness turn.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import StrEnum

from octop_gateway.models import (
    AudioContent,
    FileContent,
    ImageContent,
    InboundMessage,
    TextContent,
    VideoContent,
)

from octop.i18n import tr
from octop.infra.gateway.slash.parser import parse_slash
from octop.infra.gateway.steer.coordinator import SteerCoordinator
from octop.infra.gateway.steer.types import (
    SKIP_AGENT_TURN_META,
    SteerAdmitIdle,
    SteerAdmitOk,
    SteerAdmitReject,
)
from octop.infra.gateway.threads import ThreadRegistry
from octop.infra.utils.ulid import new_ulid

logger = logging.getLogger(__name__)

STEER_ACK_DEBOUNCE_SECONDS = 30.0
REJECT_ACK_RATE_LIMIT_SECONDS = 10.0
_DEBOUNCE_PRUNE_AFTER = 256


class ImBusyOutcome(StrEnum):
    BUSY_HANDLED = "busy_handled"
    PROCEED_NORMAL = "proceed_normal"


@dataclass(frozen=True)
class ImBusyResult:
    outcome: ImBusyOutcome
    ack_text: str | None = None


class ImSteerAckDebouncer:
    """Per-(session_key, sender_id) steered-ack debounce + reject rate limit."""

    def __init__(
        self,
        *,
        steered_window: float = STEER_ACK_DEBOUNCE_SECONDS,
        reject_window: float = REJECT_ACK_RATE_LIMIT_SECONDS,
    ) -> None:
        self._steered_window = steered_window
        self._reject_window = reject_window
        self._last_steered: dict[tuple[str, str], float] = {}
        self._last_reject: dict[tuple[str, str], float] = {}

    def _prune(self, store: dict[tuple[str, str], float], *, window: float, now: float) -> None:
        if len(store) < _DEBOUNCE_PRUNE_AFTER:
            return
        stale = [k for k, ts in store.items() if (now - ts) >= window]
        for k in stale:
            store.pop(k, None)

    def allow_steered(self, session_key: str, sender_id: str, *, now: float | None = None) -> bool:
        key = (session_key, sender_id)
        ts = time.monotonic() if now is None else now
        self._prune(self._last_steered, window=self._steered_window, now=ts)
        prev = self._last_steered.get(key)
        if prev is not None and (ts - prev) < self._steered_window:
            return False
        self._last_steered[key] = ts
        return True

    def allow_reject(self, session_key: str, sender_id: str, *, now: float | None = None) -> bool:
        key = (session_key, sender_id)
        ts = time.monotonic() if now is None else now
        self._prune(self._last_reject, window=self._reject_window, now=ts)
        prev = self._last_reject.get(key)
        if prev is not None and (ts - prev) < self._reject_window:
            return False
        self._last_reject[key] = ts
        return True


def is_pure_text_inbound(message: InboundMessage) -> bool:
    """True when every content part is TextContent (no media)."""
    parts = list(message.content or [])
    if not parts:
        return True
    for part in parts:
        if isinstance(part, (ImageContent, VideoContent, AudioContent, FileContent)):
            return False
        if not isinstance(part, TextContent):
            return False
    return True


def platform_client_pending_id(message: InboundMessage) -> str:
    meta = message.metadata if isinstance(message.metadata, dict) else {}
    for key in ("msg_id", "message_id", "platform_message_id"):
        raw = meta.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        if isinstance(raw, (int, float)) and str(raw):
            return str(raw)
    return new_ulid()


def sender_id_from_message(message: InboundMessage) -> str:
    """Per-member id for debounce; fall back to chat subject only when unknown."""
    meta = message.metadata if isinstance(message.metadata, dict) else {}
    for key in ("sender_id", "from_user_id"):
        raw = meta.get(key)
        if raw not in (None, ""):
            return str(raw)
    if message.channel_subject and message.channel_subject.subject_id:
        return str(message.channel_subject.subject_id)
    return "unknown"


def mark_skip_agent_turn(message: InboundMessage) -> None:
    meta = dict(message.metadata or {})
    meta[SKIP_AGENT_TURN_META] = True
    message.metadata = meta


def should_skip_agent_turn(message: InboundMessage) -> bool:
    meta = message.metadata if isinstance(message.metadata, dict) else {}
    return bool(meta.get(SKIP_AGENT_TURN_META))


def _reject_ack_key(reason: str) -> str:
    mapping = {
        "attachments": "channel.steer.reject_attachments",
        "queue_full": "channel.steer.reject_queue_full",
        "input_too_large": "channel.steer.reject_input_too_large",
        "hitl_paused": "channel.steer.reject_hitl_paused",
        "empty_input": "channel.steer.reject_empty",
        "turn_not_steerable": "channel.steer.reject_not_steerable",
        "turn_already_cancelled": "channel.steer.reject_not_steerable",
        "expected_turn_id_mismatch": "channel.steer.reject_not_steerable",
        "expected_turn_id_required": "channel.steer.reject_not_steerable",
        "no_active_turn": "channel.steer.reject_not_steerable",
    }
    return mapping.get(reason, "channel.steer.reject_generic")


def format_steered_ack(locale: str) -> str:
    return tr("channel.steer.ack_steered", locale)


def format_reject_ack(reason: str, locale: str) -> str:
    return tr(_reject_ack_key(reason), locale)


def format_discard_ack(reason: str, locale: str) -> str:
    mapping = {
        "turn_cancelled": "channel.steer.discarded_cancelled",
        "turn_failed": "channel.steer.discarded_failed",
        "continuation_exhausted": "channel.steer.discarded_exhausted",
        "hitl_paused": "channel.steer.discarded_hitl",
    }
    return tr(mapping.get(reason, "channel.steer.discarded_cancelled"), locale)


def try_admit_im_steer(
    *,
    steer: SteerCoordinator,
    message: InboundMessage,
    thread_id: str | None,
    session_key: str,
    locale: str,
    debouncer: ImSteerAckDebouncer,
    hitl_paused: bool = False,
) -> ImBusyResult:
    """Classify a dequeued IM message against the active turn.

    Virtual channels (dashboard/cli) must not call this — they own their WS path.
    """
    channel_type = str(getattr(message, "channel_type", "") or "")
    if ThreadRegistry.is_virtual_channel(channel_type):
        return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)

    cmd = parse_slash(getattr(message, "text", None))
    if cmd is not None:
        # /stop|/cancel are handled in gateway pre-lock (cancel + discard). Any
        # slash must reach the dispatcher — never mark_skip here.
        return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)

    if not thread_id or not str(thread_id).strip():
        return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)

    tid = str(thread_id).strip()
    # Parked HITL must reach GlobalProcessor ask/approval resolution, not steer.
    if steer.is_hitl_paused(tid):
        return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)
    if not steer.is_active(tid):
        return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)

    sender = sender_id_from_message(message)

    if not is_pure_text_inbound(message):
        mark_skip_agent_turn(message)
        ack = None
        if debouncer.allow_reject(session_key, sender):
            ack = format_reject_ack("attachments", locale)
        return ImBusyResult(outcome=ImBusyOutcome.BUSY_HANDLED, ack_text=ack)

    # expected_turn_id=None + delivery=auto: admit() binds to the live turn under
    # lock so a rollover between checks cannot reject with expected_turn_id_mismatch.
    result = steer.admit_or_idle(
        tid,
        text=str(getattr(message, "text", "") or ""),
        client_pending_id=platform_client_pending_id(message),
        expected_turn_id=None,
        delivery="auto",
        hitl_paused=hitl_paused,
    )

    if isinstance(result, SteerAdmitIdle):
        return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)

    if isinstance(result, SteerAdmitOk):
        mark_skip_agent_turn(message)
        ack = None
        if result.emit_queued and debouncer.allow_steered(session_key, sender):
            ack = format_steered_ack(locale)
        return ImBusyResult(outcome=ImBusyOutcome.BUSY_HANDLED, ack_text=ack)

    if isinstance(result, SteerAdmitReject):
        # hitl_paused: let the plain-text answer reach GlobalProcessor ask/approval.
        if str(result.reason) == "hitl_paused":
            return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)
        mark_skip_agent_turn(message)
        ack = None
        if debouncer.allow_reject(session_key, sender):
            ack = format_reject_ack(str(result.reason), locale)
        return ImBusyResult(outcome=ImBusyOutcome.BUSY_HANDLED, ack_text=ack)

    logger.warning("im steer unexpected admit result: %r", result)
    return ImBusyResult(outcome=ImBusyOutcome.PROCEED_NORMAL)


__all__ = [
    "ImBusyOutcome",
    "ImBusyResult",
    "ImSteerAckDebouncer",
    "REJECT_ACK_RATE_LIMIT_SECONDS",
    "SKIP_AGENT_TURN_META",
    "STEER_ACK_DEBOUNCE_SECONDS",
    "format_discard_ack",
    "format_reject_ack",
    "format_steered_ack",
    "is_pure_text_inbound",
    "mark_skip_agent_turn",
    "platform_client_pending_id",
    "sender_id_from_message",
    "should_skip_agent_turn",
    "try_admit_im_steer",
]
