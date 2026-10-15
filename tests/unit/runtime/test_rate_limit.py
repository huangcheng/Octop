"""Unit tests for the runtime dial rate limiter."""

from __future__ import annotations

from octop.infra.runtime.rate_limit import RuntimeRateLimiter


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_burst_of_ten_then_refill() -> None:
    clock = _Clock()
    limiter = RuntimeRateLimiter(clock=clock)
    ip, eid = "203.0.113.9", "enroll-1"

    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial(ip, eid) is True
    assert limiter.consume_dial(ip, eid) is False

    # One minute later the bucket is full again (10/min refill).
    clock.advance(60.0)
    assert limiter.consume_dial(ip, eid) is True


def test_buckets_are_per_ip_and_enrollment() -> None:
    limiter = RuntimeRateLimiter()
    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial("10.0.0.1", "e1") is True
    assert limiter.consume_dial("10.0.0.1", "e1") is False
    # A different enrollment or IP has its own bucket.
    assert limiter.consume_dial("10.0.0.1", "e2") is True
    assert limiter.consume_dial("10.0.0.2", "e1") is True


def test_garbage_keys_use_the_ip_only_bucket() -> None:
    limiter = RuntimeRateLimiter()
    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial("10.0.0.1", None) is True
    assert limiter.consume_dial("10.0.0.1", None) is False
    assert limiter.consume_dial("10.0.0.1", "e1") is True


def test_successful_handshake_refund_keeps_healthy_daemon_unthrottled() -> None:
    limiter = RuntimeRateLimiter()
    ip, eid = "10.0.0.1", "e1"
    for _ in range(50):
        assert limiter.consume_dial(ip, eid) is True
        limiter.refund_dial(ip, eid)
    assert limiter.consume_dial(ip, eid) is True


def test_refund_does_not_exceed_burst() -> None:
    clock = _Clock()
    limiter = RuntimeRateLimiter(clock=clock)
    ip, eid = "10.0.0.1", "e1"
    assert limiter.consume_dial(ip, eid) is True
    for _ in range(3):
        limiter.refund_dial(ip, eid)
    # 1 consumed + 3 refunds, capped at burst 10 -> 10 more fit, 11th fails.
    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial(ip, eid) is True
    assert limiter.consume_dial(ip, eid) is False


def test_five_credential_failures_block_for_fifteen_minutes() -> None:
    clock = _Clock()
    limiter = RuntimeRateLimiter(clock=clock)
    ip, eid = "10.0.0.1", "e1"

    for _ in range(RuntimeRateLimiter.CREDENTIAL_FAILURE_THRESHOLD - 1):
        limiter.record_credential_failure(ip, eid)
        assert limiter.is_blocked(ip, eid) is False

    limiter.record_credential_failure(ip, eid)
    assert limiter.is_blocked(ip, eid) is True

    clock.advance(RuntimeRateLimiter.BLOCK_SECONDS - 1)
    assert limiter.is_blocked(ip, eid) is True
    clock.advance(2)
    assert limiter.is_blocked(ip, eid) is False


def test_ip_only_block_covers_resolved_dials_from_that_ip() -> None:
    limiter = RuntimeRateLimiter()
    ip = "10.0.0.1"
    for _ in range(RuntimeRateLimiter.CREDENTIAL_FAILURE_THRESHOLD):
        limiter.record_credential_failure(ip, None)

    assert limiter.is_blocked(ip, None) is True
    assert limiter.is_blocked(ip, "e1") is True
    assert limiter.is_blocked("10.0.0.2", None) is False


def test_enrollment_scoped_block_does_not_spill() -> None:
    limiter = RuntimeRateLimiter()
    for _ in range(RuntimeRateLimiter.CREDENTIAL_FAILURE_THRESHOLD):
        limiter.record_credential_failure("10.0.0.1", "e1")

    assert limiter.is_blocked("10.0.0.1", "e1") is True
    assert limiter.is_blocked("10.0.0.1", "e2") is False
    assert limiter.is_blocked("10.0.0.2", "e1") is False


def test_failure_counter_resets_after_block() -> None:
    clock = _Clock()
    limiter = RuntimeRateLimiter(clock=clock)
    ip, eid = "10.0.0.1", "e1"
    for _ in range(RuntimeRateLimiter.CREDENTIAL_FAILURE_THRESHOLD):
        limiter.record_credential_failure(ip, eid)
    assert limiter.is_blocked(ip, eid) is True

    clock.advance(RuntimeRateLimiter.BLOCK_SECONDS + 1)
    assert limiter.is_blocked(ip, eid) is False
    # Counter restarted from zero: another full threshold is tolerated first.
    for _ in range(RuntimeRateLimiter.CREDENTIAL_FAILURE_THRESHOLD - 1):
        limiter.record_credential_failure(ip, eid)
    assert limiter.is_blocked(ip, eid) is False


def test_memory_stays_bounded_under_many_keys() -> None:
    limiter = RuntimeRateLimiter()
    for i in range(2000):
        limiter.record_credential_failure(f"10.0.{i // 256}.{i % 256}", f"e{i}")
    assert limiter.tracked_keys <= RuntimeRateLimiter._GC_THRESHOLD


def test_injected_clock_drives_bucket_math() -> None:
    clock = _Clock()
    limiter = RuntimeRateLimiter(clock=clock)
    ip, eid = "10.0.0.1", "e1"

    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial(ip, eid) is True
    assert limiter.consume_dial(ip, eid) is False

    # 10/min -> one token per 6 seconds; a partial minute refills exactly one.
    clock.advance(6.0)
    assert limiter.consume_dial(ip, eid) is True
    assert limiter.consume_dial(ip, eid) is False

    clock.advance(60.0)
    for _ in range(RuntimeRateLimiter.DIAL_BURST):
        assert limiter.consume_dial(ip, eid) is True
