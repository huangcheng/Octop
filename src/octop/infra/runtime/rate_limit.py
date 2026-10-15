"""Dial rate limiting and credential-failure blocking for /api/runtime/ws.

Two independent mechanisms, both keyed on the direct peer address:

* **Dial limit** — token bucket, 10 dials/minute, keyed ``(ip, enrollment_id)``
  for resolvable keys and ``(ip,)`` alone for garbage keys. A completed
  handshake refunds its token, so healthy daemons reconnecting on schedule are
  never throttled; only failed/abusive dials consume budget.
* **Credential-failure block** — 5 failures block the key for 15 minutes, keyed
  the same way. Blocked keys are rejected before any auth work happens.

X-Forwarded-For is deliberately ignored here: the endpoint uses the direct
peer address, so a caller cannot escape its bucket by varying a header. (If a
trusted-proxy list is ever added to OctopConfig, resolve it before calling
into this limiter.)

All state is in-memory and self-pruning; a single event loop is assumed.
"""

from __future__ import annotations

import time
from collections.abc import Callable

DialKey = tuple[str, str | None]


class RuntimeRateLimiter:
    """Token-bucket dial limiter plus credential-failure blocks."""

    DIAL_LIMIT_PER_MINUTE = 10
    DIAL_BURST = 10
    CREDENTIAL_FAILURE_THRESHOLD = 5
    BLOCK_SECONDS = 15 * 60

    # Lazy GC watermark: above this many tracked keys, expired entries drop.
    _GC_THRESHOLD = 512
    _BUCKET_IDLE_SECONDS = 3600

    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._dial_buckets: dict[DialKey, tuple[float, float]] = {}
        self._failures: dict[DialKey, int] = {}
        self._blocked_until: dict[DialKey, float] = {}

    @staticmethod
    def _key(ip: str, enrollment_id: str | None) -> DialKey:
        return (ip, enrollment_id if enrollment_id else None)

    def is_blocked(self, ip: str, enrollment_id: str | None = None) -> bool:
        """Whether either the exact key or the IP-only garbage bucket is blocked."""
        now = self._clock()
        for key in (self._key(ip, enrollment_id), self._key(ip, None)):
            until = self._blocked_until.get(key)
            if until is not None and now < until:
                return True
        return False

    def consume_dial(self, ip: str, enrollment_id: str | None = None) -> bool:
        """Take one dial token; False means the bucket is empty (reject dial)."""
        key = self._key(ip, enrollment_id)
        now = self._clock()
        tokens, last = self._dial_buckets.get(key, (float(self.DIAL_BURST), now))
        tokens = min(float(self.DIAL_BURST), tokens + (now - last) * self._refill_rate())
        if tokens < 1.0:
            self._dial_buckets[key] = (tokens, now)
            return False
        self._dial_buckets[key] = (tokens - 1.0, now)
        return True

    def refund_dial(self, ip: str, enrollment_id: str | None = None) -> None:
        """Return a token for a completed handshake (successful dials are exempt)."""
        key = self._key(ip, enrollment_id)
        entry = self._dial_buckets.get(key)
        if entry is None:
            return
        tokens, last = entry
        self._dial_buckets[key] = (min(float(self.DIAL_BURST), tokens + 1.0), last)

    def record_credential_failure(self, ip: str, enrollment_id: str | None = None) -> None:
        """Count a bad credential; the fifth consecutive one blocks the key."""
        key = self._key(ip, enrollment_id)
        count = self._failures.get(key, 0) + 1
        if count >= self.CREDENTIAL_FAILURE_THRESHOLD:
            self._failures.pop(key, None)
            self._blocked_until[key] = self._clock() + self.BLOCK_SECONDS
        else:
            self._failures[key] = count
        self._gc()

    @property
    def tracked_keys(self) -> int:
        """Diagnostic for tests: total keys held across all maps."""
        return len(self._dial_buckets) + len(self._failures) + len(self._blocked_until)

    def _refill_rate(self) -> float:
        return self.DIAL_LIMIT_PER_MINUTE / 60.0

    def _gc(self) -> None:
        total = self.tracked_keys
        if total < self._GC_THRESHOLD:
            return
        now = self._clock()
        self._blocked_until = {k: v for k, v in self._blocked_until.items() if v > now}
        self._dial_buckets = {
            k: v for k, v in self._dial_buckets.items() if now - v[1] < self._BUCKET_IDLE_SECONDS
        }
        self._failures.clear()
