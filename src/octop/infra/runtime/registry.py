"""In-memory registry of live runtime sessions.

The hub dials nothing itself: runtimes connect outbound to ``/api/runtime/ws``
and each completed handshake registers one :class:`RuntimeSession` here.
Presence is derived from the registry — an enrollment is *online* iff at least
one live session exists. Memory stays bounded by the number of active
enrollments: sessions are removed on close, and during a key-rotation overlap
at most two sessions coexist per enrollment (current key + previous key).

Single event loop assumed; all registry operations are non-blocking.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from octop.infra.runtime import protocol
from octop.infra.utils.ulid import new_ulid

if TYPE_CHECKING:
    from octop.infra.runtime.protocol import FrameEnvelope

#: Hub-side capability set; rt.welcome advertises the intersection with the
#: daemon's rt.hello capabilities.
HUB_CAPABILITIES: tuple[str, ...] = ("fs", "exec", "pty")

#: Default per-session quotas advertised in rt.welcome, built from the shared
#: protocol constants so hub and daemon never drift apart.
DEFAULT_QUOTAS: dict[str, int] = {
    "frame_cap_bytes": protocol.FRAME_CAP_BYTES,
    "chunk_bytes": protocol.CHUNK_BYTES,
    "initial_credit_bytes": protocol.INITIAL_CREDIT_BYTES,
    "exec_queue_bytes": protocol.EXEC_QUEUE_BYTES,
    "fs_stream_queue_chunks": protocol.FS_STREAM_QUEUE_CHUNKS,
    "control_rate_cap_fps": protocol.CONTROL_RATE_CAP_FPS,
    "data_rate_cap_fps": protocol.DATA_RATE_CAP_FPS,
}

#: Close code a superseded session receives when a newer dial evicts it.
CLOSE_SUPERSEDED = 4005

_session_seq = itertools.count(1)

#: Transport close hook injected by the endpoint that owns the session.
CloseImpl = Callable[[int, str], Awaitable[None]]
#: Pluggable per-frame dispatch callback (wave 3/4 plug exec/fs handlers).
FrameHandler = Callable[["FrameEnvelope"], Awaitable[None]]


class RuntimeSession:
    """One live runtime connection and its negotiated session state.

    ``closed`` is set exactly once, on ANY close path (eviction, protocol
    error, graceful bye, disconnect), so in-flight futures from later waves can
    ``await session.closed`` instead of racing their own timeouts.
    """

    def __init__(
        self,
        *,
        enrollment_id: str,
        generation: int,
        key_slot: str,
        capabilities: tuple[str, ...],
        client_ip: str,
        close_impl: CloseImpl,
    ) -> None:
        self.session_id = new_ulid()
        self.enrollment_id = enrollment_id
        self.generation = generation
        #: "current" or "previous" — old-key (previous) dials never evict.
        self.key_slot = key_slot
        self.capabilities = tuple(capabilities)
        self.client_ip = client_ip
        self.connected_at = float(next(_session_seq))
        self.closed = asyncio.Event()
        self.close_code: int | None = None
        self.close_reason = ""
        self._close_impl = close_impl
        self._frame_handler: FrameHandler | None = None

    @property
    def is_live(self) -> bool:
        return not self.closed.is_set()

    @property
    def frame_handler(self) -> FrameHandler | None:
        return self._frame_handler

    def set_frame_handler(self, handler: FrameHandler | None) -> None:
        """Plug (or unplug) the per-frame dispatch callback for this session."""
        self._frame_handler = handler

    async def close_with(self, code: int, reason: str = "") -> None:
        """Idempotent close: record code/reason, unblock waiters, close transport."""
        if self.closed.is_set():
            return
        self.close_code = code
        self.close_reason = reason
        self.closed.set()
        await self._close_impl(code, reason)


class RuntimeSessionRegistry:
    """Live sessions keyed by enrollment_id.

    Values are ``{session_id: session}`` so a rotation overlap can hold both
    the current-key and previous-key session of one enrollment. Stale entries
    (closed sessions not yet unregistered) are swept lazily so the table never
    grows with dead connections.
    """

    _SWEEP_THRESHOLD = 256

    def __init__(self) -> None:
        self._by_enrollment: dict[str, dict[str, RuntimeSession]] = {}

    def register(self, session: RuntimeSession) -> None:
        self._by_enrollment.setdefault(session.enrollment_id, {})[session.session_id] = session
        if len(self) > self._SWEEP_THRESHOLD:
            self._sweep()

    def get(self, enrollment_id: str) -> RuntimeSession | None:
        """The most recently connected live session for the enrollment, if any."""
        sessions = self._live_sessions(enrollment_id)
        if not sessions:
            return None
        return max(sessions, key=lambda s: s.connected_at)

    def sessions(self, enrollment_id: str) -> list[RuntimeSession]:
        """All live sessions for the enrollment (both key slots during overlap)."""
        return self._live_sessions(enrollment_id)

    def remove(self, session: RuntimeSession) -> None:
        bucket = self._by_enrollment.get(session.enrollment_id)
        if bucket is None:
            return
        bucket.pop(session.session_id, None)
        if not bucket:
            self._by_enrollment.pop(session.enrollment_id, None)

    def online(self, enrollment_id: str) -> bool:
        return bool(self._live_sessions(enrollment_id))

    def online_enrollment_ids(self) -> set[str]:
        return {
            eid
            for eid, bucket in self._by_enrollment.items()
            if any(s.is_live for s in bucket.values())
        }

    def __len__(self) -> int:
        return sum(len(bucket) for bucket in self._by_enrollment.values())

    def _live_sessions(self, enrollment_id: str) -> list[RuntimeSession]:
        bucket = self._by_enrollment.get(enrollment_id)
        if not bucket:
            return []
        return [s for s in bucket.values() if s.is_live]

    def _sweep(self) -> None:
        """Drop closed sessions; keeps memory O(active enrollments)."""
        for enrollment_id in list(self._by_enrollment):
            bucket = self._by_enrollment[enrollment_id]
            for session_id in [sid for sid, s in bucket.items() if not s.is_live]:
                del bucket[session_id]
            if not bucket:
                del self._by_enrollment[enrollment_id]
