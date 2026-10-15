"""Unit tests for the in-memory runtime session registry."""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

from octop.infra.runtime import protocol
from octop.infra.runtime.registry import (
    CLOSE_SUPERSEDED,
    DEFAULT_QUOTAS,
    HUB_CAPABILITIES,
    RuntimeSession,
    RuntimeSessionRegistry,
)


async def _noop_close(code: int, reason: str) -> None:  # pragma: no cover - trivial
    del code, reason


def _session(
    enrollment_id: str = "e1",
    *,
    generation: int = 1,
    key_slot: str = "current",
    capabilities: tuple[str, ...] = ("fs", "exec"),
    client_ip: str = "10.0.0.1",
) -> RuntimeSession:
    return RuntimeSession(
        enrollment_id=enrollment_id,
        generation=generation,
        key_slot=key_slot,
        capabilities=capabilities,
        client_ip=client_ip,
        close_impl=_noop_close,
    )


def test_hub_capabilities_are_the_fs_exec_pty_set() -> None:
    assert HUB_CAPABILITIES == ("fs", "exec", "pty")


def test_default_quotas_come_from_protocol_constants() -> None:
    assert DEFAULT_QUOTAS["frame_cap_bytes"] == protocol.FRAME_CAP_BYTES
    assert DEFAULT_QUOTAS["chunk_bytes"] == protocol.CHUNK_BYTES
    assert DEFAULT_QUOTAS["initial_credit_bytes"] == protocol.INITIAL_CREDIT_BYTES
    assert DEFAULT_QUOTAS["exec_queue_bytes"] == protocol.EXEC_QUEUE_BYTES
    assert DEFAULT_QUOTAS["fs_stream_queue_chunks"] == protocol.FS_STREAM_QUEUE_CHUNKS
    assert DEFAULT_QUOTAS["control_rate_cap_fps"] == protocol.CONTROL_RATE_CAP_FPS
    assert DEFAULT_QUOTAS["data_rate_cap_fps"] == protocol.DATA_RATE_CAP_FPS


def test_registry_module_imports_no_api_layer() -> None:
    """The registry is infra: it must never pull in the api package."""
    source = Path(inspect.getsourcefile(RuntimeSessionRegistry) or "").read_text(encoding="utf-8")
    assert "octop.api" not in source
    assert "octop.cli" not in source


async def test_register_get_online_remove() -> None:
    reg = RuntimeSessionRegistry()
    session = _session()
    reg.register(session)

    assert reg.online("e1") is True
    assert reg.online("other") is False
    assert reg.get("e1") is session
    assert len(reg) == 1

    reg.remove(session)
    assert reg.online("e1") is False
    assert reg.get("e1") is None
    assert len(reg) == 0
    # Removal is idempotent.
    reg.remove(session)
    assert len(reg) == 0


async def test_overlap_sessions_coexist_and_get_prefers_newest_live() -> None:
    reg = RuntimeSessionRegistry()
    previous = _session(key_slot="previous", generation=0)
    current = _session(key_slot="current")
    reg.register(previous)
    reg.register(current)

    assert len(reg.sessions("e1")) == 2
    assert reg.get("e1") is current
    assert reg.online("e1") is True

    reg.remove(current)
    assert reg.get("e1") is previous
    assert reg.online("e1") is True


async def test_closed_sessions_are_not_online_and_get_skips_them() -> None:
    reg = RuntimeSessionRegistry()
    dead = _session()
    alive = _session()
    reg.register(dead)
    reg.register(alive)
    await dead.close_with(CLOSE_SUPERSEDED, "superseded")

    assert reg.online("e1") is True
    assert reg.get("e1") is alive
    assert len(reg.sessions("e1")) == 1  # only the live one is listed


async def test_close_with_is_idempotent_and_invokes_close_impl_once() -> None:
    calls: list[tuple[int, str]] = []

    async def close_impl(code: int, reason: str) -> None:
        calls.append((code, reason))

    session = RuntimeSession(
        enrollment_id="e1",
        generation=1,
        key_slot="current",
        capabilities=("fs",),
        client_ip="10.0.0.1",
        close_impl=close_impl,
    )
    await session.close_with(CLOSE_SUPERSEDED, "superseded")
    await session.close_with(1000, "ignored")

    assert calls == [(CLOSE_SUPERSEDED, "superseded")]
    assert session.close_code == CLOSE_SUPERSEDED
    assert session.close_reason == "superseded"
    assert session.closed.is_set()
    assert session.is_live is False


async def test_closed_event_unblocks_waiters() -> None:
    session = _session()
    waiter = asyncio.ensure_future(session.closed.wait())
    await session.close_with(1000, "bye")
    assert await asyncio.wait_for(waiter, timeout=1) is True


async def test_frame_handler_plug_and_unplug() -> None:
    session = _session()
    assert session.frame_handler is None

    handled: list[str] = []

    async def handler(frame: object) -> None:
        del frame
        handled.append("x")

    session.set_frame_handler(handler)
    assert session.frame_handler is handler
    assert handler is not None
    await session.frame_handler(object())  # type: ignore[arg-type]
    assert handled == ["x"]

    session.set_frame_handler(None)
    assert session.frame_handler is None


async def test_memory_stays_bounded_under_churn() -> None:
    reg = RuntimeSessionRegistry()
    for i in range(1000):
        session = _session(enrollment_id=f"e{i % 5}")
        reg.register(session)
        await session.close_with(1005, "gone")
        reg.remove(session)
        assert len(reg) == 0

    # Closed sessions that were never removed are swept lazily on register,
    # so abandoned connections cannot pin memory.
    for i in range(600):
        dead = _session(enrollment_id="churn", generation=i)
        await dead.close_with(1005, "gone")
        reg.register(dead)
    assert len(reg) <= RuntimeSessionRegistry._SWEEP_THRESHOLD
    assert reg.online("churn") is False
    reg.register(_session(enrollment_id="churn"))
    assert reg.online("churn") is True  # live sessions survive the sweep


async def test_online_enrollment_ids() -> None:
    reg = RuntimeSessionRegistry()
    reg.register(_session(enrollment_id="a"))
    reg.register(_session(enrollment_id="b"))
    dead = _session(enrollment_id="c")
    reg.register(dead)
    await dead.close_with(1005, "gone")

    assert reg.online_enrollment_ids() == {"a", "b"}
