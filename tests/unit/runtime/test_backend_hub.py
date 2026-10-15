"""Unit tests for the hub-side RuntimeBackendHub cache (spec §7.2)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.backend import RemoteRuntimeBackend, RuntimeBackendHub
from octop.infra.runtime.registry import RuntimeSession, RuntimeSessionRegistry


def _row(status: str = "active") -> SimpleNamespace:
    return SimpleNamespace(status=status)


class _FakeRepo:
    def __init__(self, rows: dict[str, Any]) -> None:
        self.rows = rows
        self.reads = 0

    def get_by_enrollment_id(self, enrollment_id: str) -> Any:
        self.reads += 1
        return self.rows.get(enrollment_id)


async def _noop_close(code: int, reason: str) -> None:  # pragma: no cover
    del code, reason


def _session(enrollment_id: str = "E1") -> RuntimeSession:
    return RuntimeSession(
        enrollment_id=enrollment_id,
        generation=1,
        key_slot="current",
        capabilities=("fs",),
        client_ip="127.0.0.1",
        close_impl=_noop_close,
    )


async def test_link_for_binds_registered_session_send_hook_and_loop() -> None:
    registry = RuntimeSessionRegistry()
    hub = RuntimeBackendHub(registry)
    session = _session()
    registry.register(session)
    # Registered but the session loop does not own the socket yet: offline.
    assert hub.link_for("E1") is None

    async def send(text: str) -> None:  # pragma: no cover - never invoked here
        del text

    session.set_sender(send)
    link = hub.link_for("E1")
    assert link is not None
    assert link.session is session
    assert link.send is send
    assert link.loop is session.loop

    registry.remove(session)
    session.closed.set()
    assert hub.link_for("E1") is None


async def test_backend_for_caches_by_enrollment_and_content_root() -> None:
    registry = RuntimeSessionRegistry()
    repo = _FakeRepo({"E1": _row()})
    hub = RuntimeBackendHub(registry, enrollment_repo=repo)

    shared_a = hub.backend_for("E1", allow_exec=True, workspace_base="/ws", agent_root="shared")
    shared_b = hub.backend_for("E1", allow_exec=True, workspace_base="/ws", agent_root="shared")
    assert shared_a is shared_b
    assert isinstance(shared_a, RemoteRuntimeBackend)
    # Cache hit: the enrollment repo is not re-read (spec §7.2 wiring note).
    assert repo.reads == 1

    agent_one = hub.backend_for(
        "E1", allow_exec=True, workspace_base="/ws", agent_root="auto", agent_id="AG1"
    )
    agent_two = hub.backend_for(
        "E1", allow_exec=True, workspace_base="/ws", agent_root="auto", agent_id="AG2"
    )
    assert agent_one is not agent_two
    assert agent_one is not shared_a
    assert agent_one.content_root == "/ws/agents/AG1"
    assert agent_two.content_root == "/ws/agents/AG2"


async def test_backend_for_checks_status_only_at_cache_miss() -> None:
    registry = RuntimeSessionRegistry()
    repo = _FakeRepo({"E1": _row(), "GONE": None, "DEAD": _row(status="revoked")})
    hub = RuntimeBackendHub(registry, enrollment_repo=repo)

    with pytest.raises(OctopError) as exc_info:
        hub.backend_for("GONE", allow_exec=False, workspace_base="/ws")
    assert exc_info.value.code is ErrorCode.NOT_FOUND

    with pytest.raises(OctopError) as exc_info:
        hub.backend_for("DEAD", allow_exec=False, workspace_base="/ws")
    assert exc_info.value.code is ErrorCode.RUNTIME_UNSUPPORTED

    backend = hub.backend_for("E1", allow_exec=False, workspace_base="/ws")
    assert backend.allow_exec is False
    # Revocation after caching: spec-finalize re-resolution upstream is the
    # guard; the cached stable proxy keeps failing ops offline instead.
    repo.rows["E1"] = _row(status="revoked")
    assert hub.backend_for("E1", allow_exec=False, workspace_base="/ws") is backend


async def test_backend_for_cache_is_bounded() -> None:
    registry = RuntimeSessionRegistry()
    hub = RuntimeBackendHub(registry, enrollment_repo=_FakeRepo({"E1": _row()}))
    for index in range(RuntimeBackendHub.MAX_CACHED_BACKENDS + 8):
        hub.backend_for(
            "E1", allow_exec=True, workspace_base="/ws", agent_root="auto", agent_id=f"A{index}"
        )
    assert len(hub._backends) <= RuntimeBackendHub.MAX_CACHED_BACKENDS


async def test_backend_for_without_repo_constructs_offline_safe_proxy() -> None:
    hub = RuntimeBackendHub(RuntimeSessionRegistry())
    backend = hub.backend_for("E1", allow_exec=True, workspace_base="/ws")
    assert isinstance(backend, RemoteRuntimeBackend)
    assert backend.id == "remote_runtime:E1"
