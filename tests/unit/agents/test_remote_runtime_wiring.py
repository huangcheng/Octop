"""remote_runtime backend wiring through AgentManager (spec §7.2 seam).

Covers the wave-5 glue: finalized remote_runtime specs resolve against the
enrollment repo, ``_backend_workspace_for_row`` swaps the harness dict for the
prebuilt hub-side proxy, and ``_build_harness_config`` reuses that instance as
the harness backend instead of re-resolving the spec.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from octop.config import OctopConfig
from octop.infra.agents.manager import AgentManager
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import build_shared_services
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.utils.paths import PathLayout


class _StubHub:
    """Minimal RuntimeBackendHub stand-in returning an identity-checkable proxy."""

    def __init__(self) -> None:
        self.backend = object()
        self.calls: list[dict[str, Any]] = []

    def backend_for(
        self,
        enrollment_id: str,
        *,
        allow_exec: bool,
        workspace_base: str,
        agent_root: str = "auto",
        agent_id: str | None = None,
    ) -> Any:
        self.calls.append(
            {
                "enrollment_id": enrollment_id,
                "allow_exec": allow_exec,
                "workspace_base": workspace_base,
                "agent_root": agent_root,
                "agent_id": agent_id,
            }
        )
        return self.backend


@pytest.fixture
def services(tmp_path: Path) -> SimpleNamespace:
    paths = PathLayout(tmp_path / ".octop")
    paths.ensure_root()
    db = SqlitePool(paths.db)
    run_migrations(db)
    built = build_shared_services(db=db, paths=paths, config=OctopConfig())
    owner_id = built.repos.user_repo.create(username="owner", password_hash="x", role="user")
    other_id = built.repos.user_repo.create(username="other", password_hash="x", role="user")
    return SimpleNamespace(
        repos=built.repos, paths=built.paths, owner_id=owner_id, other_id=other_id
    )


def _enroll(services: Any, *, enrollment_id: str = "E1", allow_exec: bool = True) -> None:
    services.repos.runtime_enrollment_repo.create(
        enrollment_id=enrollment_id,
        owner_user_id=services.owner_id,
        display_name="machine",
        key_hash="hash",
        allow_exec=allow_exec,
        default_workspace_base="/machines/ws",
        active_cap=10,
    )


def _agent_row(
    services: Any, *, agent_id: str = "AG1", user_id: int | None = None, enrollment: str = "E1"
) -> Any:
    services.repos.agent_repo.create(
        agent_id=agent_id,
        user_id=user_id,
        name="remote-agent",
        config_json=json.dumps(
            {
                "backend": {
                    "type": "remote_runtime",
                    "enrollment_id": enrollment,
                    "agent_root": "shared",
                }
            }
        ),
    )
    row = services.repos.agent_repo.get(agent_id)
    assert row is not None
    return row


def test_remote_runtime_spec_resolves_enrollment_state(services: Any) -> None:
    _enroll(services, allow_exec=True)
    _agent_row(services)
    manager = AgentManager(repos=services.repos, paths=services.paths)

    spec = manager.resolved_backend_spec("AG1")

    assert spec["type"] == "remote_runtime"
    # allow_exec + workspace_base default came from the enrollment row via the
    # resolver (spec §7.1), not from the hub.
    assert spec["allow_exec"] is True
    assert spec["workspace_base"] == "/machines/ws"


def test_workspace_uses_prebuilt_hub_instance(services: Any) -> None:
    _enroll(services)
    row = _agent_row(services)
    hub = _StubHub()
    manager = AgentManager(repos=services.repos, paths=services.paths, runtime_hub=hub)

    ws = manager._backend_workspace_for_row(row)

    assert ws.backend is hub.backend
    assert hub.calls == [
        {
            "enrollment_id": "E1",
            "allow_exec": True,
            "workspace_base": "/machines/ws",
            "agent_root": "shared",
            "agent_id": "AG1",
        }
    ]


def test_harness_config_reuses_prebuilt_instance(services: Any) -> None:
    _enroll(services)
    row = _agent_row(services)
    hub = _StubHub()
    manager = AgentManager(repos=services.repos, paths=services.paths, runtime_hub=hub)

    cfg = manager._build_harness_config(row)

    assert cfg.backend is hub.backend


def test_workspace_without_hub_fails_fast(services: Any) -> None:
    _enroll(services)
    row = _agent_row(services)
    manager = AgentManager(repos=services.repos, paths=services.paths)

    with pytest.raises(OctopError) as exc_info:
        manager._backend_workspace_for_row(row)
    assert exc_info.value.code is ErrorCode.RUNTIME_UNSUPPORTED


def test_enrollment_ownership_enforced_at_resolution(services: Any) -> None:
    _enroll(services)  # owner_id
    row = _agent_row(services, agent_id="AG2", user_id=services.other_id)
    manager = AgentManager(repos=services.repos, paths=services.paths, runtime_hub=_StubHub())

    with pytest.raises(OctopError) as exc_info:
        manager._backend_workspace_for_row(row)
    assert exc_info.value.code is ErrorCode.FORBIDDEN


def test_revoked_enrollment_rejected_at_resolution(services: Any) -> None:
    services.repos.runtime_enrollment_repo.create(
        enrollment_id="REV",
        owner_user_id=services.owner_id,
        display_name="machine-revoked",
        key_hash="hash2",
        status="revoked",
        active_cap=10,
    )
    row = _agent_row(services, agent_id="AG3", enrollment="REV")
    manager = AgentManager(repos=services.repos, paths=services.paths, runtime_hub=_StubHub())

    with pytest.raises(OctopError) as exc_info:
        manager._backend_workspace_for_row(row)
    assert exc_info.value.code is ErrorCode.RUNTIME_UNSUPPORTED
