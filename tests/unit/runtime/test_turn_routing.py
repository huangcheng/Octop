"""Unit tests for turn-scoped run_target resolution and delegating backends."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.delegating import (
    FailClosedBackend,
    PrefixSplitRemoteBackend,
    TurnDelegatingBackend,
    build_fail_closed_revoked,
    get_turn_backend,
    turn_backend_scope,
)
from octop.infra.runtime.turn_routing import (
    RUN_TARGET_HUB,
    RUN_TARGET_INVALID,
    default_thread_run_target,
    effective_run_target,
    is_hub_routable_path,
    normalize_run_target,
    resolve_dashboard_run_target,
    tool_time_runtime_error,
)


class _Hub:
    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> tuple[str, str]:
        return ("hub", file_path)

    def execute(self, command: str, *, timeout: int | None = None) -> tuple[str, str]:
        return ("hub-exec", command)


class _Remote:
    id = "remote:E1"

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> tuple[str, str]:
        return ("remote", file_path)

    def execute(self, command: str, *, timeout: int | None = None) -> tuple[str, str]:
        return ("remote-exec", command)


def test_normalize_and_effective() -> None:
    assert normalize_run_target(None) is None
    assert normalize_run_target("hub") is None
    assert normalize_run_target("  ") is None
    assert normalize_run_target("E1") == "E1"
    assert effective_run_target(None) == RUN_TARGET_HUB
    assert effective_run_target("invalid") == RUN_TARGET_INVALID


def test_hub_routable_paths() -> None:
    assert is_hub_routable_path("/.octop/skills/x")
    assert is_hub_routable_path(".octop/auth/token")
    assert is_hub_routable_path("/inbound/a.png")
    assert not is_hub_routable_path("/SOUL.md")
    assert not is_hub_routable_path("/skills/user/SKILL.md")


def test_resolve_dashboard_run_target_authz() -> None:
    repo = SimpleNamespace(
        get_by_enrollment_id=lambda eid: SimpleNamespace(
            enrollment_id=eid, status="active", owner_user_id=7
        )
    )
    resolved = resolve_dashboard_run_target("E1", enrollment_repo=repo, acting_user_id=7)
    assert resolved.enrollment_id == "E1"
    assert not resolved.fail_closed

    with pytest.raises(OctopError) as exc:
        resolve_dashboard_run_target("E1", enrollment_repo=repo, acting_user_id=99)
    assert exc.value.code is ErrorCode.RUNTIME_FORBIDDEN_TARGET

    revoked = SimpleNamespace(
        get_by_enrollment_id=lambda eid: SimpleNamespace(
            enrollment_id=eid, status="revoked", owner_user_id=7
        )
    )
    with pytest.raises(OctopError) as exc2:
        resolve_dashboard_run_target("E1", enrollment_repo=revoked, acting_user_id=7)
    assert exc2.value.code is ErrorCode.RUNTIME_REVOKED

    invalid = resolve_dashboard_run_target("invalid", enrollment_repo=repo, acting_user_id=7)
    assert invalid.fail_closed


def test_default_thread_run_target_q1() -> None:
    repo = SimpleNamespace(
        get_by_enrollment_id=lambda eid: (
            SimpleNamespace(enrollment_id=eid, status="active", owner_user_id=1)
            if eid == "E1"
            else None
        )
    )
    assert (
        default_thread_run_target(agent_default="E1", enrollment_repo=repo, acting_user_id=1)
        == "E1"
    )
    assert (
        default_thread_run_target(agent_default="E1", enrollment_repo=repo, acting_user_id=2)
        is None
    )
    assert (
        default_thread_run_target(agent_default="missing", enrollment_repo=repo, acting_user_id=1)
        is None
    )


def test_tool_time_offline_skips_hub_paths() -> None:
    resolved = resolve_dashboard_run_target(
        "E1",
        enrollment_repo=SimpleNamespace(
            get_by_enrollment_id=lambda eid: SimpleNamespace(
                enrollment_id=eid, status="active", owner_user_id=1
            )
        ),
        acting_user_id=1,
    )
    assert tool_time_runtime_error(resolved, online=False, path="/.octop/x") is None
    err = tool_time_runtime_error(resolved, online=False, path="/SOUL.md")
    assert err is not None and err.code is ErrorCode.RUNTIME_OFFLINE


def test_prefix_split_routes_and_exec() -> None:
    split = PrefixSplitRemoteBackend(_Remote(), _Hub())  # type: ignore[arg-type]
    assert split.read("/SOUL.md") == ("remote", "/SOUL.md")
    assert split.read("/.octop/skills/a") == ("hub", "/.octop/skills/a")
    assert split.execute("ls") == ("remote-exec", "ls")


def test_fail_closed_and_turn_delegate_contextvar() -> None:
    closed = build_fail_closed_revoked()
    assert isinstance(closed, FailClosedBackend)
    result = closed.read("/SOUL.md")
    assert result.error and "invalid" in result.error.lower()

    hub = _Hub()
    delegate = TurnDelegatingBackend(hub)
    assert delegate.read("/x") == ("hub", "/x")
    remote = _Remote()
    with turn_backend_scope(remote):  # type: ignore[arg-type]
        assert get_turn_backend() is remote
        assert delegate.read("/x") == ("remote", "/x")
        assert delegate.execute("echo") == ("remote-exec", "echo")
    assert get_turn_backend() is None
    assert delegate.read("/x") == ("hub", "/x")


def test_runtime_hub_invalidate() -> None:
    from octop.infra.runtime.backend import RuntimeBackendHub
    from octop.infra.runtime.registry import RuntimeSessionRegistry

    hub = RuntimeBackendHub(RuntimeSessionRegistry(), enrollment_repo=None)
    # Populate cache via backend_for without enrollment checks (repo=None).
    b1 = hub.backend_for("E1", allow_exec=True, workspace_base="/ws", agent_id="A1")
    b2 = hub.backend_for("E1", allow_exec=True, workspace_base="/ws", agent_id="A1")
    assert b1 is b2
    assert hub.invalidate("E1") == 1
    b3 = hub.backend_for("E1", allow_exec=True, workspace_base="/ws", agent_id="A1")
    assert b3 is not b1
