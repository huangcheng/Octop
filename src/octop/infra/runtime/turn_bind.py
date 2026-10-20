"""Build the frozen turn backend for a resolved ``run_target`` (§6.2)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from deepagents.backends.protocol import SandboxBackendProtocol

from octop.infra.runtime.backend import RuntimeBackendHub
from octop.infra.runtime.delegating import (
    FailClosedBackend,
    PrefixSplitRemoteBackend,
    TurnDelegatingBackend,
    build_fail_closed_revoked,
    build_machine_turn_backend,
)
from octop.infra.runtime.turn_routing import ResolvedRunTarget


def hub_filesystem_backend(workspace_dir: str | Path) -> Any:
    """Local filesystem backend for hub-routable prefixes (``.octop``, ``inbound``)."""
    from octop_harness.backends import resolve_backend  # noqa: PLC0415

    root = Path(workspace_dir).expanduser().resolve()
    return resolve_backend(
        {"type": "filesystem", "root_dir": str(root), "virtual_mode": True},
        workspace_dir=root,
    )


def unwrap_hub_backend(backend: Any) -> Any:
    """Return the hub leg under a :class:`TurnDelegatingBackend`, else *backend*."""
    if isinstance(backend, TurnDelegatingBackend):
        return backend.hub
    return backend


def bind_turn_backend(
    resolved: ResolvedRunTarget,
    *,
    hub_backend: Any,
    runtime_hub: RuntimeBackendHub | None,
    enrollment_repo: Any | None = None,
    agent_id: str | None = None,
    workspace_dir: str | Path | None = None,
) -> SandboxBackendProtocol | FailClosedBackend | PrefixSplitRemoteBackend | Any:
    """Return the backend to freeze into the turn ContextVar.

    * ``hub`` / unset → return ``hub_backend`` (caller may skip ContextVar set)
    * ``invalid`` / fail_closed → :class:`FailClosedBackend`
    * enrollment → prefix-split remote + hub filesystem (offline remote raises lazily)
    """
    if resolved.fail_closed:
        return build_fail_closed_revoked()
    if resolved.enrollment_id is None:
        return hub_backend

    if runtime_hub is None:
        return build_fail_closed_revoked()

    eid = resolved.enrollment_id
    allow_exec = False
    workspace_base = ""
    agent_root = "auto"
    if enrollment_repo is not None:
        row = enrollment_repo.get_by_enrollment_id(eid)
        if row is not None:
            allow_exec = bool(getattr(row, "allow_exec", False))
            workspace_base = str(getattr(row, "default_workspace_base", "") or "")
    remote = runtime_hub.backend_for(
        eid,
        allow_exec=allow_exec,
        workspace_base=workspace_base,
        agent_root=agent_root,
        agent_id=agent_id,
    )
    # Prefer an explicit local filesystem for hub prefixes so ``.octop`` never
    # hits a remote_runtime hub leg that rejects system paths.
    hub_leg: Any = hub_backend
    if workspace_dir is not None:
        hub_leg = hub_filesystem_backend(workspace_dir)
    else:
        hub_leg = unwrap_hub_backend(hub_backend)
    return build_machine_turn_backend(remote=remote, hub=hub_leg)
