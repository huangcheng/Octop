"""Resolve agent ``backend`` config (named refs, composite routes)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from octop.infra.backend.adapter import row_to_backend_spec
from octop.infra.errors import ErrorCode, OctopError


def default_agent_backend_spec(workspace_dir: Path) -> dict[str, Any]:
    """Harness backend for agents with no explicit ``backend`` in config.

    On POSIX this matches harness ``DEFAULT_BACKEND_SPEC`` (host-rooted virtual
    paths). harness ``resolve_backend(..., workspace_dir=..., system_files_path=...)``
    wraps that host root in a composite whose ``artifacts_root`` is the agent
    workspace system-files prefix (Octop: ``.octop``), so deepagents conversation
    history stays with skills / sessions. On Windows ``root_dir='/'`` resolves to
    the process current-drive root, which often differs from the drive hosting
    ``workspace_dir`` — scope the default to the agent workspace instead.
    """
    from octop_harness.backends import DEFAULT_BACKEND_SPEC  # noqa: PLC0415

    if os.name == "nt":
        return {
            "type": "local_shell",
            "root_dir": str(workspace_dir.resolve()),
            "virtual_mode": True,
        }
    return dict(DEFAULT_BACKEND_SPEC)


def windows_neutralize_host_root(spec: Any, *, workspace_dir: Path) -> Any:
    """Windows: rewrite local backends rooted at ``/`` to the agent workspace.

    ``root_dir: "/"`` is the dashboard's default for local backends. On Windows
    it resolves to the *current drive root* (often a different drive than the
    agent workspace), so deepagents virtual-path checks reject every workspace
    path with ``Path ... outside root directory``. Only ``root_dir`` is rewritten
    so ``type`` / ``virtual_mode`` and other fields stay intact.

    Applies to top-level ``local_shell`` / ``filesystem`` specs and to the
    ``default`` of composite specs (the default is what anchors the agent
    workspace). Route sub-backends are user-pinned and left untouched — a route
    root of ``/`` is the caller's explicit choice and harmless to loading.
    """
    if os.name != "nt":
        return spec
    if not isinstance(spec, dict):
        return spec
    kind = spec.get("type")
    if kind in ("local_shell", "filesystem") and _is_host_root(spec.get("root_dir")):
        return {**spec, "root_dir": str(workspace_dir.resolve())}
    if (
        kind == "composite"
        and isinstance(spec.get("default"), dict)
        and _is_host_root(spec["default"].get("root_dir"))
    ):
        default = spec["default"]
        return {
            **spec,
            "default": {**default, "root_dir": str(workspace_dir.resolve())},
        }
    return spec


def _is_host_root(root: Any) -> bool:
    """True when *root* is an explicitly host-rooted local backend path.

    Only an *explicit* ``/``, ``\\`` or empty string counts (the dashboard's
    default). A missing ``root_dir`` is left alone — harness already falls back
    to ``workspace_dir`` for local backends, which is workspace-scoped by
    design.
    """
    return root is not None and str(root).strip() in ("/", "\\", "")


def resolve_agent_backend_spec(
    spec: Any,
    *,
    repo: Any | None,
    owner_user_id: int | None = None,
    enrollment_repo: Any | None = None,
    is_admin: bool = False,
) -> Any:
    """Expand ``named`` refs (and composite trees) into harness-ready specs.

    ``remote_runtime`` specs are finalized against ``enrollment_repo``: the
    referenced enrollment must exist, be active, and belong to
    ``owner_user_id`` (admin bypass) — enforced here, at spec-expansion time
    (spec §7.1 authorization).  The spec gains ``allow_exec`` (and a
    workspace_base default) from the enrollment row so the pure
    :func:`backend_spec_supports_execution` can gate execution without a
    repo.  Without ``enrollment_repo`` the spec resolves execution-disabled.
    """
    if spec is None:
        return None
    if not isinstance(spec, dict):
        return spec

    kind = spec.get("type")
    if kind == "named":
        name = spec.get("name")
        if not repo or not name:
            raise ValueError("named backend requires a configured storage backend name")
        row = repo.get_by_name(str(name))
        if row is None:
            raise ValueError(f"storage backend {name!r} not found")
        inner = row_to_backend_spec(row)
        if inner is None:
            raise ValueError(f"storage backend {name!r} is incomplete")
        return resolve_agent_backend_spec(
            inner,
            repo=repo,
            owner_user_id=owner_user_id,
            enrollment_repo=enrollment_repo,
            is_admin=is_admin,
        )

    if kind == "composite":
        default = spec.get("default")
        if default is None:
            raise ValueError("composite backend requires a default sub-spec")
        routes = spec.get("routes") or {}
        if not isinstance(routes, dict):
            raise ValueError("composite backend routes must be a dict")
        out: dict[str, Any] = {
            "type": "composite",
            "default": resolve_agent_backend_spec(
                default,
                repo=repo,
                owner_user_id=owner_user_id,
                enrollment_repo=enrollment_repo,
                is_admin=is_admin,
            ),
            "routes": {
                str(prefix): resolve_agent_backend_spec(
                    sub,
                    repo=repo,
                    owner_user_id=owner_user_id,
                    enrollment_repo=enrollment_repo,
                    is_admin=is_admin,
                )
                for prefix, sub in routes.items()
            },
        }
        exec_target = spec.get("exec_target")
        if exec_target is not None:
            # Octop-reserved key: which sub-backend receives ``execute``.
            # deepagents composites always execute on their default, so the
            # agent wiring wraps the harness-resolved composite in
            # ExecTargetCompositeBackend (infra/runtime/backend.py); harness
            # resolve_backend must never see the key (split_exec_target).
            out["exec_target"] = resolve_agent_backend_spec(
                exec_target,
                repo=repo,
                owner_user_id=owner_user_id,
                enrollment_repo=enrollment_repo,
                is_admin=is_admin,
            )
        return out

    cleaned = dict(spec)
    if kind not in ("named", "composite") and "name" in cleaned:
        del cleaned["name"]
    if kind == "postgres":
        from octop.infra.backend.postgres_spec import normalize_postgres_spec

        return normalize_postgres_spec(cleaned)
    if kind == "remote_runtime":
        return _finalize_remote_runtime_spec(
            cleaned,
            owner_user_id=owner_user_id,
            enrollment_repo=enrollment_repo,
            is_admin=is_admin,
        )
    return cleaned


def _finalize_remote_runtime_spec(
    spec: dict[str, Any],
    *,
    owner_user_id: int | None,
    enrollment_repo: Any | None,
    is_admin: bool,
) -> dict[str, Any]:
    """Attach enrollment state (allow_exec, base default) + ownership check."""
    enrollment_id = str(spec.get("enrollment_id") or "").strip()
    if not enrollment_id:
        raise OctopError(ErrorCode.NOT_FOUND, "remote_runtime backend requires an enrollment_id")
    spec["enrollment_id"] = enrollment_id
    if enrollment_repo is None:
        # No enrollment store wired (e.g. pure spec plumbing): resolve
        # execution-disabled rather than guessing capabilities.
        spec["allow_exec"] = False
        return spec
    row = enrollment_repo.get_by_enrollment_id(enrollment_id)
    if row is None:
        raise OctopError(ErrorCode.NOT_FOUND, f"runtime enrollment {enrollment_id!r} not found")
    if not is_admin and row.owner_user_id != owner_user_id:
        raise OctopError(
            ErrorCode.FORBIDDEN,
            f"runtime enrollment {enrollment_id!r} belongs to another user",
        )
    if str(row.status) != "active":
        raise OctopError(
            ErrorCode.RUNTIME_UNSUPPORTED,
            f"runtime enrollment {enrollment_id!r} is {row.status}",
        )
    spec["allow_exec"] = bool(row.allow_exec)
    if not spec.get("workspace_base") and row.default_workspace_base:
        spec["workspace_base"] = row.default_workspace_base
    return spec


def split_exec_target(spec: Any) -> tuple[Any, Any | None]:
    """Split a resolved composite spec into (harness spec, exec sub-spec).

    ``exec_target`` is an Octop-reserved composite key; the harness builder
    would pass it into ``MountedCompositeBackend`` and crash, so callers pass
    the stripped spec to ``resolve_backend`` and construct
    ``ExecTargetCompositeBackend`` from the returned exec sub-spec.
    """
    if not isinstance(spec, dict) or "exec_target" not in spec:
        return spec, None
    stripped = dict(spec)
    return stripped, stripped.pop("exec_target")


def collect_named_storage_backend_refs(spec: Any) -> set[str]:
    """Collect ``storage_backends.name`` values referenced by an agent backend spec tree."""
    if spec is None:
        return set()
    if isinstance(spec, str):
        if spec.startswith("named:"):
            return {spec.split(":", 1)[1]}
        return set()
    if not isinstance(spec, dict):
        return set()
    kind = spec.get("type")
    if kind == "named":
        name = spec.get("name")
        return {str(name)} if name else set()
    if kind == "composite":
        out: set[str] = set()
        out |= collect_named_storage_backend_refs(spec.get("default"))
        routes = spec.get("routes")
        if isinstance(routes, dict):
            for sub in routes.values():
                out |= collect_named_storage_backend_refs(sub)
        return out
    return set()


def rewrite_named_storage_backend_refs(spec: Any, *, old: str, new: str) -> Any:
    """Return a copy of *spec* with named storage refs rewritten from *old* → *new*."""
    if old == new:
        return spec
    if spec is None:
        return spec
    if isinstance(spec, str):
        if spec == f"named:{old}":
            return f"named:{new}"
        return spec
    if not isinstance(spec, dict):
        return spec
    kind = spec.get("type")
    if kind == "named":
        if spec.get("name") == old:
            return {**spec, "name": new}
        return spec
    if kind == "composite":
        out = dict(spec)
        if "default" in out:
            out["default"] = rewrite_named_storage_backend_refs(
                out.get("default"),
                old=old,
                new=new,
            )
        routes = out.get("routes")
        if isinstance(routes, dict):
            out["routes"] = {
                key: rewrite_named_storage_backend_refs(value, old=old, new=new)
                for key, value in routes.items()
            }
        return out
    return spec


def find_agents_using_storage_backend(
    *,
    agent_repo: Any,
    get_config: Any,
    backend_name: str,
) -> list[dict[str, str]]:
    """Return agents whose ``config.backend`` references *backend_name* (named / composite)."""
    refs: list[dict[str, str]] = []
    for row in agent_repo.list_all():
        cfg = get_config(row.agent_id)
        if not isinstance(cfg, dict):
            continue
        if backend_name in collect_named_storage_backend_refs(cfg.get("backend")):
            refs.append({"agent_id": row.agent_id, "name": row.name})
    return refs


def backend_spec_supports_execution(spec: Any) -> bool:
    """True when the backend spec resolves to a sandbox/shell backend.

    deepagents ``FilesystemMiddleware`` rejects filesystem ``permissions`` when
    the backend implements ``SandboxBackendProtocol`` (e.g. ``local_shell``).
    ``remote_runtime`` counts only when the enrollment granted ``allow_exec``
    (resolved onto the spec at adapter time); composites also honor their
    ``exec_target`` sub-spec.
    """
    if spec is None:
        return True
    if isinstance(spec, str):
        return spec in {"local_shell", "docker", "opensandbox"}
    if not isinstance(spec, dict):
        return False
    kind = spec.get("type")
    if kind in {"local_shell", "docker", "opensandbox"}:
        return True
    if kind == "remote_runtime":
        return bool(spec.get("allow_exec"))
    if kind == "composite":
        if backend_spec_supports_execution(spec.get("default")):
            return True
        routes = spec.get("routes")
        if isinstance(routes, dict) and any(
            backend_spec_supports_execution(route) for route in routes.values()
        ):
            return True
        exec_target = spec.get("exec_target")
        if exec_target is not None:
            return backend_spec_supports_execution(exec_target)
        return False
    return False
