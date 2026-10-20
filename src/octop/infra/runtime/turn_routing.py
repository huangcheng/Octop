"""Turn-scoped run-target resolution (spec §5.2 / §6.2).

``threads.run_target`` is the sole live switch for dashboard chat. Values:

* ``NULL`` / ``\"hub\"`` → hub backends
* enrollment ULID → remote machine (+ hub prefix-split for system paths)
* ``\"invalid\"`` → fail-closed ``RUNTIME_REVOKED`` (never silently hub)

IM / cron / gateway turns never consult ``run_target`` — always hub.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from octop.infra.errors import ErrorCode, OctopError

#: Sentinel stored on threads after revoke; reserved (not a valid ULID shape).
RUN_TARGET_INVALID = "invalid"
#: Explicit hub selection (equivalent to NULL for routing).
RUN_TARGET_HUB = "hub"

EffectiveRunTarget = Literal["hub", "invalid"] | str  # str = enrollment_id

#: Path prefixes that stay on the hub even when ``run_target`` is a machine.
#: Leading-slash form; matching is path-segment aware.
HUB_ROUTE_PREFIXES: tuple[str, ...] = ("/.octop/", "/inbound/")


@dataclass(frozen=True)
class ResolvedRunTarget:
    """Frozen routing decision for one turn."""

    #: Normalized effective target recorded on turn projection.
    effective: EffectiveRunTarget
    #: Enrollment id when ``effective`` is a machine; else ``None``.
    enrollment_id: str | None
    #: True when every workspace file/exec tool must fail ``RUNTIME_REVOKED``.
    fail_closed: bool


def normalize_run_target(raw: str | None) -> str | None:
    """Return canonical storage form, or ``None`` for hub-equivalent."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text == RUN_TARGET_HUB:
        return None
    return text


def effective_run_target(raw: str | None) -> EffectiveRunTarget:
    """Map stored ``run_target`` to the value recorded on a turn."""
    normalized = normalize_run_target(raw)
    if normalized is None:
        return RUN_TARGET_HUB
    return normalized


def is_hub_routable_path(path: str) -> bool:
    """True when *path* is hub-local under a machine ``run_target`` (§6.1)."""
    norm = "/" + str(path).replace("\\", "/").lstrip("/")
    for prefix in HUB_ROUTE_PREFIXES:
        root = prefix.rstrip("/")
        if norm == root or norm.startswith(prefix):
            return True
    return False


def resolve_dashboard_run_target(
    raw: str | None,
    *,
    enrollment_repo: Any | None,
    acting_user_id: int,
) -> ResolvedRunTarget:
    """Resolve dashboard ``run_target`` at turn start (authz + fail-closed).

    Does **not** check online presence — offline is a lazy tool-time
    ``RUNTIME_OFFLINE`` so pure-chat turns may still complete (§5.2).
    """
    normalized = normalize_run_target(raw)
    if normalized is None:
        return ResolvedRunTarget(effective=RUN_TARGET_HUB, enrollment_id=None, fail_closed=False)
    if normalized == RUN_TARGET_INVALID:
        return ResolvedRunTarget(effective=RUN_TARGET_INVALID, enrollment_id=None, fail_closed=True)

    if enrollment_repo is None:
        raise OctopError(
            ErrorCode.RUNTIME_FORBIDDEN_TARGET,
            f"runtime enrollment {normalized!r} is not available",
        )
    row = enrollment_repo.get_by_enrollment_id(normalized)
    # Error precedence (§5.2): missing/revoked → REVOKED; foreign → FORBIDDEN.
    if row is None or str(getattr(row, "status", "") or "") != "active":
        raise OctopError(
            ErrorCode.RUNTIME_REVOKED,
            f"runtime enrollment {normalized!r} is revoked or missing",
        )
    owner_id = int(getattr(row, "owner_user_id", getattr(row, "user_id", -1)))
    if owner_id != int(acting_user_id):
        raise OctopError(
            ErrorCode.RUNTIME_FORBIDDEN_TARGET,
            f"runtime enrollment {normalized!r} is not owned by the acting user",
        )
    return ResolvedRunTarget(effective=normalized, enrollment_id=normalized, fail_closed=False)


def tool_time_runtime_error(
    resolved: ResolvedRunTarget,
    *,
    online: bool,
    path: str | None = None,
) -> OctopError | None:
    """Lazy tool-time gate for machine path-class / exec (§5.2 precedence).

    Hub-routable paths never yield ``RUNTIME_OFFLINE``. Returns ``None`` when
    the op may proceed.
    """
    if resolved.fail_closed or resolved.effective == RUN_TARGET_INVALID:
        return OctopError(
            ErrorCode.RUNTIME_REVOKED,
            "runtime target is invalid; re-select hub or another machine",
        )
    if resolved.enrollment_id is None:
        return None
    if path is not None and is_hub_routable_path(path):
        return None
    if not online:
        return OctopError(
            ErrorCode.RUNTIME_OFFLINE,
            f"runtime machine {resolved.enrollment_id!r} is offline",
        )
    return None


def default_thread_run_target(
    *,
    agent_default: str | None,
    enrollment_repo: Any | None,
    acting_user_id: int,
) -> str | None:
    """Q1: seed a new dashboard thread from agent ``default_run_target``.

    Returns an enrollment id when the default is still active+owned; else
    ``None`` (云端电脑 / hub).
    """
    candidate = normalize_run_target(agent_default)
    if candidate is None or candidate == RUN_TARGET_INVALID:
        return None
    if enrollment_repo is None:
        return None
    row = enrollment_repo.get_by_enrollment_id(candidate)
    if row is None or str(getattr(row, "status", "") or "") != "active":
        return None
    owner_id = int(getattr(row, "owner_user_id", getattr(row, "user_id", -1)))
    if owner_id != int(acting_user_id):
        return None
    return candidate
