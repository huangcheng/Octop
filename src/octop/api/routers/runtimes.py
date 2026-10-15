"""HTTP API for remote runtime enrollment management."""

from __future__ import annotations

import hashlib
import secrets
import time
from typing import Any, cast

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from octop.api.deps import current_user, get_server
from octop.infra.db.repos.runtime_enrollments import (
    EnrollmentConflictError,
    EnrollmentQuotaError,
    RuntimeEnrollmentRepo,
    RuntimeEnrollmentRow,
)
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.users.identity import User
from octop.infra.utils.ulid import new_ulid

router = APIRouter()

# Hard cap on simultaneously active enrollments per user (spec §7.1).
MAX_ACTIVE_ENROLLMENTS = 10


class RuntimeIssueBody(BaseModel):
    display_name: str = Field(..., min_length=1, description="Human-readable machine name")
    default_workspace_base: str | None = Field(
        default=None, description="Workspace base path suggested to agents"
    )
    allow_exec: bool = Field(
        default=False, description="Allow unsandboxed shell execution on the machine"
    )


def _repo(server: Any) -> RuntimeEnrollmentRepo:
    if server.services is None:
        raise OctopError(ErrorCode.SETUP_REQUIRED, "services unavailable")
    return cast(RuntimeEnrollmentRepo, server.services.runtime_enrollment_repo)


def _owned(repo: RuntimeEnrollmentRepo, enrollment_id: str, user: User) -> RuntimeEnrollmentRow:
    row = repo.get_by_enrollment_id(enrollment_id)
    if row is None or (row.owner_user_id != user.id and not user.is_admin):
        raise OctopError(ErrorCode.NOT_FOUND, "enrollment not found")
    return row


def _new_key() -> tuple[str, str]:
    """Return ``(plaintext, sha256_hex)`` for a fresh ``rk_`` credential."""
    raw = f"rk_{secrets.token_hex(32)}"
    return raw, hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _public(row: RuntimeEnrollmentRow) -> dict[str, Any]:
    """Never expose key hashes or conflict IP telemetry in list views."""
    return {
        "enrollment_id": row.enrollment_id,
        "machine_id": row.machine_id,
        "display_name": row.display_name,
        "allow_exec": row.allow_exec,
        "default_workspace_base": row.default_workspace_base,
        "status": row.status,
        "generation": row.generation,
        "last_seen_at": row.last_seen_at,
        "rotation_pending": row.previous_key_hash is not None,
        "rotation_expires_at": row.rotation_expires_at,
        "machine_metadata": row.machine_metadata,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.get("/runtimes", summary="List remote runtime enrollments")
async def list_runtimes(
    user: User = Depends(current_user),
    server: Any = Depends(get_server),
) -> list[dict[str, Any]]:
    repo = _repo(server)
    rows = repo.list_all() if user.is_admin else repo.list_for_user(user.id)
    return [_public(r) for r in rows]


@router.post("/runtimes", status_code=201, summary="Issue a runtime enrollment")
async def issue_runtime(
    body: RuntimeIssueBody,
    request: Request,
    user: User = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    repo = _repo(server)
    name = body.display_name.strip()
    if not name:
        raise OctopError(ErrorCode.AGENT_ID_INVALID, "display name required")

    enrollment_id = new_ulid()
    key, key_hash = _new_key()
    try:
        row = repo.create(
            enrollment_id=enrollment_id,
            owner_user_id=user.id,
            display_name=name,
            key_hash=key_hash,
            allow_exec=body.allow_exec,
            default_workspace_base=body.default_workspace_base,
            active_cap=MAX_ACTIVE_ENROLLMENTS,
        )
    except EnrollmentQuotaError as exc:
        raise OctopError(
            ErrorCode.AGENT_QUOTA_EXCEEDED,
            "active runtime enrollment limit reached",
            details={"limit": MAX_ACTIVE_ENROLLMENTS},
            status=409,
        ) from exc
    except EnrollmentConflictError as exc:
        raise OctopError(
            ErrorCode.BRIDGE_DISPLAY_NAME_TAKEN, "display name taken", status=409
        ) from exc

    base = str(request.base_url).rstrip("/")
    enroll_command = (
        f"octop runtime enroll --server {base} --key-file <path-to-key-file> "
        f"--ca-bundle <path-to-ca-bundle>"
    )
    return {
        **_public(row),
        "enroll_command": enroll_command,
        "key_shown_once": key,
    }


@router.delete("/runtimes/{enrollment_id}", summary="Revoke a runtime enrollment")
async def revoke_runtime(
    enrollment_id: str,
    user: User = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    repo = _repo(server)
    _owned(repo, enrollment_id, user)
    row = repo.revoke(enrollment_id)
    if row is None:
        raise OctopError(ErrorCode.NOT_FOUND, "enrollment not found")
    return _public(row)


@router.post("/runtimes/{enrollment_id}/rotate", summary="Rotate the runtime credential")
async def rotate_runtime(
    enrollment_id: str,
    user: User = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    repo = _repo(server)
    row = _owned(repo, enrollment_id, user)
    now = int(time.time())
    if (
        row.previous_key_hash is not None
        and row.rotation_expires_at is not None
        and row.rotation_expires_at > now
    ):
        raise OctopError(
            ErrorCode.RUNTIME_ROTATION_PENDING,
            "rotation already pending; use or wait for it to expire",
            details={"rotation_expires_at": row.rotation_expires_at},
            status=409,
        )
    key, new_hash = _new_key()
    updated = repo.rotate(enrollment_id, new_key_hash=new_hash)
    if updated is None:
        raise OctopError(ErrorCode.NOT_FOUND, "enrollment not found")
    return {**_public(updated), "key_shown_once": key}


@router.get("/runtimes/{enrollment_id}/status", summary="Runtime enrollment status")
async def runtime_status(
    enrollment_id: str,
    user: User = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    repo = _repo(server)
    row = _owned(repo, enrollment_id, user)
    result = _public(row)
    registry = getattr(getattr(server, "app_runtime", None), "runtime_registry", None)
    result["online"] = bool(registry is not None and registry.online(enrollment_id))
    return result
