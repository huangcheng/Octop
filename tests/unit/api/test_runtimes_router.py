"""Unit tests for the /api/runtimes HTTP adapter."""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from octop.api.routers import runtimes
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.runtime_enrollments import RuntimeEnrollmentRepo
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.users.identity import User


@pytest.fixture
def db(tmp_path: Path) -> SqlitePool:
    pool = SqlitePool(tmp_path / "runtimes.db")
    run_migrations(pool)
    with pool.connect() as conn:
        for name in ("alice", "bob"):
            conn.execute(
                "INSERT INTO users(username, password_hash, role, created_at) "
                "VALUES (?, 'x', ?, ?)",
                (name, "user", int(time.time())),
            )
    return pool


def _server(db: SqlitePool) -> SimpleNamespace:
    return SimpleNamespace(
        services=SimpleNamespace(runtime_enrollment_repo=RuntimeEnrollmentRepo(db))
    )


def _user(uid: int, *, admin: bool = False) -> User:
    return User(id=uid, username=f"u{uid}", role="admin" if admin else "user", display_name=None)


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/runtimes",
            "headers": [(b"host", b"hub.example.com")],
            "query_string": b"",
        }
    )


@pytest.mark.asyncio
async def test_issue_returns_key_once_and_enroll_command(db: SqlitePool) -> None:
    server = _server(db)
    res = await runtimes.issue_runtime(
        runtimes.RuntimeIssueBody(display_name="Laptop", allow_exec=True),
        _request(),
        user=_user(1),
        server=server,
    )
    assert res["enrollment_id"]
    assert res["key_shown_once"].startswith("rk_")
    assert res["allow_exec"] is True
    assert res["enroll_command"].startswith("octop runtime enroll --server http://hub.example.com")
    # Key material never leaks into subsequent listings.
    listing = await runtimes.list_runtimes(user=_user(1), server=server)
    assert "key_shown_once" not in listing[0]
    assert "key_hash" not in listing[0]


@pytest.mark.asyncio
async def test_issue_enforces_active_cap(db: SqlitePool) -> None:
    server = _server(db)
    for i in range(runtimes.MAX_ACTIVE_ENROLLMENTS):
        await runtimes.issue_runtime(
            runtimes.RuntimeIssueBody(display_name=f"m{i}"),
            _request(),
            user=_user(1),
            server=server,
        )
    with pytest.raises(OctopError) as raised:
        await runtimes.issue_runtime(
            runtimes.RuntimeIssueBody(display_name="over"),
            _request(),
            user=_user(1),
            server=server,
        )
    assert raised.value.status == 409


@pytest.mark.asyncio
async def test_issue_rejects_duplicate_display_name(db: SqlitePool) -> None:
    server = _server(db)
    await runtimes.issue_runtime(
        runtimes.RuntimeIssueBody(display_name="Laptop"),
        _request(),
        user=_user(1),
        server=server,
    )
    with pytest.raises(OctopError) as raised:
        await runtimes.issue_runtime(
            runtimes.RuntimeIssueBody(display_name="Laptop"),
            _request(),
            user=_user(1),
            server=server,
        )
    assert raised.value.code == ErrorCode.BRIDGE_DISPLAY_NAME_TAKEN


@pytest.mark.asyncio
async def test_list_scopes_to_owner_admin_sees_all(db: SqlitePool) -> None:
    server = _server(db)
    await runtimes.issue_runtime(
        runtimes.RuntimeIssueBody(display_name="a1"),
        _request(),
        user=_user(1),
        server=server,
    )
    await runtimes.issue_runtime(
        runtimes.RuntimeIssueBody(display_name="b1"),
        _request(),
        user=_user(2),
        server=server,
    )
    own = await runtimes.list_runtimes(user=_user(1), server=server)
    assert [r["display_name"] for r in own] == ["a1"]
    admin = await runtimes.list_runtimes(user=_user(1, admin=True), server=server)
    assert len(admin) == 2


@pytest.mark.asyncio
async def test_revoke_and_status_and_ownership(db: SqlitePool) -> None:
    server = _server(db)
    res = await runtimes.issue_runtime(
        runtimes.RuntimeIssueBody(display_name="Laptop"),
        _request(),
        user=_user(1),
        server=server,
    )
    eid = res["enrollment_id"]

    with pytest.raises(OctopError) as raised:
        await runtimes.runtime_status(eid, user=_user(2), server=server)
    assert raised.value.code == ErrorCode.NOT_FOUND

    revoked = await runtimes.revoke_runtime(eid, user=_user(1), server=server)
    assert revoked["status"] == "revoked"

    status = await runtimes.runtime_status(eid, user=_user(1), server=server)
    assert status["rotation_pending"] is False


@pytest.mark.asyncio
async def test_rotate_blocks_while_unconsumed_and_allows_after_expiry(
    db: SqlitePool,
) -> None:
    server = _server(db)
    res = await runtimes.issue_runtime(
        runtimes.RuntimeIssueBody(display_name="Laptop"),
        _request(),
        user=_user(1),
        server=server,
    )
    eid = res["enrollment_id"]

    first = await runtimes.rotate_runtime(eid, user=_user(1), server=server)
    assert first["key_shown_once"].startswith("rk_")
    assert first["rotation_pending"] is True

    with pytest.raises(OctopError) as raised:
        await runtimes.rotate_runtime(eid, user=_user(1), server=server)
    assert raised.value.status == 409

    # Expire the window: rotation becomes allowed again.
    with db.connect() as conn:
        conn.execute(
            "UPDATE runtime_enrollments SET rotation_expires_at = ? WHERE enrollment_id = ?",
            (int(time.time()) - 1, eid),
        )
    second = await runtimes.rotate_runtime(eid, user=_user(1), server=server)
    assert second["key_shown_once"].startswith("rk_")
