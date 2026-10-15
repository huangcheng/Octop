"""Tests for the runtime enrollment repository."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.runtime_enrollments import (
    EnrollmentConflictError,
    EnrollmentQuotaError,
    RuntimeEnrollmentRepo,
)


@pytest.fixture
def db(tmp_path: Path) -> SqlitePool:
    pool = SqlitePool(tmp_path / "runtime.db")
    run_migrations(pool)
    with pool.connect() as conn:
        conn.execute(
            "INSERT INTO users(username, password_hash, role, created_at) "
            "VALUES ('alice', 'x', 'admin', ?)",
            (int(time.time()),),
        )
        conn.execute(
            "INSERT INTO users(username, password_hash, role, created_at) "
            "VALUES ('bob', 'x', 'user', ?)",
            (int(time.time()),),
        )
    return pool


def _create(repo: RuntimeEnrollmentRepo, *, name: str = "Laptop", user: int = 1):
    return repo.create(
        enrollment_id=f"enr-{name}",
        owner_user_id=user,
        display_name=name,
        key_hash=f"hash-{name}",
    )


def test_create_and_get_roundtrip(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    row = _create(repo, name="Laptop", user=1)
    assert row.status == "active"
    assert row.allow_exec is False
    assert row.generation == 0
    assert row.previous_key_hash is None

    fetched = repo.get_by_enrollment_id("enr-Laptop")
    assert fetched is not None
    assert fetched.owner_user_id == 1
    assert repo.get_for_owner("enr-Laptop", 1) is not None
    assert repo.get_for_owner("enr-Laptop", 2) is None


def test_get_by_key_hash_prefers_current_slot(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    _create(repo)
    repo.rotate("enr-Laptop", new_key_hash="hash-new")

    assert repo.get_by_key_hash("hash-new")[1] == "current"
    assert repo.get_by_key_hash("hash-Laptop")[1] == "previous"
    assert repo.get_by_key_hash("missing") is None


def test_rotate_keeps_slot_chain_and_allow_exec(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    repo.create(
        enrollment_id="e1",
        owner_user_id=1,
        display_name="Box",
        key_hash="old",
        allow_exec=True,
    )
    rotated = repo.rotate("e1", new_key_hash="new")
    assert rotated is not None
    assert rotated.previous_key_hash == "old"
    assert rotated.key_hash == "new"
    assert rotated.allow_exec is True
    assert rotated.rotation_expires_at is not None
    assert rotated.rotation_expires_at > int(time.time())
    assert rotated.rotation_expires_at <= int(time.time()) + 24 * 60 * 60 + 5


def test_list_and_active_count_cap_data(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    for i in range(3):
        repo.create(
            enrollment_id=f"e{i}",
            owner_user_id=1,
            display_name=f"m{i}",
            key_hash=f"h{i}",
        )
    assert len(repo.list_for_user(1)) == 3
    assert repo.count_active_for_user(1) == 3
    repo.revoke("e1")
    assert repo.count_active_for_user(1) == 2
    assert len(repo.list_for_user(2)) == 0
    assert len(repo.list_all()) == 3


def test_revoke_flips_status_and_keeps_row(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    _create(repo)
    revoked = repo.revoke("enr-Laptop")
    assert revoked is not None
    assert revoked.status == "revoked"
    assert repo.get_by_enrollment_id("enr-Laptop") is not None


def test_mark_seen_updates_metadata(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    _create(repo)
    before = int(time.time())
    repo.mark_seen("enr-Laptop", machine_metadata='{"os": "darwin"}')
    row = repo.get_by_enrollment_id("enr-Laptop")
    assert row is not None
    assert row.last_seen_at is not None and row.last_seen_at >= before
    assert row.machine_metadata == '{"os": "darwin"}'


def test_record_conflict(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    _create(repo)
    repo.record_conflict("enr-Laptop", ip="203.0.113.7")
    row = repo.get_by_enrollment_id("enr-Laptop")
    assert row is not None
    assert row.last_conflict_ip == "203.0.113.7"
    assert row.last_conflict_at is not None


def test_bump_generation_is_atomic_and_returns_new_value(
    db: SqlitePool,
) -> None:
    repo = RuntimeEnrollmentRepo(db)
    _create(repo)
    assert repo.bump_generation("enr-Laptop") == 1
    assert repo.bump_generation("enr-Laptop") == 2
    row = repo.get_by_enrollment_id("enr-Laptop")
    assert row is not None
    assert row.generation == 2
    assert repo.bump_generation("missing") is None


def test_display_name_unique_per_user(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    repo.create(
        enrollment_id="e-u1",
        owner_user_id=1,
        display_name="Same",
        key_hash="h1",
    )
    repo.create(
        enrollment_id="e-u2",
        owner_user_id=2,
        display_name="Same",
        key_hash="h2",
    )
    with pytest.raises(EnrollmentConflictError):
        repo.create(
            enrollment_id="e-u1b",
            owner_user_id=1,
            display_name="Same",
            key_hash="h3",
        )


def test_active_cap_enforced_atomically(db: SqlitePool) -> None:
    repo = RuntimeEnrollmentRepo(db)
    for i in range(3):
        repo.create(
            enrollment_id=f"cap-{i}",
            owner_user_id=1,
            display_name=f"m{i}",
            key_hash=f"k{i}",
            active_cap=3,
        )
    with pytest.raises(EnrollmentQuotaError):
        repo.create(
            enrollment_id="cap-3",
            owner_user_id=1,
            display_name="m3",
            key_hash="k3",
            active_cap=3,
        )
    # Revoked rows do not count toward the cap.
    repo.revoke("cap-0")
    row = repo.create(
        enrollment_id="cap-4",
        owner_user_id=1,
        display_name="m4",
        key_hash="k4",
        active_cap=3,
    )
    assert row.enrollment_id == "cap-4"
