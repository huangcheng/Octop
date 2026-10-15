"""Runtime enrollment rows — per-user local machines acting as agent backends."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from octop.infra.db.pool import DatabasePool
from octop.infra.db.repos._base import DbRow, map_rows, now_ts

# Rotation overlap window: the old key stays valid until the new key is used
# or this many seconds pass (spec §9).
ROTATION_WINDOW_SECONDS = 24 * 60 * 60

try:
    from psycopg import errors as pg_errors
except ImportError:  # pragma: no cover - optional PostgreSQL driver
    pg_errors = None  # type: ignore[assignment]


class EnrollmentConflictError(Exception):
    """UNIQUE(user_id, display_name) violation — the DB is the true guard."""


class EnrollmentQuotaError(Exception):
    """Active-enrollment cap reached; enforced atomically at insert time."""


def _is_unique_violation(exc: BaseException) -> bool:
    if isinstance(exc, sqlite3.IntegrityError):
        return "unique" in str(exc).lower()
    return pg_errors is not None and isinstance(exc, pg_errors.UniqueViolation)


@dataclass(frozen=True)
class RuntimeEnrollmentRow:
    pk: int
    enrollment_id: str
    machine_id: str | None
    owner_user_id: int
    display_name: str
    key_hash: str
    previous_key_hash: str | None
    rotation_expires_at: int | None
    allow_exec: bool
    default_workspace_base: str | None
    status: str
    generation: int
    last_seen_at: int | None
    last_conflict_at: int | None
    last_conflict_ip: str | None
    machine_metadata: str | None
    created_at: int
    updated_at: int

    @classmethod
    def from_row(cls, r: DbRow) -> RuntimeEnrollmentRow:
        return cls(
            pk=int(r["id"]),
            enrollment_id=str(r["enrollment_id"]),
            machine_id=_clean(r["machine_id"]),
            owner_user_id=int(r["user_id"]),
            display_name=str(r["display_name"] or ""),
            key_hash=str(r["key_hash"]),
            previous_key_hash=_clean(r["previous_key_hash"]),
            rotation_expires_at=r["rotation_expires_at"],
            allow_exec=bool(int(r["allow_exec"] or 0)),
            default_workspace_base=_clean(r["default_workspace_base"]),
            status=str(r["status"] or "active"),
            generation=int(r["generation"] or 0),
            last_seen_at=r["last_seen_at"],
            last_conflict_at=r["last_conflict_at"],
            last_conflict_ip=_clean(r["last_conflict_ip"]),
            machine_metadata=r["machine_metadata"],
            created_at=int(r["created_at"]),
            updated_at=int(r["updated_at"]),
        )


def _clean(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


class RuntimeEnrollmentRepo:
    def __init__(self, db: DatabasePool) -> None:
        self._db = db

    def create(
        self,
        *,
        enrollment_id: str,
        owner_user_id: int,
        display_name: str,
        key_hash: str,
        allow_exec: bool = False,
        default_workspace_base: str | None = None,
        status: str = "active",
        active_cap: int | None = None,
    ) -> RuntimeEnrollmentRow:
        ts = now_ts()
        try:
            # Single-statement conditional insert keeps the active-row cap atomic
            # on both dialects (SQLite single-writer; PG statement-level snapshot).
            if active_cap is None:
                with self._db.transaction() as conn:
                    conn.execute(
                        "INSERT INTO runtime_enrollments("
                        "enrollment_id, user_id, display_name, key_hash, allow_exec, "
                        "default_workspace_base, status, created_at, updated_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            enrollment_id,
                            owner_user_id,
                            display_name,
                            key_hash,
                            1 if allow_exec else 0,
                            (default_workspace_base or "").strip() or None,
                            status,
                            ts,
                            ts,
                        ),
                    )
            else:
                with self._db.transaction() as conn:
                    cur = conn.execute(
                        "INSERT INTO runtime_enrollments("
                        "enrollment_id, user_id, display_name, key_hash, allow_exec, "
                        "default_workspace_base, status, created_at, updated_at"
                        ") SELECT ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE ("
                        "SELECT COUNT(*) FROM runtime_enrollments "
                        "WHERE user_id = ? AND status = 'active') < ?",
                        (
                            enrollment_id,
                            owner_user_id,
                            display_name,
                            key_hash,
                            1 if allow_exec else 0,
                            (default_workspace_base or "").strip() or None,
                            status,
                            ts,
                            ts,
                            owner_user_id,
                            active_cap,
                        ),
                    )
                    if cur.rowcount == 0:
                        raise EnrollmentQuotaError(f"active enrollment cap reached: {active_cap}")
        except Exception as exc:  # noqa: BLE001 - dialect-specific integrity errors
            if isinstance(exc, EnrollmentQuotaError):
                raise
            if _is_unique_violation(exc):
                raise EnrollmentConflictError(str(exc)) from exc
            raise
        row = self.get_by_enrollment_id(enrollment_id)
        if row is None:
            raise EnrollmentConflictError(f"enrollment {enrollment_id} vanished")
        return row

    def get_by_enrollment_id(self, enrollment_id: str) -> RuntimeEnrollmentRow | None:
        with self._db.connect() as conn:
            r = conn.execute(
                "SELECT * FROM runtime_enrollments WHERE enrollment_id = ?",
                (enrollment_id,),
            ).fetchone()
        return RuntimeEnrollmentRow.from_row(r) if r else None

    def get_for_owner(self, enrollment_id: str, owner_user_id: int) -> RuntimeEnrollmentRow | None:
        row = self.get_by_enrollment_id(enrollment_id)
        if row is None or row.owner_user_id != owner_user_id:
            return None
        return row

    def get_by_key_hash(self, key_hash: str) -> tuple[RuntimeEnrollmentRow, str] | None:
        """Resolve an enrollment by credential; current key outranks previous.

        Returns ``(row, slot)`` where slot is ``"current"`` or ``"previous"``
        so callers can apply rotation semantics (old-key dials never evict).
        """
        with self._db.connect() as conn:
            r = conn.execute(
                "SELECT * FROM runtime_enrollments WHERE key_hash = ?",
                (key_hash,),
            ).fetchone()
            if r is None:
                r = conn.execute(
                    "SELECT * FROM runtime_enrollments WHERE previous_key_hash = ?",
                    (key_hash,),
                ).fetchone()
                if r is None:
                    return None
                return RuntimeEnrollmentRow.from_row(r), "previous"
        return RuntimeEnrollmentRow.from_row(r), "current"

    def list_for_user(self, owner_user_id: int) -> list[RuntimeEnrollmentRow]:
        with self._db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runtime_enrollments WHERE user_id = ? "
                "ORDER BY created_at ASC, id ASC",
                (owner_user_id,),
            ).fetchall()
        return map_rows(rows, RuntimeEnrollmentRow)

    def list_all(self) -> list[RuntimeEnrollmentRow]:
        with self._db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runtime_enrollments ORDER BY created_at ASC, id ASC"
            ).fetchall()
        return map_rows(rows, RuntimeEnrollmentRow)

    def count_active_for_user(self, owner_user_id: int) -> int:
        with self._db.connect() as conn:
            r = conn.execute(
                "SELECT COUNT(*) FROM runtime_enrollments WHERE user_id = ? AND status = 'active'",
                (owner_user_id,),
            ).fetchone()
        return int(r[0] if r is not None else 0)

    def revoke(self, enrollment_id: str) -> RuntimeEnrollmentRow | None:
        ts = now_ts()
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE runtime_enrollments SET status = 'revoked', updated_at = ? "
                "WHERE enrollment_id = ?",
                (ts, enrollment_id),
            )
        return self.get_by_enrollment_id(enrollment_id)

    def rotate(self, enrollment_id: str, *, new_key_hash: str) -> RuntimeEnrollmentRow | None:
        """Single-slot chain: current key demotes to previous, new key takes over.

        The old key stays valid until the new one is used or the 24h window
        expires. ``allow_exec`` is preserved; the API layer re-prompts consent.
        """
        ts = now_ts()
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE runtime_enrollments SET previous_key_hash = key_hash, "
                "key_hash = ?, rotation_expires_at = ?, updated_at = ? "
                "WHERE enrollment_id = ?",
                (new_key_hash, ts + ROTATION_WINDOW_SECONDS, ts, enrollment_id),
            )
        return self.get_by_enrollment_id(enrollment_id)

    def mark_seen(self, enrollment_id: str, *, machine_metadata: str | None = None) -> None:
        ts = now_ts()
        with self._db.transaction() as conn:
            if machine_metadata is not None:
                conn.execute(
                    "UPDATE runtime_enrollments SET last_seen_at = ?, "
                    "machine_metadata = ?, updated_at = ? WHERE enrollment_id = ?",
                    (ts, machine_metadata, ts, enrollment_id),
                )
            else:
                conn.execute(
                    "UPDATE runtime_enrollments SET last_seen_at = ?, updated_at = ? "
                    "WHERE enrollment_id = ?",
                    (ts, ts, enrollment_id),
                )

    def record_conflict(self, enrollment_id: str, *, ip: str) -> None:
        ts = now_ts()
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE runtime_enrollments SET last_conflict_at = ?, "
                "last_conflict_ip = ?, updated_at = ? WHERE enrollment_id = ?",
                (ts, ip, ts, enrollment_id),
            )

    def bump_generation(self, enrollment_id: str) -> int | None:
        """Atomically advance generation; the new value fences stale frames."""
        ts = now_ts()
        with self._db.transaction() as conn:
            r = conn.execute(
                "UPDATE runtime_enrollments SET generation = generation + 1, "
                "updated_at = ? WHERE enrollment_id = ? "
                "RETURNING generation",
                (ts, enrollment_id),
            ).fetchone()
        return int(r[0]) if r is not None else None
