-- Schema v21: remote runtime enrollments (per-user local machines).

CREATE TABLE IF NOT EXISTS runtime_enrollments (
  id                      INTEGER PRIMARY KEY AUTOINCREMENT,
  enrollment_id           TEXT NOT NULL UNIQUE,
  machine_id              TEXT,
  user_id                 INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  display_name            TEXT NOT NULL,
  key_hash                TEXT NOT NULL,
  previous_key_hash       TEXT,
  rotation_expires_at     INTEGER,
  allow_exec              INTEGER NOT NULL DEFAULT 0,
  default_workspace_base  TEXT,
  status                  TEXT NOT NULL DEFAULT 'active',
  generation              INTEGER NOT NULL DEFAULT 0,
  last_seen_at            INTEGER,
  last_conflict_at        INTEGER,
  last_conflict_ip        TEXT,
  machine_metadata        TEXT,
  created_at              INTEGER NOT NULL,
  updated_at              INTEGER NOT NULL,
  UNIQUE(user_id, display_name)
);

CREATE INDEX IF NOT EXISTS idx_runtime_enrollments_key_hash
  ON runtime_enrollments(key_hash);

CREATE INDEX IF NOT EXISTS idx_runtime_enrollments_previous_key_hash
  ON runtime_enrollments(previous_key_hash);

UPDATE _schema_version SET version = 21;
