"""Unit tests for the runtime enrollment store (spec §6)."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from octop.infra.runtime.enrollment_store import (
    ENV_RUNTIME_HOME,
    SCHEMA_VERSION,
    STATE_ACTIVE,
    EnrollmentMissingError,
    EnrollmentStore,
    RuntimeEnrollment,
    default_home,
    new_machine_id,
)
from octop.infra.utils.json_file import JsonFileCorruptError

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX file modes only")


def make_enrollment(**overrides: object) -> RuntimeEnrollment:
    defaults: dict[str, object] = {
        "hub_url": "https://hub.example",
        "key": "rk_" + "a" * 43,
        "machine_id": "01JTESTMACHINEID0001",
        "roots": ("/srv/roots",),
    }
    defaults.update(overrides)
    return RuntimeEnrollment(**defaults)  # type: ignore[arg-type]


def test_round_trip_preserves_fields(tmp_path: Path) -> None:
    store = EnrollmentStore(tmp_path / "runtime.json")
    enrollment = make_enrollment(
        hub_pin="sha256/abc",
        last_generation=7,
        last_close_code=4002,
        proxy="socks5://127.0.0.1:1080",
        ca_bundle="/etc/ssl/custom.pem",
        pending_pin="sha256/def",
    )
    store.save(enrollment)

    loaded = store.load_required()
    assert loaded.hub_url == enrollment.hub_url
    assert loaded.key == enrollment.key
    assert loaded.machine_id == enrollment.machine_id
    assert loaded.roots == ("/srv/roots",)
    assert loaded.hub_pin == "sha256/abc"
    assert loaded.last_generation == 7
    assert loaded.state == STATE_ACTIVE
    assert loaded.last_close_code == 4002
    assert loaded.proxy == "socks5://127.0.0.1:1080"
    assert loaded.ca_bundle == "/etc/ssl/custom.pem"
    assert loaded.pending_pin == "sha256/def"


def test_saved_document_is_schema_versioned(tmp_path: Path) -> None:
    path = tmp_path / "runtime.json"
    EnrollmentStore(path).save(make_enrollment())
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["schema_version"] == SCHEMA_VERSION
    assert doc["machine_id"]


@posix_only
def test_save_is_atomic_and_mode_0600(tmp_path: Path) -> None:
    path = tmp_path / "runtime.json"
    store = EnrollmentStore(path)
    store.save(make_enrollment())
    store.save(make_enrollment(last_generation=3))  # rewrite must stay atomic too

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    # Atomic writes leave no temp siblings behind.
    leftovers = [item.name for item in tmp_path.iterdir() if item.name != "runtime.json"]
    assert leftovers == []
    assert json.loads(path.read_text(encoding="utf-8"))["last_generation"] == 3


def test_missing_file_loads_none(tmp_path: Path) -> None:
    store = EnrollmentStore(tmp_path / "runtime.json")
    assert store.load() is None
    with pytest.raises(EnrollmentMissingError):
        store.load_required()


def test_corrupt_file_raises(tmp_path: Path) -> None:
    path = tmp_path / "runtime.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(JsonFileCorruptError):
        EnrollmentStore(path).load()


def test_update_mutates_and_persists(tmp_path: Path) -> None:
    store = EnrollmentStore(tmp_path / "runtime.json")
    store.save(make_enrollment())

    def bump(enrollment: RuntimeEnrollment) -> None:
        enrollment.last_generation = 11
        enrollment.last_close_code = 1006

    store.update(bump)
    reloaded = store.load_required()
    assert reloaded.last_generation == 11
    assert reloaded.last_close_code == 1006
    assert reloaded.hub_url == "https://hub.example"  # untouched fields survive


def test_from_json_ignores_unknown_keys(tmp_path: Path) -> None:
    doc = make_enrollment().to_json()
    doc["future_field"] = {"nested": True}
    loaded = RuntimeEnrollment.from_json(doc)
    assert loaded.hub_url == "https://hub.example"
    assert loaded.last_generation == 0


def test_env_override_selects_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(ENV_RUNTIME_HOME, str(tmp_path))
    assert default_home() == tmp_path
    store = EnrollmentStore()  # path resolved from the env override
    store.save(make_enrollment())
    assert (tmp_path / "runtime.json").is_file()


def test_machine_ids_are_unique() -> None:
    assert new_machine_id() != new_machine_id()
    assert len(new_machine_id()) == 26  # canonical ULID length
