"""CLI tests for `octop runtime` enroll / status / re-pin."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from octop.cli.main import cli
from octop.infra.runtime.enrollment_store import (
    ENV_RUNTIME_HOME,
    EnrollmentStore,
)

KEY = "rk_" + "k" * 43


@pytest.fixture(autouse=True)
def _isolate_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "runtime-home"
    monkeypatch.setenv(ENV_RUNTIME_HOME, str(home))
    monkeypatch.setenv("HOME", str(tmp_path))
    # Path.home() prefers USERPROFILE on Windows; pin it so enroll's default
    # root resolves to the isolated home on every platform.
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("OCTOP_HOME", str(tmp_path / "octop"))
    return home


def _store() -> EnrollmentStore:
    return EnrollmentStore()


def test_enroll_writes_store(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    result = CliRunner().invoke(
        cli,
        [
            "runtime",
            "enroll",
            "--server",
            "https://hub.example",
            "--key",
            KEY,
            "--roots",
            str(root),
        ],
    )
    assert result.exit_code == 0, result.output
    enrollment = _store().load_required()
    assert enrollment.hub_url == "https://hub.example"
    assert enrollment.key == KEY
    assert enrollment.roots == (str(root),)
    assert enrollment.machine_id  # ULID generated once
    assert enrollment.state == "active"


def test_enroll_rejects_non_https_url() -> None:
    result = CliRunner().invoke(
        cli, ["runtime", "enroll", "--server", "http://hub.example", "--key", KEY]
    )
    assert result.exit_code != 0
    assert "--server" in result.output
    assert _store().load() is None


def test_enroll_allows_localhost_dev_http() -> None:
    result = CliRunner().invoke(
        cli,
        ["runtime", "enroll", "--server", "http://127.0.0.1:8000", "--key", KEY],
    )
    assert result.exit_code == 0, result.output
    assert _store().load_required().hub_url == "http://127.0.0.1:8000"


def test_enroll_rejects_bad_key() -> None:
    result = CliRunner().invoke(
        cli, ["runtime", "enroll", "--server", "https://hub.example", "--key", "not-a-key"]
    )
    assert result.exit_code != 0
    assert _store().load() is None


def test_enroll_reads_key_from_file(tmp_path: Path) -> None:
    key_file = tmp_path / "rk.txt"
    key_file.write_text(KEY + "\n", encoding="utf-8")
    result = CliRunner().invoke(
        cli,
        [
            "runtime",
            "enroll",
            "--server",
            "https://hub.example",
            "--key-file",
            str(key_file),
        ],
    )
    assert result.exit_code == 0, result.output
    assert _store().load_required().key == KEY


def test_enroll_replace_gate(tmp_path: Path) -> None:
    runner = CliRunner()
    first = runner.invoke(cli, ["runtime", "enroll", "--server", "https://a.example", "--key", KEY])
    assert first.exit_code == 0, first.output
    original = _store().load_required()

    # A differing enrollment without --replace is refused and leaves the store intact.
    second = runner.invoke(
        cli,
        [
            "runtime",
            "enroll",
            "--server",
            "https://b.example",
            "--key",
            "rk_" + "j" * 43,
        ],
    )
    assert second.exit_code != 0
    assert "--replace" in second.output
    assert _store().load_required().hub_url == "https://a.example"

    # With --replace the store is overwritten but the machine identity survives.
    third = runner.invoke(
        cli,
        [
            "runtime",
            "enroll",
            "--server",
            "https://b.example",
            "--key",
            "rk_" + "j" * 43,
            "--replace",
        ],
    )
    assert third.exit_code == 0, third.output
    replaced = _store().load_required()
    assert replaced.hub_url == "https://b.example"
    assert replaced.key == "rk_" + "j" * 43
    assert replaced.machine_id == original.machine_id

    # Re-running the identical enroll stays idempotent (no --replace needed).
    fourth = runner.invoke(
        cli,
        [
            "runtime",
            "enroll",
            "--server",
            "https://b.example",
            "--key",
            "rk_" + "j" * 43,
        ],
    )
    assert fourth.exit_code == 0, fourth.output


def test_status_output(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli, ["runtime", "enroll", "--server", "https://hub.example", "--key", KEY]
    )
    assert result.exit_code == 0, result.output
    store = _store()
    store.update(
        lambda e: (
            setattr(e, "last_generation", 9),
            setattr(e, "last_close_code", 4002),
        )
    )
    status = CliRunner().invoke(cli, ["runtime", "status"])
    assert status.exit_code == 0, status.output
    assert "https://hub.example" in status.output
    assert "active" in status.output
    assert "9" in status.output
    assert "4002" in status.output
    enrollment = store.load_required()
    assert enrollment.machine_id in status.output
    assert str(tmp_path) in status.output  # root disk-free line names the root


def test_status_requires_enrollment() -> None:
    result = CliRunner().invoke(cli, ["runtime", "status"])
    assert result.exit_code != 0
    assert "enroll" in result.output


def test_re_pin_accept(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli, ["runtime", "enroll", "--server", "https://hub.example", "--key", KEY]
    )
    assert result.exit_code == 0, result.output
    store = _store()

    # Nothing pending yet: informational, successful no-op.
    none = CliRunner().invoke(cli, ["runtime", "re-pin", "--accept"])
    assert none.exit_code == 0, none.output

    store.update(lambda e: setattr(e, "pending_pin", "sha256/changed"))
    accepted = CliRunner().invoke(cli, ["runtime", "re-pin", "--accept"])
    assert accepted.exit_code == 0, accepted.output
    enrollment = store.load_required()
    assert enrollment.hub_pin == "sha256/changed"
    assert enrollment.pending_pin is None


def test_service_status_not_installed() -> None:
    result = CliRunner().invoke(cli, ["runtime", "service", "status"])
    assert result.exit_code == 0, result.output
    assert "installed=false" in result.output
