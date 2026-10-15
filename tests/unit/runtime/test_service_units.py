"""Unit tests for runtime rootless service units (spec §6)."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path

import pytest

from octop.infra.runtime.service_units import (
    LAUNCHD_LABEL,
    RuntimeService,
    detect_mode,
    render_launchd_plist,
    render_systemd_user_unit,
)

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX-only service semantics")


def fake_runner(calls: list[list[str]]):
    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    return run


def test_render_systemd_user_unit() -> None:
    unit = render_systemd_user_unit("/usr/local/bin/octop", Path("/home/dev/.octop-runtime"))
    assert "ExecStart=/usr/local/bin/octop runtime run" in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=5" in unit
    assert "WantedBy=default.target" in unit
    assert "Environment=OCTOP_RUNTIME_HOME=/home/dev/.octop-runtime" in unit
    assert "After=network-online.target" in unit


def test_render_launchd_plist() -> None:
    home = Path("/Users/dev/.octop-runtime")
    plist_text = render_launchd_plist("/usr/local/bin/octop", home)
    doc = plistlib.loads(plist_text)
    assert doc["Label"] == LAUNCHD_LABEL
    assert doc["ProgramArguments"] == ["/usr/local/bin/octop", "runtime", "run"]
    assert doc["RunAtLoad"] is True
    assert doc["KeepAlive"] is True
    assert doc["EnvironmentVariables"]["OCTOP_RUNTIME_HOME"] == str(home)
    assert doc["StandardOutPath"] == str(home / "logs" / "runtime.log")


def test_windows_raises_not_implemented(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    with pytest.raises(NotImplementedError, match="Windows service installation"):
        detect_mode()
    with pytest.raises(NotImplementedError, match="octop runtime run"):
        RuntimeService().install()


@posix_only
def test_install_is_idempotent(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    svc = RuntimeService(
        runtime_home=tmp_path / "runtime",
        octop_bin=Path("/usr/local/bin/octop"),
        mode="systemd",
        home=tmp_path,
        runner=fake_runner(calls),
    )
    assert svc.install() is True
    unit = svc.path
    assert unit.is_file()
    assert "ExecStart=/usr/local/bin/octop runtime run" in unit.read_text(encoding="utf-8")
    reload_calls = [c for c in calls if "daemon-reload" in c]
    assert len(reload_calls) == 1

    # Double install with identical content: no rewrite, no reload.
    assert svc.install() is False
    assert len([c for c in calls if "daemon-reload" in c]) == 1


@posix_only
def test_uninstall_absent_unit_is_noop(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    svc = RuntimeService(
        runtime_home=tmp_path / "runtime",
        mode="systemd",
        home=tmp_path,
        runner=fake_runner(calls),
    )
    assert svc.uninstall() is False
    assert calls == []


@posix_only
def test_uninstall_after_install_removes_unit(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    svc = RuntimeService(
        runtime_home=tmp_path / "runtime",
        mode="systemd",
        home=tmp_path,
        runner=fake_runner(calls),
    )
    svc.install()
    assert svc.uninstall() is True
    assert not svc.path.is_file()
    # disable --now, then daemon-reload after unlink
    assert any("disable" in c for c in calls)
    assert len([c for c in calls if "daemon-reload" in c]) == 2


@posix_only
def test_status_reports_not_installed(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    svc = RuntimeService(
        runtime_home=tmp_path / "runtime",
        mode="systemd",
        home=tmp_path,
        runner=fake_runner(calls),
    )
    status = svc.status()
    assert status.installed is False
    assert status.running is False
    assert calls == []
