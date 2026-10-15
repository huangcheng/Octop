"""Rootless service units for the remote runtime daemon (spec §6).

macOS gets a user-domain LaunchAgent (``launchctl bootstrap gui/$UID``),
Linux a systemd ``--user`` unit (``systemctl --user``). Everything runs as
the enrolling user — no root, no inbound ports. Windows is phase-1
foreground-only: every operation raises :class:`NotImplementedError` with a
clear message pointing at ``octop runtime run``.

All operations are idempotent: installing twice with unchanged content is a
no-op, uninstalling an absent unit succeeds silently.
"""

from __future__ import annotations

import dataclasses
import getpass
import os
import plistlib
import shutil
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

LAUNCHD_LABEL = "com.octop.runtime"
SYSTEMD_UNIT_NAME = "octop-runtime.service"

Runner = Callable[..., subprocess.CompletedProcess[str]]

WINDOWS_MESSAGE = (
    "Windows service installation is not supported in this phase; "
    "run `octop runtime run` in the foreground instead."
)


@dataclasses.dataclass(frozen=True)
class UnitStatus:
    installed: bool
    running: bool | None  # None = state unknown (probe failed)
    detail: str = ""


def detect_mode() -> str:
    """``launchd`` | ``systemd`` | raise NotImplementedError on Windows."""
    if sys.platform == "darwin":
        return "launchd"
    if sys.platform.startswith(("linux", "freebsd")):
        return "systemd"
    raise NotImplementedError(WINDOWS_MESSAGE)


def unit_path(mode: str, home: Path | None = None) -> Path:
    owner_home = home if home is not None else Path.home()
    if mode == "launchd":
        return owner_home / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
    return owner_home / ".config" / "systemd" / "user" / SYSTEMD_UNIT_NAME


def log_path(runtime_home: Path) -> Path:
    return runtime_home / "logs" / "runtime.log"


def _octop_bin(explicit: Path | None) -> str:
    if explicit is not None:
        return str(explicit)
    # Same resolution order as the system service installer: the octop next
    # to the running interpreter (venv installs), then PATH.
    for candidate in (
        Path(sys.executable).parent / "octop",
        Path(sys.executable).parent / "octop.exe",
    ):
        if candidate.is_file():
            return str(candidate)
    return shutil.which("octop") or "octop"


def render_launchd_plist(octop_bin: str, runtime_home: Path) -> str:
    """User-domain LaunchAgent plist: keep-alive, dial-out runtime daemon."""
    doc = {
        "Label": LAUNCHD_LABEL,
        "ProgramArguments": [octop_bin, "runtime", "run"],
        "EnvironmentVariables": {
            "OCTOP_RUNTIME_HOME": str(runtime_home),
            "OCTOP_SERVICE_MODE": "launchd",
        },
        "WorkingDirectory": str(runtime_home),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(log_path(runtime_home)),
        "StandardErrorPath": str(log_path(runtime_home)),
    }
    return plistlib.dumps(doc).decode("utf-8")


def render_systemd_user_unit(octop_bin: str, runtime_home: Path) -> str:
    """systemd --user unit: restart on failure, user-session default target."""
    return (
        "[Unit]\n"
        "Description=Octop Remote Runtime daemon\n"
        "After=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=simple\n"
        f"Environment=OCTOP_RUNTIME_HOME={runtime_home}\n"
        "Environment=OCTOP_SERVICE_MODE=systemd\n"
        f"ExecStart={octop_bin} runtime run\n"
        "Restart=on-failure\n"
        "RestartSec=5\n"
        "TimeoutStopSec=30\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )


def _run(
    runner: Runner, argv: Sequence[str], *, tolerate: Sequence[int] = ()
) -> subprocess.CompletedProcess[str]:
    proc = runner(list(argv), capture_output=True, text=True, check=False)
    if proc.returncode != 0 and proc.returncode not in tolerate:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"{' '.join(argv)} failed ({proc.returncode}): {detail}")
    return proc


class RuntimeService:
    """Install/start/stop/status the runtime daemon unit for the current user."""

    def __init__(
        self,
        *,
        runtime_home: Path | None = None,
        octop_bin: Path | None = None,
        mode: str | None = None,
        home: Path | None = None,
        runner: Runner | None = None,
    ) -> None:
        self._home = home if home is not None else Path.home()
        self.runtime_home = (
            runtime_home if runtime_home is not None else self._home / ".octop-runtime"
        )
        self._octop_bin = octop_bin
        self._explicit_mode = mode
        self._runner: Runner = runner if runner is not None else _subprocess_run

    @property
    def mode(self) -> str:
        return self._explicit_mode if self._explicit_mode is not None else detect_mode()

    @property
    def path(self) -> Path:
        return unit_path(self.mode, self._home)

    def render(self) -> str:
        octop_bin = _octop_bin(self._octop_bin)
        if self.mode == "launchd":
            return render_launchd_plist(octop_bin, self.runtime_home)
        return render_systemd_user_unit(octop_bin, self.runtime_home)

    def install(self) -> bool:
        """Write the unit and load it. Returns True when anything changed."""
        mode = self.mode  # raises NotImplementedError on Windows
        content = self.render()
        path = self.path
        if path.is_file():
            try:
                if path.read_text(encoding="utf-8") == content:
                    return False  # already installed, identical content
            except UnicodeDecodeError:
                pass  # fall through and rewrite
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        if mode == "launchd":
            self._launchd_load(path)
        else:
            _run(self._runner, ["systemctl", "--user", "daemon-reload"])
            _run(self._runner, ["systemctl", "--user", "enable", "--now", SYSTEMD_UNIT_NAME])
            self._enable_linger()
        return True

    def uninstall(self) -> bool:
        """Remove the unit. Returns True when a unit was actually removed."""
        mode = self.mode
        path = self.path
        if not path.is_file():
            return False
        if mode == "launchd":
            self._launchd_unload()
        else:
            _run(
                self._runner,
                ["systemctl", "--user", "disable", "--now", SYSTEMD_UNIT_NAME],
                tolerate=(1, 5),
            )
        path.unlink()
        if mode == "systemd":
            _run(self._runner, ["systemctl", "--user", "daemon-reload"])
        return True

    def start(self) -> None:
        if self.mode == "launchd":
            self._launchd_load(self.path)
            return
        _run(self._runner, ["systemctl", "--user", "start", SYSTEMD_UNIT_NAME])

    def stop(self) -> None:
        if self.mode == "launchd":
            _run(
                self._runner,
                ["launchctl", "kill", "SIGTERM", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"],
                tolerate=(3,),
            )
            return
        _run(self._runner, ["systemctl", "--user", "stop", SYSTEMD_UNIT_NAME])

    def status(self) -> UnitStatus:
        mode = self.mode
        path = self.path
        if not path.is_file():
            return UnitStatus(installed=False, running=False, detail="not installed")
        if mode == "launchd":
            proc = self._runner(
                ["launchctl", "print", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0:
                running = '"state" = "running"' in (proc.stdout or "") or "pid = " in (
                    proc.stdout or ""
                )
                return UnitStatus(installed=True, running=running, detail=path.name)
            return UnitStatus(installed=True, running=False, detail=path.name)
        proc = self._runner(
            ["systemctl", "--user", "is-active", SYSTEMD_UNIT_NAME],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return UnitStatus(installed=True, running=True, detail=path.name)
        if proc.returncode == 3:  # LSB: inactive / failed
            return UnitStatus(installed=True, running=False, detail=path.name)
        return UnitStatus(installed=True, running=None, detail=path.name)

    # -- launchd helpers ------------------------------------------------------

    def _launchd_target(self) -> str:
        return f"gui/{os.getuid()}"

    def _launchd_load(self, path: Path) -> None:
        target = self._launchd_target()
        proc = self._runner(
            ["launchctl", "bootstrap", target, str(path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return
        stderr = (proc.stderr or "").lower()
        if "already bootstrapped" in stderr or proc.returncode == 5:
            _run(self._runner, ["launchctl", "kickstart", f"{target}/{LAUNCHD_LABEL}"])
            return
        raise RuntimeError(
            f"launchctl bootstrap failed ({proc.returncode}): {(proc.stderr or '').strip()}"
        )

    def _launchd_unload(self) -> None:
        _run(
            self._runner,
            ["launchctl", "bootout", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"],
            tolerate=(3, 5, 113),
        )

    def _enable_linger(self) -> None:
        """User units stop at logout unless lingering is enabled; best-effort."""
        _run(self._runner, ["loginctl", "enable-linger", getpass.getuser()], tolerate=(1,))


def _subprocess_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    # NOCA:DangerousSubprocessUseAudit(argv list with shell=False; launchctl/systemctl/loginctl user-unit management)
    return subprocess.run(argv, **kwargs)  # noqa: S603
