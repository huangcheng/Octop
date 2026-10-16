"""`octop runtime` — enroll and run this machine as a remote runtime (spec §6).

Enrollment lives in ``~/.octop-runtime/runtime.json`` (``OCTOP_RUNTIME_HOME``
override). All user-facing output goes through :func:`octop.i18n.tr`; the CLI
imports infra modules only.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn
from urllib.parse import urlparse

import click

from octop.i18n import tr
from octop.infra.runtime.client import ClientStatus, RuntimeClient
from octop.infra.runtime.enrollment_store import (
    EnrollmentMissingError,
    EnrollmentStore,
    RuntimeEnrollment,
    new_machine_id,
)
from octop.infra.runtime.exec_executor import ExecExecutor
from octop.infra.runtime.executor import FsExecutor

if TYPE_CHECKING:
    from octop.infra.runtime.service_units import RuntimeService

_LOCALHOST_DEV_HOSTS = {"localhost", "127.0.0.1", "::1"}
_HOLD_DETAILS = {
    "auth_failed",
    "pin_mismatch",
    "protocol_hold",
    "subprotocol_rejected",
    "dial_refused",
    "persist_failed",
}


def _locale() -> str:
    from octop.cli.support.db import resolve_cli_locale

    return resolve_cli_locale()


def _fail(message: str) -> NoReturn:
    click.echo(message, err=True)
    raise SystemExit(1) from None


def _store() -> EnrollmentStore:
    return EnrollmentStore()


def _validate_hub_url(raw: str, locale: str) -> str:
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if parsed.scheme == "https" and parsed.netloc:
        return raw.rstrip("/")
    if parsed.scheme == "http" and host in _LOCALHOST_DEV_HOSTS:
        # Explicit localhost dev flag only (spec §9: WSS required otherwise).
        return raw.rstrip("/")
    _fail(tr("runtime.enroll_invalid_url", locale))


def _resolve_key(
    locale: str,
    key: str | None,
    key_file: str | None,
) -> str:
    if not key and key_file:
        path = Path(key_file).expanduser()
        if not path.is_file():
            _fail(tr("runtime.enroll_key_file_missing", locale, path=path))
        key = path.read_text(encoding="utf-8").strip()
    if not key and not sys.stdin.isatty():
        key = sys.stdin.read().strip() or None
    if not key:
        _fail(tr("runtime.enroll_key_required", locale))
    if not key.startswith("rk_"):
        _fail(tr("runtime.enroll_invalid_key", locale))
    return key


def _validate_proxy(raw: str | None, locale: str) -> str | None:
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https", "socks5", "socks5h") or not parsed.netloc:
        _fail(tr("runtime.enroll_invalid_proxy", locale))
    return raw


def _enrollment_differs(existing: RuntimeEnrollment, incoming: RuntimeEnrollment) -> bool:
    return (
        existing.hub_url != incoming.hub_url
        or existing.key != incoming.key
        or existing.roots != incoming.roots
        or existing.proxy != incoming.proxy
        or existing.ca_bundle != incoming.ca_bundle
    )


@click.group()
def runtime() -> None:
    """Enroll and run this machine as a remote runtime for an Octop hub."""


@runtime.command("enroll")
@click.option(
    "--server",
    required=True,
    help="Hub base URL (https:// required; http localhost allowed for dev).",
)
@click.option("--key", default=None, help="rk_ enrollment key (or use --key-file / stdin).")
@click.option("--key-file", default=None, help="File containing the rk_ enrollment key.")
@click.option(
    "--roots",
    multiple=True,
    default=None,
    help="Filesystem root exposed to the hub (repeatable; defaults to the home directory).",
)
@click.option("--ca-bundle", default=None, help="Path to a CA bundle for hub TLS verification.")
@click.option(
    "--proxy", default=None, help="Proxy URL for the hub connection (default: HTTPS_PROXY env)."
)
@click.option(
    "--replace", is_flag=True, default=False, help="Overwrite an existing differing enrollment."
)
def enroll(
    server: str,
    key: str | None,
    key_file: str | None,
    roots: tuple[str, ...] | None,
    ca_bundle: str | None,
    proxy: str | None,
    replace: bool,
) -> None:
    """Write the enrollment store and print next-step guidance."""
    locale = _locale()
    hub_url = _validate_hub_url(server, locale)
    resolved_key = _resolve_key(locale, key, key_file)
    resolved_roots = tuple(
        os.path.normpath(str(Path(root).expanduser())) for root in (roots or (str(Path.home()),))
    )
    resolved_ca = None
    if ca_bundle:
        ca_path = Path(ca_bundle).expanduser()
        if not ca_path.is_file():
            _fail(tr("runtime.enroll_ca_bundle_missing", locale, path=ca_path))
        resolved_ca = str(ca_path)
    resolved_proxy = _validate_proxy(proxy, locale)

    store = _store()
    incoming = RuntimeEnrollment(
        hub_url=hub_url,
        key=resolved_key,
        machine_id=new_machine_id(),
        roots=resolved_roots,
        proxy=resolved_proxy,
        ca_bundle=resolved_ca,
    )
    existing = store.load()
    if existing is not None and _enrollment_differs(existing, incoming):
        if not replace:
            _fail(tr("runtime.enroll_exists", locale, hub=existing.hub_url))
        # Same machine, refreshed credentials: keep the machine identity and
        # the TOFU pin; clear session bookkeeping for the fresh enrollment.
        incoming.machine_id = existing.machine_id
        incoming.hub_pin = existing.hub_pin
        incoming.last_generation = existing.last_generation
        click.echo(tr("runtime.enroll_replaced", locale, hub=hub_url))
    else:
        if existing is not None:
            incoming.machine_id = existing.machine_id
            incoming.hub_pin = existing.hub_pin
            incoming.last_generation = existing.last_generation
        click.echo(tr("runtime.enroll_ok", locale, hub=hub_url))
    try:
        store.save(incoming)
    except OSError as exc:
        _fail(tr("runtime.enroll_write_failed", locale, error=exc))
    click.echo(tr("runtime.enroll_guidance", locale))


@runtime.command("re-pin")
@click.option("--accept", is_flag=True, default=False, help="Accept the observed hub TLS pin.")
def re_pin(accept: bool) -> None:
    """Accept a changed hub TLS pin after the daemon flagged a mismatch."""
    locale = _locale()
    if not accept:
        _fail(tr("runtime.repin_accept_required", locale))
    store = _store()
    try:
        enrollment = store.load_required()
    except EnrollmentMissingError:
        _fail(tr("runtime.not_enrolled", locale))
    if not enrollment.pending_pin:
        click.echo(tr("runtime.repin_none", locale))
        return
    enrollment.hub_pin = enrollment.pending_pin
    enrollment.pending_pin = None
    try:
        store.save(enrollment)
    except OSError as exc:
        _fail(tr("runtime.enroll_write_failed", locale, error=exc))
    click.echo(tr("runtime.repin_ok", locale))


@runtime.group("service")
def service() -> None:
    """Manage the rootless runtime service (LaunchAgent / systemd --user)."""


def _service_op(locale: str) -> RuntimeService:
    from octop.infra.runtime.service_units import RuntimeService

    try:
        return RuntimeService()
    except NotImplementedError as exc:
        _fail(str(exc))


@service.command("install")
def service_install() -> None:
    locale = _locale()
    svc = _service_op(locale)
    try:
        changed = svc.install()
    except RuntimeError as exc:
        _fail(str(exc))
    if changed:
        click.echo(tr("runtime.service_installed", locale, unit=svc.path.name))
    else:
        click.echo(tr("runtime.service_unchanged", locale, unit=svc.path.name))


@service.command("uninstall")
def service_uninstall() -> None:
    locale = _locale()
    svc = _service_op(locale)
    try:
        removed = svc.uninstall()
    except RuntimeError as exc:
        _fail(str(exc))
    if removed:
        click.echo(tr("runtime.service_uninstalled", locale, unit=svc.path.name))
    else:
        click.echo(tr("runtime.service_not_installed", locale))


@service.command("start")
def service_start() -> None:
    locale = _locale()
    svc = _service_op(locale)
    try:
        svc.start()
    except RuntimeError as exc:
        _fail(str(exc))
    click.echo(tr("runtime.service_started", locale, unit=svc.path.name))


@service.command("stop")
def service_stop() -> None:
    locale = _locale()
    svc = _service_op(locale)
    try:
        svc.stop()
    except RuntimeError as exc:
        _fail(str(exc))
    click.echo(tr("runtime.service_stopped", locale, unit=svc.path.name))


@service.command("status")
def service_status() -> None:
    locale = _locale()
    svc = _service_op(locale)
    status = svc.status()
    running = "?" if status.running is None else str(status.running).lower()
    click.echo(
        tr(
            "runtime.service_status",
            locale,
            unit=status.detail or svc.path.name,
            installed=str(status.installed).lower(),
            running=running,
        )
    )


@runtime.command("status")
def status() -> None:
    """Show enrollment state, hub, machine id, generation, and root disk space."""
    locale = _locale()
    store = _store()
    try:
        enrollment = store.load_required()
    except EnrollmentMissingError:
        _fail(tr("runtime.not_enrolled", locale))
    rows = [
        (tr("runtime.state", locale), enrollment.state),
        (tr("runtime.hub", locale), enrollment.hub_url),
        (tr("runtime.machine_id", locale), enrollment.machine_id),
        (tr("runtime.generation", locale), str(enrollment.last_generation)),
        (
            tr("runtime.last_close_code", locale),
            "-" if enrollment.last_close_code is None else str(enrollment.last_close_code),
        ),
    ]
    for label, value in rows:
        click.echo(f"{label}: {value}")
    for root in enrollment.roots:
        try:
            free = shutil.disk_usage(root).free
        except OSError:
            click.echo(tr("runtime.root_unavailable", locale, root=root))
            continue
        click.echo(tr("runtime.root_free", locale, root=root, free=_format_bytes(free)))
    if enrollment.pending_pin:
        click.echo(tr("runtime.status_pending_pin", locale, pin=enrollment.pending_pin))


def _format_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024
    return f"{amount:.1f} TiB"


@runtime.command("run")
def run() -> None:
    """Run the runtime daemon in the foreground until SIGTERM/SIGINT."""
    locale = _locale()
    store = _store()
    enrollment = store.load()
    if enrollment is None:
        _fail(tr("runtime.not_enrolled", locale))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    click.echo(
        tr("runtime.run_starting", locale, hub=enrollment.hub_url, machine_id=enrollment.machine_id)
    )
    assert enrollment is not None  # checked above
    roots = tuple(enrollment.roots) if enrollment.roots else (str(Path.home()),)
    client = RuntimeClient(
        store,
        on_status=lambda s: _echo_hold(s, locale),
        fs_executor=FsExecutor(roots),
        exec_executor=ExecExecutor(roots),
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run_client(client))
    click.echo(tr("runtime.run_stopped", locale))


async def _run_client(client: RuntimeClient) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Non-main thread or a platform without loop signal handlers.
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop_event.set)
    runner = asyncio.create_task(client.run())
    waiter = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait({runner, waiter}, return_when=asyncio.FIRST_COMPLETED)
    if waiter in done:
        # Graceful SIGTERM/SIGINT: rt.bye, then let run() unwind on its own.
        await client.stop()
    for task in pending:
        if task is not runner:
            task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await runner


def _echo_hold(status: ClientStatus, locale: str) -> None:
    if status.detail in _HOLD_DETAILS:
        click.echo(tr("runtime.run_hold", locale, detail=status.detail))
