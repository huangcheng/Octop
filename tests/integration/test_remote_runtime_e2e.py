"""End-to-end remote-runtime wiring (needs no external services).

Boots a real uvicorn hub on 127.0.0.1 (ephemeral port), dials it with the REAL
``RuntimeClient`` daemon (FsExecutor + ExecExecutor bound to a temp roots
dir), then drives the hub-side ``RuntimeBackendHub`` proxy exactly like a
harness agent does: sync ``SandboxBackendProtocol`` ops from worker threads
bridged onto the session loop.  After the daemon stops, ops must map to
``RUNTIME_OFFLINE``.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
import uvicorn

from octop.api.app import build_app
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.backend import RuntimeBackendHub
from octop.infra.runtime.client import RuntimeClient
from octop.infra.runtime.enrollment_store import EnrollmentStore, RuntimeEnrollment
from octop.infra.runtime.exec_executor import ExecExecutor
from octop.infra.runtime.executor import FsExecutor
from octop.infra.server import OctopServer
from octop.infra.utils.ulid import new_ulid
from tests.support.app import ensure_control_plane_bound
from tests.support.auth import bearer, bootstrap_admin, login
from tests.support.harness import patch_harness

_TIMEOUT_S = 30.0


@pytest.fixture
def _no_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    # websockets reads HTTPS_PROXY/ALL_PROXY even for 127.0.0.1 dials.
    for name in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(name, raising=False)


async def _wait_until(
    predicate: Callable[[], bool], what: str, *, timeout: float = _TIMEOUT_S
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, f"timed out waiting for: {what}"
        await asyncio.sleep(0.05)


async def _bound_port() -> tuple[int, socket.socket]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    return int(sock.getsockname()[1]), sock


@pytest.mark.asyncio
async def test_remote_runtime_end_to_end(tmp_octop_home: Path, _no_proxy: None) -> None:
    roots = tmp_octop_home.parent / "runtime-root"
    roots.mkdir(parents=True, exist_ok=True)
    port, sock = await _bound_port()
    base = f"http://127.0.0.1:{port}"

    with patch_harness():
        srv = OctopServer(home=tmp_octop_home)
        await srv.start()
        try:
            if not srv.database_bound:
                await ensure_control_plane_bound(srv)
            assert srv.app_runtime is not None
            await srv.app_runtime.proactive_scheduler.shutdown()
            srv.app_runtime.proactive_scheduler.suspend()

            server = uvicorn.Server(
                uvicorn.Config(
                    build_app(srv),
                    host="127.0.0.1",
                    port=port,
                    log_level="warning",
                    lifespan="off",
                )
            )
            thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[sock])))
            thread.start()
            try:
                await _wait_until(lambda: server.started, "uvicorn hub to start")

                async with httpx.AsyncClient(base_url=base, timeout=10.0) as http:
                    await bootstrap_admin(http, tmp_octop_home)
                    token = await login(http)
                    issued = await http.post(
                        "/api/runtimes",
                        headers=bearer(token),
                        json={
                            "display_name": "e2e-machine",
                            "allow_exec": True,
                            "default_workspace_base": str(roots),
                        },
                    )
                    assert issued.status_code == 201, issued.text
                    doc = issued.json()
                enrollment_id = str(doc["enrollment_id"])
                key = str(doc["key_shown_once"])

                store = EnrollmentStore(tmp_octop_home.parent / "runtime.json")
                store.save(
                    RuntimeEnrollment(
                        hub_url=base,
                        key=key,
                        machine_id=new_ulid(),
                        roots=(str(roots),),
                    )
                )
                client = RuntimeClient(
                    store,
                    capabilities=("fs", "exec"),
                    workspace_base=str(roots),
                    fs_executor=FsExecutor((str(roots),)),
                    exec_executor=ExecExecutor((str(roots),)),
                )
                client_task = asyncio.create_task(client.run())
                try:
                    registry = srv.app_runtime.runtime_registry
                    await _wait_until(
                        lambda: registry.online(enrollment_id),
                        "runtime session to come online",
                    )

                    hub = RuntimeBackendHub(
                        registry,
                        enrollment_repo=srv.services.runtime_enrollment_repo,
                    )
                    backend = hub.backend_for(
                        enrollment_id,
                        allow_exec=True,
                        workspace_base=str(roots),
                        agent_root="shared",
                    )

                    write = await asyncio.wait_for(
                        asyncio.to_thread(backend.write, "hello.txt", "hi"), _TIMEOUT_S
                    )
                    assert write.error is None, write.error

                    read = await asyncio.wait_for(
                        asyncio.to_thread(backend.read, "hello.txt"), _TIMEOUT_S
                    )
                    assert read.error is None, read.error
                    assert read.file_data is not None
                    assert read.file_data["content"] == "hi"
                    assert (roots / "hello.txt").read_text(encoding="utf-8") == "hi"

                    executed = await asyncio.wait_for(
                        asyncio.to_thread(backend.execute, "echo e2e-ok"), _TIMEOUT_S
                    )
                    assert executed.exit_code == 0, executed.output
                    assert "e2e-ok" in executed.output

                    listed = await asyncio.wait_for(asyncio.to_thread(backend.ls, "/"), _TIMEOUT_S)
                    assert listed.error is None, listed.error
                    assert listed.entries is not None
                    assert any(entry.get("path") == "/hello.txt" for entry in listed.entries)

                    await client.stop()
                    await asyncio.wait_for(client_task, _TIMEOUT_S)
                    await _wait_until(
                        lambda: not registry.online(enrollment_id),
                        "hub to drop the stopped session",
                    )
                    with pytest.raises(OctopError) as exc_info:
                        await asyncio.wait_for(
                            asyncio.to_thread(backend.read, "hello.txt"), _TIMEOUT_S
                        )
                    assert exc_info.value.code is ErrorCode.RUNTIME_OFFLINE
                finally:
                    client_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await client_task
                    with contextlib.suppress(Exception):
                        await client.stop()
            finally:
                server.should_exit = True
                thread.join(timeout=_TIMEOUT_S)
                assert not thread.is_alive(), "uvicorn hub did not shut down"
        finally:
            await srv.stop()
