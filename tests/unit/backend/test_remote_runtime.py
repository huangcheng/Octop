"""Adapter / resolver / probe coverage for the ``remote_runtime`` backend kind."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from octop.infra.backend.adapter import (
    row_to_backend_spec,
    storage_backend_kind_agent_resolvable,
    storage_spec_previewable,
)
from octop.infra.backend.probe import probe_storage_backend
from octop.infra.backend.resolver import (
    backend_spec_supports_execution,
    resolve_agent_backend_spec,
    split_exec_target,
)
from octop.infra.db.repos.backends import BackendRow
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.backend import SessionLink
from octop.infra.runtime.protocol import (
    FrameType,
    FsWrite,
    decode_frame,
    encode_frame,
)
from octop.infra.runtime.registry import RuntimeSession

# ---------------------------------------------------------------------------
# adapter: storage_backends row → spec
# ---------------------------------------------------------------------------


def _row(kind: str, config: dict[str, Any] | None = None) -> BackendRow:
    return BackendRow(
        id=1,
        name="rt",
        kind=kind,
        endpoint=None,
        access_key=None,
        secret_key=None,
        bucket=None,
        region=None,
        config_json=json.dumps(config) if config is not None else None,
        note=None,
        enabled=1,
        created_at=0,
        updated_at=0,
    )


def test_remote_runtime_row_maps_to_spec() -> None:
    spec = row_to_backend_spec(
        _row(
            "remote_runtime",
            {"enrollment_id": "E1", "workspace_base": "/ws", "agent_root": "shared"},
        )
    )
    assert spec == {
        "type": "remote_runtime",
        "enrollment_id": "E1",
        "workspace_base": "/ws",
        "agent_root": "shared",
    }


def test_remote_runtime_row_requires_enrollment_id() -> None:
    assert row_to_backend_spec(_row("remote_runtime", {})) is None
    assert row_to_backend_spec(_row("remote_runtime")) is None


def test_remote_runtime_agent_resolvable_and_not_previewable() -> None:
    assert storage_backend_kind_agent_resolvable("remote_runtime") is True
    assert storage_spec_previewable({"type": "remote_runtime"}) is False


def test_existing_kinds_unaffected() -> None:
    # Regression guard: the added branch must not disturb neighbours.
    assert row_to_backend_spec(_row("opensandbox", {"image": "python:3.12"})) is not None
    assert row_to_backend_spec(_row("filesystem", {"root_dir": "/tmp/x"})) == {
        "type": "filesystem",
        "root_dir": "/tmp/x",
        "virtual_mode": True,
    }
    assert storage_spec_previewable({"type": "filesystem"}) is True


# ---------------------------------------------------------------------------
# resolver: exec_target composites + supports_execution
# ---------------------------------------------------------------------------


def test_composite_exec_target_resolved_and_split() -> None:
    spec = {
        "type": "composite",
        "default": {"type": "filesystem", "root_dir": "/hub/ws"},
        "routes": {"/remote/": {"type": "remote_runtime", "enrollment_id": "E1"}},
        "exec_target": {"type": "remote_runtime", "enrollment_id": "E1"},
    }
    resolved = resolve_agent_backend_spec(spec, repo=None)
    assert resolved["exec_target"]["type"] == "remote_runtime"
    assert resolved["routes"]["/remote/"]["type"] == "remote_runtime"

    harness_spec, exec_sub = split_exec_target(resolved)
    assert "exec_target" not in harness_spec
    assert harness_spec["type"] == "composite"
    assert exec_sub is not None and exec_sub["type"] == "remote_runtime"
    # Without an enrollment repo the exec capability resolves off.
    assert backend_spec_supports_execution(exec_sub) is False


def test_supports_execution_composite_via_exec_target() -> None:
    spec = {
        "type": "composite",
        "default": {"type": "filesystem", "root_dir": "/hub/ws"},
        "routes": {},
        "exec_target": {"type": "remote_runtime", "enrollment_id": "E1", "allow_exec": True},
    }
    assert backend_spec_supports_execution(spec) is True
    spec["exec_target"]["allow_exec"] = False
    assert backend_spec_supports_execution(spec) is False
    # Local kinds keep working (regression).
    assert backend_spec_supports_execution({"type": "local_shell"}) is True
    assert backend_spec_supports_execution({"type": "filesystem"}) is False


# ---------------------------------------------------------------------------
# probe: live session fs roundtrip + exec leg
# ---------------------------------------------------------------------------


def _one_chunk(data: bytes, rid: str | None) -> tuple[FrameType, Any, str | None]:
    """Single-chunk fs.write-carrier reply (executor contract)."""
    return (
        FrameType.FS_WRITE,
        FsWrite(path="__echo__", data=data, chunk_index=0, total=1),
        rid,
    )


async def _noop_close(code: int, reason: str) -> None:  # pragma: no cover
    del code, reason


class _FakeRuntime:
    """Minimal peer answering fs roundtrips and one exec leg."""

    def __init__(self, *, exec_ok: bool = True) -> None:
        self.session = RuntimeSession(
            enrollment_id="E1",
            generation=1,
            key_slot="current",
            capabilities=("fs", "exec"),
            client_ip="127.0.0.1",
            close_impl=_noop_close,
        )
        self.exec_ok = exec_ok
        self.files: dict[str, bytes] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def link(self) -> SessionLink:
        return SessionLink(session=self.session, send=self.send, loop=asyncio.get_running_loop())

    async def send(self, text: str) -> None:
        frame = decode_frame(text)
        payload = frame.payload
        rid = frame.request_id
        replies: list[tuple[FrameType, Any, str | None]] = []
        if frame.frame_type is FrameType.FS_WRITE and hasattr(payload, "path"):
            if payload.chunk_index == payload.total - 1:
                self.files[payload.path] = self.files.get(payload.path, b"") + payload.data
                replies.append(
                    (
                        FrameType.FS_WRITE,
                        FsWrite(path="__echo__", data=b"{}", chunk_index=0, total=1),
                        rid,
                    )
                )
            else:
                self.files[payload.path] = self.files.get(payload.path, b"") + payload.data
        elif frame.frame_type is FrameType.FS_READ:
            body = self.files.get(payload.path, b"")
            replies.append(_one_chunk(body, rid))
        elif frame.frame_type is FrameType.FS_DELETE:
            self.files.pop(payload.path, None)
            replies.append(_one_chunk(b"{}", rid))
        elif frame.frame_type is FrameType.FS_LS:
            doc = json.dumps({"entries": [], "truncated": False}).encode()
            replies.append(_one_chunk(doc, rid))
        elif frame.frame_type is FrameType.FS_STAT:
            body = self.files.get(payload.path, b"")
            doc = json.dumps({"size": len(body), "is_dir": False, "etag": "1:1:1"}).encode()
            replies.append(_one_chunk(doc, rid))
        elif frame.frame_type is FrameType.FS_GLOB or frame.frame_type is FrameType.FS_GREP:
            doc = json.dumps({"matches": [], "truncated": False}).encode()
            replies.append(_one_chunk(doc, rid))
        elif frame.frame_type is FrameType.EXEC_OPEN:
            from octop.infra.runtime.protocol import ExecOutput, ExecStream

            exec_id = payload.exec_id
            if self.exec_ok:
                replies.append(
                    (
                        FrameType.EXEC_OUTPUT,
                        ExecOutput(
                            exec_id=exec_id,
                            stream=ExecStream.STDOUT,
                            data=b"octop-remote-runtime-probe",
                        ),
                        None,
                    )
                )
            replies.append(
                (
                    FrameType.EXEC_OUTPUT,
                    ExecOutput(
                        exec_id=exec_id,
                        stream=ExecStream.EXIT,
                        exit_code=0 if self.exec_ok else 1,
                        reason=None,
                        request_id=rid,
                    ),
                    None,
                )
            )
        for frame_type, reply_payload, reply_rid in replies:
            text = encode_frame(
                self.session.generation, 1, frame_type, reply_payload, request_id=reply_rid
            )
            task = asyncio.create_task(self._deliver(decode_frame(text)))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _deliver(self, frame: Any) -> None:
        handler = self.session.frame_handler
        if handler is not None:
            await handler(frame)


async def test_probe_remote_runtime_roundtrip_with_exec_leg() -> None:
    fake = _FakeRuntime(exec_ok=True)
    link = fake.link

    def provider() -> SessionLink | None:
        return link

    row = _row(
        "remote_runtime",
        {"enrollment_id": "E1", "workspace_base": "/ws", "agent_root": "shared"},
    )

    result = await asyncio.to_thread(
        probe_storage_backend,
        row,
        runtime_session_provider=provider,
        allow_exec=True,
    )

    assert result["ok"] is True, result
    # Probe cleanup removed its file.
    assert all(not p.startswith("/ws/.probe") for p in fake.files), fake.files


async def test_probe_remote_runtime_exec_leg_failure_fails_probe() -> None:
    fake = _FakeRuntime(exec_ok=False)
    link = fake.link

    def provider() -> SessionLink | None:
        return link

    row = _row(
        "remote_runtime",
        {"enrollment_id": "E1", "workspace_base": "/ws", "agent_root": "shared"},
    )

    result = await asyncio.to_thread(
        probe_storage_backend,
        row,
        runtime_session_provider=provider,
        allow_exec=True,
    )

    assert result["ok"] is False
    assert "execute leg" in result["message"]


async def test_probe_remote_runtime_offline_and_unwired() -> None:
    row = _row("remote_runtime", {"enrollment_id": "E1"})

    unwired = await asyncio.to_thread(probe_storage_backend, row)
    assert unwired["ok"] is False
    assert "live-session provider" in unwired["message"]

    offline = await asyncio.to_thread(
        probe_storage_backend, row, runtime_session_provider=lambda: None
    )
    assert offline["ok"] is False
    assert "offline" in offline["message"]

    incomplete = await asyncio.to_thread(probe_storage_backend, _row("remote_runtime", {}))
    assert incomplete["ok"] is False
    assert incomplete["message"] == "configuration incomplete"


def test_missing_enrollment_id_raises_not_found() -> None:
    with pytest.raises(OctopError) as excinfo:
        resolve_agent_backend_spec(
            {"type": "remote_runtime", "enrollment_id": "  "},
            repo=None,
            owner_user_id=1,
            enrollment_repo=None,
        )
    assert excinfo.value.code is ErrorCode.NOT_FOUND
