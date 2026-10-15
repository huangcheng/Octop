"""Unit tests for the hub-side remote_runtime backend proxy (spec §7.2/§8).

A scripted fake runtime peer answers through the same encode/decode codec the
wire uses; no network, no executor.  Sync ``SandboxBackendProtocol`` methods
are driven through ``asyncio.to_thread`` exactly like the harness does.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from deepagents.backends.protocol import SandboxBackendProtocol
from deepagents.middleware.filesystem import supports_execution as da_supports_execution

from octop.infra.backend.resolver import (
    backend_spec_supports_execution,
    resolve_agent_backend_spec,
)
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.backend import (
    ExecTargetCompositeBackend,
    RemoteRuntimeBackend,
    SessionLink,
)
from octop.infra.runtime.protocol import (
    CHUNK_BYTES,
    ErrorFrame,
    ErrorKind,
    ExecClose,
    ExecOpen,
    ExecOutput,
    ExecStream,
    ExitReason,
    FrameType,
    FsWrite,
    RtCredit,
    decode_frame,
    encode_frame,
)
from octop.infra.runtime.registry import RuntimeSession

# ---------------------------------------------------------------------------
# Fake runtime peer
# ---------------------------------------------------------------------------

Reply = tuple[FrameType, Any, str | None]
Responder = Callable[[Any], list[Reply]]


async def _noop_close(code: int, reason: str) -> None:  # pragma: no cover
    del code, reason


class FakeRuntime:
    """Scripted daemon: hub frames arrive in ``sent``; replies re-enter the hub."""

    def __init__(
        self,
        session: RuntimeSession,
        responder: Responder | None = None,
        *,
        grant_per_write_chunk: int = 0,
    ) -> None:
        self.session = session
        self.responder = responder
        self.grant_per_write_chunk = grant_per_write_chunk
        self.sent: list[Any] = []
        self.received: list[Any] = []  # frames the fake emitted back to the hub
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def link(self) -> SessionLink:
        return SessionLink(
            session=self.session,
            send=self.send,
            loop=asyncio.get_running_loop(),
        )

    async def send(self, text: str) -> None:
        frame = decode_frame(text)
        self.sent.append(frame)
        replies: list[Reply] = []
        if self.responder is not None:
            replies = list(self.responder(frame))
        if self.grant_per_write_chunk > 0 and frame.frame_type is FrameType.FS_WRITE:
            replies.append(
                (
                    FrameType.RT_CREDIT,
                    RtCredit(
                        stream_id=frame.request_id or "",
                        bytes=self.grant_per_write_chunk,
                    ),
                    None,
                )
            )
        for frame_type, payload, request_id in replies:
            # "__RID__" echoes the originating request (scripted responders
            # do not know the ULID the hub generated).
            rid = frame.request_id if request_id == "__RID__" else request_id
            self.emit(frame_type, payload, rid)

    def emit(self, frame_type: FrameType, payload: Any, request_id: str | None = None) -> None:
        text = encode_frame(self.session.generation, 1, frame_type, payload, request_id=request_id)
        frame = decode_frame(text)
        self.received.append(frame)
        task = asyncio.create_task(self._deliver(frame))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _deliver(self, frame: Any) -> None:
        handler = self.session.frame_handler
        if handler is not None:
            await handler(frame)

    def sent_of(self, frame_type: FrameType) -> list[Any]:
        return [f for f in self.sent if f.frame_type is frame_type]

    def received_of(self, frame_type: FrameType) -> list[Any]:
        return [f for f in self.received if f.frame_type is frame_type]


def _session(enrollment_id: str = "E1", generation: int = 1) -> RuntimeSession:
    return RuntimeSession(
        enrollment_id=enrollment_id,
        generation=generation,
        key_slot="current",
        capabilities=("fs", "exec"),
        client_ip="127.0.0.1",
        close_impl=_noop_close,
    )


def _backend(
    fake: FakeRuntime,
    state: dict[str, SessionLink | None],
    **kwargs: Any,
) -> RemoteRuntimeBackend:
    kwargs.setdefault("fs_chunk_timeout_s", 0.5)
    kwargs.setdefault("fs_total_timeout_s", 5.0)
    kwargs.setdefault("exec_idle_timeout_s", 10.0)
    kwargs.setdefault("exec_close_grace_s", 0.1)
    return RemoteRuntimeBackend(
        "E1",
        session_provider=lambda: state["link"],
        **kwargs,
    )


def _chunks_reply(pieces: list[bytes], *, truncated: bool = False) -> list[Reply]:
    """Chunked fs.write-carrier reply pieces (total = len(pieces))."""
    out: list[Reply] = []
    for index, piece in enumerate(pieces):
        out.append(
            (
                FrameType.FS_WRITE,
                FsWrite(
                    path="__echo__",
                    data=piece,
                    chunk_index=index,
                    total=len(pieces),
                    truncated=truncated and index == len(pieces) - 1,
                ),
                "__RID__",
            )
        )
    return out


def _raw_reply(data: bytes) -> list[Reply]:
    return _chunks_reply([data])


def _doc_reply_bytes(doc: dict[str, Any]) -> list[Reply]:
    return _chunks_reply([json.dumps(doc).encode("utf-8")])


async def _wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "condition never became true"
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# read: line slicing over byte chunks
# ---------------------------------------------------------------------------


async def test_read_slices_lines_across_byte_chunks() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _chunks_reply([b"alpha\nbe", b"ta\ngamma\n"])
            if frame.frame_type is FrameType.FS_READ
            else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    result = await asyncio.to_thread(backend.read, "/notes.txt", 1, 1)

    assert result.error is None
    assert result.file_data is not None
    assert result.file_data["content"] == "beta\n"
    assert (result.start_line, result.end_line, result.next_offset) == (2, 2, 2)
    assert result.total_lines == 3
    # Virtual path mapped to the content root join.
    read_frames = fake.sent_of(FrameType.FS_READ)
    assert len(read_frames) == 1
    assert read_frames[0].payload.path == "notes.txt"


async def test_read_full_window_and_beyond_eof() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _raw_reply(b"one\ntwo\n") if frame.frame_type is FrameType.FS_READ else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    full = await asyncio.to_thread(backend.read, "/f.txt")
    assert full.file_data is not None and full.file_data["content"] == "one\ntwo\n"
    assert full.total_lines == 2

    beyond = await asyncio.to_thread(backend.read, "/f.txt", 5, 10)
    assert beyond.error is None
    assert beyond.file_data is not None and beyond.file_data["content"] == ""

    zero = await asyncio.to_thread(backend.read, "/f.txt", 0, 0)
    assert zero.no_lines_requested is True


async def test_read_refuses_windows_beyond_fetch_cap() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _raw_reply(b"0123456789abcdefghij") if frame.frame_type is FrameType.FS_READ else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, read_fetch_max_bytes=8)

    result = await asyncio.to_thread(backend.read, "/big.txt", 1, 5)

    assert result.error is not None
    assert "read window" in result.error


# ---------------------------------------------------------------------------
# write: chunking + credit
# ---------------------------------------------------------------------------


async def test_write_chunks_within_credit_window() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: _raw_reply(b"{}") if frame.frame_type is FrameType.FS_WRITE else [],
        # Initial credit covers exactly one chunk; each received chunk earns
        # the next one — the hub must never oversend.
        grant_per_write_chunk=CHUNK_BYTES,
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, initial_credit_bytes=CHUNK_BYTES)

    payload = "x" * (CHUNK_BYTES * 2 + 10)
    result = await asyncio.to_thread(backend.write, "/out.bin", payload)

    assert result.error is None
    assert result.path == "/out.bin"
    chunks = fake.sent_of(FrameType.FS_WRITE)
    assert [c.payload.chunk_index for c in chunks] == [0, 1, 2]
    assert all(c.payload.total == 3 for c in chunks)
    assert all(c.payload.path == "out.bin" for c in chunks)
    assert b"".join(c.payload.data for c in chunks) == payload.encode("utf-8")
    # Credit grants flowed runtime→hub for the stream.
    credits = fake.received_of(FrameType.RT_CREDIT)
    assert credits and all(c.payload.bytes == CHUNK_BYTES for c in credits)


async def test_write_credit_stall_times_out() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: _raw_reply(b"{}") if frame.frame_type is FrameType.FS_WRITE else [],
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, initial_credit_bytes=8, fs_chunk_timeout_s=0.05)

    result = await asyncio.to_thread(backend.write, "/stall.txt", "y" * 64)

    assert result.error is not None
    assert "credit" in result.error


# ---------------------------------------------------------------------------
# integration guards: import boundary + harness prebuilt acceptance
# ---------------------------------------------------------------------------


def test_backend_module_imports_no_api_or_cli_layer() -> None:
    """The hub proxy is infra: only infra + deepagents protocol imports."""
    source = Path(inspect.getsourcefile(RemoteRuntimeBackend) or "").read_text(encoding="utf-8")
    assert "octop.api" not in source
    assert "octop.cli" not in source


def test_harness_resolve_backend_accepts_prebuilt_instance() -> None:
    """Spike outcome A: prebuilt instances pass through untouched."""
    from octop_harness.backends import resolve_backend

    backend = RemoteRuntimeBackend("E1", session_provider=lambda: None)
    assert resolve_backend(backend) is backend
    assert da_supports_execution(backend) is True


# ---------------------------------------------------------------------------
# session lifecycle: swap kills in-flight ops
# ---------------------------------------------------------------------------


async def test_inflight_ops_die_when_session_closes() -> None:
    session = _session()
    fake = FakeRuntime(session, responder=lambda frame: [])  # never answers
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    task = asyncio.create_task(asyncio.to_thread(backend.read, "/f.txt"))
    await _wait_for(lambda: bool(fake.sent_of(FrameType.FS_READ)))
    await session.close_with(1006, "disconnect")

    with pytest.raises(OctopError) as excinfo:
        await task
    assert excinfo.value.code is ErrorCode.RUNTIME_OFFLINE


async def test_revoked_session_maps_to_runtime_revoked() -> None:
    session = _session()
    fake = FakeRuntime(session, responder=lambda frame: [])
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    task = asyncio.create_task(asyncio.to_thread(backend.read, "/f.txt"))
    await _wait_for(lambda: bool(fake.sent_of(FrameType.FS_READ)))
    await session.close_with(4002, "revoked")

    with pytest.raises(OctopError) as excinfo:
        await task
    assert excinfo.value.code is ErrorCode.RUNTIME_REVOKED


async def test_generation_swap_fails_old_ops_and_next_op_attaches_new_session() -> None:
    old_session = _session(generation=1)
    old_fake = FakeRuntime(old_session, responder=lambda frame: [])
    state: dict[str, SessionLink | None] = {"link": old_fake.link}
    backend = _backend(old_fake, state)

    stale = asyncio.create_task(asyncio.to_thread(backend.read, "/f.txt"))
    await _wait_for(lambda: bool(old_fake.sent_of(FrameType.FS_READ)))

    new_session = _session(generation=2)
    new_fake = FakeRuntime(new_session, responder=lambda frame: _raw_reply(b"fresh\n"))
    state["link"] = new_fake.link
    # Any new op attaches the newer session, which fails the in-flight one.
    fresh = asyncio.create_task(asyncio.to_thread(backend.read, "/g.txt"))

    with pytest.raises(OctopError) as excinfo:
        await stale
    assert excinfo.value.code is ErrorCode.RUNTIME_OFFLINE
    assert (await fresh).file_data is not None
    # The new requests ride the new generation.
    assert new_fake.sent_of(FrameType.FS_READ)[0].generation == 2


async def test_offline_provider_fails_fast_without_loop_hop() -> None:
    state: dict[str, SessionLink | None] = {"link": None}
    backend = RemoteRuntimeBackend("E1", session_provider=lambda: state["link"])

    with pytest.raises(OctopError) as excinfo:
        backend.read("/f.txt")
    assert excinfo.value.code is ErrorCode.RUNTIME_OFFLINE


async def test_sync_op_on_session_loop_is_refused() -> None:
    session = _session()
    fake = FakeRuntime(session, responder=lambda frame: [])
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    with pytest.raises(OctopError) as excinfo:
        backend.read("/f.txt")  # called directly on the running loop
    assert excinfo.value.code is ErrorCode.RUNTIME_UNSUPPORTED


# ---------------------------------------------------------------------------
# edit: harness uniqueness contract + etag conflict mapping
# ---------------------------------------------------------------------------


def _fs_fake(
    stat_doc: dict[str, Any] | None = None,
    file_bytes: bytes = b"",
    edit_reply: Reply | None = None,
) -> FakeRuntime:
    stat_doc = stat_doc or {"size": len(file_bytes), "is_dir": False, "etag": "1:2:3"}

    def respond(frame: Any) -> list[Reply]:
        if frame.frame_type is FrameType.FS_STAT:
            return _doc_reply_bytes(stat_doc)
        if frame.frame_type is FrameType.FS_READ:
            return _raw_reply(file_bytes)
        if frame.frame_type is FrameType.FS_EDIT:
            if edit_reply is not None:
                ft, payload, rid = edit_reply
                return [(ft, payload, frame.request_id if rid == "__RID__" else rid)]
            return _doc_reply_bytes({"occurrences": 1, "etag": "9:9:9"})
        return []

    return FakeRuntime(_session(), responder=respond)


async def test_edit_rejects_non_unique_without_replace_all() -> None:
    fake = _fs_fake(file_bytes=b"foo and foo")
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    result = await asyncio.to_thread(backend.edit, "/f.txt", "foo", "bar")

    assert result.error is not None and "not unique" in result.error
    # The wire never saw fs.edit.
    assert fake.sent_of(FrameType.FS_EDIT) == []


async def test_edit_sends_etag_and_maps_occurrences() -> None:
    fake = _fs_fake(file_bytes=b"foo bar")
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    result = await asyncio.to_thread(backend.edit, "/f.txt", "foo", "baz")

    assert result.error is None
    assert result.path == "/f.txt"
    assert result.occurrences == 1
    edit_frames = fake.sent_of(FrameType.FS_EDIT)
    assert len(edit_frames) == 1
    assert edit_frames[0].payload.etag == "1:2:3"
    assert edit_frames[0].payload.find == "foo"


async def test_edit_conflict_maps_to_conflict_error() -> None:
    fake = _fs_fake(
        file_bytes=b"foo bar",
        edit_reply=(
            FrameType.ERROR,
            ErrorFrame(request_id="__RID__", code=ErrorKind.CONFLICT, detail="etag mismatch"),
            "__RID__",
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    result = await asyncio.to_thread(backend.edit, "/f.txt", "foo", "bar")

    assert result.error is not None and "conflict" in result.error
    assert result.occurrences is None


# ---------------------------------------------------------------------------
# ls / delete / glob / grep
# ---------------------------------------------------------------------------


async def test_ls_maps_entries_back_to_virtual_paths() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _doc_reply_bytes(
                {
                    "entries": [
                        {"path": "/ws/agents/A1/a.py", "is_dir": False, "size": 3},
                        {"path": "/ws/agents/A1/sub", "is_dir": True},
                    ],
                    "truncated": False,
                }
            )
            if frame.frame_type is FrameType.FS_LS
            else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, workspace_base="/ws", agent_id="A1")

    result = await asyncio.to_thread(backend.ls, "/")

    assert result.error is None
    assert [e["path"] for e in (result.entries or [])] == ["/a.py", "/sub"]
    assert result.entries[0]["is_dir"] is False and result.entries[0]["size"] == 3
    # The ls request targeted the per-agent content root.
    assert fake.sent_of(FrameType.FS_LS)[0].payload.path == "/ws/agents/A1"


async def test_delete_is_recursive_and_maps_errors() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _raw_reply(b"{}") if frame.frame_type is FrameType.FS_DELETE else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    result = await asyncio.to_thread(backend.delete, "/dir")

    assert result.error is None and result.path == "/dir"
    assert fake.sent_of(FrameType.FS_DELETE)[0].payload.recursive is True


async def test_glob_caps_and_path_filter() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _doc_reply_bytes(
                {
                    "matches": ["/ws/a.py", "/ws/sub/b.py", "/ws/c.js"],
                    "truncated": True,
                }
            )
            if frame.frame_type is FrameType.FS_GLOB
            else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, workspace_base="/ws")

    result = await asyncio.to_thread(backend.glob, "*.py", "/sub")

    assert result.error is None
    assert result.matches is not None
    assert [m["path"] for m in result.matches] == ["/sub/b.py"]
    assert result.truncated is True
    assert result.truncation_reason == "transport"


async def test_grep_filters_by_path_glob_and_max_count() -> None:
    session = _session()
    matches = [
        {"path": "/ws/a.py", "line": 1, "text": "TODO fix"},
        {"path": "/ws/b.js", "line": 2, "text": "TODO here"},
        {"path": "/ws/a.py", "line": 9, "text": "TODO again"},
    ]
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _doc_reply_bytes({"matches": matches}) if frame.frame_type is FrameType.FS_GREP else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, workspace_base="/ws")

    scoped = await asyncio.to_thread(backend.grep, "TODO", "/a.py")
    assert scoped.error is None
    assert [m["line"] for m in (scoped.matches or [])] == [1, 9]

    globbed = await asyncio.to_thread(backend.grep, "TODO", None, "*.py")
    assert [m["path"] for m in (globbed.matches or [])] == ["/a.py", "/a.py"]

    capped = await asyncio.to_thread(backend.grep, "TODO", None, None, max_count=1)
    assert len(capped.matches or []) == 1
    assert capped.truncated is True


# ---------------------------------------------------------------------------
# .octop system-tree filter + path escape
# ---------------------------------------------------------------------------


async def test_octop_system_paths_are_rejected() -> None:
    session = _session()
    fake = FakeRuntime(session, responder=lambda frame: _raw_reply(b"{}"))
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    write_result = await asyncio.to_thread(backend.write, "/.octop/session.db", "x")
    assert write_result.error is not None
    assert ".octop" in write_result.error and "hub" in write_result.error
    assert fake.sent == []

    read_result = await asyncio.to_thread(backend.read, "/.octop/auth.json")
    assert read_result.error is not None and ".octop" in (read_result.error or "")

    escape = await asyncio.to_thread(backend.write, "/../etc/passwd", "x")
    assert escape.error is not None and "'..'" in escape.error


# ---------------------------------------------------------------------------
# execute: merge order, truncation, queue cap, timers
# ---------------------------------------------------------------------------


def _exec_fake(
    outputs: list[tuple[ExecStream, bytes]],
    *,
    exit_after: bool = True,
    exit_code: int = 0,
    truncated_last: bool = False,
    on_close_exit: int | None = None,
) -> FakeRuntime:
    def respond(frame: Any) -> list[Reply]:
        if frame.frame_type is FrameType.EXEC_OPEN:
            rid = frame.request_id or ""
            exec_id = frame.payload.exec_id
            replies: list[Reply] = [
                (
                    FrameType.EXEC_OUTPUT,
                    ExecOutput(
                        exec_id=exec_id,
                        stream=stream,
                        data=data,
                        truncated=truncated_last and index == len(outputs) - 1,
                    ),
                    None,
                )
                for index, (stream, data) in enumerate(outputs)
            ]
            if exit_after:
                replies.append(
                    (
                        FrameType.EXEC_OUTPUT,
                        ExecOutput(
                            exec_id=exec_id,
                            stream=ExecStream.EXIT,
                            exit_code=exit_code,
                            reason=ExitReason.EXITED,
                            request_id=rid,
                        ),
                        None,
                    )
                )
            return replies
        if frame.frame_type is FrameType.EXEC_CLOSE and on_close_exit is not None:
            return [
                (
                    FrameType.EXEC_OUTPUT,
                    ExecOutput(
                        exec_id=frame.payload.exec_id,
                        stream=ExecStream.EXIT,
                        exit_code=on_close_exit,
                        reason=ExitReason.KILLED,
                    ),
                    None,
                )
            ]
        return []

    return FakeRuntime(_session(), responder=respond)


async def test_execute_merges_streams_in_arrival_order() -> None:
    fake = _exec_fake(
        [
            (ExecStream.STDOUT, b"alpha "),
            (ExecStream.STDERR, b"warn "),
            (ExecStream.STDOUT, b"omega"),
        ]
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    response = await asyncio.to_thread(backend.execute, "echo hi")

    assert response.output == "alpha warn omega"
    assert response.exit_code == 0
    assert response.truncated is False
    open_frames = fake.sent_of(FrameType.EXEC_OPEN)
    assert len(open_frames) == 1
    payload = open_frames[0].payload
    assert isinstance(payload, ExecOpen)
    assert payload.cmd == ("/bin/sh", "-c", "echo hi")
    assert payload.timeout_s == 600


async def test_execute_truncation_marker_sets_flag() -> None:
    fake = _exec_fake([(ExecStream.STDOUT, b"partial")], truncated_last=True)
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    response = await asyncio.to_thread(backend.execute, "cat /dev/urandom")

    assert response.output == "partial"
    assert response.truncated is True


async def test_execute_queue_cap_drain_discards() -> None:
    import octop.infra.runtime.backend as backend_mod

    fake = _exec_fake(
        [
            (ExecStream.STDOUT, b"keep-"),
            (ExecStream.STDOUT, b"discard-me-please"),
        ]
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(backend_mod, "EXEC_QUEUE_BYTES", 8)
        response = await asyncio.to_thread(backend.execute, "yes")

    assert response.output == "keep-"
    assert response.truncated is True


async def test_execute_idle_timeout_sends_close_then_resolves() -> None:
    fake = _exec_fake([], exit_after=False)  # wedged executor: no frames at all
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, exec_idle_timeout_s=0.05, exec_close_grace_s=0.05)

    response = await asyncio.to_thread(backend.execute, "sleep 100")

    assert response.exit_code == 124
    close_frames = fake.sent_of(FrameType.EXEC_CLOSE)
    assert len(close_frames) == 1
    assert isinstance(close_frames[0].payload, ExecClose)


async def test_execute_exit_frame_within_grace_wins() -> None:
    fake = _exec_fake([], exit_after=False, on_close_exit=137)
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, exec_idle_timeout_s=0.05, exec_close_grace_s=1.0)

    response = await asyncio.to_thread(backend.execute, "sleep 100")

    assert response.exit_code == 137


async def test_execute_disabled_returns_error_response() -> None:
    session = _session()
    fake = FakeRuntime(session, responder=lambda frame: [])
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, allow_exec=False)

    response = await asyncio.to_thread(backend.execute, "echo hi")

    assert response.exit_code is None
    assert "disabled" in response.output
    assert fake.sent == []


# ---------------------------------------------------------------------------
# upload / download
# ---------------------------------------------------------------------------


async def test_upload_download_batch_semantics() -> None:
    session = _session()
    fake = FakeRuntime(
        session,
        responder=lambda frame: (
            _raw_reply(b"{}")
            if frame.frame_type is FrameType.FS_WRITE
            else _raw_reply(b"file-body")
            if frame.frame_type is FrameType.FS_READ
            else []
        ),
    )
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state)

    uploads = await asyncio.to_thread(
        backend.upload_files, [("/ok.txt", b"data"), ("/.octop/x", b"nope")]
    )
    assert uploads[0].error is None
    assert uploads[1].error is not None and ".octop" in (uploads[1].error or "")

    downloads = await asyncio.to_thread(backend.download_files, ["/ok.txt"])
    assert downloads[0].content == b"file-body"
    assert downloads[0].error is None


# ---------------------------------------------------------------------------
# fs timers: per-chunk timeout sends fs.cancel
# ---------------------------------------------------------------------------


async def test_fs_chunk_timeout_sends_cancel() -> None:
    session = _session()
    fake = FakeRuntime(session, responder=lambda frame: [])
    state: dict[str, SessionLink | None] = {"link": fake.link}
    backend = _backend(fake, state, fs_chunk_timeout_s=0.05)

    result = await asyncio.to_thread(backend.read, "/stall.txt")

    assert result.error is not None
    assert fake.sent_of(FrameType.FS_CANCEL), "timeout must send fs.cancel"


# ---------------------------------------------------------------------------
# ownership enforcement at spec resolution
# ---------------------------------------------------------------------------


class _EnrollmentRow:
    def __init__(
        self,
        enrollment_id: str,
        owner_user_id: int,
        *,
        allow_exec: bool,
        status: str = "active",
        default_workspace_base: str | None = None,
    ) -> None:
        self.enrollment_id = enrollment_id
        self.owner_user_id = owner_user_id
        self.allow_exec = allow_exec
        self.status = status
        self.default_workspace_base = default_workspace_base


class _EnrollmentRepo:
    def __init__(self, *rows: _EnrollmentRow) -> None:
        self.rows = {r.enrollment_id: r for r in rows}

    def get_by_enrollment_id(self, enrollment_id: str) -> _EnrollmentRow | None:
        return self.rows.get(enrollment_id)


_REPO = _EnrollmentRepo(
    _EnrollmentRow("E1", 7, allow_exec=True, default_workspace_base="/data"),
    _EnrollmentRow("E2", 9, allow_exec=False),
    _EnrollmentRow("E3", 7, allow_exec=True, status="revoked"),
)


def test_ownership_rejection_for_foreign_enrollment() -> None:
    with pytest.raises(OctopError) as excinfo:
        resolve_agent_backend_spec(
            {"type": "remote_runtime", "enrollment_id": "E1"},
            repo=None,
            owner_user_id=8,
            enrollment_repo=_REPO,
        )
    assert excinfo.value.code is ErrorCode.FORBIDDEN


def test_ownership_rejection_for_unknown_owner() -> None:
    with pytest.raises(OctopError) as excinfo:
        resolve_agent_backend_spec(
            {"type": "remote_runtime", "enrollment_id": "E1"},
            repo=None,
            owner_user_id=None,
            enrollment_repo=_REPO,
        )
    assert excinfo.value.code is ErrorCode.FORBIDDEN


def test_admin_bypass_and_spec_enrichment() -> None:
    spec = resolve_agent_backend_spec(
        {"type": "remote_runtime", "enrollment_id": "E1"},
        repo=None,
        owner_user_id=999,
        enrollment_repo=_REPO,
        is_admin=True,
    )
    assert spec["allow_exec"] is True
    assert spec["workspace_base"] == "/data"
    assert backend_spec_supports_execution(spec) is True


def test_inactive_enrollment_degrades_to_unsupported() -> None:
    with pytest.raises(OctopError) as excinfo:
        resolve_agent_backend_spec(
            {"type": "remote_runtime", "enrollment_id": "E3"},
            repo=None,
            owner_user_id=7,
            enrollment_repo=_REPO,
        )
    assert excinfo.value.code is ErrorCode.RUNTIME_UNSUPPORTED


def test_missing_enrollment_and_no_repo_defaults() -> None:
    with pytest.raises(OctopError) as excinfo:
        resolve_agent_backend_spec(
            {"type": "remote_runtime", "enrollment_id": "nope"},
            repo=None,
            owner_user_id=7,
            enrollment_repo=_REPO,
        )
    assert excinfo.value.code is ErrorCode.NOT_FOUND

    offline = resolve_agent_backend_spec(
        {"type": "remote_runtime", "enrollment_id": "E1"}, repo=None
    )
    assert offline["allow_exec"] is False
    assert backend_spec_supports_execution(offline) is False


# ---------------------------------------------------------------------------
# exec-target composite wrapper
# ---------------------------------------------------------------------------


class _FilesStub:
    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> Any:
        return ("files", file_path, offset, limit)


class _ExecStub(SandboxBackendProtocol):
    def __init__(self) -> None:
        self.commands: list[str] = []

    @property
    def id(self) -> str:  # noqa: A003
        return "exec-stub"

    def execute(self, command: str, *, timeout: int | None = None) -> Any:
        self.commands.append(command)
        return ("exec", command, timeout)


def test_exec_target_composite_routes_ops_and_execution() -> None:
    files = _FilesStub()
    exec_stub = _ExecStub()
    composite = ExecTargetCompositeBackend(files, exec_stub)

    assert isinstance(composite, SandboxBackendProtocol)
    assert da_supports_execution(composite) is True
    assert composite.read("/f.txt", 1, 2) == ("files", "/f.txt", 1, 2)
    assert composite.execute("ls -la", timeout=5) == ("exec", "ls -la", 5)
    assert exec_stub.commands == ["ls -la"]
    assert composite.id.endswith("exec-stub")
