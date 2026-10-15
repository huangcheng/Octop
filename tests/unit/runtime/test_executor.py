"""Unit tests for the daemon-side fs executor (spec §8/§8.2).

Reply convention under test: successes ride ``fs.write`` frames echoing the
request id (raw bytes for ``fs.read``, JSON docs otherwise; reassembled via
:class:`ChunkBuffer`), failures are ``error`` frames. Containment, atomicity,
credit, quotas, cancellation, and sweep semantics per spec.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest

from octop.infra.runtime.client import RuntimeClient
from octop.infra.runtime.enrollment_store import EnrollmentStore
from octop.infra.runtime.executor import TMP_PREFIX, FsExecutor
from octop.infra.runtime.protocol import (
    CHUNK_BYTES,
    ChunkBuffer,
    ErrorFrame,
    ErrorKind,
    FrameEnvelope,
    FrameType,
    FsCancel,
    FsDelete,
    FsEdit,
    FsMkdir,
    FsPath,
    FsPattern,
    FsRead,
    FsRename,
    FsWrite,
    RtCredit,
    decode_frame,
)

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX-only semantics")


# --- helpers -------------------------------------------------------------------


class RecordingSender:
    def __init__(self) -> None:
        self.frames: list[tuple[FrameType, Any, str | None]] = []

    async def __call__(self, frame_type: FrameType, payload: Any, request_id: str | None) -> None:
        self.frames.append((frame_type, payload, request_id))

    def replies(self, rid: str) -> list[tuple[FrameType, Any, str | None]]:
        return [f for f in self.frames if f[2] == rid]

    def error(self, rid: str) -> ErrorFrame | None:
        for frame_type, payload, request_id in self.frames:
            if frame_type is FrameType.ERROR and request_id == rid:
                assert isinstance(payload, ErrorFrame)
                return payload
        return None

    def doc(self, rid: str) -> dict[str, Any] | None:
        buffer = ChunkBuffer()
        assembled: bytes | None = None
        for frame_type, payload, request_id in self.frames:
            if frame_type is FrameType.FS_WRITE and request_id == rid:
                assert isinstance(payload, FsWrite)
                assembled = buffer.add(
                    payload.chunk_index, payload.total, payload.data, payload.truncated
                )
        if assembled is None:
            return None
        return json.loads(assembled)

    def raw(self, rid: str) -> bytes | None:
        buffer = ChunkBuffer()
        assembled: bytes | None = None
        for frame_type, payload, request_id in self.frames:
            if frame_type is FrameType.FS_WRITE and request_id == rid:
                assert isinstance(payload, FsWrite)
                assembled = buffer.add(
                    payload.chunk_index, payload.total, payload.data, payload.truncated
                )
        return assembled


def make_executor(tmp_path: Path, **kwargs: Any) -> tuple[FsExecutor, RecordingSender]:
    sender = RecordingSender()
    executor = FsExecutor([str(tmp_path)], sender=sender, **kwargs)  # type: ignore[list-item]
    return executor, sender


def env(frame_type: FrameType, payload: Any, request_id: str) -> FrameEnvelope:
    return FrameEnvelope(1, 1, frame_type, request_id, payload)


async def op(executor: FsExecutor, frame_type: FrameType, payload: Any, request_id: str) -> None:
    await getattr(executor, f"handle_{frame_type.value.replace('.', '_')}")(
        env(frame_type, payload, request_id)
    )


async def write_remote(
    executor: FsExecutor, path: str, data: bytes, request_id: str, *, append: bool = False
) -> None:
    await op(executor, FrameType.FS_WRITE, FsWrite(path=path, data=data, append=append), request_id)


async def read_remote(
    executor: FsExecutor,
    sender: RecordingSender,
    path: str,
    request_id: str,
    *,
    offset: int = 0,
    length: int | None = None,
) -> bytes | None:
    await op(
        executor, FrameType.FS_READ, FsRead(path=path, offset=offset, length=length), request_id
    )
    return sender.raw(request_id)


async def start_read(
    executor: FsExecutor, path: str, request_id: str, *, length: int | None = None
) -> asyncio.Task[None]:
    task = asyncio.create_task(
        op(executor, FrameType.FS_READ, FsRead(path=path, length=length), request_id)
    )
    for _ in range(200):  # until admitted and (credit-)blocked
        if request_id in executor._read_tasks:
            break
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.02)
    return task


async def grant_credit(executor: FsExecutor, stream_id: str, nbytes: int) -> None:
    await op(executor, FrameType.RT_CREDIT, RtCredit(stream_id=stream_id, bytes=nbytes), stream_id)


def local(path: Path, name: str, content: bytes = b"") -> Path:
    target = path / name
    target.write_bytes(content)
    return target


def temps_in(tmp_path: Path) -> list[str]:
    return [p.name for p in tmp_path.rglob(f"{TMP_PREFIX}*")]


# --- roundtrip: mkdir/write/read/stat -------------------------------------------


async def test_write_read_stat_roundtrip(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_MKDIR, FsMkdir(path=str(tmp_path / "d")), "r1")
    assert sender.doc("r1") == {}
    await write_remote(executor, str(tmp_path / "d/a.txt"), b"hello world", "r2")
    doc = sender.doc("r2")
    assert doc is not None and doc["etag"]

    await op(executor, FrameType.FS_STAT, FsPath(path=str(tmp_path / "d/a.txt")), "r3")
    stat = sender.doc("r3")
    assert stat == {
        "size": 11,
        "is_dir": False,
        "etag": stat["etag"],
        "mtime_ns": stat["mtime_ns"],
    }
    assert stat["etag"] == doc["etag"]
    assert await read_remote(executor, sender, str(tmp_path / "d/a.txt"), "r4") == b"hello world"
    assert (
        await read_remote(executor, sender, str(tmp_path / "d/a.txt"), "r5", offset=6, length=5)
        == b"world"
    )
    # short read: length clamped at EOF
    assert (
        await read_remote(executor, sender, str(tmp_path / "d/a.txt"), "r6", offset=6, length=500)
        == b"world"
    )
    # EOF: offset beyond size -> single empty chunk
    assert await read_remote(executor, sender, str(tmp_path / "d/a.txt"), "r7", offset=99) == b""
    # directory stat
    await op(executor, FrameType.FS_STAT, FsPath(path=str(tmp_path / "d")), "r8")
    assert sender.doc("r8") == {
        "size": sender.doc("r8")["size"],
        "is_dir": True,
        "etag": sender.doc("r8")["etag"],
        "mtime_ns": sender.doc("r8")["mtime_ns"],
    }
    assert sender.error("r1") is None


async def test_empty_file_read_is_eof(tmp_path: Path) -> None:
    local(tmp_path, "empty.txt", b"")
    executor, sender = make_executor(tmp_path)
    assert await read_remote(executor, sender, str(tmp_path / "empty.txt"), "r1") == b""


# --- containment ---------------------------------------------------------------


async def test_dotdot_and_relative_rejected(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "../x")), "r1")
    assert sender.error("r1") is not None and sender.error("r1").code is ErrorKind.PATH_ESCAPE
    await write_remote(executor, str(tmp_path / "a/../.."), b"x", "r2")
    assert sender.error("r2").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
    await op(executor, FrameType.FS_STAT, FsPath(path="relative/x"), "r3")
    assert sender.error("r3").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]


async def test_outside_root_rejected(tmp_path: Path) -> None:
    outside = local(tmp_path.parent, "octop_outside_target.txt", b"secret")
    try:
        executor, sender = make_executor(tmp_path)
        for rid, frame_type, payload in [
            ("r1", FrameType.FS_READ, FsRead(path=str(outside))),
            ("r2", FrameType.FS_STAT, FsPath(path=str(outside))),
            ("r3", FrameType.FS_WRITE, FsWrite(path=str(outside), data=b"x")),
            ("r4", FrameType.FS_DELETE, FsDelete(path=str(outside))),
            ("r5", FrameType.FS_RENAME, FsRename(old=str(outside), new=str(tmp_path / "z"))),
        ]:
            await op(executor, frame_type, payload, rid)
            assert sender.error(rid) is not None
            assert sender.error(rid).code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        assert outside.read_bytes() == b"secret"
    finally:
        outside.unlink(missing_ok=True)


@posix_only
async def test_symlink_escape_rejected(tmp_path: Path) -> None:
    outside_dir = tmp_path.parent / "octop_outside_dir"
    outside_dir.mkdir(exist_ok=True)
    try:
        victim = local(outside_dir, "victim.txt", b"secret")
        local(tmp_path, "file.txt", b"kept")
        os.symlink(victim, tmp_path / "link.txt")
        os.symlink(outside_dir, tmp_path / "linkdir")
        executor, sender = make_executor(tmp_path)
        await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "link.txt")), "r1")
        assert sender.error("r1").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        await write_remote(executor, str(tmp_path / "link.txt"), b"evil", "r2")
        assert sender.error("r2").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        await op(executor, FrameType.FS_STAT, FsPath(path=str(tmp_path / "link.txt")), "r3")
        assert sender.error("r3").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        await op(
            executor,
            FrameType.FS_EDIT,
            FsEdit(path=str(tmp_path / "link.txt"), find="a", replace="b"),
            "r4",
        )
        assert sender.error("r4").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        # symlink as an intermediate component
        await op(
            executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "linkdir/victim.txt")), "r5"
        )
        assert sender.error("r5").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        assert victim.read_bytes() == b"secret"
    finally:
        (tmp_path / "link.txt").unlink(missing_ok=True)
        (tmp_path / "linkdir").unlink(missing_ok=True)
        import shutil

        shutil.rmtree(outside_dir, ignore_errors=True)


@posix_only
async def test_symlink_swap_mid_edit_hits_containment(tmp_path: Path) -> None:
    """The macOS residual rename-swap race fixture (spec §8.2): a swapped-in
    symlink is caught by O_NOFOLLOW, never silently followed."""
    outside_dir = tmp_path.parent / "octop_swap_dir"
    outside_dir.mkdir(exist_ok=True)
    try:
        victim = local(outside_dir, "victim.txt", b"outside content")
        target = local(tmp_path, "doc.txt", b"inside content")
        executor, sender = make_executor(tmp_path)
        await op(executor, FrameType.FS_STAT, FsPath(path=str(target)), "r1")
        etag = sender.doc("r1")["etag"]
        # swap: replace the target with a symlink to the outside victim
        target.unlink()
        os.symlink(victim, target)
        await op(
            executor,
            FrameType.FS_EDIT,
            FsEdit(path=str(target), find="a", replace="b", etag=etag),
            "r2",
        )
        assert sender.error("r2").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        assert victim.read_bytes() == b"outside content"
    finally:
        (tmp_path / "doc.txt").unlink(missing_ok=True)
        import shutil

        shutil.rmtree(outside_dir, ignore_errors=True)


@posix_only
async def test_fifo_rejected_not_regular(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / "pipe")
    executor, sender = make_executor(tmp_path, chunk_timeout_seconds=2.0)
    await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "pipe")), "r1")
    error = sender.error("r1")
    assert error is not None and error.code is ErrorKind.INTERNAL
    assert "regular" in error.detail
    await write_remote(executor, str(tmp_path / "pipe"), b"x", "r2")
    error = sender.error("r2")
    assert error is not None and "regular" in error.detail


async def test_directory_write_rejected(tmp_path: Path) -> None:
    (tmp_path / "adir").mkdir()
    executor, sender = make_executor(tmp_path)
    await write_remote(executor, str(tmp_path / "adir"), b"x", "r1")
    error = sender.error("r1")
    assert error is not None and error.code is ErrorKind.INTERNAL
    await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "adir")), "r2")

    error = sender.error("r2")
    assert error is not None and error.code is ErrorKind.INTERNAL


async def test_roots_revalidated_per_operation(tmp_path: Path) -> None:
    current = [str(tmp_path)]
    sender = RecordingSender()
    executor = FsExecutor(lambda: list(current), sender=sender, chunk_timeout_seconds=1.0)
    target = local(tmp_path, "a.txt", b"data")
    assert await read_remote(executor, sender, str(target), "r1") == b"data"
    current.clear()  # roots shrank below the in-use path
    await op(executor, FrameType.FS_READ, FsRead(path=str(target)), "r2")
    assert sender.error("r2").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
    await write_remote(executor, str(target), b"x", "r3")
    assert sender.error("r3").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
    assert target.read_bytes() == b"data"


# --- atomic write / append / edit -------------------------------------------------


async def test_atomic_write_interrupted_leaves_target(tmp_path: Path) -> None:
    target = local(tmp_path, "a.txt", b"old")
    executor, sender = make_executor(tmp_path)
    # chunk 0 of 2: temp staged, target untouched
    await op(
        executor,
        FrameType.FS_WRITE,
        FsWrite(path=str(target), data=b"new-part-1|", chunk_index=0, total=2),
        "r1",
    )
    assert target.read_bytes() == b"old"
    assert len(temps_in(tmp_path)) == 1
    assert sender.doc("r1") is None
    await op(executor, FrameType.FS_CANCEL, FsCancel(request_id="r1"), "cancel")
    assert target.read_bytes() == b"old"
    assert temps_in(tmp_path) == []
    assert sender.doc("r1") is None  # cancelled streams reply nothing
    # a fresh write still works afterwards
    await write_remote(executor, str(target), b"brand new", "r2")
    assert target.read_bytes() == b"brand new"
    assert sender.doc("r2") is not None


async def test_append_torn_residual_documented(tmp_path: Path) -> None:
    """Append writes each batch in-place with fsync: an interrupted transfer
    keeps already-written batches (documented torn residual, spec §8)."""
    target = local(tmp_path, "log.txt", b"first\n")
    executor, sender = make_executor(tmp_path)
    await op(
        executor,
        FrameType.FS_WRITE,
        FsWrite(path=str(target), data=b"second\n", append=True, chunk_index=0, total=2),
        "r1",
    )
    assert target.read_bytes() == b"first\nsecond\n"
    await op(executor, FrameType.FS_CANCEL, FsCancel(request_id="r1"), "cancel")
    assert target.read_bytes() == b"first\nsecond\n"  # residual kept
    assert temps_in(tmp_path) == []


async def test_write_append_creates_and_completes(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await write_remote(executor, str(tmp_path / "n.txt"), b"a", "r1", append=True)
    await write_remote(executor, str(tmp_path / "n.txt"), b"b", "r2", append=True)
    assert (tmp_path / "n.txt").read_bytes() == b"ab"
    assert sender.doc("r2")["etag"]


async def test_edit_etag_conflict_and_replacement(tmp_path: Path) -> None:
    target = local(tmp_path, "a.txt", b"foo foo")
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_STAT, FsPath(path=str(target)), "r1")
    etag = sender.doc("r1")["etag"]
    await op(
        executor,
        FrameType.FS_EDIT,
        FsEdit(path=str(target), find="foo", replace="bar", etag="bogus"),
        "r2",
    )
    assert sender.error("r2").code is ErrorKind.CONFLICT  # type: ignore[union-attr]
    assert target.read_bytes() == b"foo foo"
    # all=False: first occurrence only; occurrences = replacements made
    await op(
        executor,
        FrameType.FS_EDIT,
        FsEdit(path=str(target), find="foo", replace="bar", etag=etag),
        "r3",
    )
    doc = sender.doc("r3")
    assert doc["occurrences"] == 1
    assert target.read_bytes() == b"bar foo"
    # all=True replaces every occurrence
    target.write_bytes(b"foo foo")
    await op(executor, FrameType.FS_STAT, FsPath(path=str(target)), "r4")
    await op(
        executor,
        FrameType.FS_EDIT,
        FsEdit(
            path=str(target), find="foo", replace="baz", all=True, etag=sender.doc("r4")["etag"]
        ),
        "r5",
    )
    assert sender.doc("r5")["occurrences"] == 2
    assert target.read_bytes() == b"baz baz"
    # zero occurrences: no replacement, hub maps occurrences==0 to its error
    await op(executor, FrameType.FS_EDIT, FsEdit(path=str(target), find="nope", replace="x"), "r6")
    assert sender.doc("r6")["occurrences"] == 0
    assert target.read_bytes() == b"baz baz"


async def test_rename_regular_only_and_contained(tmp_path: Path) -> None:
    a = local(tmp_path, "a.txt", b"x")
    b = tmp_path / "b.txt"
    (tmp_path / "dir").mkdir()
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_RENAME, FsRename(old=str(a), new=str(b)), "r1")
    assert sender.error("r1") is None
    assert b.read_bytes() == b"x" and not a.exists()
    await op(
        executor,
        FrameType.FS_RENAME,
        FsRename(old=str(tmp_path / "dir"), new=str(tmp_path / "dir2")),
        "r2",
    )
    assert sender.error("r2").code is ErrorKind.CAPABILITY_UNSUPPORTED  # type: ignore[union-attr]
    outside = tmp_path.parent / "octop_outside_ren.txt"
    try:
        await op(executor, FrameType.FS_RENAME, FsRename(old=str(b), new=str(outside)), "r3")
        assert sender.error("r3").code is ErrorKind.PATH_ESCAPE  # type: ignore[union-attr]
        assert b.exists() and not outside.exists()
        await op(
            executor, FrameType.FS_RENAME, FsRename(old=str(tmp_path / "missing"), new=str(b)), "r4"
        )
        error = sender.error("r4")
        assert error is not None and error.code is ErrorKind.INTERNAL
    finally:
        outside.unlink(missing_ok=True)


async def test_delete_semantics(tmp_path: Path) -> None:
    local(tmp_path, "f.txt", b"x")
    (tmp_path / "dir" / "sub").mkdir(parents=True)
    local(tmp_path / "dir", "child.txt", b"y")
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_DELETE, FsDelete(path=str(tmp_path / "dir")), "r1")
    error = sender.error("r1")
    assert error is not None and "not empty" in error.detail
    assert (tmp_path / "dir").exists()
    await op(
        executor, FrameType.FS_DELETE, FsDelete(path=str(tmp_path / "dir"), recursive=True), "r2"
    )
    assert not (tmp_path / "dir").exists()
    await op(executor, FrameType.FS_DELETE, FsDelete(path=str(tmp_path / "f.txt")), "r3")
    assert not (tmp_path / "f.txt").exists()
    (tmp_path / "emptydir").mkdir()
    await op(executor, FrameType.FS_DELETE, FsDelete(path=str(tmp_path / "emptydir")), "r4")
    assert not (tmp_path / "emptydir").exists()


@posix_only
async def test_delete_symlink_only_removes_link(tmp_path: Path) -> None:
    outside_dir = tmp_path.parent / "octop_del_dir"
    outside_dir.mkdir(exist_ok=True)
    try:
        victim = local(outside_dir, "v.txt", b"keep")
        os.symlink(victim, tmp_path / "l.txt")
        executor, sender = make_executor(tmp_path)
        await op(executor, FrameType.FS_DELETE, FsDelete(path=str(tmp_path / "l.txt")), "r1")
        assert sender.error("r1") is None
        assert not (tmp_path / "l.txt").exists()
        assert victim.read_bytes() == b"keep"
    finally:
        (tmp_path / "l.txt").unlink(missing_ok=True)
        import shutil

        shutil.rmtree(outside_dir, ignore_errors=True)


async def test_mkdir_recursive_default(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_MKDIR, FsMkdir(path=str(tmp_path / "a/b/c")), "r1")
    assert (tmp_path / "a/b/c").is_dir()
    await op(
        executor, FrameType.FS_MKDIR, FsMkdir(path=str(tmp_path / "a/b/c")), "r2"
    )  # idempotent
    assert sender.doc("r2") == {}
    await op(
        executor, FrameType.FS_MKDIR, FsMkdir(path=str(tmp_path / "x/y"), recursive=False), "r3"
    )
    assert sender.error("r3") is not None  # missing parent
    await op(executor, FrameType.FS_MKDIR, FsMkdir(path=str(tmp_path / "a"), recursive=False), "r4")
    assert sender.error("r4") is not None  # EEXIST


# --- ls / glob / grep ---------------------------------------------------------------


async def test_ls_single_level_with_truncation(tmp_path: Path) -> None:
    for i in range(6):
        local(tmp_path, f"f{i}.txt", b"x")
    (tmp_path / "sub").mkdir()
    executor, sender = make_executor(tmp_path, list_cap=5)
    await op(executor, FrameType.FS_LS, FsPath(path=str(tmp_path)), "r1")
    doc = sender.doc("r1")
    assert doc["truncated"] is True
    assert len(doc["entries"]) == 5
    assert [e["path"].rsplit("/", 1)[-1] for e in doc["entries"]][:5] == [
        "f0.txt",
        "f1.txt",
        "f2.txt",
        "f3.txt",
        "f4.txt",
    ]
    assert all("is_dir" in e and "size" in e for e in doc["entries"])
    await op(executor, FrameType.FS_LS, FsPath(path=str(tmp_path / "sub")), "r2")
    assert sender.doc("r2") == {"entries": [], "truncated": False}


async def test_glob_matches_and_skips_symlinks(tmp_path: Path) -> None:
    local(tmp_path, "a.py", b"alpha")
    (tmp_path / "sub").mkdir()
    local(tmp_path / "sub", "b.py", b"beta")
    local(tmp_path / "sub", "c.txt", b"gamma")
    if os.name == "posix":
        os.symlink(tmp_path / "a.py", tmp_path / "link.py")
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_GLOB, FsPattern(pattern="**/*.py"), "r1")
    doc = sender.doc("r1")
    assert sorted(Path(m).name for m in doc["matches"]) == ["a.py", "b.py"]
    assert doc["truncated"] is False
    await op(executor, FrameType.FS_GLOB, FsPattern(pattern="*.py"), "r2")
    assert [Path(m).name for m in sender.doc("r2")["matches"]] == ["a.py"]
    # absolute pattern + scoped base
    await op(
        executor, FrameType.FS_GLOB, FsPattern(pattern="**/*.py", path=str(tmp_path / "sub")), "r3"
    )
    assert [Path(m).name for m in sender.doc("r3")["matches"]] == ["b.py"]


async def test_glob_cap_truncation_marker(tmp_path: Path) -> None:
    for i in range(3):
        local(tmp_path, f"m{i}.py", b"x")
    executor, sender = make_executor(tmp_path, list_cap=2)
    await op(executor, FrameType.FS_GLOB, FsPattern(pattern="*.py"), "r1")
    doc = sender.doc("r1")
    assert doc["truncated"] is True and len(doc["matches"]) == 2


async def test_grep_lines_binary_and_scope(tmp_path: Path) -> None:
    local(tmp_path, "a.py", b"alpha needle\nother\n")
    (tmp_path / "sub").mkdir()
    local(tmp_path / "sub", "c.txt", b"needle here\n")
    local(tmp_path, "bin.dat", b"needle\x00binary")
    if os.name == "posix":
        os.symlink(tmp_path / "a.py", tmp_path / "link.py")
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_GREP, FsPattern(pattern="needle"), "r1")
    doc = sender.doc("r1")
    got = {(Path(m["path"]).name, m["line"], m["text"]) for m in doc["matches"]}
    assert got == {("a.py", 1, "alpha needle"), ("c.txt", 1, "needle here")}
    assert all(Path(m["path"]).name != "bin.dat" for m in doc["matches"])
    assert doc["truncated"] is False
    await op(
        executor, FrameType.FS_GREP, FsPattern(pattern="needle", path=str(tmp_path / "sub")), "r2"
    )
    assert [Path(m["path"]).name for m in sender.doc("r2")["matches"]] == ["c.txt"]


async def test_grep_cap_and_doc_chunking(tmp_path: Path) -> None:
    # many long-named entries force a multi-chunk doc reply
    for i in range(300):
        local(tmp_path, f"n{i:03d}" + "x" * 200 + ".txt", b"hit\n")
    executor, sender = make_executor(tmp_path, list_cap=10_000)
    await op(executor, FrameType.FS_GREP, FsPattern(pattern="hit"), "r1")
    doc = sender.doc("r1")
    assert doc is not None and len(doc["matches"]) == 300
    chunks = [p for t, p, r in sender.frames if t is FrameType.FS_WRITE and r == "r1"]
    assert len(chunks) > 1  # exercised ChunkBuffer-compatible reassembly
    assert all(len(c.data) <= CHUNK_BYTES for c in chunks)


async def test_grep_invalid_regex_is_per_request_error(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await op(executor, FrameType.FS_GREP, FsPattern(pattern="("), "r1")
    error = sender.error("r1")
    assert error is not None and error.code is ErrorKind.INTERNAL
    # executor (and session) still serve afterwards
    await op(executor, FrameType.FS_STAT, FsPath(path=str(tmp_path)), "r2")
    assert sender.doc("r2") is not None


# --- credit -------------------------------------------------------------------------


async def test_read_within_credit_and_replenishment(tmp_path: Path) -> None:
    local(tmp_path, "a.txt", b"0123456789ab")  # 12 bytes
    executor, sender = make_executor(tmp_path, initial_credit_bytes=4, chunk_timeout_seconds=2.0)
    task = await start_read(executor, str(tmp_path / "a.txt"), "r1")
    assert sender.raw("r1") is None  # blocked: no oversend without credit
    await grant_credit(executor, "r1", 8)  # 4 + 8 >= 12
    await asyncio.wait_for(task, 2.0)
    assert sender.raw("r1") == b"0123456789ab"
    assert sender.error("r1") is None


async def test_read_credit_wait_timeout_is_per_request(tmp_path: Path) -> None:
    local(tmp_path, "a.txt", b"0123456789ab")
    executor, sender = make_executor(tmp_path, initial_credit_bytes=4, chunk_timeout_seconds=0.05)
    task = await start_read(executor, str(tmp_path / "a.txt"), "r1")
    await asyncio.wait_for(task, 2.0)
    error = sender.error("r1")
    assert error is not None and error.code is ErrorKind.TIMEOUT
    # session survives: the next read (within initial credit) succeeds
    local(tmp_path, "b.txt", b"ok")
    assert await read_remote(executor, sender, str(tmp_path / "b.txt"), "r2") == b"ok"


async def test_write_oversend_is_stream_error_not_death(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, initial_credit_bytes=8)
    await write_remote(executor, str(tmp_path / "a.txt"), b"x" * 100, "r1")
    error = sender.error("r1")
    assert error is not None and error.code is ErrorKind.INTERNAL
    assert "oversend" in error.detail
    assert not (tmp_path / "a.txt").exists()
    assert temps_in(tmp_path) == []
    # executor keeps serving
    local(tmp_path, "b.txt", b"ok")
    assert await read_remote(executor, sender, str(tmp_path / "b.txt"), "r2") == b"ok"


async def test_write_replenishes_credit_to_hub(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, initial_credit_bytes=8)
    # two chunks: 6 bytes consume the 8-byte grant to 2 remaining (< half),
    # replenishing before the 4-byte tail can flow
    await op(
        executor,
        FrameType.FS_WRITE,
        FsWrite(path=str(tmp_path / "a.txt"), data=b"012345", chunk_index=0, total=2),
        "r1",
    )
    await op(
        executor,
        FrameType.FS_WRITE,
        FsWrite(path=str(tmp_path / "a.txt"), data=b"6789", chunk_index=1, total=2),
        "r1",
    )
    error = sender.error("r1")
    assert error is None, error
    credits = [p for t, p, r in sender.frames if t is FrameType.RT_CREDIT]
    assert credits  # replenishment grants flowed for the consumed chunks
    assert (tmp_path / "a.txt").read_bytes() == b"0123456789"


async def test_single_write_cap(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, single_write_cap_bytes=8)
    await write_remote(executor, str(tmp_path / "a.txt"), b"x" * 9, "r1")
    error = sender.error("r1")
    assert error is not None and error.code is ErrorKind.QUOTA_EXCEEDED
    assert temps_in(tmp_path) == []


# --- quotas / duplicates / cancel -------------------------------------------------------


async def test_duplicate_in_flight_request(tmp_path: Path) -> None:
    local(tmp_path, "a.txt", b"0123456789ab")
    executor, sender = make_executor(tmp_path, initial_credit_bytes=4, chunk_timeout_seconds=5.0)
    task = await start_read(executor, str(tmp_path / "a.txt"), "r1")
    await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "a.txt")), "r1")
    assert sender.error("r1").code is ErrorKind.DUPLICATE_REQUEST  # type: ignore[union-attr]
    await grant_credit(executor, "r1", 8)
    await asyncio.wait_for(task, 2.0)
    assert sender.raw("r1") == b"0123456789ab"


async def test_inflight_cap_and_release(tmp_path: Path) -> None:
    local(tmp_path, "a.txt", b"0123456789ab")
    executor, sender = make_executor(
        tmp_path, max_inflight_ops=2, initial_credit_bytes=4, chunk_timeout_seconds=5.0
    )
    t1 = await start_read(executor, str(tmp_path / "a.txt"), "r1")
    t2 = await start_read(executor, str(tmp_path / "a.txt"), "r2")
    await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "a.txt"), length=1), "r3")
    assert sender.error("r3").code is ErrorKind.QUOTA_EXCEEDED  # type: ignore[union-attr]
    await grant_credit(executor, "r1", 8)
    await grant_credit(executor, "r2", 8)
    await asyncio.wait_for(asyncio.gather(t1, t2), 2.0)
    # slots freed: a new op is admitted
    assert await read_remote(executor, sender, str(tmp_path / "a.txt"), "r4", length=1) == b"0"


async def test_cancel_releases_admission_and_locks(tmp_path: Path) -> None:
    local(tmp_path, "a.txt", b"0123456789ab")
    executor, sender = make_executor(
        tmp_path, max_inflight_ops=1, initial_credit_bytes=4, chunk_timeout_seconds=5.0
    )
    task = await start_read(executor, str(tmp_path / "a.txt"), "r1")
    await op(executor, FrameType.FS_CANCEL, FsCancel(request_id="r1"), "cancel")
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2.0)
    assert sender.error("r1") is None  # silent drop for cancelled streams
    # the admission slot and any held locks are free again
    local(tmp_path, "b.txt", b"xy")
    assert await read_remote(executor, sender, str(tmp_path / "b.txt"), "r2") == b"xy"
    target = local(tmp_path, "c.txt", b"v1")
    await op(executor, FrameType.FS_EDIT, FsEdit(path=str(target), find="v1", replace="v2"), "r3")
    await op(executor, FrameType.FS_EDIT, FsEdit(path=str(target), find="v2", replace="v3"), "r4")
    assert target.read_bytes() == b"v3"  # per-path lock released after edit


async def test_read_rate_admission(tmp_path: Path) -> None:
    local(tmp_path, "a.txt", b"0123456789")
    executor, sender = make_executor(tmp_path, read_rate_bytes_per_minute=5)
    assert await read_remote(executor, sender, str(tmp_path / "a.txt"), "r1", length=4) == b"0123"
    await op(executor, FrameType.FS_READ, FsRead(path=str(tmp_path / "a.txt"), length=4), "r2")
    assert sender.error("r2").code is ErrorKind.QUOTA_EXCEEDED  # type: ignore[union-attr]
    # already-admitted small reads may still finish
    assert await read_remote(executor, sender, str(tmp_path / "a.txt"), "r3", length=1) == b"0"


# --- temp sweeper ------------------------------------------------------------------------


async def test_sweep_removes_only_orphan_temps(tmp_path: Path) -> None:
    stray = tmp_path / f"{TMP_PREFIX}orphan"
    stray.write_bytes(b"junk")
    target = local(tmp_path, "a.txt", b"old")
    executor, sender = make_executor(tmp_path)
    await op(
        executor,
        FrameType.FS_WRITE,
        FsWrite(path=str(target), data=b"p1", chunk_index=0, total=2),
        "r1",
    )
    live = temps_in(tmp_path)
    assert sorted(live) == sorted([stray.name, live[0]])
    removed = await executor.sweep()
    assert removed == 1
    assert not stray.exists()
    assert len(temps_in(tmp_path)) == 1  # the live transfer temp survives
    await op(executor, FrameType.FS_CANCEL, FsCancel(request_id="r1"), "cancel")
    assert await executor.sweep() == 0  # cancel already unlinked it
    assert temps_in(tmp_path) == []


# --- client wiring -------------------------------------------------------------------------


class FakeConn:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)


async def test_client_wiring_routes_fs_frames(tmp_path: Path) -> None:
    executor = FsExecutor([str(tmp_path)])
    client = RuntimeClient(EnrollmentStore(tmp_path / "enrollment.json"), fs_executor=executor)
    assert client._handlers[FrameType.FS_READ] == executor.handle_fs_read
    assert client._handlers[FrameType.FS_CANCEL] == executor.handle_fs_cancel
    assert client._handlers[FrameType.RT_CREDIT] == executor.handle_rt_credit
    # not connected: replies fail as op errors, never crash the loop
    with pytest.raises(ConnectionError):
        await client._send_frame(FrameType.RT_BYE)
    # connected: replies carry generation and request id
    conn = FakeConn()
    client._conn = conn
    client._generation = 7
    target = local(tmp_path, "a.txt", b"data")
    await client._handlers[FrameType.FS_READ](
        env(FrameType.FS_READ, FsRead(path=str(target)), "req-1")
    )
    reply = decode_frame(conn.sent[0])
    assert reply.generation == 7
    assert reply.frame_type is FrameType.FS_WRITE
    assert reply.request_id == "req-1"
    assert isinstance(reply.payload, FsWrite) and reply.payload.data == b"data"
    await client.stop()
    assert executor._closed


async def test_client_without_executor_unchanged(tmp_path: Path) -> None:
    client = RuntimeClient(EnrollmentStore(tmp_path / "enrollment.json"))
    assert not any(t.value.startswith("fs.") for t in client._handlers)
