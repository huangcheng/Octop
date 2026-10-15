"""Unit tests for the daemon-side exec executor (spec §8/§8.1/§8.2/§8.3).

Reply convention under test: execs stream ``exec.output`` frames keyed by the
hub-provided ``exec_id`` (stdout/stderr DATA, credit-accounted) and terminate
with a single ``exit`` frame (drain-before-exit, ``request_id`` echo, reason
∈ exited|killed|timeout). ``exec.open`` failures are ``error`` frames. Kill
paths escalate group TERM → grace → group KILL and reap.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from octop.infra.runtime.client import RuntimeClient
from octop.infra.runtime.enrollment_store import EnrollmentStore
from octop.infra.runtime.exec_executor import (
    DEFAULT_CONCURRENT_EXECS,
    ExecExecutor,
    ExecStart,
    build_spawn_env,
    clamp_timeout_s,
    clean_env_floor,
)
from octop.infra.runtime.executor import FsExecutor
from octop.infra.runtime.protocol import (
    ErrorFrame,
    ErrorKind,
    ExecInput,
    ExecOpen,
    ExecOutput,
    ExecStream,
    ExitReason,
    FrameEnvelope,
    FrameType,
    RtCredit,
)

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX-only semantics")

PY = sys.executable
ECHO_HI = (PY, "-c", "print('hi')")
SLEEP_30 = (PY, "-c", "import time; time.sleep(30)")
#: Writes 200 bytes (truncation fires), survives 0.4 s, then exits cleanly —
#: proves the process keeps running past the output cap.
CHATTY = (
    PY,
    "-c",
    "import sys, time; sys.stdout.write('x' * 200); sys.stdout.flush();"
    " time.sleep(0.4); print('END')",
)
INTERLEAVED = (
    PY,
    "-c",
    "import sys\n"
    "for i in range(3):\n"
    "    sys.stdout.write(f'out{i}\\n'); sys.stdout.flush()\n"
    "    sys.stderr.write(f'err{i}\\n'); sys.stderr.flush()\n",
)
UPPERCAT = (
    PY,
    "-c",
    "import sys\ndata = sys.stdin.read()\nsys.stdout.write(data.upper())\n",
)
SLEEP_SILENT = (PY, "-c", "import time; time.sleep(0.6)")


# --- helpers -------------------------------------------------------------------


class RecordingSender:
    def __init__(self) -> None:
        self.frames: list[tuple[FrameType, Any, str | None]] = []

    async def __call__(self, frame_type: FrameType, payload: Any, request_id: str | None) -> None:
        self.frames.append((frame_type, payload, request_id))

    def outputs(self, exec_id: str) -> list[ExecOutput]:
        return [
            p
            for t, p, _ in self.frames
            if t is FrameType.EXEC_OUTPUT and isinstance(p, ExecOutput) and p.exec_id == exec_id
        ]

    def exit_frame(self, exec_id: str) -> ExecOutput | None:
        for p in self.outputs(exec_id):
            if p.stream is ExecStream.EXIT:
                return p
        return None

    def stream_bytes(self, exec_id: str, stream: ExecStream) -> bytes:
        return b"".join(p.data for p in self.outputs(exec_id) if p.stream is stream)

    def activity_count(self, exec_id: str) -> int:
        return sum(1 for p in self.outputs(exec_id) if p.stream is ExecStream.ACTIVITY)

    def error(self, rid: str) -> ErrorFrame | None:
        for t, p, request_id in self.frames:
            if t is FrameType.ERROR and request_id == rid:
                assert isinstance(p, ErrorFrame)
                return p
        return None

    def credits(self, stream_id: str) -> list[int]:
        return [
            p.bytes
            for t, p, _ in self.frames
            if t is FrameType.RT_CREDIT and isinstance(p, RtCredit) and p.stream_id == stream_id
        ]


def make_executor(tmp_path: Path, **kwargs: Any) -> tuple[ExecExecutor, RecordingSender]:
    sender = RecordingSender()
    defaults: dict[str, Any] = {
        "kill_grace_seconds": 0.5,
        "drain_grace_seconds": 0.5,
        "credit_wait_timeout_seconds": 2.0,
    }
    defaults.update(kwargs)
    executor = ExecExecutor([str(tmp_path)], sender=sender, **defaults)  # type: ignore[list-item]
    return executor, sender


def open_frame(payload: ExecOpen) -> FrameEnvelope:
    return FrameEnvelope(1, 1, FrameType.EXEC_OPEN, payload.request_id, payload)


def payload(rid: str, cmd: tuple[str, ...] = ECHO_HI, **kwargs: Any) -> ExecOpen:
    kwargs.setdefault("work_dir", "")
    return ExecOpen(request_id=rid, exec_id=f"ex_{rid}", cmd=cmd, **kwargs)


async def open_exec(executor: ExecExecutor, pl: ExecOpen) -> None:
    await executor.handle_exec_open(open_frame(pl))


async def start_open(executor: ExecExecutor, pl: ExecOpen) -> asyncio.Task[None]:
    """Open in the background; returns once the request id is in flight."""
    task = asyncio.create_task(open_exec(executor, pl))
    assert await until(lambda: pl.request_id in executor._requests)
    return task


async def send_input(executor: ExecExecutor, exec_id: str, **kwargs: Any) -> None:
    await executor.handle_exec_input(
        FrameEnvelope(1, 1, FrameType.EXEC_INPUT, None, ExecInput(exec_id=exec_id, **kwargs))
    )


async def send_close(executor: ExecExecutor, exec_id: str) -> None:
    from octop.infra.runtime.protocol import ExecClose

    await executor.handle_exec_close(
        FrameEnvelope(1, 1, FrameType.EXEC_CLOSE, None, ExecClose(exec_id=exec_id))
    )


async def grant_credit(executor: ExecExecutor, exec_id: str, nbytes: int) -> None:
    await executor.handle_rt_credit(
        FrameEnvelope(1, 1, FrameType.RT_CREDIT, None, RtCredit(stream_id=exec_id, bytes=nbytes))
    )


async def until(predicate: Any, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return bool(predicate())


# --- pure helpers ----------------------------------------------------------------


def test_clamp_timeout() -> None:
    assert clamp_timeout_s(0) == 1
    assert clamp_timeout_s(-5) == 1
    assert clamp_timeout_s(30) == 30
    assert clamp_timeout_s(600) == 600
    assert clamp_timeout_s(99999) == 3600


def test_clean_env_floor_per_os() -> None:
    floor = clean_env_floor()
    if os.name == "posix":
        assert {"PATH", "HOME", "TMPDIR", "LANG"} <= set(floor)
    else:
        assert {"PATH", "TEMP", "TMP", "SystemRoot", "ComSpec"} <= set(floor)


def test_build_spawn_env_layers() -> None:
    base = payload("r", env=(("FOO", "1"),))
    env = build_spawn_env(base)
    assert env["FOO"] == "1"
    assert env["PATH"]  # floor still applied
    explicit = build_spawn_env(payload("r", env=(("FOO", "1"),), unset_env=("PATH", "FOO")))
    assert "PATH" not in explicit
    assert "FOO" not in explicit
    caller_managed = build_spawn_env(payload("r", clean_env=True, env=(("ONLY", "1"),)))
    assert caller_managed == {"ONLY": "1"}


def test_default_concurrency_quota_values() -> None:
    assert DEFAULT_CONCURRENT_EXECS == 4
    executor = ExecExecutor([("/nonexistent",)], max_concurrent_execs=99)
    assert executor._max_concurrent == 16


# --- open / exit -----------------------------------------------------------------


async def test_spawn_exit_echo(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await open_exec(executor, payload("r1"))
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert exit_frame.exit_code == 0
    assert exit_frame.reason is ExitReason.EXITED
    assert exit_frame.request_id == "r1"
    assert exit_frame.truncated is False
    assert sender.stream_bytes("ex_r1", ExecStream.STDOUT) == b"hi\n"
    assert sender.error("r1") is None
    # exit frame is the last exec.output for the exec (drain-before-exit)
    assert sender.outputs("ex_r1")[-1].stream is ExecStream.EXIT
    assert not executor._execs
    assert "r1" in executor._closed_ids


async def test_stdout_and_stderr_order_preserved_per_stream(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await open_exec(executor, payload("r1", cmd=INTERLEAVED))
    assert sender.stream_bytes("ex_r1", ExecStream.STDOUT) == b"out0\nout1\nout2\n"
    assert sender.stream_bytes("ex_r1", ExecStream.STDERR) == b"err0\nerr1\nerr2\n"
    assert sender.exit_frame("ex_r1") is not None


async def test_hard_timeout_kills_with_reason_timeout(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    started = time.monotonic()
    await open_exec(executor, payload("r1", cmd=SLEEP_30, timeout_s=1))
    elapsed = time.monotonic() - started
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert exit_frame.reason is ExitReason.TIMEOUT
    # signal death on POSIX (negative), nonzero terminate code on Windows
    assert exit_frame.exit_code is not None and exit_frame.exit_code != 0
    assert elapsed < 10  # daemon-side kill, not the 30 s command


@posix_only
async def test_close_escalates_term_to_kill(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, kill_grace_seconds=0.5)
    task = await start_open(
        executor,
        payload(
            "r1",
            cmd=("/bin/sh", "-c", "trap '' TERM; echo ready; sleep 300"),
            timeout_s=60,
        ),
    )
    # "ready" is printed after the trap is installed — closing earlier would
    # race the shell's own startup and kill it with a plain TERM.
    await until(lambda: any(p.stream is ExecStream.STDOUT for p in sender.outputs("ex_r1")))
    started = time.monotonic()
    await send_close(executor, "ex_r1")
    await task
    elapsed = time.monotonic() - started
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert exit_frame.reason is ExitReason.KILLED
    # TERM was trapped and ignored; SIGKILL finished it.
    assert exit_frame.exit_code == -signal.SIGKILL
    assert elapsed >= 0.4  # the grace window was honored


async def test_output_truncation_keeps_process_running(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, output_cap_bytes=16)
    await open_exec(executor, payload("r1", cmd=CHATTY))
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert exit_frame.truncated is True
    forwarded = len(sender.stream_bytes("ex_r1", ExecStream.STDOUT))
    assert forwarded <= 16
    # truncation never kills the process: it ran to a clean exit
    assert exit_frame.exit_code == 0
    assert exit_frame.reason is ExitReason.EXITED


async def test_activity_frames_while_silent(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, activity_interval_seconds=0.05)
    await open_exec(executor, payload("r1", cmd=SLEEP_SILENT))
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert sender.activity_count("ex_r1") >= 2
    # activity (liveness) frames precede the exit frame
    streams = [p.stream for p in sender.outputs("ex_r1")]
    assert streams[-1] is ExecStream.EXIT
    assert ExecStream.ACTIVITY in streams


async def test_stdin_write_and_eof(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, initial_credit_bytes=16)
    task = await start_open(executor, payload("r1", cmd=UPPERCAT, timeout_s=30))
    await until(lambda: executor._execs["ex_r1"].proc is not None)
    await send_input(executor, "ex_r1", data=b"hello ")
    await send_input(executor, "ex_r1", data=b"world")
    await send_input(executor, "ex_r1", eof=True)
    await task
    assert sender.stream_bytes("ex_r1", ExecStream.STDOUT) == b"HELLO WORLD"
    # inbound credit replenishment mirrors the fs hysteresis
    assert sender.credits("ex_r1")


async def test_input_after_stdin_close_is_noop(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    task = await start_open(executor, payload("r1", cmd=UPPERCAT, timeout_s=30))
    await until(lambda: executor._execs["ex_r1"].proc is not None)
    await send_input(executor, "ex_r1", data=b"abc", eof=True)
    await send_input(executor, "ex_r1", data=b"ignored")  # closed stdin: no-op
    await send_input(executor, "ex_r1", eof=True)  # double eof: no-op
    await task
    assert sender.stream_bytes("ex_r1", ExecStream.STDOUT) == b"ABC"
    assert sender.error("r1") is None


async def test_input_and_close_for_unknown_exec_drop_silently(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await send_input(executor, "ex_nope", data=b"x", eof=True)
    await send_close(executor, "ex_nope")
    assert not sender.frames


async def test_resize_ignored_without_pty(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    task = await start_open(executor, payload("r1", cmd=SLEEP_30, timeout_s=30))
    await until(lambda: executor._execs["ex_r1"].proc is not None)
    await send_input(executor, "ex_r1", resize=(120, 40))  # debug-logged, no error
    await send_close(executor, "ex_r1")
    await task
    assert sender.error("r1") is None
    assert sender.exit_frame("ex_r1") is not None


@posix_only
async def test_pty_basic_echo_and_resize(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    task = await start_open(
        executor,
        payload("r1", cmd=("/bin/sh", "-c", "echo hi"), pty=True, winsize=(80, 24), timeout_s=30),
    )
    await until(lambda: executor._execs["ex_r1"].master_fd is not None)
    await send_input(executor, "ex_r1", resize=(120, 40))
    await task
    assert b"hi" in sender.stream_bytes("ex_r1", ExecStream.STDOUT)
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert exit_frame.exit_code == 0
    assert exit_frame.reason is ExitReason.EXITED


async def test_work_dir_containment_rejected(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    outside = tmp_path.parent
    await open_exec(executor, payload("r1", work_dir=str(outside)))
    error = sender.error("r1")
    assert error is not None
    assert error.code is ErrorKind.PATH_ESCAPE
    assert not executor._execs
    # a contained work_dir works and is used as cwd
    sub = tmp_path / "wd"
    sub.mkdir()
    await open_exec(executor, payload("r2", work_dir=str(sub)))
    assert sender.exit_frame("ex_r2") is not None


async def test_quota_enforced_then_freed(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path, max_concurrent_execs=1)
    first = await start_open(executor, payload("r1", cmd=SLEEP_30, timeout_s=30))
    await open_exec(executor, payload("r2"))
    error = sender.error("r2")
    assert error is not None
    assert error.code is ErrorKind.QUOTA_EXCEEDED
    await send_close(executor, "ex_r1")
    await first
    # the slot is freed once the first exec terminates
    await open_exec(executor, payload("r3"))
    assert sender.exit_frame("ex_r3") is not None


async def test_duplicate_request_dedupe(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    first = await start_open(executor, payload("r1", cmd=SLEEP_30, timeout_s=30))
    original = executor._execs["ex_r1"]
    # identical payload fingerprint -> same exec_id re-attached, no error
    await open_exec(executor, payload("r1", cmd=SLEEP_30, timeout_s=30))
    assert sender.error("r1") is None
    activities = [p for p in sender.outputs("ex_r1") if p.stream is ExecStream.ACTIVITY]
    assert activities  # the ack carries the same exec_id
    assert original is executor._execs["ex_r1"]  # re-attached, not respawned
    # fingerprint mismatch (env differs) -> duplicate_request
    await open_exec(executor, payload("r1", cmd=SLEEP_30, timeout_s=30, env=(("X", "1"),)))
    error = sender.error("r1")
    assert error is not None
    assert error.code is ErrorKind.DUPLICATE_REQUEST
    assert not executor._execs["ex_r1"].killed  # never someone else's process
    await send_close(executor, "ex_r1")
    await first


async def test_completed_request_id_is_duplicate(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    await open_exec(executor, payload("r1"))
    assert sender.exit_frame("ex_r1") is not None
    await open_exec(executor, payload("r1"))
    error = sender.error("r1")
    assert error is not None
    assert error.code is ErrorKind.DUPLICATE_REQUEST


async def test_session_drop_kills_all_execs(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    tasks = [
        await start_open(executor, payload(rid, cmd=SLEEP_30, timeout_s=60)) for rid in ("r1", "r2")
    ]
    await executor.close()  # client.stop() teardown path
    for task in tasks:
        await task
    for rid in ("r1", "r2"):
        exit_frame = sender.exit_frame(f"ex_{rid}")
        assert exit_frame is not None
        assert exit_frame.reason is ExitReason.KILLED
    assert not executor._execs


@posix_only
async def test_close_kills_entire_process_group(tmp_path: Path) -> None:
    executor, sender = make_executor(tmp_path)
    task = await start_open(
        executor,
        payload(
            "r1",
            cmd=("/bin/sh", "-c", "sleep 300 & sleep 300 & wait"),
            timeout_s=60,
        ),
    )
    run = executor._execs["ex_r1"]
    await until(lambda: run.proc is not None and run.proc.returncode is None)
    pgid = run.proc.pid  # start_new_session: pgid == pid
    await send_close(executor, "ex_r1")
    await task
    assert sender.exit_frame("ex_r1") is not None
    # the whole group (both grandchildren) is gone
    with pytest.raises(ProcessLookupError):
        os.killpg(pgid, 0)


async def test_on_exec_start_callback_fires(tmp_path: Path) -> None:
    seen: list[ExecStart] = []

    async def on_start(info: ExecStart) -> None:
        seen.append(info)

    sender = RecordingSender()
    executor = ExecExecutor(
        [str(tmp_path)],
        sender=sender,
        on_exec_start=on_start,
        kill_grace_seconds=0.5,
    )
    await open_exec(executor, payload("r1"))
    assert len(seen) == 1
    assert seen[0].exec_id == "ex_r1"
    assert seen[0].request_id == "r1"
    assert seen[0].cmd == ECHO_HI

    # a failing callback never breaks the exec
    def boom(_info: ExecStart) -> None:
        raise RuntimeError("audit sink down")

    executor._on_exec_start = boom
    await open_exec(executor, payload("r2"))
    assert sender.exit_frame("ex_r2") is not None


async def test_credit_starvation_truncates_stream(tmp_path: Path) -> None:
    executor, sender = make_executor(
        tmp_path, initial_credit_bytes=4, credit_wait_timeout_seconds=0.2
    )
    chatter = (PY, "-c", "print('hello world')")  # 12 bytes vs 4 granted
    await open_exec(executor, payload("r1", cmd=chatter))
    exit_frame = sender.exit_frame("ex_r1")
    assert exit_frame is not None
    assert exit_frame.truncated is True
    assert len(sender.stream_bytes("ex_r1", ExecStream.STDOUT)) <= 4
    # replenishment unblocks a starved stream
    executor2, sender2 = make_executor(tmp_path, initial_credit_bytes=1)

    async def drip_credit() -> None:
        # grant the moment the exec's credit waiter exists (before/while the
        # first send blocks, never after the starvation timeout)
        await until(lambda: "ex_r1" in executor2._credit_events)
        await grant_credit(executor2, "ex_r1", 64)

    await asyncio.gather(open_exec(executor2, payload("r1")), drip_credit())
    assert sender2.stream_bytes("ex_r1", ExecStream.STDOUT) == b"hi\n"


# --- client wiring ----------------------------------------------------------------


class FakeConn:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)


async def test_client_wiring_routes_exec_frames(tmp_path: Path) -> None:
    executor = ExecExecutor([str(tmp_path)])
    client = RuntimeClient(EnrollmentStore(tmp_path / "enrollment.json"), exec_executor=executor)
    assert client._handlers[FrameType.EXEC_OPEN] == executor.handle_exec_open
    assert client._handlers[FrameType.EXEC_INPUT] == executor.handle_exec_input
    assert client._handlers[FrameType.EXEC_CLOSE] == executor.handle_exec_close
    assert client._handlers[FrameType.RT_CREDIT] == client._route_credit
    await client.stop()
    assert executor._closed


async def test_rt_credit_fans_out_to_both_executors(tmp_path: Path) -> None:
    fs = FsExecutor([str(tmp_path)])
    ex = ExecExecutor([str(tmp_path)])
    client = RuntimeClient(
        EnrollmentStore(tmp_path / "enrollment.json"), fs_executor=fs, exec_executor=ex
    )
    # bound in either order, rt.credit reaches both executors
    task = await start_open(ex, payload("r1", cmd=SLEEP_30, timeout_s=30))
    await client._handlers[FrameType.RT_CREDIT](
        FrameEnvelope(1, 1, FrameType.RT_CREDIT, None, RtCredit(stream_id="ex_r1", bytes=64))
    )
    assert ex._out_credit.granted("ex_r1") == 64 + 512 * 1024
    # foreign (fs) stream ids are ignored without error
    await client._handlers[FrameType.RT_CREDIT](
        FrameEnvelope(1, 1, FrameType.RT_CREDIT, None, RtCredit(stream_id="fs_stream", bytes=64))
    )
    await send_close(ex, "ex_r1")
    await task


async def test_client_without_exec_executor_unchanged(tmp_path: Path) -> None:
    client = RuntimeClient(EnrollmentStore(tmp_path / "enrollment.json"))
    assert not any(t.value.startswith("exec.") for t in client._handlers)
