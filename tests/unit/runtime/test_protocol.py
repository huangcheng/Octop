"""Codec unit tests for the shared runtime protocol (spec §5/§8)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from octop.infra.runtime.protocol import (
    CHUNK_BYTES,
    CONTROL_RATE_CAP_FPS,
    DATA_RATE_CAP_FPS,
    EXEC_QUEUE_BYTES,
    FRAME_CAP_BYTES,
    FS_STREAM_QUEUE_CHUNKS,
    INITIAL_CREDIT_BYTES,
    ChunkBuffer,
    CreditLedger,
    ErrorFrame,
    ErrorKind,
    ExecClose,
    ExecInput,
    ExecOpen,
    ExecOutput,
    ExecStream,
    ExitReason,
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
    ProtocolFormatError,
    RtBye,
    RtCredit,
    RtHello,
    RtPing,
    RtPong,
    RtReady,
    RtRotate,
    RtWelcome,
    chunk_bytes,
    decode_frame,
    encode_frame,
    fits_frame_cap,
    is_control,
    is_stale,
)

GEN = 7
MINOR = 1


def rt_hello() -> RtHello:
    return RtHello(
        hostname="box",
        os="darwin",
        arch="arm64",
        client_version="1.2.3",
        workspace_base="/Users/x/w",
        roots=("/Users/x/w", "/tmp/r"),
        free_bytes=12345,
        mtime_granularity_ns=1000,
        capabilities=("fs", "exec"),
    )


def exec_open() -> ExecOpen:
    return ExecOpen(
        request_id="req_1",
        exec_id="ex_1",
        cmd=("bash", "-c", "echo hi"),
        work_dir="/Users/x/w",
        env=(("PATH", "/usr/bin"),),
        timeout_s=60,
        pty=True,
        winsize=(80, 24),
        clean_env=True,
        unset_env=("LC_ALL",),
    )


# One sample payload per frame type: round-trip must be lossless.
SAMPLES: list[tuple[FrameType, Any, str | None]] = [
    (FrameType.RT_HELLO, rt_hello(), None),
    (
        FrameType.RT_WELCOME,
        RtWelcome(
            generation=GEN,
            capabilities=("fs",),
            quotas={"max_execs": 4},
            initial_credit_bytes=INITIAL_CREDIT_BYTES,
            rotation_pending=False,
        ),
        None,
    ),
    (FrameType.RT_READY, RtReady(generation=GEN, initial_credit_bytes=INITIAL_CREDIT_BYTES), None),
    (FrameType.RT_CREDIT, RtCredit(stream_id="req_1", bytes=4096), None),
    (FrameType.RT_ROTATE, RtRotate(rotation_pending=True), None),
    (FrameType.RT_BYE, RtBye(), None),
    (FrameType.RT_PING, RtPing(), None),
    (FrameType.RT_PONG, RtPong(), None),
    (FrameType.FS_LS, FsPath(path="/w/a"), "req_ls"),
    (FrameType.FS_STAT, FsPath(path="/w/a.txt"), "req_stat"),
    (FrameType.FS_MKDIR, FsMkdir(path="/w/d", recursive=False), "req_mkdir"),
    (FrameType.FS_READ, FsRead(path="/w/a.txt", offset=10, length=100), "req_read"),
    (
        FrameType.FS_WRITE,
        FsWrite(path="/w/a.txt", data=b"hello", append=True, chunk_index=0, total=1),
        "req_write",
    ),
    (
        FrameType.FS_EDIT,
        FsEdit(path="/w/a.txt", find="a", replace="b", all=True, etag="sha256:aa"),
        "req_edit",
    ),
    (FrameType.FS_DELETE, FsDelete(path="/w/d", recursive=True), "req_del"),
    (FrameType.FS_RENAME, FsRename(old="/w/a", new="/w/b"), "req_mv"),
    (FrameType.FS_GLOB, FsPattern(pattern="**/*.py"), "req_glob"),
    (FrameType.FS_GREP, FsPattern(pattern="needle"), "req_grep"),
    (FrameType.FS_CANCEL, FsCancel(request_id="req_read"), None),
    (FrameType.EXEC_OPEN, exec_open(), "req_1"),
    (
        FrameType.EXEC_INPUT,
        ExecInput(exec_id="ex_1", data=b"stdin\n", eof=True, resize=(120, 40)),
        None,
    ),
    (
        FrameType.EXEC_OUTPUT,
        ExecOutput(exec_id="ex_1", stream=ExecStream.STDERR, data=b"err"),
        None,
    ),
    (FrameType.EXEC_CLOSE, ExecClose(exec_id="ex_1"), None),
    (FrameType.ERROR, ErrorFrame(request_id="req_1", code=ErrorKind.CONFLICT, detail="etag"), None),
]


@pytest.mark.parametrize(("frame_type", "payload", "request_id"), SAMPLES)
def test_round_trip_every_frame_type(
    frame_type: FrameType, payload: Any, request_id: str | None
) -> None:
    wire = encode_frame(GEN, MINOR, frame_type, payload, request_id)
    env = decode_frame(wire)
    assert env.generation == GEN
    assert env.minor_version == MINOR
    assert env.frame_type == frame_type
    assert env.request_id == request_id
    assert env.payload == payload


def test_wire_envelope_layout_and_compactness() -> None:
    wire = encode_frame(GEN, MINOR, FrameType.RT_CREDIT, RtCredit(stream_id="s", bytes=1))
    doc = json.loads(wire)
    assert doc["v"] == [GEN, MINOR]
    assert doc["t"] == "rt.credit"
    assert doc["r"] is None
    assert doc["d"] == {"stream_id": "s", "bytes": 1}
    assert " " not in wire  # compact encoding


def test_bytes_payload_travels_base64() -> None:
    payload = ExecInput(exec_id="e", data=b"\x00binary\xff")
    doc = json.loads(encode_frame(GEN, MINOR, FrameType.EXEC_INPUT, payload))
    assert "data" not in doc["d"]
    assert doc["d"]["data_b64"]
    assert decode_frame(json.dumps(doc)).payload == payload


def test_exec_output_exit_round_trip() -> None:
    payload = ExecOutput(
        exec_id="e",
        stream=ExecStream.EXIT,
        exit_code=0,
        reason=ExitReason.EXITED,
        request_id="req_1",
    )
    env = decode_frame(encode_frame(GEN, MINOR, FrameType.EXEC_OUTPUT, payload))
    assert env.payload == payload
    assert env.payload.stream is ExecStream.EXIT


@pytest.mark.parametrize(
    ("bad", "why"),
    [
        ("not json", "invalid JSON"),
        ("[]", "not an object"),
        ('{"t":"rt.bye"}', "missing header"),
        ('{"v":[1],"t":"rt.bye","r":null,"d":{}}', "short header"),
        ('{"v":["x",1],"t":"rt.bye","r":null,"d":{}}', "non-int header"),
        ('{"v":[1,1],"t":"nope","r":null,"d":{}}', "unknown type"),
        ('{"v":[1,1],"t":"rt.bye","r":5,"d":{}}', "non-string request id"),
        ('{"v":[1,1],"t":"rt.hello","r":null,"d":{}}', "missing fields"),
        ('{"v":[1,1],"t":"rt.hello","r":null,"d":{"hostname":"h"}}', "missing fields"),
        (
            '{"v":[1,1],"t":"rt.credit","r":null,"d":{"stream_id":"s","bytes":1,"x":2}}',
            "unknown field",
        ),
        (
            '{"v":[1,1],"t":"rt.bye","r":null,"d":{"hostname":"h"}}',
            "unknown field",
        ),
        (
            '{"v":[1,1],"t":"error","r":null,"d":{"request_id":null,"code":"bogus"}}',
            "bad enum",
        ),
        (
            '{"v":[1,1],"t":"exec.input","r":null,"d":{"exec_id":"e","data":"raw"}}',
            "raw bytes field",
        ),
        (
            '{"v":[1,1],"t":"exec.input","r":null,"d":{"exec_id":"e","data_b64":"!!!"}}',
            "bad base64",
        ),
        ('{"v":[1,1],"t":"rt.credit","r":null,"d":[1,2]}', "payload not object"),
    ],
)
def test_malformed_frames_raise(bad: str, why: str) -> None:
    with pytest.raises(ProtocolFormatError):
        decode_frame(bad)


def test_oversize_frame_rejected() -> None:
    assert fits_frame_cap("x" * FRAME_CAP_BYTES)
    assert not fits_frame_cap("x" * (FRAME_CAP_BYTES + 1))
    payload = FsWrite(path="/w/a", data=b"a" * (FRAME_CAP_BYTES))
    wire = encode_frame(GEN, MINOR, FrameType.FS_WRITE, payload)
    with pytest.raises(ProtocolFormatError):
        decode_frame(wire)


def test_constants_match_spec() -> None:
    assert FRAME_CAP_BYTES == 1024 * 1024
    assert CHUNK_BYTES == 64 * 1024
    assert EXEC_QUEUE_BYTES == 4 * 1024 * 1024
    assert FS_STREAM_QUEUE_CHUNKS == 8
    assert INITIAL_CREDIT_BYTES == 512 * 1024
    assert DATA_RATE_CAP_FPS == 1000
    assert CONTROL_RATE_CAP_FPS == 100


# --- is_control --------------------------------------------------------------


@pytest.mark.parametrize("frame_type", list(FrameType))
def test_control_classification_by_type(frame_type: FrameType) -> None:
    env = decode_frame(encode_frame(GEN, MINOR, frame_type, SAMPLES_BY_TYPE[frame_type]))
    expected_ctl = frame_type.value.startswith("rt.") or frame_type.value in (
        "fs.cancel",
        "exec.close",
        "error",
    )
    if frame_type is FrameType.EXEC_OUTPUT:
        assert is_control(env) is False  # stdout sample
        assert is_control(env.frame_type, ExecOutput(exec_id="e", stream=ExecStream.EXIT)) is True
        assert (
            is_control(env.frame_type, ExecOutput(exec_id="e", stream=ExecStream.ACTIVITY)) is True
        )
        assert (
            is_control(env.frame_type, ExecOutput(exec_id="e", stream=ExecStream.STDERR)) is False
        )
    else:
        assert is_control(env) is expected_ctl


def test_transport_opcode_names_are_control() -> None:
    assert is_control("ping")
    assert is_control("pong")
    assert is_control("close")
    assert not is_control("fs.read")


# --- generation fencing ------------------------------------------------------


def test_is_stale() -> None:
    assert is_stale(3, 4)
    assert is_stale(5, 4)
    assert not is_stale(4, 4)


# --- credit ledger -----------------------------------------------------------


def test_ledger_spec_example() -> None:
    ledger = CreditLedger()
    ledger.grant("s", INITIAL_CREDIT_BYTES)  # 524288
    assert ledger.may_send("s", INITIAL_CREDIT_BYTES)
    ledger.record_sent("s", INITIAL_CREDIT_BYTES)
    # Spec example: granted 524288, sent 524288 -> the next byte must wait.
    assert not ledger.may_send("s", 1)
    ledger.grant("s", 1)
    assert ledger.may_send("s", 1)
    assert not ledger.may_send("s", 2)


def test_ledger_streams_are_independent_and_released() -> None:
    ledger = CreditLedger()
    ledger.grant("a", 10)
    ledger.grant("b", 100)
    ledger.record_sent("a", 10)
    assert not ledger.may_send("a", 1)
    assert ledger.may_send("b", 100)
    ledger.release("a")
    assert ledger.granted("a") == 0
    assert ledger.sent("a") == 0
    assert not ledger.may_send("a", 1)  # released: no stale credit remains
    assert ledger.may_send("b", 100)


def test_ledger_grant_must_be_positive() -> None:
    ledger = CreditLedger()
    with pytest.raises(ValueError):
        ledger.grant("s", 0)


# --- chunking ----------------------------------------------------------------


def test_chunk_bytes_split() -> None:
    data = bytes(range(256)) * 1024  # 256 KiB
    chunks = chunk_bytes(data)
    assert len(chunks) == 4
    assert all(len(c) == CHUNK_BYTES for c in chunks[:-1])
    assert b"".join(chunks) == data


def test_chunk_bytes_small_and_empty() -> None:
    assert chunk_bytes(b"abc") == [b"abc"]
    assert chunk_bytes(b"") == [b""]


def test_chunk_buffer_in_order_and_out_of_order() -> None:
    parts = [b"aa", b"bb", b"cc"]
    buf = ChunkBuffer()
    assert buf.add(0, 3, parts[0]) is None
    assert buf.add(2, 3, parts[2]) is None  # out of order
    assert buf.add(1, 3, parts[1]) == b"aabbcc"


def test_chunk_buffer_truncation_marker_on_known_total() -> None:
    buf = ChunkBuffer()
    assert buf.add(0, 5, b"aa") is None
    result = buf.add(1, 5, b"bb", truncated=True)
    assert result == b"aabb"
    assert buf.truncated is True


def test_chunk_buffer_unknown_total_completes_only_via_marker() -> None:
    buf = ChunkBuffer()
    assert buf.add(0, -1, b"aa") is None
    assert buf.add(1, -1, b"bb") is None
    assert not buf.truncated
    assert buf.add(2, -1, b"cc", truncated=True) == b"aabbcc"
    assert buf.truncated


def test_chunk_buffer_truncation_with_gap_raises() -> None:
    buf = ChunkBuffer()
    assert buf.add(0, -1, b"aa") is None
    assert buf.add(2, -1, b"cc") is None
    with pytest.raises(ProtocolFormatError):
        buf.add(3, -1, b"dd", truncated=True)


def test_chunk_buffer_rejects_bad_input() -> None:
    buf = ChunkBuffer()
    with pytest.raises(ProtocolFormatError):
        buf.add(-1, 2, b"x")
    buf.add(0, 2, b"x")
    with pytest.raises(ProtocolFormatError):
        buf.add(0, 3, b"x")  # total mismatch
    with pytest.raises(ProtocolFormatError):
        buf.add(2, 2, b"x")  # out of range
    buf2 = ChunkBuffer()
    buf2.add(0, 2, b"x")
    with pytest.raises(ProtocolFormatError):
        buf2.add(0, 2, b"y")  # conflicting duplicate
    assert buf2.add(0, 2, b"x") is None  # identical duplicate tolerated


def _sample_by_type() -> dict[FrameType, Any]:
    return {ft: payload for ft, payload, _ in SAMPLES}


SAMPLES_BY_TYPE = _sample_by_type()
