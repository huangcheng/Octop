"""Shared codec for the remote runtime WebSocket protocol (spec §5/§8).

Wire format
-----------
Every WebSocket text message is one compact JSON object:

    {"v": [generation, minor_version], "t": "<frame_type>",
     "r": <request_id | null>, "d": {<payload fields>}}

- ``v`` is the codec header: ``[generation, minor_version]``. The generation
  advances only on a completed hello -> welcome handshake and is carried by
  EVERY frame; frames whose generation does not match the session generation
  are dropped (``is_stale``).
- ``t`` is the frame type (``rt.*`` / ``fs.*`` / ``exec.*`` / ``error``).
- ``r`` is the request id (``null`` for session-scoped control frames).
- ``d`` is the typed payload. Bytes-valued payload fields are transported
  base64-encoded under ``<field>_b64`` keys (never raw, the envelope is JSON).

Structurally malformed frames (bad JSON, missing/ill-typed header fields,
unknown frame type, wrong payload shape) raise :class:`ProtocolFormatError`;
callers map that to close code ``4004``. Oversize messages (> FRAME_CAP_BYTES)
are rejected the same way. A pure stdlib module: the daemon and the hub import
it identically.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import json
from enum import Enum, StrEnum
from typing import Any, get_type_hints

# --- Codec constants (spec §8 / G5) -----------------------------------------

#: Hard cap on one wire frame (encoded JSON text, bytes). Oversize -> 4004.
FRAME_CAP_BYTES = 1024 * 1024
#: Chunk size for streamed bytes payloads (fs.write / exec streams).
CHUNK_BYTES = 64 * 1024
#: Bounded queue for exec stream output, per exec.
EXEC_QUEUE_BYTES = 4 * 1024 * 1024
#: Bounded queue for fs streams, in chunks, per stream.
FS_STREAM_QUEUE_CHUNKS = 8
#: Fixed initial credit grant per stream, negotiated in welcome/ready.
INITIAL_CREDIT_BYTES = 512 * 1024
#: Data frame rate cap (fps) — violations are per-op, not session death.
DATA_RATE_CAP_FPS = 1000
#: Control frame rate cap (fps) — violation -> 4004.
CONTROL_RATE_CAP_FPS = 100


class ProtocolFormatError(Exception):
    """Structurally malformed frame — terminal close code 4004."""


class FrameType(StrEnum):
    """Every frame type on the wire (spec §8 table)."""

    RT_HELLO = "rt.hello"
    RT_WELCOME = "rt.welcome"
    RT_READY = "rt.ready"
    RT_CREDIT = "rt.credit"
    RT_ROTATE = "rt.rotate"
    RT_BYE = "rt.bye"
    RT_PING = "rt.ping"
    RT_PONG = "rt.pong"
    FS_LS = "fs.ls"
    FS_STAT = "fs.stat"
    FS_MKDIR = "fs.mkdir"
    FS_READ = "fs.read"
    FS_WRITE = "fs.write"
    FS_EDIT = "fs.edit"
    FS_DELETE = "fs.delete"
    FS_RENAME = "fs.rename"
    FS_GLOB = "fs.glob"
    FS_GREP = "fs.grep"
    FS_CANCEL = "fs.cancel"
    EXEC_OPEN = "exec.open"
    EXEC_INPUT = "exec.input"
    EXEC_OUTPUT = "exec.output"
    EXEC_CLOSE = "exec.close"
    ERROR = "error"


class ExecStream(StrEnum):
    STDOUT = "stdout"
    STDERR = "stderr"
    #: liveness heartbeat; control class, flows at zero data credit.
    ACTIVITY = "activity"
    EXIT = "exit"


class ExitReason(StrEnum):
    EXITED = "exited"
    KILLED = "killed"
    TIMEOUT = "timeout"


class ErrorKind(StrEnum):
    UNSUPPORTED_FRAME = "unsupported_frame"
    CAPABILITY_UNSUPPORTED = "capability_unsupported"
    PATH_ESCAPE = "path_escape"
    QUOTA_EXCEEDED = "quota_exceeded"
    CONFLICT = "conflict"
    DUPLICATE_REQUEST = "duplicate_request"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    INTERNAL = "internal"


# --- Typed payloads ----------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class RtHello:
    hostname: str
    os: str
    arch: str
    client_version: str
    workspace_base: str
    roots: tuple[str, ...]
    free_bytes: int
    mtime_granularity_ns: int
    capabilities: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class RtWelcome:
    generation: int
    capabilities: tuple[str, ...]
    quotas: dict[str, int]
    initial_credit_bytes: int
    rotation_pending: bool


@dataclasses.dataclass(frozen=True)
class RtReady:
    generation: int
    initial_credit_bytes: int


@dataclasses.dataclass(frozen=True)
class RtCredit:
    stream_id: str
    bytes: int


@dataclasses.dataclass(frozen=True)
class RtRotate:
    rotation_pending: bool = True


@dataclasses.dataclass(frozen=True)
class RtBye:
    pass


@dataclasses.dataclass(frozen=True)
class RtPing:
    """Application-level keepalive request (Starlette hides WS-layer pings)."""

    pass


@dataclasses.dataclass(frozen=True)
class RtPong:
    """Application-level keepalive reply."""

    pass


@dataclasses.dataclass(frozen=True)
class FsPath:
    path: str


@dataclasses.dataclass(frozen=True)
class FsMkdir:
    path: str
    # Create is safe, delete is destructive: asymmetric defaults (spec §8).
    recursive: bool = True


@dataclasses.dataclass(frozen=True)
class FsRead:
    path: str
    offset: int = 0
    length: int | None = None


@dataclasses.dataclass(frozen=True)
class FsWrite:
    path: str
    data: bytes = b""
    append: bool = False
    chunk_index: int = 0
    total: int = 1
    truncated: bool = False


@dataclasses.dataclass(frozen=True)
class FsEdit:
    path: str
    find: str
    replace: str
    all: bool = False
    etag: str | None = None


@dataclasses.dataclass(frozen=True)
class FsDelete:
    path: str
    recursive: bool = False


@dataclasses.dataclass(frozen=True)
class FsRename:
    old: str
    new: str


@dataclasses.dataclass(frozen=True)
class FsPattern:
    pattern: str
    #: Optional walk base for glob/grep (absolute, contained in roots);
    #: empty walks every advertised root. Hub-side scoping aid.
    path: str = ""


@dataclasses.dataclass(frozen=True)
class FsCancel:
    request_id: str


@dataclasses.dataclass(frozen=True)
class ExecOpen:
    request_id: str
    exec_id: str
    cmd: tuple[str, ...]
    work_dir: str
    env: tuple[tuple[str, str], ...] = ()
    timeout_s: int = 600
    pty: bool = False
    winsize: tuple[int, int] | None = None
    clean_env: bool = False
    unset_env: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class ExecInput:
    exec_id: str
    data: bytes = b""
    eof: bool = False
    resize: tuple[int, int] | None = None
    chunk_index: int = 0
    total: int = 1
    truncated: bool = False


@dataclasses.dataclass(frozen=True)
class ExecOutput:
    exec_id: str
    stream: ExecStream
    data: bytes = b""
    exit_code: int | None = None
    reason: ExitReason | None = None
    request_id: str | None = None
    chunk_index: int = 0
    total: int = 1
    truncated: bool = False


@dataclasses.dataclass(frozen=True)
class ExecClose:
    exec_id: str


@dataclasses.dataclass(frozen=True)
class ErrorFrame:
    request_id: str | None
    code: ErrorKind
    detail: str = ""


#: frame type -> payload dataclass. All types are registered; malformed
#: payloads are detected against the dataclass field set.
PAYLOAD_TYPES: dict[FrameType, type] = {
    FrameType.RT_HELLO: RtHello,
    FrameType.RT_WELCOME: RtWelcome,
    FrameType.RT_READY: RtReady,
    FrameType.RT_CREDIT: RtCredit,
    FrameType.RT_ROTATE: RtRotate,
    FrameType.RT_BYE: RtBye,
    FrameType.RT_PING: RtPing,
    FrameType.RT_PONG: RtPong,
    FrameType.FS_LS: FsPath,
    FrameType.FS_STAT: FsPath,
    FrameType.FS_MKDIR: FsMkdir,
    FrameType.FS_READ: FsRead,
    FrameType.FS_WRITE: FsWrite,
    FrameType.FS_EDIT: FsEdit,
    FrameType.FS_DELETE: FsDelete,
    FrameType.FS_RENAME: FsRename,
    FrameType.FS_GLOB: FsPattern,
    FrameType.FS_GREP: FsPattern,
    FrameType.FS_CANCEL: FsCancel,
    FrameType.EXEC_OPEN: ExecOpen,
    FrameType.EXEC_INPUT: ExecInput,
    FrameType.EXEC_OUTPUT: ExecOutput,
    FrameType.EXEC_CLOSE: ExecClose,
    FrameType.ERROR: ErrorFrame,
}

# Enum-valued fields: wire carries the value string, dataclass wants the enum.
_ENUM_FIELDS: dict[type, dict[str, type[Enum]]] = {
    ExecOutput: {"stream": ExecStream, "reason": ExitReason},
    ErrorFrame: {"code": ErrorKind},
}


# --- Envelope ----------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class FrameEnvelope:
    generation: int
    minor_version: int
    frame_type: FrameType
    request_id: str | None
    payload: Any = None


def _payload_to_wire(payload_cls: type, payload: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in dataclasses.fields(payload_cls):
        value = getattr(payload, field.name)
        if isinstance(value, bytes):
            out[f"{field.name}_b64"] = base64.b64encode(value).decode("ascii")
        elif isinstance(value, Enum):
            out[field.name] = value.value
        elif isinstance(value, dict):
            out[field.name] = dict(value)
        elif isinstance(value, tuple):
            out[field.name] = [item.value if isinstance(item, Enum) else item for item in value]
        else:
            out[field.name] = value
    return out


def _coerce(hint: Any, value: Any) -> Any:
    """Convert wire JSON (lists) back to payload types (tuples)."""
    if value is None:
        return None
    args = getattr(hint, "__args__", ()) or ()
    origins = [getattr(hint, "__origin__", None), *(getattr(a, "__origin__", None) for a in args)]
    if isinstance(value, list) and tuple in origins:
        tuple_hint = (
            hint
            if getattr(hint, "__origin__", None) is tuple
            else next(a for a in args if getattr(a, "__origin__", None) is tuple)
        )
        inner = tuple_hint.__args__[0] if tuple_hint.__args__ else None
        return tuple(_coerce(inner, item) for item in value)
    return value


def _payload_from_wire(payload_cls: type, wire: Any) -> Any:
    if not isinstance(wire, dict):
        raise ProtocolFormatError("payload must be a JSON object")
    hints = get_type_hints(payload_cls)
    known = {f.name for f in dataclasses.fields(payload_cls)}
    enum_fields = _ENUM_FIELDS.get(payload_cls, {})
    kwargs: dict[str, Any] = {}
    for key, value in wire.items():
        if key.endswith("_b64") and key[: -len("_b64")] in known:
            name = key[: -len("_b64")]
            if not isinstance(value, str):
                raise ProtocolFormatError(f"{name}: base64 field must be a string")
            try:
                kwargs[name] = base64.b64decode(value, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ProtocolFormatError(f"{name}: invalid base64") from exc
            continue
        if key not in known:
            raise ProtocolFormatError(f"unknown payload field {key!r}")
        enum_cls = enum_fields.get(key)
        try:
            if enum_cls is not None:
                # Nullable enum (e.g. ExitReason | None) passes None through.
                kwargs[key] = None if value is None else enum_cls(value)
            elif key in hints and hints[key] is bytes:
                raise ProtocolFormatError(f"{key}: expected {key}_b64")
            else:
                kwargs[key] = _coerce(hints.get(key), value)
        except ValueError as exc:
            raise ProtocolFormatError(f"{key}: invalid enum value {value!r}") from exc
    missing = {
        f.name
        for f in dataclasses.fields(payload_cls)
        if f.name not in kwargs
        and (f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING)
    }
    if missing:
        raise ProtocolFormatError(f"missing payload fields: {sorted(missing)}")
    try:
        return payload_cls(**kwargs)
    except TypeError as exc:
        raise ProtocolFormatError(f"invalid payload: {exc}") from exc


def encode_frame(
    generation: int,
    minor_version: int,
    frame_type: FrameType,
    payload: Any = None,
    request_id: str | None = None,
) -> str:
    """Serialize one frame to its compact JSON wire form."""
    payload_cls = PAYLOAD_TYPES.get(frame_type)
    if payload_cls is None:
        raise ProtocolFormatError(f"unknown frame type {frame_type!r}")
    doc = {
        "v": [generation, minor_version],
        "t": frame_type.value,
        "r": request_id,
        "d": _payload_to_wire(payload_cls, payload) if payload is not None else {},
    }
    return json.dumps(doc, separators=(",", ":"))


def fits_frame_cap(encoded: str | bytes) -> bool:
    """Whether an encoded frame fits FRAME_CAP_BYTES."""
    size = len(encoded.encode("utf-8")) if isinstance(encoded, str) else len(encoded)
    return size <= FRAME_CAP_BYTES


def decode_frame(text: str | bytes) -> FrameEnvelope:
    """Parse one wire frame; malformed structure -> ProtocolFormatError."""
    if not fits_frame_cap(text):
        raise ProtocolFormatError("frame exceeds FRAME_CAP_BYTES")
    try:
        doc = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProtocolFormatError("invalid JSON") from exc
    if not isinstance(doc, dict):
        raise ProtocolFormatError("envelope must be a JSON object")
    version = doc.get("v")
    if (
        not isinstance(version, list)
        or len(version) != 2
        or not all(isinstance(item, int) and not isinstance(item, bool) for item in version)
    ):
        raise ProtocolFormatError("header v must be [generation, minor_version]")
    generation, minor_version = version
    frame_type_raw = doc.get("t")
    if not isinstance(frame_type_raw, str):
        raise ProtocolFormatError("t must be a string")
    try:
        frame_type = FrameType(frame_type_raw)
    except ValueError as exc:
        raise ProtocolFormatError(f"unknown frame type {frame_type_raw!r}") from exc
    request_id = doc.get("r")
    if request_id is not None and not isinstance(request_id, str):
        raise ProtocolFormatError("r must be a string or null")
    payload = _payload_from_wire(PAYLOAD_TYPES[frame_type], doc.get("d", {}))
    return FrameEnvelope(
        generation=generation,
        minor_version=minor_version,
        frame_type=frame_type,
        request_id=request_id,
        payload=payload,
    )


# --- Frame classification (spec §5) ------------------------------------------


def is_control(frame: FrameEnvelope | FrameType, payload: Any = None) -> bool:
    """CONTROL class = queue/credit-exempt, rate-capped frames.

    ping/pong and WS close are transport opcodes, not typed frames; the
    ``ping``/``pong``/``close`` names classify as control for symmetry.
    ``exec.output`` splits: stdout/stderr chunks are DATA, exit/activity
    heartbeats are CONTROL.
    """
    if isinstance(frame, FrameEnvelope):
        frame_type = frame.frame_type
        payload = frame.payload
    else:
        frame_type = frame
    name = frame_type.value if isinstance(frame_type, FrameType) else str(frame_type)
    if name in ("ping", "pong", "close"):
        return True
    if name.startswith("rt."):
        return True
    if name in ("fs.cancel", "exec.close", "error"):
        return True
    if name == "exec.output" and isinstance(payload, ExecOutput):
        return payload.stream in (ExecStream.EXIT, ExecStream.ACTIVITY)
    return False


# --- Generation fencing ------------------------------------------------------


def is_stale(frame_generation: int, session_generation: int) -> bool:
    """Both sides drop frames whose generation predates the session."""
    return frame_generation != session_generation


# --- Credit accounting (bytes, per stream) -----------------------------------


class CreditLedger:
    """Per-stream byte credit: cumulative sent MUST NOT exceed cumulative granted.

    Oversend is NOT raised here — the caller treats a failed :meth:`may_send`
    as the per-stream truncation + stream error path (never session death).
    """

    def __init__(self) -> None:
        self._granted: dict[str, int] = {}
        self._sent: dict[str, int] = {}

    def grant(self, stream_id: str, n: int) -> None:
        if n <= 0:
            raise ValueError("grant must be positive")
        self._granted[stream_id] = self._granted.get(stream_id, 0) + n

    def record_sent(self, stream_id: str, n: int) -> None:
        self._sent[stream_id] = self._sent.get(stream_id, 0) + n

    def may_send(self, stream_id: str, n: int) -> bool:
        """Whether cumulative sent + n stays within cumulative granted."""
        return self._sent.get(stream_id, 0) + n <= self._granted.get(stream_id, 0)

    def granted(self, stream_id: str) -> int:
        return self._granted.get(stream_id, 0)

    def sent(self, stream_id: str) -> int:
        return self._sent.get(stream_id, 0)

    def release(self, stream_id: str) -> None:
        """Stream completed/cancelled/errored: drop its counters."""
        self._granted.pop(stream_id, None)
        self._sent.pop(stream_id, None)


# --- Chunking ----------------------------------------------------------------


def chunk_bytes(data: bytes, chunk_size: int = CHUNK_BYTES) -> list[bytes]:
    """Split payload into CHUNK_BYTES chunks (last chunk may be short)."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)] or [b""]


class ChunkBuffer:
    """Out-of-order reassembly buffer for one chunked stream.

    ``total < 0`` means unknown length (list-ish replies): completion then
    arrives via a truncation marker instead of the last index. A truncation
    marker on a known-total stream completes it early with ``truncated=True``.
    """

    def __init__(self) -> None:
        self._chunks: dict[int, bytes] = {}
        self._total: int | None = None
        self._truncated: bool = False

    @property
    def truncated(self) -> bool:
        return self._truncated

    def add(
        self, chunk_index: int, total: int, data: bytes, truncated: bool = False
    ) -> bytes | None:
        """Add one chunk; return the reassembled bytes when complete, else None."""
        if chunk_index < 0:
            raise ProtocolFormatError("chunk_index must be >= 0")
        if self._total is None:
            self._total = total
        elif self._total != total:
            raise ProtocolFormatError(f"total mismatch: {total} != {self._total}")
        if chunk_index in self._chunks and self._chunks[chunk_index] != data:
            raise ProtocolFormatError(f"conflicting duplicate chunk {chunk_index}")
        if self._total >= 0 and chunk_index >= self._total:
            raise ProtocolFormatError(
                f"chunk_index {chunk_index} out of range for total {self._total}"
            )
        if truncated:
            self._truncated = True
        self._chunks[chunk_index] = data
        if self._truncated:
            complete = True
        elif self._total >= 0:
            complete = len(self._chunks) == self._total
        else:
            # Unknown length: only the truncation marker completes the stream.
            complete = False
        if complete:
            last = max(self._chunks)
            if len(self._chunks) != last + 1:
                raise ProtocolFormatError(
                    f"gap in chunks: have {sorted(self._chunks)}, expected 0..{last}"
                )
            return b"".join(self._chunks[i] for i in range(last + 1))
        return None
