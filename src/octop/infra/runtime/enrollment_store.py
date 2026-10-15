"""On-disk enrollment store for the remote runtime daemon (spec §6).

``runtime.json`` lives at ``~/.octop-runtime/runtime.json`` (override the
directory with ``OCTOP_RUNTIME_HOME``) and is schema-versioned. It holds the
hub URL, the ``rk_`` credential, the machine's stable ``machine_id`` (a ULID
generated once at first enroll and preserved across ``--replace``), the
roots allowlist, the TOFU hub TLS pin, and session bookkeeping (last
generation, state, last close code).

The file is written atomically (temp file + ``os.replace``) and, on POSIX,
with mode 0600 — it contains a credential comparable to an SSH private key.
Persist failures propagate to the caller, which MUST hold (never dial with
unpersisted state).
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import stat
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from octop.infra.utils.json_file import JsonFileCorruptError
from octop.infra.utils.ulid import new_ulid

SCHEMA_VERSION = 1

#: Env var overriding the runtime home directory (tests point it at tmp_path).
ENV_RUNTIME_HOME = "OCTOP_RUNTIME_HOME"

STORE_FILENAME = "runtime.json"
DEFAULT_HOME_NAME = ".octop-runtime"

#: Enrollment lifecycle states (spec §6).
STATE_ACTIVE = "active"
STATE_REVOKED = "revoked"
STATE_ROTATED = "rotated"
STATE_VERSION_MISMATCH = "version_mismatch"
STATE_PROTOCOL_HOLD = "protocol_hold"


class EnrollmentMissingError(FileNotFoundError):
    """No enrollment exists at the store path yet."""


@dataclasses.dataclass
class RuntimeEnrollment:
    """One enrollment row, mirrored to and from ``runtime.json``."""

    hub_url: str
    key: str
    machine_id: str
    roots: tuple[str, ...]
    hub_pin: str | None = None
    last_generation: int = 0
    state: str = STATE_ACTIVE
    last_close_code: int | None = None
    # Optional dial tuning persisted at enroll time.
    proxy: str | None = None
    ca_bundle: str | None = None
    # Observed hub TLS pin that did not match ``hub_pin``; presence means the
    # daemon holds until ``octop runtime re-pin --accept`` (spec §6 TOFU).
    pending_pin: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "hub_url": self.hub_url,
            "key": self.key,
            "machine_id": self.machine_id,
            "roots": list(self.roots),
            "hub_pin": self.hub_pin,
            "last_generation": self.last_generation,
            "state": self.state,
            "last_close_code": self.last_close_code,
            "proxy": self.proxy,
            "ca_bundle": self.ca_bundle,
            "pending_pin": self.pending_pin,
        }

    @classmethod
    def from_json(cls, doc: dict[str, Any]) -> RuntimeEnrollment:
        """Build an enrollment from parsed JSON.

        Unknown keys are ignored so a newer daemon can add fields without
        breaking an older reader; unknown ``schema_version`` values are read
        best-effort the same way.
        """
        if not isinstance(doc.get("roots"), list):
            raise JsonFileCorruptError(Path("<runtime.json>"), "roots must be a list")
        raw_close = doc.get("last_close_code")
        return cls(
            hub_url=str(doc.get("hub_url") or ""),
            key=str(doc.get("key") or ""),
            machine_id=str(doc.get("machine_id") or ""),
            roots=tuple(str(item) for item in doc["roots"]),
            hub_pin=_opt_str(doc.get("hub_pin")),
            last_generation=int(doc.get("last_generation") or 0),
            state=str(doc.get("state") or STATE_ACTIVE),
            last_close_code=int(raw_close) if raw_close is not None else None,
            proxy=_opt_str(doc.get("proxy")),
            ca_bundle=_opt_str(doc.get("ca_bundle")),
            pending_pin=_opt_str(doc.get("pending_pin")),
        )


def _opt_str(value: Any) -> str | None:
    return str(value) if value is not None else None


def default_home() -> Path:
    override = os.environ.get(ENV_RUNTIME_HOME)
    if override:
        return Path(override).expanduser()
    return Path.home() / DEFAULT_HOME_NAME


class EnrollmentStore:
    """Load/save :class:`RuntimeEnrollment` at a fixed path, atomically."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path if path is not None else default_home() / STORE_FILENAME

    def exists(self) -> bool:
        return self.path.is_file()

    def load(self) -> RuntimeEnrollment | None:
        """Return the enrollment, or ``None`` when the file is absent."""
        if not self.path.is_file():
            return None
        raw = self.path.read_text(encoding="utf-8")
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise JsonFileCorruptError(self.path, f"line {exc.lineno}, column {exc.colno}") from exc
        if not isinstance(doc, dict):
            raise JsonFileCorruptError(
                self.path, f"expected a JSON object, got {type(doc).__name__}"
            )
        enrollment = RuntimeEnrollment.from_json(doc)
        # Schema-level sanity, not just parse sanity: a torn or hand-edited
        # file must never produce a dialable-but-wrong identity silently.
        if not enrollment.hub_url or not enrollment.key or not enrollment.machine_id:
            raise JsonFileCorruptError(self.path, "missing hub_url, key, or machine_id")
        return enrollment

    def load_required(self) -> RuntimeEnrollment:
        enrollment = self.load()
        if enrollment is None:
            raise EnrollmentMissingError(self.path)
        return enrollment

    def save(self, enrollment: RuntimeEnrollment) -> None:
        """Persist ``enrollment`` atomically (temp + replace, 0600 on POSIX).

        Propagates ``OSError`` — the caller decides to hold rather than dial
        with unpersisted state.
        """
        payload = (json.dumps(enrollment.to_json(), indent=2) + "\n").encode("utf-8")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
        )
        try:
            try:
                os.write(fd, payload)
            finally:
                os.close(fd)
            if os.name == "posix":
                # The store holds an rk_ credential: always 0600, never the
                # umask-derived default that write_json_atomic uses for
                # config files shared across service users.
                os.chmod(tmp_name, stat.S_IRUSR | stat.S_IWUSR)
            os.replace(tmp_name, self.path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise

    def update(self, mutate: Callable[[RuntimeEnrollment], None]) -> RuntimeEnrollment:
        """Read-modify-write helper: mutate a copy, persist, return it."""
        enrollment = self.load_required()
        mutate(enrollment)
        self.save(enrollment)
        return enrollment


def new_machine_id() -> str:
    """Fresh machine ULID (call once per machine; keep across re-enroll)."""
    return new_ulid()
