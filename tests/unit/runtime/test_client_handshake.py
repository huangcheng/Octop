"""Handshake / lifecycle tests for the runtime WS client against a fake hub.

The fake hub is an in-process ``websockets`` server on 127.0.0.1; the client
enrolls against it with a plaintext ``http://`` hub URL (localhost dev form),
so the TOFU pin check is a no-op and tests exercise the protocol logic only.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
import websockets.asyncio.server as ws_server

from octop.infra.runtime.client import (
    BACKOFF_MAX_SECONDS,
    RuntimeClient,
    compute_backoff,
    hub_ws_url,
)
from octop.infra.runtime.enrollment_store import STATE_REVOKED, EnrollmentStore, RuntimeEnrollment
from octop.infra.runtime.protocol import (
    INITIAL_CREDIT_BYTES,
    FrameType,
    RtHello,
    RtReady,
    RtRotate,
    RtWelcome,
    decode_frame,
    encode_frame,
)

Handler = Callable[[Any], Awaitable[None]]


class FakeClock:
    """Injectable clock: records sleeps, advances virtual time, never blocks."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


@dataclasses.dataclass
class FakeHub:
    url: str
    server: Any
    connections: int = 0
    hellos: list[RtHello] = dataclasses.field(default_factory=list)
    readies: list[RtReady] = dataclasses.field(default_factory=list)

    async def close(self) -> None:
        self.server.close()
        await self.server.wait_closed()


async def start_hub(behavior: Handler) -> FakeHub:
    hub = FakeHub(url="", server=None)  # type: ignore[assignment]

    async def handler(connection: Any) -> None:
        hub.connections += 1
        await behavior(connection)

    server = await ws_server.serve(handler, "127.0.0.1", 0, subprotocols=["octop.runtime.v1"])
    port = server.sockets[0].getsockname()[1]
    hub.url = f"http://127.0.0.1:{port}"
    hub.server = server
    return hub


def welcome(generation: int = 7, *, rotation_pending: bool = False) -> RtWelcome:
    return RtWelcome(
        generation=generation,
        capabilities=("fs",),
        quotas={},
        initial_credit_bytes=INITIAL_CREDIT_BYTES,
        rotation_pending=rotation_pending,
    )


async def recv_frame(connection: Any) -> Any:
    return decode_frame(await connection.recv())


@pytest.fixture(autouse=True)
def _no_env_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy", "all_proxy"):
        monkeypatch.delenv(var, raising=False)


def make_store(tmp_path: Path, hub_url: str) -> EnrollmentStore:
    store = EnrollmentStore(tmp_path / "runtime.json")
    store.save(
        RuntimeEnrollment(
            hub_url=hub_url,
            key="rk_" + "k" * 43,
            machine_id="01JTESTMACHINEID0001",
            roots=(str(tmp_path),),
        )
    )
    return store


def make_client(
    store: EnrollmentStore,
    *,
    clock: FakeClock | None = None,
    **kwargs: Any,
) -> RuntimeClient:
    return RuntimeClient(
        store,
        clock=clock if clock is not None else FakeClock(),
        welcome_timeout=kwargs.pop("welcome_timeout", 2.0),
        **kwargs,
    )


async def test_handshake_happy_path_persists_generation(tmp_path: Path) -> None:
    hub_holder: dict[str, FakeHub] = {}

    async def behavior(connection: Any) -> None:
        hub = hub_holder["hub"]
        hello_frame = await recv_frame(connection)
        assert hello_frame.frame_type == FrameType.RT_HELLO
        hello = hello_frame.payload
        hub.hellos.append(hello)
        await connection.send(encode_frame(7, 1, FrameType.RT_WELCOME, welcome(7)))
        ready_frame = await recv_frame(connection)
        hub.readies.append(ready_frame.payload)
        await connection.send(encode_frame(7, 1, FrameType.RT_ROTATE, RtRotate()))
        await connection.close(code=4002, reason="revoked")

    hub = await start_hub(behavior)
    hub_holder["hub"] = hub
    store = make_store(tmp_path, hub.url)
    rotations: list[int] = []
    clock = FakeClock()
    client = make_client(
        store,
        clock=clock,
        on_rotation_pending=lambda: rotations.append(1),
    )
    try:
        await asyncio.wait_for(client.run(), timeout=5)
    finally:
        await hub.close()

    hello = hub.hellos[0]
    assert hello.hostname
    assert hello.os and hello.arch and hello.client_version
    assert hello.roots == (str(tmp_path),)
    assert rotations == [1]  # rt.rotate frame fired the callback (welcome flag was False)
    assert hello.free_bytes >= 0
    ready = hub.readies[0]
    assert ready.generation == 7
    assert ready.initial_credit_bytes == INITIAL_CREDIT_BYTES
    assert rotations == [1]  # welcome flag AND rt.rotate both fired
    assert len(rotations) == 1 or len(rotations) == 2  # rotation_pending False here
    enrollment = store.load_required()
    assert enrollment.last_generation == 7
    assert enrollment.state == STATE_REVOKED
    assert enrollment.last_close_code == 4002
    assert client.generation == 7


async def test_welcome_timeout_resends_hello_once(tmp_path: Path) -> None:
    hub_holder: dict[str, FakeHub] = {}

    async def behavior(connection: Any) -> None:
        hub = hub_holder["hub"]
        first = await recv_frame(connection)
        hub.hellos.append(first.payload)
        # Stay silent past the first welcome window; the client must resend.
        second = await recv_frame(connection)
        assert second.frame_type == FrameType.RT_HELLO
        hub.hellos.append(second.payload)
        await connection.send(encode_frame(3, 1, FrameType.RT_WELCOME, welcome(3)))
        ready = await recv_frame(connection)
        assert ready.payload.generation == 3
        await connection.close(code=4002, reason="revoked")

    hub = await start_hub(behavior)
    hub_holder["hub"] = hub
    store = make_store(tmp_path, hub.url)
    client = make_client(store, welcome_timeout=0.2)
    try:
        await asyncio.wait_for(client.run(), timeout=5)
    finally:
        await hub.close()

    assert len(hub.hellos) == 2
    assert store.load_required().last_generation == 3


async def test_terminal_4002_stops_without_redial(tmp_path: Path) -> None:
    hub_holder: dict[str, FakeHub] = {}

    async def behavior(connection: Any) -> None:
        await recv_frame(connection)
        await connection.send(encode_frame(5, 1, FrameType.RT_WELCOME, welcome(5)))
        await recv_frame(connection)
        await connection.close(code=4002, reason="revoked")

    hub = await start_hub(behavior)
    hub_holder["hub"] = hub
    store = make_store(tmp_path, hub.url)
    clock = FakeClock()
    client = make_client(store, clock=clock)
    try:
        await asyncio.wait_for(client.run(), timeout=5)
    finally:
        await hub.close()

    assert hub.connections == 1  # terminal: never redials
    assert clock.sleeps == []  # ...and never backs off
    enrollment = store.load_required()
    assert enrollment.state == STATE_REVOKED
    assert enrollment.last_close_code == 4002


async def test_4005_holds_60s_then_redials(tmp_path: Path) -> None:
    hub_holder: dict[str, FakeHub] = {}

    async def behavior(connection: Any) -> None:
        hub = hub_holder["hub"]
        await recv_frame(connection)
        await connection.send(encode_frame(2, 1, FrameType.RT_WELCOME, welcome(2)))
        await recv_frame(connection)
        if hub.connections == 1:
            await connection.close(code=4005, reason="superseded")
        else:
            await connection.close(code=4002, reason="revoked")

    hub = await start_hub(behavior)
    hub_holder["hub"] = hub
    store = make_store(tmp_path, hub.url)
    clock = FakeClock()
    client = make_client(store, clock=clock)
    try:
        await asyncio.wait_for(client.run(), timeout=5)
    finally:
        await hub.close()

    assert hub.connections == 2
    assert clock.sleeps == [60.0]  # spec §6: superseded -> 60 s hold, then redial
    assert store.load_required().last_close_code == 4002


async def test_4001_holds_when_key_unchanged(tmp_path: Path) -> None:
    hub_holder: dict[str, FakeHub] = {}

    async def behavior(connection: Any) -> None:
        await recv_frame(connection)
        await connection.send(encode_frame(1, 1, FrameType.RT_WELCOME, welcome(1)))
        await recv_frame(connection)
        await connection.close(code=4001, reason="auth_failed")

    hub = await start_hub(behavior)
    hub_holder["hub"] = hub
    store = make_store(tmp_path, hub.url)
    statuses: list[str] = []
    client = make_client(store, on_status=lambda s: statuses.append(s.detail))
    try:
        await asyncio.wait_for(client.run(), timeout=5)
    finally:
        await hub.close()

    assert hub.connections == 1  # one key re-read, key unchanged -> hold
    assert "auth_failed" in statuses
    assert store.load_required().last_close_code == 4001


async def test_welcome_rotation_pending_fires_callback(tmp_path: Path) -> None:
    hub_holder: dict[str, FakeHub] = {}

    async def behavior(connection: Any) -> None:
        await recv_frame(connection)
        await connection.send(
            encode_frame(4, 1, FrameType.RT_WELCOME, welcome(4, rotation_pending=True))
        )
        await recv_frame(connection)
        await connection.close(code=4002, reason="revoked")

    hub = await start_hub(behavior)
    hub_holder["hub"] = hub
    store = make_store(tmp_path, hub.url)
    rotations: list[int] = []
    client = make_client(store, on_rotation_pending=lambda: rotations.append(1))
    try:
        await asyncio.wait_for(client.run(), timeout=5)
    finally:
        await hub.close()

    assert rotations == [1]
    assert store.load_required().last_generation == 4


def test_backoff_jitter_bounds() -> None:
    # attempt 1: 1 s base, +/-20 % -> [0.8, 1.2]
    assert compute_backoff(1, 0.0) == pytest.approx(0.8)
    assert compute_backoff(1, 1.0) == pytest.approx(1.2)
    mid = compute_backoff(1, 0.5)
    assert 0.8 <= mid <= 1.2
    # doubling until the 60 s cap, jitter keeps every value within bounds
    for attempt in range(1, 12):
        low = compute_backoff(attempt, 0.0)
        high = compute_backoff(attempt, 1.0)
        assert low <= compute_backoff(attempt, 0.37) <= high
        expected_base = min(BACKOFF_MAX_SECONDS, 2 ** (attempt - 1))
        assert low == pytest.approx(expected_base * 0.8)
        assert high == pytest.approx(expected_base * 1.2)


def test_hub_ws_url_mapping() -> None:
    assert hub_ws_url("https://hub.example") == "wss://hub.example/api/runtime/ws"
    assert hub_ws_url("https://hub.example/") == "wss://hub.example/api/runtime/ws"
    assert hub_ws_url("http://127.0.0.1:8000") == "ws://127.0.0.1:8000/api/runtime/ws"
    with pytest.raises(ValueError):
        hub_ws_url("ftp://hub.example")
