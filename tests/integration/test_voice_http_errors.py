"""HTTP-level voice error envelopes: usage path (502) and probe glue (200)."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

import octop.infra.voice.adapters as voice_adapters

TENCENT_EXTRA = {"secret_id": "sid", "secret_key": "sk", "region": "ap-guangzhou"}


async def _create_provider(
    client: httpx.AsyncClient,
    auth: dict[str, str],
    *,
    name: str,
    api_key: str | None,
    extra: dict[str, Any] | None = None,
) -> None:
    r = await client.post(
        "/api/admin/voice/providers",
        headers=auth,
        json={
            "name": name,
            "kind": "tencent",
            "capability": "both",
            "api_key": api_key,
            "extra_json": json.dumps(extra) if extra else None,
        },
    )
    r.raise_for_status()


async def _set_active(client: httpx.AsyncClient, auth: dict[str, str], **body: str) -> None:
    r = await client.put("/api/voice/active", headers=auth, json=body)
    r.raise_for_status()


def _raising(exc: Exception) -> Any:
    async def fake(row: Any, audio: bytes, *, mime: str, language: str) -> Any:
        raise exc

    return fake


async def test_tts_preflight_returns_envelope_before_streaming(
    env: tuple[httpx.AsyncClient, Any, dict[str, str]],
) -> None:
    client, _srv, auth = env
    await _create_provider(client, auth, name="tc-bad", api_key="no-colon")
    await _set_active(client, auth, tts="tc-bad")

    r = await client.post("/api/voice/tts", headers=auth, json={"text": "hi"})
    assert r.status_code == 502
    body = r.json()
    assert body["error"]["code"] == "VOICE_PROVIDER_ERROR"
    assert body["error"]["details"] == {"detail": "Tencent Cloud requires secret_id and secret_key"}


async def test_stt_provider_error_returns_envelope(
    env: tuple[httpx.AsyncClient, Any, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _srv, auth = env
    await _create_provider(client, auth, name="tc-ok", api_key="sid:sk", extra=TENCENT_EXTRA)
    await _set_active(client, auth, stt="tc-ok")
    monkeypatch.setattr(
        voice_adapters,
        "transcribe_tencent",
        _raising(RuntimeError("UnsupportedOperation.ServerNotOpen: ASR service is not open.")),
    )

    r = await client.post(
        "/api/voice/stt",
        headers=auth,
        files={"audio": ("a.wav", b"RIFF", "audio/wav")},
        data={"language": "zh-CN"},
    )
    assert r.status_code == 502
    body = r.json()
    assert body["error"]["code"] == "VOICE_PROVIDER_ERROR"
    assert "ServerNotOpen" in body["error"]["message"]
