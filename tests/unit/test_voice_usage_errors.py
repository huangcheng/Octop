"""Usage-path voice failures map to OctopError envelopes, never bare 500s."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.settings import SettingsRepo
from octop.infra.db.repos.voice_providers import VoiceProviderRepo
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.voice import adapters
from octop.infra.voice.manager import VoiceManager


@pytest.fixture
def voice_env(tmp_path: Path) -> tuple[VoiceManager, VoiceProviderRepo]:
    db = SqlitePool(tmp_path / "octop.db")
    run_migrations(db)
    repo = VoiceProviderRepo(db)
    mgr = VoiceManager(settings_repo=SettingsRepo(db), voice_provider_repo=repo)
    return mgr, repo


def _create(
    repo: VoiceProviderRepo,
    *,
    name: str,
    kind: str,
    api_key: str | None,
    extra: dict[str, Any] | None = None,
) -> None:
    repo.create(
        name=name,
        kind=kind,
        capability="both",
        base_url=None,
        api_key=api_key,
        extra_json=json.dumps(extra) if extra else None,
        note=None,
    )


TENCENT_EXTRA = {"secret_id": "sid", "secret_key": "sk", "region": "ap-guangzhou"}


@pytest.mark.asyncio
async def test_transcribe_wraps_provider_error(
    voice_env: tuple[VoiceManager, VoiceProviderRepo], monkeypatch: pytest.MonkeyPatch
) -> None:
    voice_mgr, repo = voice_env

    async def boom(row: Any, audio: bytes, *, mime: str, language: str) -> Any:
        raise RuntimeError("UnsupportedOperation.ServerNotOpen: ASR service is not open.")

    monkeypatch.setattr(adapters, "transcribe_tencent", boom)
    _create(repo, name="tc", kind="tencent", api_key="sid:sk", extra=TENCENT_EXTRA)

    with pytest.raises(OctopError) as exc:
        await voice_mgr.transcribe(b"wav", mime="audio/wav", provider_name="tc")
    assert exc.value.code == ErrorCode.VOICE_PROVIDER_ERROR
    assert exc.value.status == 502
    en = exc.value.to_envelope(locale="en")
    assert (
        en["error"]["message"]
        == "Voice provider error: UnsupportedOperation.ServerNotOpen: ASR service is not open."
    )
    zh = exc.value.to_envelope(locale="zh")
    assert zh["error"]["message"].startswith("语音服务错误：")


@pytest.mark.asyncio
async def test_transcribe_passes_octop_errors_through(
    voice_env: tuple[VoiceManager, VoiceProviderRepo], monkeypatch: pytest.MonkeyPatch
) -> None:
    voice_mgr, repo = voice_env

    async def disabled(row: Any, audio: bytes, *, mime: str, language: str) -> Any:
        raise OctopError(ErrorCode.VOICE_PROVIDER_DISABLED, "disabled")

    monkeypatch.setattr(adapters, "transcribe_openai", disabled)
    _create(repo, name="oa", kind="openai", api_key="k")

    with pytest.raises(OctopError) as exc:
        await voice_mgr.transcribe(b"wav", mime="audio/wav", provider_name="oa")
    assert exc.value.code == ErrorCode.VOICE_PROVIDER_DISABLED


def test_preflight_tts_reports_missing_credentials(
    voice_env: tuple[VoiceManager, VoiceProviderRepo],
) -> None:
    voice_mgr, repo = voice_env
    _create(repo, name="bad", kind="tencent", api_key="no-colon")
    with pytest.raises(OctopError) as exc:
        voice_mgr.preflight_tts("bad")
    assert exc.value.code == ErrorCode.VOICE_PROVIDER_ERROR
    assert exc.value.details == {"detail": "Tencent Cloud requires secret_id and secret_key"}


def test_preflight_tts_reports_missing_api_key(
    voice_env: tuple[VoiceManager, VoiceProviderRepo],
) -> None:
    voice_mgr, repo = voice_env
    _create(repo, name="oa-empty", kind="openai", api_key=None)
    with pytest.raises(OctopError) as exc:
        voice_mgr.preflight_tts("oa-empty")
    assert exc.value.code == ErrorCode.VOICE_PROVIDER_ERROR


def test_preflight_tts_passes_browser_edge_and_configured(
    voice_env: tuple[VoiceManager, VoiceProviderRepo],
) -> None:
    voice_mgr, repo = voice_env
    _create(repo, name="tc-ok", kind="tencent", api_key="sid:sk", extra=TENCENT_EXTRA)
    voice_mgr.preflight_tts(None)  # default active: browser
    voice_mgr.preflight_tts("edge")
    voice_mgr.preflight_tts("tc-ok")
