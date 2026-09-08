"""Integration: expert portable zip export/import (#499)."""

from __future__ import annotations

import asyncio
import io
import zipfile
from typing import Any
from urllib.parse import unquote

import pytest

from tests.support.auth import create_user


async def test_export_import_expert_zip_always_creates_copy(
    env: tuple[Any, Any, dict[str, str]],
) -> None:
    client, server, auth = env
    created = await client.post(
        "/api/agents/from-expert/default",
        headers=auth,
        json={"name": "运维助手"},
    )
    assert created.status_code == 201, created.text
    source_id = created.json()["agent_id"]

    exported = await client.get(
        f"/api/agents/{source_id}/export-expert.zip",
        headers=auth,
    )
    assert exported.status_code == 200, exported.text
    cd = exported.headers.get("content-disposition", "")
    assert "filename*" in cd
    assert "运维助手" in unquote(cd) or "%E8%BF%90" in cd
    names = set(zipfile.ZipFile(io.BytesIO(exported.content)).namelist())
    assert "manifest.json" in names
    assert any(n.endswith("SOUL.md") or n == "SOUL.md" for n in names)

    first = await client.post(
        "/api/experts/import-zip",
        headers={**auth, "Accept-Language": "zh-CN"},
        files={"file": ("expert.zip", exported.content, "application/zip")},
    )
    assert first.status_code == 201, first.text
    body = first.json()
    assert body["agent_id"] != source_id
    assert body["name"] == "运维助手-副本"

    source_row = (await client.get(f"/api/agents/{source_id}", headers=auth)).json()
    assert source_row["name"] == "运维助手"

    second = await client.post(
        "/api/experts/import-zip",
        headers={**auth, "Accept-Language": "zh-CN"},
        files={"file": ("expert.zip", exported.content, "application/zip")},
    )
    assert second.status_code == 201, second.text
    assert second.json()["name"] == "运维助手-副本1"
    assert second.json()["agent_id"] not in {source_id, body["agent_id"]}

    # Package table must stay untouched by import (zero global packages created).
    assert server.services is not None
    assert server.services.skill_package_repo.list_all() == []


async def test_import_expert_zip_slip_rejected(
    env: tuple[Any, Any, dict[str, str]],
) -> None:
    client, _srv, auth = env
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("../etc/passwd", b"x")
        zf.writestr("SOUL.md", b"# s")
    before = (await client.get("/api/agents", headers=auth)).json()
    response = await client.post(
        "/api/experts/import-zip",
        headers=auth,
        files={"file": ("bad.zip", buf.getvalue(), "application/zip")},
    )
    assert response.status_code == 400, response.text
    after = (await client.get("/api/agents", headers=auth)).json()
    assert len(after) == len(before)


async def test_import_expert_zip_requires_auth(
    env: tuple[Any, Any, dict[str, str]],
) -> None:
    client, _srv, _auth = env
    response = await client.post(
        "/api/experts/import-zip",
        files={"file": ("x.zip", b"PK\x05\x06" + b"\x00" * 18, "application/zip")},
    )
    assert response.status_code in {401, 403}


async def test_import_expert_zip_seed_failure_cleans_agent(
    env: tuple[Any, Any, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed workspace seed must not leave an orphan agent row."""
    from octop.infra.agents.experts import portable_zip as pz
    from octop.infra.errors import ErrorCode, OctopError

    client, server, auth = env
    before = (await client.get("/api/agents", headers=auth)).json()

    async def boom(**_kwargs: Any) -> None:
        raise OctopError(ErrorCode.INTERNAL_ERROR, "seed failed")

    monkeypatch.setattr(pz, "seed_expert_directory", boom)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "manifest.json",
            b'{"name":"broken-seed","version":1}',
        )
        zf.writestr("SOUL.md", b"# soul\n")

    response = await client.post(
        "/api/experts/import-zip",
        headers=auth,
        files={"file": ("bad.zip", buf.getvalue(), "application/zip")},
    )
    assert response.status_code >= 400, response.text
    after = (await client.get("/api/agents", headers=auth)).json()
    assert len(after) == len(before)
    assert server.services is not None
    assert all(row.name != "broken-seed" for row in server.services.agent_repo.list_all())


async def test_import_expert_zip_timeout(
    env: tuple[Any, Any, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from octop.infra.agents.experts import portable_zip as pz

    client, _srv, auth = env
    monkeypatch.setattr(pz, "EXPERT_ZIP_IMPORT_TIMEOUT_S", 0.01)

    async def slow_seed(**_kwargs: Any) -> None:
        await asyncio.sleep(1)

    monkeypatch.setattr(pz, "seed_expert_directory", slow_seed)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", b'{"name":"slow-seed","version":1}')
        zf.writestr("SOUL.md", b"# soul\n")

    before = (await client.get("/api/agents", headers=auth)).json()
    response = await client.post(
        "/api/experts/import-zip",
        headers=auth,
        files={"file": ("slow.zip", buf.getvalue(), "application/zip")},
    )
    assert response.status_code == 400, response.text
    body = response.json()["error"]
    assert body["details"].get("reason") == "import_timeout"
    after = (await client.get("/api/agents", headers=auth)).json()
    assert len(after) == len(before)


async def test_export_published_expert_zip(
    env: tuple[Any, Any, dict[str, str]],
) -> None:
    client, _srv, admin_auth = env
    owner_auth = await create_user(client, admin_auth, username="zip_pub_owner")
    created = await client.post(
        "/api/agents/from-expert/default",
        headers=owner_auth,
        json={"name": "publish-zip-src"},
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["agent_id"]
    published = await client.post(
        f"/api/agents/{agent_id}/publish-expert",
        headers=owner_auth,
        json={"name": "Published Zip Expert"},
    )
    assert published.status_code == 201, published.text
    expert_id = published.json()["id"]

    response = await client.get(
        f"/api/experts/published/{expert_id}/export.zip",
        headers=owner_auth,
    )
    assert response.status_code == 200, response.text
    cd = response.headers.get("content-disposition", "")
    assert "Published" in cd or "filename*" in cd
    assert "manifest.json" in zipfile.ZipFile(io.BytesIO(response.content)).namelist()
