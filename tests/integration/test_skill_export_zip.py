"""Integration: skill export.zip (#499)."""

from __future__ import annotations

import io
import zipfile
from typing import Any
from urllib.parse import unquote

import pytest

SAMPLE = """---
name: PDF 阅读器
description: Read and summarize PDF files
---
# PDF Reader
"""


@pytest.fixture
async def env(env_with_agent: Any) -> Any:
    yield env_with_agent


async def test_export_skill_zip_contains_slug_and_display_filename(env: Any) -> None:
    client, _srv, auth, aid = env
    created = await client.post(
        f"/api/agents/{aid}/skills",
        headers=auth,
        json={"name": "pdf-reader", "content": SAMPLE},
    )
    assert created.status_code == 201, created.text

    response = await client.get(
        f"/api/agents/{aid}/skills/pdf-reader/export.zip",
        headers=auth,
    )
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/zip")
    cd = response.headers.get("content-disposition", "")
    assert "filename*" in cd
    assert "PDF" in unquote(cd) or "%E9%98%85" in cd

    names = zipfile.ZipFile(io.BytesIO(response.content)).namelist()
    assert "pdf-reader/SKILL.md" in names
    body = zipfile.ZipFile(io.BytesIO(response.content)).read("pdf-reader/SKILL.md").decode()
    assert "name: pdf-reader" in body
    assert "Read and summarize PDF files" in body


async def test_export_skill_zip_missing_404(env: Any) -> None:
    client, _srv, auth, aid = env
    response = await client.get(
        f"/api/agents/{aid}/skills/no-such-skill/export.zip",
        headers=auth,
    )
    assert response.status_code == 404


async def test_export_package_skill_zip(env: Any) -> None:
    client, _srv, auth, _aid = env
    package_id = (
        await client.post(
            "/api/skill-packages",
            headers=auth,
            json={"name": "Office-Export"},
        )
    ).json()["id"]
    created = await client.post(
        f"/api/skill-packages/{package_id}/skills",
        headers=auth,
        json={"name": "sheet-reader", "content": SAMPLE},
    )
    assert created.status_code == 200, created.text

    response = await client.get(
        f"/api/skill-packages/{package_id}/skills/sheet-reader/export.zip",
        headers=auth,
    )
    assert response.status_code == 200, response.text
    names = zipfile.ZipFile(io.BytesIO(response.content)).namelist()
    assert "sheet-reader/SKILL.md" in names


async def test_import_skill_zip_fresh_keeps_slug(env: Any) -> None:
    """S2: missing skill → install under original ASCII slug."""
    client, _srv, auth, aid = env
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "pdf-reader/SKILL.md",
            "---\nname: pdf-reader\ndescription: d\n---\n# PDF\n",
        )
        zf.writestr("pdf-reader/notes.txt", "n")

    response = await client.post(
        f"/api/agents/{aid}/skills/import-zip",
        headers=auth,
        files={"file": ("skills.zip", buf.getvalue(), "application/zip")},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["imported"] == 1
    assert body["skills"][0]["slug"] == "pdf-reader"
    assert body["skills"][0]["copied"] is False

    listed = (await client.get(f"/api/agents/{aid}/skills", headers=auth)).json()
    slugs = {row["slug"] for row in listed}
    assert "pdf-reader" in slugs


async def test_import_skill_zip_collision_copies(env: Any) -> None:
    """S3/S4: existing slug → -copy; original unchanged."""
    client, _srv, auth, aid = env
    created = await client.post(
        f"/api/agents/{aid}/skills",
        headers=auth,
        json={"name": "pdf-reader", "content": SAMPLE},
    )
    assert created.status_code == 201, created.text

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "pdf-reader/SKILL.md",
            "---\nname: pdf-reader\ndescription: imported copy\n---\n# Copy\n",
        )

    response = await client.post(
        f"/api/agents/{aid}/skills/import-zip",
        headers=auth,
        files={"file": ("skills.zip", buf.getvalue(), "application/zip")},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["imported"] == 1
    assert body["skills"][0]["slug"] == "pdf-reader-copy"
    assert body["skills"][0]["copied"] is True

    original = await client.get(f"/api/agents/{aid}/skills/pdf-reader", headers=auth)
    assert original.status_code == 200
    assert "Read and summarize PDF files" in original.json()["raw"]

    copy = await client.get(f"/api/agents/{aid}/skills/pdf-reader-copy", headers=auth)
    assert copy.status_code == 200
    assert "imported copy" in copy.json()["raw"]
    assert "name: pdf-reader-copy" in copy.json()["raw"]


async def test_import_package_skill_zip_collision_copies(env: Any) -> None:
    client, _srv, auth, _aid = env
    package_id = (
        await client.post(
            "/api/skill-packages",
            headers=auth,
            json={"name": "Office-Import"},
        )
    ).json()["id"]
    assert (
        await client.post(
            f"/api/skill-packages/{package_id}/skills",
            headers=auth,
            json={"name": "sheet-reader", "content": SAMPLE},
        )
    ).status_code == 200

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "sheet-reader/SKILL.md",
            "---\nname: sheet-reader\ndescription: from zip\n---\n# Sheet\n",
        )

    response = await client.post(
        f"/api/skill-packages/{package_id}/skills/import-zip",
        headers=auth,
        files={"file": ("skills.zip", buf.getvalue(), "application/zip")},
    )
    assert response.status_code == 201, response.text
    assert response.json()["skills"][0]["slug"] == "sheet-reader-copy"

    skills = (await client.get(f"/api/skill-packages/{package_id}", headers=auth)).json()["skills"]
    slugs = {row["slug"] for row in skills}
    assert slugs == {"sheet-reader", "sheet-reader-copy"}
