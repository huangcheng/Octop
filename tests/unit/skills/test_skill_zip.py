"""Unit tests for skill zip packing (#499)."""

from __future__ import annotations

import asyncio
import io
import zipfile
from pathlib import Path

import pytest

from octop.infra.errors import ErrorCode, OctopError
from octop.infra.skills.skill_zip import (
    assert_ascii_skill_slug,
    build_skill_zip_bytes,
    collect_package_skill_files,
    rewrite_skill_md_name,
)


def test_assert_ascii_skill_slug_rejects_non_ascii() -> None:
    with pytest.raises(OctopError) as exc:
        assert_ascii_skill_slug("中文技能")
    assert exc.value.code is ErrorCode.SLASH_BAD_ARGS


def test_rewrite_skill_md_name_keeps_description() -> None:
    raw = b"---\nname: old\ndescription: Keep me\n---\n# Body\n"
    out = rewrite_skill_md_name(raw, slug="old-copy").decode()
    assert "name: old-copy" in out
    assert "description: Keep me" in out
    assert "# Body" in out


def test_build_skill_zip_bytes_prefixes_slug(tmp_path: Path) -> None:
    skill_dir = tmp_path / "pdf-reader"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(
        "---\nname: PDF Reader\ndescription: d\n---\n# PDF\n",
        encoding="utf-8",
    )
    (skill_dir / "notes.txt").write_text("n", encoding="utf-8")
    files = collect_package_skill_files(tmp_path, "pdf-reader")
    data = build_skill_zip_bytes(slug="pdf-reader", files=files)
    names = zipfile.ZipFile(io.BytesIO(data)).namelist()
    assert "pdf-reader/SKILL.md" in names
    assert "pdf-reader/notes.txt" in names
    skill_md = zipfile.ZipFile(io.BytesIO(data)).read("pdf-reader/SKILL.md").decode()
    assert "name: pdf-reader" in skill_md
    assert "description: d" in skill_md


@pytest.mark.asyncio
async def test_import_portable_skill_zip_copy_on_collision() -> None:
    from octop.infra.skills.skill_zip import import_portable_skill_zip

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("demo/SKILL.md", b"---\nname: demo\ndescription: x\n---\n# D\n")

    written: list[str] = []

    async def install(slug: str, files: list[tuple[str, bytes]]) -> None:
        written.append(slug)
        assert any(path == "SKILL.md" for path, _ in files)

    first = await import_portable_skill_zip(
        data=buf.getvalue(),
        existing_slugs=set(),
        install_skill=install,
    )
    assert first[0]["slug"] == "demo"
    assert first[0]["copied"] is False

    second = await import_portable_skill_zip(
        data=buf.getvalue(),
        existing_slugs={"demo"},
        install_skill=install,
    )
    assert second[0]["slug"] == "demo-copy"
    assert second[0]["copied"] is True
    assert written == ["demo", "demo-copy"]

    written.clear()
    third = await import_portable_skill_zip(
        data=buf.getvalue(),
        existing_slugs={"demo"},
        install_skill=install,
        overwrite=True,
    )
    assert third[0]["slug"] == "demo"
    assert third[0]["copied"] is False
    assert third[0]["overwritten"] is True
    assert written == ["demo"]


@pytest.mark.asyncio
async def test_import_portable_skill_zip_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import octop.infra.skills.skill_zip as skill_zip

    monkeypatch.setattr(skill_zip, "SKILL_ZIP_IMPORT_TIMEOUT_S", 0.01)

    async def slow_install(_slug: str, _files: list[tuple[str, bytes]]) -> None:
        await asyncio.sleep(1)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("demo/SKILL.md", b"---\nname: demo\n---\n")

    with pytest.raises(OctopError) as exc:
        await skill_zip.import_portable_skill_zip(
            data=buf.getvalue(),
            existing_slugs=set(),
            install_skill=slow_install,
        )
    assert exc.value.code is ErrorCode.SLASH_BAD_ARGS
    assert exc.value.details.get("reason") == "import_timeout"
