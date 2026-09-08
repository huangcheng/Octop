"""Unit tests for portable expert zip localize / extract (#499)."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from harness_agent.backends.workspace import BackendWorkspace

from octop.infra.agents.experts.catalog import MANIFEST_FILENAME, WORKSPACE_MANIFEST_PATH
from octop.infra.agents.experts.portable_zip import (
    build_expert_export_zip,
    extract_expert_zip,
)
from octop.infra.errors import ErrorCode, OctopError


def _workspace(root: Path) -> BackendWorkspace:
    return BackendWorkspace(LocalShellBackend(root_dir=str(root), virtual_mode=False), root)


class _FakePackageStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def list_skill_summaries(self, package_id: str) -> list[dict[str, str]]:
        skills = self.root / package_id / "skills"
        if not skills.is_dir():
            return []
        return [{"slug": p.name} for p in skills.iterdir() if p.is_dir()]

    def package_skills_dir(self, package_id: str) -> Path:
        return self.root / package_id / "skills"


@pytest.mark.asyncio
async def test_export_localizes_package_skills_workspace_wins(tmp_path: Path) -> None:
    source_dir = tmp_path / "agent"
    source_dir.mkdir()
    source = _workspace(source_dir)
    await source.aupload_many(
        [
            (
                WORKSPACE_MANIFEST_PATH,
                json.dumps({"id": "src", "label": {"zh": "A", "en": "A"}}).encode(),
            ),
            ("SOUL.md", b"# soul"),
            ("skills/shared/SKILL.md", b"---\nname: shared\n---\n# workspace wins\n"),
        ]
    )

    packages = tmp_path / "packages"
    pkg_skill = packages / "pkg1" / "skills" / "shared"
    pkg_skill.mkdir(parents=True)
    (pkg_skill / "SKILL.md").write_text("---\nname: shared\n---\n# package\n", encoding="utf-8")
    pkg_extra = packages / "pkg1" / "skills" / "from-pkg"
    pkg_extra.mkdir(parents=True)
    (pkg_extra / "SKILL.md").write_text("---\nname: from-pkg\n---\n# pkg\n", encoding="utf-8")

    data = await build_expert_export_zip(
        workspace=source,
        source_agent_id="src-id",
        source_name="运维助手",
        package_ids=["pkg1"],
        store=_FakePackageStore(packages),  # type: ignore[arg-type]
    )
    names = set(zipfile.ZipFile(io.BytesIO(data)).namelist())
    assert "skills/shared/SKILL.md" in names
    assert "skills/from-pkg/SKILL.md" in names
    shared = zipfile.ZipFile(io.BytesIO(data)).read("skills/shared/SKILL.md").decode()
    assert "workspace wins" in shared
    manifest = json.loads(zipfile.ZipFile(io.BytesIO(data)).read(MANIFEST_FILENAME))
    assert manifest["octop_portable"] == "expert-v1"
    assert manifest["source_agent_id"] == "src-id"
    assert manifest["source_name"] == "运维助手"


@pytest.mark.asyncio
async def test_export_allows_missing_workspace_manifest(tmp_path: Path) -> None:
    source_dir = tmp_path / "agent"
    source_dir.mkdir()
    source = _workspace(source_dir)
    await source.aupload_many([("SOUL.md", b"# soul only")])

    data = await build_expert_export_zip(
        workspace=source,
        source_agent_id="a1",
        source_name="Bare",
        package_ids=[],
        store=_FakePackageStore(tmp_path),  # type: ignore[arg-type]
    )
    names = zipfile.ZipFile(io.BytesIO(data)).namelist()
    assert MANIFEST_FILENAME in names
    assert "SOUL.md" in names


def test_extract_rejects_zip_slip(tmp_path: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("../etc/passwd", b"nope")
        zf.writestr("SOUL.md", b"# s")
    with pytest.raises(OctopError) as exc:
        extract_expert_zip(buf.getvalue())
    assert exc.value.code is ErrorCode.SLASH_BAD_ARGS


def test_extract_strips_wrapper_directory(tmp_path: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "MyExpert/manifest.json",
            json.dumps({"id": "x", "source_name": "MyExpert"}).encode(),
        )
        zf.writestr("MyExpert/SOUL.md", b"# soul")
        zf.writestr("MyExpert/skills/demo/SKILL.md", b"---\nname: demo\n---\n")
    root = extract_expert_zip(buf.getvalue())
    try:
        assert (root / "SOUL.md").is_file()
        assert (root / "skills" / "demo" / "SKILL.md").is_file()
        assert (root / MANIFEST_FILENAME).is_file()
    finally:
        import shutil

        shutil.rmtree(root, ignore_errors=True)


def test_extract_keeps_avatar_path() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(MANIFEST_FILENAME, b'{"id":"x"}')
        zf.writestr("SOUL.md", b"# s")
        zf.writestr(".octop/avatar.png", b"\x89PNG\r\n\x1a\n" + b"\x00" * 8)
    root = extract_expert_zip(buf.getvalue())
    try:
        assert (root / ".octop" / "avatar.png").is_file()
    finally:
        import shutil

        shutil.rmtree(root, ignore_errors=True)
