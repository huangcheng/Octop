"""Portable expert zip export/import (issue #499).

Export reuses publish seed filtering and materializes mounted global skill
packages into ``skills/`` (workspace wins on slug clash). Import always creates
a new agent with a fresh id and a collision-free display name.
"""

from __future__ import annotations

import asyncio
import io
import json
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from harness_agent.backends.workspace import BackendWorkspace

from octop.infra.agents.avatar import AVATAR_RELPATHS, LEGACY_AVATAR_RELPATHS
from octop.infra.agents.experts.catalog import MANIFEST_FILENAME, seed_expert_directory
from octop.infra.agents.experts.copy_names import allocate_display_copy_name
from octop.infra.agents.experts.publish import (
    export_agent_workspace_to_dir,
    is_seedable_export_path,
)
from octop.infra.agents.manager import AgentCreateSpec, skill_package_ids_list
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.skills.skill_package_store import SkillPackageStore
from octop.infra.utils.frontmatter import parse_frontmatter

PORTABLE_MARK = "expert-v1"
MAX_EXPERT_ZIP_BYTES = 100 * 1024 * 1024
EXPERT_ZIP_IMPORT_TIMEOUT_S = 120.0
_MAX_EXPERT_ZIP_BYTES = MAX_EXPERT_ZIP_BYTES
_SAFE_PATH = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")
_AVATAR_PATHS = frozenset(AVATAR_RELPATHS + LEGACY_AVATAR_RELPATHS)
_SKIP_ZIP_PARTS = frozenset({"__macosx", ".ds_store"})


def _normalize_zip_relpath(name: str) -> str | None:
    """Return a posix relative path, or None for zip-slip / empty entries."""
    raw = name.replace("\\", "/").strip().lstrip("/")
    if not raw or raw.endswith("/"):
        return None
    parts = [p for p in raw.split("/") if p not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        return None
    return "/".join(parts)


def _is_skipped_zip_path(rel: str) -> bool:
    lower_parts = [p.lower() for p in rel.split("/")]
    return any(part in _SKIP_ZIP_PARTS for part in lower_parts)


def _is_portable_member(path: str) -> bool:
    if path == MANIFEST_FILENAME or path in _AVATAR_PATHS:
        return True
    return is_seedable_export_path(path)


def _detect_wrapper_dir(rels: list[str]) -> str | None:
    """Strip a single outer folder added by OS 'compress folder' (not skills/agents)."""
    tops = {rel.split("/", 1)[0] for rel in rels if rel}
    if len(tops) != 1:
        return None
    wrapper = next(iter(tops))
    if wrapper in {"skills", "agents", ".octop"} or wrapper == MANIFEST_FILENAME:
        return None
    if not any("/" in rel for rel in rels):
        return None
    return wrapper


def _safe_zip_relpath(name: str) -> str | None:
    raw = _normalize_zip_relpath(name)
    if raw is None:
        return None
    if any(ord(ch) > 127 for ch in raw):
        return None
    if not _SAFE_PATH.match(raw):
        return None
    return raw


def _dir_to_zip_bytes(root: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            if _safe_zip_relpath(rel) is None:
                continue
            zf.writestr(rel, path.read_bytes())
    return buf.getvalue()


async def _localize_packages_into_skills(
    *,
    dest: Path,
    package_ids: list[str],
    store: SkillPackageStore,
) -> None:
    """Copy mounted package skills into ``dest/skills/``; workspace files win."""
    skills_root = dest / "skills"
    skills_root.mkdir(parents=True, exist_ok=True)
    existing = {p.name for p in skills_root.iterdir() if p.is_dir()}
    for package_id in package_ids:
        for summary in store.list_skill_summaries(package_id):
            slug = str(summary.get("slug") or "").strip()
            if not slug or slug in existing:
                continue
            src = store.package_skills_dir(package_id) / slug
            if not src.is_dir():
                continue
            target = skills_root / slug
            for file_path in src.rglob("*"):
                if not file_path.is_file():
                    continue
                rel = file_path.relative_to(src)
                out = target / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(file_path.read_bytes())
            existing.add(slug)


def _enrich_manifest(
    dest: Path,
    *,
    source_agent_id: str,
    source_name: str,
) -> None:
    path = dest / MANIFEST_FILENAME
    data: dict[str, Any] = {}
    if path.is_file():
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                data = parsed
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}
    data["octop_portable"] = PORTABLE_MARK
    data["source_agent_id"] = source_agent_id
    data["source_name"] = source_name
    if "label" not in data and source_name:
        data["label"] = {"zh": source_name, "en": source_name}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


async def build_expert_export_zip(
    *,
    workspace: BackendWorkspace,
    source_agent_id: str,
    source_name: str,
    package_ids: list[str],
    store: SkillPackageStore,
) -> bytes:
    """Build a self-contained expert zip (seed + localized package skills)."""
    with tempfile.TemporaryDirectory(prefix="octop-expert-export-") as tmp:
        dest = Path(tmp) / "snapshot"
        try:
            await export_agent_workspace_to_dir(
                workspace=workspace,
                dest=dest,
                allow_missing_manifest=True,
            )
        except ValueError as exc:
            raise OctopError(ErrorCode.SLASH_BAD_ARGS, str(exc)) from exc
        if package_ids:
            await _localize_packages_into_skills(dest=dest, package_ids=package_ids, store=store)
        _enrich_manifest(dest, source_agent_id=source_agent_id, source_name=source_name)
        return _dir_to_zip_bytes(dest)


def build_published_snapshot_zip(snapshot_dir: Path) -> bytes:
    """Zip an on-disk published-expert snapshot directory."""
    if not snapshot_dir.is_dir():
        raise OctopError(ErrorCode.NOT_FOUND, "published expert snapshot not found")
    return _dir_to_zip_bytes(snapshot_dir)


def extract_expert_zip(data: bytes) -> Path:
    """Extract *data* into a new temp directory; caller deletes the returned path."""
    if not data:
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "empty archive")
    if len(data) > _MAX_EXPERT_ZIP_BYTES:
        raise OctopError(
            ErrorCode.SLASH_BAD_ARGS,
            f"expert archive too large (max {_MAX_EXPERT_ZIP_BYTES // (1024 * 1024)}MB)",
        )
    root = Path(tempfile.mkdtemp(prefix="octop-expert-import-"))
    try:
        with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
            pending: list[tuple[zipfile.ZipInfo, str]] = []
            for info in zf.infolist():
                if info.is_dir():
                    continue
                rel = _normalize_zip_relpath(info.filename)
                if rel is None:
                    raise OctopError(
                        ErrorCode.SLASH_BAD_ARGS,
                        f"unsafe path in archive: {info.filename!r}",
                    )
                if _is_skipped_zip_path(rel):
                    continue
                if not _SAFE_PATH.match(rel) or any(ord(ch) > 127 for ch in rel):
                    continue
                pending.append((info, rel))
            wrapper = _detect_wrapper_dir([rel for _, rel in pending])
            for info, rel in pending:
                if wrapper:
                    prefix = f"{wrapper}/"
                    if not rel.startswith(prefix):
                        continue
                    rel = rel[len(prefix) :]
                    if not rel:
                        continue
                if rel != MANIFEST_FILENAME and not _is_portable_member(rel):
                    continue
                target = root / rel
                if not target.resolve().is_relative_to(root.resolve()):
                    raise OctopError(ErrorCode.SLASH_BAD_ARGS, "archive path escapes root")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(zf.read(info))
    except zipfile.BadZipFile as exc:
        shutil.rmtree(root, ignore_errors=True)
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "invalid zip archive") from exc
    except OctopError:
        shutil.rmtree(root, ignore_errors=True)
        raise

    if not _looks_like_expert_seed(root):
        shutil.rmtree(root, ignore_errors=True)
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "archive is not a valid expert template")
    return root


def _looks_like_expert_seed(root: Path) -> bool:
    if (root / MANIFEST_FILENAME).is_file():
        return True
    if any((root / name).is_file() for name in ("SOUL.md", "IDENTITY.md", "AGENTS.md")):
        return True
    skills = root / "skills"
    if skills.is_dir():
        for child in skills.iterdir():
            if child.is_dir() and (child / "SKILL.md").is_file():
                return True
    return False


def read_import_display_name(extracted: Path) -> str:
    manifest = extracted / MANIFEST_FILENAME
    if manifest.is_file():
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = None
        if isinstance(data, dict):
            value = data.get("source_name")
            if isinstance(value, str) and value.strip():
                return value.strip()
            label = data.get("label")
            if isinstance(label, dict):
                for loc in ("zh", "en"):
                    text = label.get(loc)
                    if isinstance(text, str) and text.strip():
                        return text.strip()
            name = data.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
    for candidate in ("SOUL.md", "IDENTITY.md"):
        path = extracted / candidate
        if path.is_file():
            meta, _ = parse_frontmatter(path.read_text(encoding="utf-8", errors="replace"))
            for key in ("name", "title"):
                value = meta.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return "imported-expert"


def resolve_import_agent_name(
    *,
    preferred: str,
    existing_names: set[str],
    locale: str = "zh",
) -> str:
    return allocate_display_copy_name(preferred, existing_names, locale=locale)


def _appearance_from_manifest(extracted: Path) -> tuple[str | None, str | None, str | None]:
    """Return ``(color, icon_name, description)`` from manifest when present."""
    path = extracted / MANIFEST_FILENAME
    if not path.is_file():
        return None, None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, None, None
    if not isinstance(data, dict):
        return None, None, None
    color = data.get("color")
    icon_name = data.get("icon_name")
    description = data.get("description")
    color_s = color.strip() if isinstance(color, str) and color.strip() else None
    icon_s = icon_name.strip() if isinstance(icon_name, str) and icon_name.strip() else None
    desc_s: str | None = None
    if isinstance(description, str) and description.strip():
        desc_s = description.strip()
    elif isinstance(description, dict):
        for loc in ("zh", "en"):
            text = description.get(loc)
            if isinstance(text, str) and text.strip():
                desc_s = text.strip()
                break
    return color_s, icon_s, desc_s


async def create_agent_from_expert_zip(
    *,
    registry: Any,
    user_id: int,
    zip_bytes: bytes,
    existing_names: set[str],
    locale: str = "zh",
    default_model: str | None = None,
) -> dict[str, Any]:
    """Always create a new agent from *zip_bytes*; never mutate existing agents."""
    try:
        return await asyncio.wait_for(
            _create_agent_from_expert_zip(
                registry=registry,
                user_id=user_id,
                zip_bytes=zip_bytes,
                existing_names=existing_names,
                locale=locale,
                default_model=default_model,
            ),
            timeout=EXPERT_ZIP_IMPORT_TIMEOUT_S,
        )
    except TimeoutError as exc:
        raise OctopError(
            ErrorCode.SLASH_BAD_ARGS,
            f"expert zip import timed out after {int(EXPERT_ZIP_IMPORT_TIMEOUT_S)}s",
            details={
                "reason": "import_timeout",
                "timeout_s": int(EXPERT_ZIP_IMPORT_TIMEOUT_S),
            },
        ) from exc


async def _create_agent_from_expert_zip(
    *,
    registry: Any,
    user_id: int,
    zip_bytes: bytes,
    existing_names: set[str],
    locale: str = "zh",
    default_model: str | None = None,
) -> dict[str, Any]:
    extracted = extract_expert_zip(zip_bytes)
    try:
        preferred = read_import_display_name(extracted)
        name = resolve_import_agent_name(
            preferred=preferred, existing_names=existing_names, locale=locale
        )
        color, icon_name, description = _appearance_from_manifest(extracted)

        async def seed(_row: Any, workspace: Any) -> None:
            await seed_expert_directory(expert_dir=extracted, workspace=workspace)

        created = await registry.create(
            AgentCreateSpec(
                name=name,
                user_id=user_id,
                description=description,
                default_model=default_model,
                skill_package_ids=[],
                color=color,
                icon_name=icon_name,
            ),
            defer_bootstrap=True,
            workspace_initializer=seed,
        )
        fresh = registry.get_row(created.agent_id) or created
        return {
            "id": fresh.id,
            "agent_id": fresh.agent_id,
            "user_id": fresh.user_id,
            "name": fresh.name,
            "description": fresh.description,
            "default_model": fresh.default_model,
            "state": fresh.last_state or "unknown",
            "last_error": fresh.last_error,
            "bootstrap_pending": not registry.is_bootstrapped(fresh.agent_id),
        }
    finally:
        shutil.rmtree(extracted, ignore_errors=True)


def package_ids_for_agent(registry: Any, agent_id: str) -> list[str]:
    cfg = registry.get_config(agent_id)
    return skill_package_ids_list(cfg if isinstance(cfg, dict) else {})
