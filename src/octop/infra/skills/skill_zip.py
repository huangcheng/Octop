"""Build / parse / import a single-skill or multi-skill zip (#499).

Export packs ``{slug}/SKILL.md`` (+ siblings) for dashboard ``parseSkillZip``.
Zip-share import always uses copy-on-collision (``-copy`` / ``-copyN``);
``overwrite=true`` create APIs are unchanged.
"""

from __future__ import annotations

import asyncio
import io
import re
import zipfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from octop.infra.agents.experts.copy_names import allocate_ascii_copy_slug
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.skills.skill_packages import (
    MAX_SKILL_BYTES,
    SkillPackageError,
    SkillPackageTooLarge,
    normalize_skill_files,
    resolve_skill_package,
    validate_skill_slug,
)
from octop.infra.utils.frontmatter import parse_frontmatter

_ASCII_SLUG = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
_NAME_LINE = re.compile(r"(?m)^name:\s*.*$")
_SKIP_MARKERS = ("__macosx/", ".ds_store")
_MAX_ZIP_BYTES = MAX_SKILL_BYTES
SKILL_ZIP_IMPORT_TIMEOUT_S = 60.0
MAX_SKILL_ZIP_BYTES = MAX_SKILL_BYTES

InstallSkillFn = Callable[[str, list[tuple[str, bytes]]], Awaitable[None]]


@dataclass(frozen=True)
class ParsedSkillFromZip:
    """One skill extracted from a portable zip (paths relative to skill root)."""

    slug: str
    files: tuple[tuple[str, bytes], ...]


def assert_ascii_skill_slug(slug: str) -> str:
    try:
        safe = validate_skill_slug(slug)
    except SkillPackageError as exc:
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "invalid skill name") from exc
    if not _ASCII_SLUG.match(safe) or any(ord(ch) > 127 for ch in safe):
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "skill directory slug must be ASCII")
    return safe


def rewrite_skill_md_name(content: bytes, *, slug: str) -> bytes:
    """Set top-level frontmatter ``name`` to *slug*; keep description/keywords/body."""
    text = content.decode("utf-8", errors="replace")
    if not text.lstrip().startswith("---"):
        return f"---\nname: {slug}\n---\n{text}".encode()
    end = text.find("\n---", 3)
    if end < 0:
        return f"---\nname: {slug}\n---\n{text}".encode()
    header = text[: end + 4]  # include closing ---
    body = text[end + 4 :]
    if _NAME_LINE.search(header):
        header = _NAME_LINE.sub(f"name: {slug}", header, count=1)
    else:
        header = header.replace("---\n", f"---\nname: {slug}\n", 1)
    return (header + body).encode("utf-8")


def build_skill_zip_bytes(*, slug: str, files: list[tuple[str, bytes]]) -> bytes:
    """Pack skill files under ``{slug}/...`` for parseSkillZip compatibility."""
    safe = assert_ascii_skill_slug(slug)
    buf = io.BytesIO()
    wrote_manifest = False
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel, content in files:
            rel_norm = rel.replace("\\", "/").lstrip("/")
            if any(part == ".." for part in PurePosixPath(rel_norm).parts):
                continue
            if rel_norm == "SKILL.md":
                content = rewrite_skill_md_name(content, slug=safe)
                wrote_manifest = True
            zf.writestr(f"{safe}/{rel_norm}", content)
        if not wrote_manifest:
            raise OctopError(ErrorCode.NOT_FOUND, f"skill {safe!r} missing SKILL.md")
    return buf.getvalue()


async def collect_workspace_skill_files(workspace: Any, slug: str) -> list[tuple[str, bytes]]:
    """List file payloads relative to ``skills/{slug}/``."""
    safe = assert_ascii_skill_slug(slug)
    manifest = await workspace.adownload_bytes(f"skills/{safe}/SKILL.md")
    if manifest is None:
        text = await workspace.aread_text(f"skills/{safe}/SKILL.md")
        if text is None:
            raise OctopError(ErrorCode.NOT_FOUND, f"skill {safe!r} not found")
        manifest = text.encode("utf-8")
    result = await workspace.aglob("**/*", f"skills/{safe}")
    files: list[tuple[str, bytes]] = [("SKILL.md", manifest)]
    if result is None:
        return files
    seen = {"SKILL.md"}
    prefix = f"skills/{safe}/"
    for entry in result.matches or []:
        if isinstance(entry, dict):
            path = entry.get("path")
            is_dir = bool(entry.get("is_dir", False))
        else:
            path = getattr(entry, "path", None)
            is_dir = bool(getattr(entry, "is_dir", False))
        if is_dir or not isinstance(path, str):
            continue
        rel = path.replace("\\", "/").lstrip("/")
        if rel.startswith(prefix):
            rel = rel[len(prefix) :]
        if not rel or rel in seen or any(part == ".." for part in PurePosixPath(rel).parts):
            continue
        raw = await workspace.adownload_bytes(f"skills/{safe}/{rel}")
        if raw is None:
            continue
        files.append((rel, raw))
        seen.add(rel)
    return files


def collect_package_skill_files(skills_dir: Path, slug: str) -> list[tuple[str, bytes]]:
    safe = assert_ascii_skill_slug(slug)
    root = skills_dir / safe
    if not (root / "SKILL.md").is_file():
        raise OctopError(ErrorCode.NOT_FOUND, f"skill {safe!r} not found")
    files: list[tuple[str, bytes]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if any(part == ".." for part in PurePosixPath(rel).parts):
            continue
        files.append((rel, path.read_bytes()))
    return files


def _normalize_zip_path(path: str) -> str:
    return path.replace("\\", "/").replace("./", "").lstrip("/")


def _is_skipped_zip_path(path: str) -> bool:
    lower = path.lower()
    return any(marker in lower for marker in _SKIP_MARKERS)


def parse_portable_skill_zip(
    data: bytes, *, root_slug_fallback: str = "imported-skill"
) -> list[ParsedSkillFromZip]:
    """Parse a skill zip into one or more skills (``{slug}/SKILL.md`` layout)."""
    if not data:
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "empty archive")
    if len(data) > _MAX_ZIP_BYTES:
        raise OctopError(
            ErrorCode.SLASH_BAD_ARGS,
            f"skill archive too large (max {_MAX_ZIP_BYTES // (1024 * 1024)}MB)",
        )
    try:
        zf = zipfile.ZipFile(io.BytesIO(data), "r")
    except zipfile.BadZipFile as exc:
        raise OctopError(ErrorCode.SLASH_BAD_ARGS, "invalid zip archive") from exc

    with zf:
        file_rels: list[str] = []
        for info in zf.infolist():
            rel = _normalize_zip_path(info.filename)
            if not rel or _is_skipped_zip_path(rel) or info.is_dir() or rel.endswith("/"):
                continue
            if any(part == ".." for part in PurePosixPath(rel).parts):
                raise OctopError(
                    ErrorCode.SLASH_BAD_ARGS,
                    f"unsafe path in archive: {info.filename!r}",
                )
            file_rels.append(rel)
        if not file_rels:
            raise OctopError(ErrorCode.SLASH_BAD_ARGS, "zip archive is empty")

        tops = {p.split("/", 1)[0] for p in file_rels if p}
        wrapper: str | None = None
        if len(tops) == 1:
            candidate = next(iter(tops))
            if not any(p == f"{candidate}/SKILL.md" for p in file_rels):
                wrapper = candidate

        strip_map: dict[str, str] = {}
        stripped: list[str] = []
        for orig in file_rels:
            key = orig
            if wrapper and orig.startswith(f"{wrapper}/"):
                key = orig[len(wrapper) + 1 :]
            if not key:
                continue
            strip_map[key] = orig
            stripped.append(key)

        groups: dict[str, list[tuple[str, bytes]]] = {}
        for rel in stripped:
            if "/" not in rel:
                slug = "__root__"
                path = rel
            else:
                slug, path = rel.split("/", 1)
            if not path:
                continue
            member = strip_map.get(rel)
            if member is None:
                continue
            try:
                content = zf.read(member)
            except KeyError:
                continue
            groups.setdefault(slug, []).append((path, content))

        root_files = groups.get("__root__")
        if root_files and any(path == "SKILL.md" for path, _ in root_files):
            for group_slug, files in list(groups.items()):
                if group_slug == "__root__":
                    continue
                if any(path == "SKILL.md" for path, _ in files):
                    continue
                for path, content in files:
                    root_files.append((f"{group_slug}/{path}", content))
                del groups[group_slug]

        parsed: list[ParsedSkillFromZip] = []
        for group_slug, files in groups.items():
            if not any(path == "SKILL.md" for path, _ in files):
                continue
            raw_slug = root_slug_fallback if group_slug == "__root__" else group_slug
            target_slug = allocate_ascii_copy_slug(raw_slug, set())
            try:
                package = resolve_skill_package(
                    slug=target_slug,
                    files=list(files),
                    source="zip",
                )
            except SkillPackageTooLarge as exc:
                raise OctopError(ErrorCode.SLASH_BAD_ARGS, str(exc)) from exc
            except SkillPackageError as exc:
                raise OctopError(ErrorCode.SLASH_BAD_ARGS, str(exc)) from exc
            rewritten = [
                (
                    path,
                    rewrite_skill_md_name(content, slug=package.slug)
                    if path == "SKILL.md"
                    else content,
                )
                for path, content in package.files
            ]
            parsed.append(ParsedSkillFromZip(slug=package.slug, files=tuple(rewritten)))

        if not parsed:
            raise OctopError(
                ErrorCode.SLASH_BAD_ARGS,
                "no valid skills found (folders must contain SKILL.md)",
            )
        return parsed


async def list_active_workspace_skill_slugs(workspace: Any) -> set[str]:
    """Return workspace skill directory names that are not soft-deleted.

    Discovers via ``SKILL.md`` file matches (not directory entries) so fake and
    remote backends that only surface files still work.
    """
    result = await workspace.aglob("SKILL.md", "skills")
    if result is None:
        return set()
    candidates: set[str] = set()
    for entry in result.matches or []:
        path = entry.get("path") if isinstance(entry, dict) else getattr(entry, "path", None)
        if not isinstance(path, str):
            continue
        rel = path.replace("\\", "/").strip("/")
        if rel.startswith("skills/"):
            rel = rel[len("skills/") :]
        if not rel.endswith("SKILL.md"):
            continue
        parent = rel[: -len("SKILL.md")].strip("/")
        if not parent or "/" in parent:
            continue
        candidates.add(parent)

    slugs: set[str] = set()
    for slug in candidates:
        text = await workspace.aread_text(f"skills/{slug}/SKILL.md")
        if text is None:
            continue
        meta, _ = parse_frontmatter(text)
        if meta.get("removed"):
            continue
        slugs.add(slug)
    return slugs


async def import_portable_skill_zip(
    *,
    data: bytes,
    existing_slugs: set[str],
    install_skill: InstallSkillFn,
    root_slug_fallback: str = "imported-skill",
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """Install skills from *data*.

    Default is copy-on-collision (``-copy`` / ``-copyN``). When *overwrite* is
    true, existing same-slug skills are replaced in place.
    """
    try:
        return await asyncio.wait_for(
            _import_portable_skill_zip(
                data=data,
                existing_slugs=existing_slugs,
                install_skill=install_skill,
                root_slug_fallback=root_slug_fallback,
                overwrite=overwrite,
            ),
            timeout=SKILL_ZIP_IMPORT_TIMEOUT_S,
        )
    except TimeoutError as exc:
        raise OctopError(
            ErrorCode.SLASH_BAD_ARGS,
            f"skill zip import timed out after {int(SKILL_ZIP_IMPORT_TIMEOUT_S)}s",
            details={
                "reason": "import_timeout",
                "timeout_s": int(SKILL_ZIP_IMPORT_TIMEOUT_S),
            },
        ) from exc


async def _import_portable_skill_zip(
    *,
    data: bytes,
    existing_slugs: set[str],
    install_skill: InstallSkillFn,
    root_slug_fallback: str = "imported-skill",
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    taken = set(existing_slugs)
    parsed = parse_portable_skill_zip(data, root_slug_fallback=root_slug_fallback)
    installed: list[dict[str, Any]] = []
    for skill in parsed:
        preferred = skill.slug
        target = preferred if overwrite else allocate_ascii_copy_slug(preferred, taken)
        files = [
            (
                path,
                rewrite_skill_md_name(content, slug=target) if path == "SKILL.md" else content,
            )
            for path, content in skill.files
        ]
        files = list(normalize_skill_files(files))
        await install_skill(target, files)
        taken.add(target)
        skill_md = next(content for path, content in files if path == "SKILL.md")
        meta, _ = parse_frontmatter(skill_md.decode("utf-8", errors="replace"))
        installed.append(
            {
                "slug": target,
                "name": str(meta.get("name") or target),
                "description": str(meta.get("description") or ""),
                "copied": (not overwrite) and target != preferred,
                "overwritten": overwrite and preferred in existing_slugs,
            }
        )
    return installed
