"""Allocate collision-free display names and ASCII skill directory slugs."""

from __future__ import annotations

import re

from octop.infra.agents.subagents.catalog import slugify

_COPY_ASCII = "copy"
_COPY_ZH = "副本"
_MAX_ATTEMPTS = 10_000


def allocate_display_copy_name(
    base: str,
    existing: set[str] | frozenset[str],
    *,
    locale: str = "zh",
) -> str:
    """Return *base* or ``{base}-副本`` / ``{base}-copy`` (+ numbered) unused in *existing*.

    Display names may contain non-ASCII. Used for agent titles, not filesystem paths.
    """
    cleaned = (base or "").strip() or "imported-expert"
    if cleaned not in existing:
        return cleaned

    marker = _COPY_ZH if str(locale).lower().startswith("zh") else _COPY_ASCII
    stem = _strip_trailing_copy_marker(cleaned, marker)
    first = f"{stem}-{marker}"
    if first not in existing:
        return first
    for n in range(1, _MAX_ATTEMPTS):
        candidate = f"{stem}-{marker}{n}"
        if candidate not in existing:
            return candidate
    return f"{stem}-{marker}-{_MAX_ATTEMPTS}"


def allocate_ascii_copy_slug(
    base: str,
    existing: set[str] | frozenset[str],
) -> str:
    """Return an ASCII-safe skill directory slug, using ``-copy`` / ``-copyN`` on clash.

    Never returns a slug containing non-ASCII characters.
    """
    stem = _ascii_skill_stem(base)
    if stem not in existing:
        return stem
    first = f"{stem}-{_COPY_ASCII}"
    if first not in existing:
        return first
    for n in range(1, _MAX_ATTEMPTS):
        candidate = f"{stem}-{_COPY_ASCII}{n}"
        if candidate not in existing:
            return candidate
    return f"{stem}-{_COPY_ASCII}-{_MAX_ATTEMPTS}"


def _ascii_skill_stem(base: str) -> str:
    raw = (base or "").strip()
    stemmed = _strip_trailing_copy_marker(raw, _COPY_ASCII)
    slug = slugify(stemmed)
    if slug:
        return slug
    # slugify drops non-latin entirely; fall back to a stable token.
    compact = re.sub(r"[^a-zA-Z0-9]+", "-", raw).strip("-").lower()
    if compact and re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", compact):
        return compact
    return "imported-skill"


def _strip_trailing_copy_marker(name: str, marker: str) -> str:
    """``foo-副本`` / ``foo-副本3`` / ``foo-copy`` / ``foo-copy2`` → ``foo``."""
    pattern = re.compile(rf"^(?P<stem>.+)-{re.escape(marker)}(?P<num>\d+)?$", re.IGNORECASE)
    match = pattern.match(name)
    if match:
        return match.group("stem")
    return name
