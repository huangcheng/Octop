"""Unit tests for copy-name / ASCII slug allocation (#499)."""

from __future__ import annotations

from octop.infra.agents.experts.copy_names import (
    allocate_ascii_copy_slug,
    allocate_display_copy_name,
)


def test_display_copy_zh_sequence() -> None:
    existing: set[str] = set()
    assert allocate_display_copy_name("运维助手", existing, locale="zh") == "运维助手"
    existing.add("运维助手")
    assert allocate_display_copy_name("运维助手", existing, locale="zh") == "运维助手-副本"
    existing.add("运维助手-副本")
    assert allocate_display_copy_name("运维助手", existing, locale="zh") == "运维助手-副本1"


def test_display_copy_en_uses_copy() -> None:
    existing = {"Ops"}
    assert allocate_display_copy_name("Ops", existing, locale="en") == "Ops-copy"


def test_ascii_slug_copy_sequence() -> None:
    existing: set[str] = set()
    assert allocate_ascii_copy_slug("pdf-reader", existing) == "pdf-reader"
    existing.add("pdf-reader")
    assert allocate_ascii_copy_slug("pdf-reader", existing) == "pdf-reader-copy"
    existing.add("pdf-reader-copy")
    assert allocate_ascii_copy_slug("pdf-reader", existing) == "pdf-reader-copy1"


def test_ascii_slug_strips_non_ascii() -> None:
    slug = allocate_ascii_copy_slug("中文技能", set())
    assert slug.isascii()
    assert slug  # non-empty
    assert "-" not in slug or all(ord(c) < 128 for c in slug)
