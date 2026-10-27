"""Shared checks for text intended for the desk's reader."""
from __future__ import annotations

import re

INTERNAL_TOKENS = (
    "M1", "M2", "M3", "insight_only", "shadow_track", "monitor_no_new_m3",
    "research_more", "promote_to_m3", "packet", "lane", "bundle", "preflight",
    "tension card", "disposition",
)
_INTERNAL = re.compile(r"\b(?:" + "|".join(map(re.escape, INTERNAL_TOKENS)) + r")\b", re.I)
_CODE = re.compile(r"\b(?:PL|PAL|Decreto|Resolución)\s*\d+(?:/\d+)?\b", re.I)
_URL = re.compile(r"https?://\S+")


def word_count(text: str) -> int:
    """Count visible words, excluding Markdown URL targets and HTML tags."""
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return len(re.findall(r"\b[\w]+(?:[’'−.-][\w]+)*\b", text))


def validate_plain_text(text: str, *, max_words: int | None = None) -> list[str]:
    issues: list[str] = []
    if not isinstance(text, str):
        return ["reader text must be a string"]
    visible = _URL.sub("", text)
    for match in _INTERNAL.finditer(visible):
        issues.append(f"internal token is not reader text: {match.group()}")
    if _CODE.search(visible) and word_count(_CODE.sub("", visible)) < 8:
        issues.append("a bill or decree code needs at least eight words of description")
    if any(len(token) >= 30 for token in visible.split()):
        issues.append("reader text contains a token of at least 30 characters")
    if max_words is not None and word_count(text) > max_words:
        issues.append(f"reader text exceeds {max_words} words")
    return issues


# A descriptive alias for callers that validate individual reader-facing fields.
validate_plain_language = validate_plain_text
