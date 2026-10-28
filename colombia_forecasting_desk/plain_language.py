"""Shared checks for text intended for the desk's reader."""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

INTERNAL_TOKENS = (
    "M1", "M2", "M3", "insight_only", "shadow_track", "monitor_no_new_m3",
    "research_more", "promote_to_m3", "packet", "lane", "bundle", "preflight",
    "tension card", "disposition",
)
_INTERNAL = re.compile(r"\b(?:" + "|".join(map(re.escape, INTERNAL_TOKENS)) + r")\b", re.I)
_CODE = re.compile(r"\b(?:PL|PAL|Decreto|Resolución)\s*\d+(?:/\d+)?\b", re.I)
_URL = re.compile(r"https?://\S+")
_DATASET_CODE = re.compile(r"\b[a-z0-9]{4}-[a-z0-9]{4}\b", re.I)
DATASET_NAMES = {
    "jbjy-vk9h": "SECOP II contracts",
    "f789-7hwg": "SECOP I contracts",
    "cb9c-h8sn": "SECOP II contract additions",
    "4n4q-k399": "recorded sanctions",
    "gdxc-w37w": "DANE's official place directory",
}


def describe_datasets(text: str) -> str:
    """Expand machine dataset identifiers in visible text, preserving URLs."""
    parts = re.split(r"(https?://\S+)", text)
    for index in range(0, len(parts), 2):
        for code, label in DATASET_NAMES.items():
            parts[index] = parts[index].replace(code, label)
        parts[index] = re.sub(r"\bsecop_ii\b", "SECOP II", parts[index])
        parts[index] = re.sub(r"\bsecop_i\b", "SECOP I", parts[index])
    return "".join(parts)


def format_cop(value: int | float | None) -> str:
    """Use English money units while keeping exact values in evidence JSON."""
    if value is None:
        return "unknown"
    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("COP amount must be a finite number") from exc
    if not amount.is_finite():
        raise ValueError("COP amount must be a finite number")
    for scale, label in ((10**12, "trillion"), (10**9, "billion"), (10**6, "million")):
        if abs(amount) >= scale:
            number = f"{amount / scale:.2f}".rstrip("0").rstrip(".")
            return f"COP {number} {label}"
    number = f"{amount:,.2f}".rstrip("0").rstrip(".")
    return f"COP {number}"


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
    for match in _DATASET_CODE.finditer(visible):
        if any(char.isdigit() for char in match.group()):
            issues.append(f"dataset code is not reader text: {match.group()}")
    if _CODE.search(visible) and word_count(_CODE.sub("", visible)) < 8:
        issues.append("a bill or decree code needs at least eight words of description")
    if any(len(token) >= 30 for token in visible.split()):
        issues.append("reader text contains a token of at least 30 characters")
    if max_words is not None and word_count(text) > max_words:
        issues.append(f"reader text exceeds {max_words} words")
    return issues


# A descriptive alias for callers that validate individual reader-facing fields.
validate_plain_language = validate_plain_text
