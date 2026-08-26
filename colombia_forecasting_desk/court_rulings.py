from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlsplit

from .cleaner import fold_accents, normalize_whitespace

CORTE_COMUNICADOS_SOURCE_ID = "corte_constitucional_comunicados"
COURT_COMMUNICATION_KIND = "official_communication"
COURT_DEADLINE_PENDING_STATUS = "pending_written_ruling"

_DECISION_REFERENCE_RE = re.compile(
    r"\b(?:Sentencia|Auto)\s+[A-Z]{1,4}-\d{1,5}"
    r"(?:\s+de\s+20\d{2}|/\d{2,4})\b",
    re.IGNORECASE,
)
_IMPLEMENTATION_OR_CORRECTION_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"\bordena(?:r|do)?\b",
        r"\bordeno\b",
        r"\border(?:ed|s|ing)?\b",
        r"\bdispuso\b",
        r"\bdebera(?:n)?\b",
        r"\bexhorta(?:r|do)?\b",
        r"\bcorreg\w*\b",
        r"\bcorrect\w*\b",
        r"\bsubsan\w*\b",
        r"\bajust\w*\b",
        r"\bimplement\w*\b",
        r"\bcumpl\w*\b",
        r"\bcompl(?:y|ies|ied|iance)\b",
    )
)
_CLOCK_LANGUAGE_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"\bplazo\b",
        r"\btermino\b",
        r"\bdeadline\b",
        r"\bdentro de\b",
        r"\bwithin\b",
        r"\ba mas tardar\b",
        r"\bantes de(?:l| la)?\b",
        r"\b(?:\d+|un|uno|dos|tres|cuatro|cinco|seis|siete|diez|doce|"
        r"quince|veinte|treinta|sesenta)\s+"
        r"(?:dias?|mes(?:es)?|anos?)\b",
    )
)
_WRITTEN_RULING_PATH_MARKERS = (
    "/relatoria/",
    "/sentencias/",
    "/autos/",
)


def court_text_signals(text: str) -> dict[str, Any]:
    """Return advisory Court-order signals without extracting a deadline."""
    clean = normalize_whitespace(text)
    folded = fold_accents(clean.lower())
    return {
        "decision_references": _unique_preserve_order(
            match.group(0) for match in _DECISION_REFERENCE_RE.finditer(clean)
        ),
        "implementation_or_correction_signal": any(
            pattern.search(folded)
            for pattern in _IMPLEMENTATION_OR_CORRECTION_PATTERNS
        ),
        "clock_language_signal": any(
            pattern.search(folded) for pattern in _CLOCK_LANGUAGE_PATTERNS
        ),
    }


def is_corte_source_id(source_id: Any) -> bool:
    return str(source_id or "").strip() == CORTE_COMUNICADOS_SOURCE_ID


def is_corte_source(source_id: Any, source_name: Any = "", url: Any = "") -> bool:
    if is_corte_source_id(source_id):
        return True
    folded_name = fold_accents(str(source_name or "").lower())
    if "corte constitucional" in folded_name:
        return True
    return urlsplit(str(url or "")).netloc.lower().endswith(
        "corteconstitucional.gov.co"
    )


def is_written_court_ruling_url(url: Any) -> bool:
    value = str(url or "").strip()
    parts = urlsplit(value)
    if not parts.netloc.lower().endswith("corteconstitucional.gov.co"):
        return False
    path = parts.path.lower()
    return any(marker in path for marker in _WRITTEN_RULING_PATH_MARKERS)


def is_court_implementation_case(*values: Any) -> bool:
    text = " ".join(_flatten_text(value) for value in values)
    signals = court_text_signals(text)
    return bool(signals["implementation_or_correction_signal"])


def _flatten_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value or "")


def _unique_preserve_order(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        clean = normalize_whitespace(str(value or ""))
        folded = clean.casefold()
        if not clean or folded in seen:
            continue
        seen.add(folded)
        result.append(clean)
    return result
