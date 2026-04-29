from __future__ import annotations

import re
import unicodedata

from bs4 import BeautifulSoup

from .models import CleanedItem, Metasource, RawItem

_TRUST_ROLE_TO_SIGNAL = {
    "official_signal": "official_update",
    "media_signal": "media_narrative",
    "polling_signal": "poll",
    "agenda_signal": "calendar_event",
    "resolution_source": "official_update",
}

SHORT_TEXT_THRESHOLD = 40
SUMMARY_MAX_CHARS = 280


def strip_html(text: str) -> str:
    if not text:
        return ""
    if "<" not in text:
        return text
    return BeautifulSoup(text, "html.parser").get_text(separator=" ", strip=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def truncate_summary(text: str, max_chars: int = SUMMARY_MAX_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    last_space = cut.rfind(" ")
    if last_space >= max_chars * 0.6:
        cut = cut[:last_space]
    return cut.rstrip(" ,.;:-") + "…"


def signal_type_for(source: Metasource) -> str:
    if source.type == "legal":
        return "court_or_regulatory_movement"
    return _TRUST_ROLE_TO_SIGNAL.get(source.trust_role, "unknown")


def fold_accents(text: str) -> str:
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )


def clean(raw: RawItem, source: Metasource) -> CleanedItem:
    title = normalize_whitespace(raw.title or "")
    clean_text = normalize_whitespace(strip_html(raw.raw_text or ""))
    if not clean_text and title:
        clean_text = title

    summary = truncate_summary(clean_text)

    notes: list[str] = []
    if not title:
        notes.append("low_quality:no_title")
    if len(clean_text) < SHORT_TEXT_THRESHOLD:
        notes.append("low_quality:short_text")
    quality_notes = ",".join(notes)

    return CleanedItem(
        id=raw.id,
        source_id=raw.source_id,
        source_name=raw.source_name,
        source_type=raw.source_type,
        url=raw.url,
        title=title,
        fetched_at=raw.fetched_at,
        published_at=raw.published_at,
        clean_text=clean_text,
        summary=summary,
        signal_type=signal_type_for(source),
        country_relevance=source.country_relevance,
        quality_notes=quality_notes,
        trust_role=source.trust_role,
        priority=source.priority,
    )
