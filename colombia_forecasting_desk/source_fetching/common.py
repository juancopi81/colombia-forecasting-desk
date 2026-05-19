from __future__ import annotations

import hashlib
import html
import io
import logging
import os
from pathlib import Path
import re
import time
import zipfile
import zlib
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree as ET

import feedparser
import httpx
from bs4 import BeautifulSoup

from ..cleaner import fold_accents, normalize_whitespace
from ..dedupe import canonicalize_url
from ..legal_identity import annotate_legal_identity, parse_legal_act_records
from ..models import Metasource, RawItem, SourceFailure

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 "
    "colombia-forecasting-desk/0.1"
)
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
}
HTTP_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
MAX_RETRIES = 2
BACKOFF_SECONDS = 1.0
ANCHORS_PER_SOURCE = 30
MIN_ANCHOR_TEXT = 10
DATE_CONTEXT_CHARS = 500
ELTIEMPO_COLOMBIA_SECTION_URL = "https://www.eltiempo.com/colombia"
ELTIEMPO_SECTION_EXTRACTION = "eltiempo_colombia_section_html"
BOT_BLOCK_MARKERS = (
    "Radware Bot Manager",
    "validate.perfdrive.com",
    "Access denied",
    "Bot Manager Block",
)
SPA_SHELL_MARKERS = ("<app-root></app-root>", "<app-root>")
PDF_TEXT_PARSE_LIMIT = 5
PDF_TEXT_MAX_BYTES = 2_000_000
PDF_TEXT_EXCERPT_CHARS = 1_200
PDF_TEXT_FULL_CHARS = 20_000
IMPRENTA_PDF_TEXT_FULL_CHARS = 60_000
PDF_TEXT_MIN_CHARS = 80
MINHACIENDA_TES_PARSE_LIMIT = 5
MINHACIENDA_TES_TEXT_MAX_CHARS = 60_000
MINHACIENDA_TES_BROWSER_TIMEOUT_MS = 30_000
MINHACIENDA_TES_BROWSER_NETWORK_IDLE_MS = 5_000
MINHACIENDA_TES_IRC_PAGES = (
    "https://www.irc.gov.co/424",  # Subastas Largo Plazo - COP
    "https://www.irc.gov.co/420",  # Subastas TES Corto Plazo
    "https://www.irc.gov.co/subastas-largo-plazo-uvr-2026",
)


def _minhacienda_tes_title_slug(title: str) -> str | None:
    folded = fold_accents(title.lower())
    if "informe tes subasta" not in folded:
        return None
    slug = re.sub(r"[^a-z0-9]+", "-", folded).strip("-")
    return slug or None
SENADO_AGENDA_PARSE_LIMIT = 2
SENADO_AGENDA_ENTRY_LIMIT = 10
GACETA_PDF_PARSE_LIMIT = 5
LEGISLATIVE_REGISTRY_DEFAULT_LIMIT = 20
BANREP_MINUTAS_PARSE_LIMIT = 2
BANREP_MINUTAS_BODY_CHARS = 4_000
BANREP_MINUTAS_BULLET_LIMIT = 6
BANREP_MINUTAS_BLOC_CHARS = 1_200
BANREP_JUNTA_BROWSER_TIMEOUT_MS = 30_000
BANREP_JUNTA_BROWSER_NETWORK_IDLE_MS = 5_000

NAV_TEXT = {
    "inicio", "contacto", "menu", "menú", "buscar", "ver mas", "ver más",
    "siguiente", "anterior", "leer mas", "leer más", "mas", "más",
    "compartir", "imprimir", "twitter", "facebook", "instagram", "youtube",
    "linkedin", "whatsapp", "telegram", "ir al contenido principal",
    "saltar al contenido", "iniciar sesión", "iniciar sesion", "registrarse",
}

MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _current_legislature_label(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    if current.month >= 7:
        return f"{current.year}-{current.year + 1}"
    return f"{current.year - 1}-{current.year}"


def _make_id(source_id: str, url: str, title: str) -> str:
    digest = hashlib.sha1(
        f"{source_id}|{canonicalize_url(url)}|{title}".encode("utf-8")
    ).hexdigest()
    return digest[:16]


def _struct_time_to_iso(st) -> str | None:
    if not st:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", st)
    except (TypeError, ValueError):
        return None


def _date_to_iso(year: int, month: int, day: int) -> str | None:
    try:
        return datetime(year, month, day, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except ValueError:
        return None


def _parse_date_text_to_iso(text: str | None) -> str | None:
    if not text:
        return None
    compact = normalize_whitespace(text)

    try:
        parsed = parsedate_to_datetime(compact)
    except (TypeError, ValueError, IndexError, OverflowError):
        parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    match = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", compact)
    if match:
        year, month, day = (int(x) for x in match.groups())
        return _date_to_iso(year, month, day)

    match = re.search(r"\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b", compact)
    if match:
        day, month, year = (int(x) for x in match.groups())
        return _date_to_iso(year, month, day)

    folded = fold_accents(compact.lower())
    month_names = "|".join(MONTHS_ES)
    match = re.search(
        rf"\b(\d{{1,2}})(?:o)?\s+(?:de\s+)?al\s+\d{{1,2}}\s+de\s+"
        rf"({month_names})\s+de\s+(\d{{4}})\b",
        folded,
    )
    if match:
        day_s, month_s, year_s = match.groups()
        return _date_to_iso(int(year_s), MONTHS_ES[month_s], int(day_s))

    match = re.search(
        rf"\b(\d{{1,2}})\s+de\s+({month_names})\s+de\s+(\d{{4}})\b",
        folded,
    )
    if match:
        day_s, month_s, year_s = match.groups()
        return _date_to_iso(int(year_s), MONTHS_ES[month_s], int(day_s))

    match = re.search(
        rf"\b({month_names})\s+(\d{{1,2}})(?:\s+de|,)?\s+(\d{{4}})\b",
        folded,
    )
    if match:
        month_s, day_s, year_s = match.groups()
        return _date_to_iso(int(year_s), MONTHS_ES[month_s], int(day_s))

    return None



def _raw_item_sort_datetime(item: RawItem) -> datetime:
    if item.published_at:
        try:
            parsed = datetime.fromisoformat(item.published_at.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _sort_raw_items_newest_first(items: list[RawItem]) -> list[RawItem]:
    return sorted(items, key=_raw_item_sort_datetime, reverse=True)


def _dedupe_raw_items_by_url(items: list[RawItem]) -> list[RawItem]:
    deduped: list[RawItem] = []
    seen: set[str] = set()
    for item in items:
        key = canonicalize_url(item.url) or f"{item.source_id}:{item.title}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped



def _http_get(
    client: httpx.Client,
    url: str,
    params: Mapping[str, str] | None = None,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.get(url, params=params)
        except httpx.TransportError as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)
                continue
            raise
        if response.status_code >= 500 and attempt < MAX_RETRIES:
            time.sleep(BACKOFF_SECONDS)
            continue
        response.raise_for_status()
        return response
    if last_exc:
        raise last_exc
    raise RuntimeError("unreachable")



def _http_post_form(
    client: httpx.Client,
    url: str,
    data: Mapping[str, str],
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.post(url, data=data)
        except httpx.TransportError as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)
                continue
            raise
        if response.status_code >= 500 and attempt < MAX_RETRIES:
            time.sleep(BACKOFF_SECONDS)
            continue
        response.raise_for_status()
        return response
    if last_exc:
        raise last_exc
    raise RuntimeError("unreachable")


class RssParseError(Exception):
    pass


class BotBlockError(Exception):
    """Raised when a fetched response looks like a bot manager block page."""


class DynamicShellError(Exception):
    """Raised when a fetched HTML response is just a JS app shell with no content."""


def _detect_bot_block(text: str) -> str | None:
    head = text[:2048]
    for marker in BOT_BLOCK_MARKERS:
        if marker.lower() in head.lower():
            return marker
    return None


def _detect_spa_shell(text: str) -> bool:
    if any(marker in text for marker in SPA_SHELL_MARKERS):
        # The shell itself is small; large pages with <app-root> are probably real.
        return len(text) < 20_000
    return False


def _chrome_executable_path() -> str | None:
    env_path = os.environ.get("COLOMBIA_FORECASTING_CHROME")
    if env_path and Path(env_path).exists():
        return env_path
    for candidate in (
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ):
        if Path(candidate).exists():
            return candidate
    return None




__all__ = [name for name in globals() if not name.startswith("__")]
