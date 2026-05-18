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

from .cleaner import fold_accents, normalize_whitespace
from .dedupe import canonicalize_url
from .legal_identity import annotate_legal_identity, parse_legal_act_records
from .models import Metasource, RawItem, SourceFailure

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 "
    "colombia-forecasting-desk/0.1"
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


def _parse_rss_entries(parsed: Any, source: Metasource, fetched_at: str) -> list[RawItem]:
    items: list[RawItem] = []
    for entry in parsed.entries or []:
        title = (entry.get("title") or "").strip()
        url = entry.get("link") or ""
        if not url:
            continue
        published_at = _struct_time_to_iso(
            entry.get("published_parsed") or entry.get("updated_parsed")
        )
        if published_at is None:
            published_at = _parse_date_text_to_iso(
                entry.get("published") or entry.get("updated") or entry.get("date")
            )
        raw_text = entry.get("summary") or entry.get("description") or ""
        items.append(
            RawItem(
                id=_make_id(source.id, url, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=url,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata={"feed_id": getattr(parsed.feed, "id", "") or ""},
            )
        )
    return items


def _first_tag_text(blob: str, names: list[str]) -> str:
    for name in names:
        match = re.search(
            rf"<{re.escape(name)}(?:\s[^>]*)?>(.*?)</{re.escape(name)}>",
            blob,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            content = match.group(1)
            if "<" not in content:
                return normalize_whitespace(html.unescape(content))
            return normalize_whitespace(
                BeautifulSoup(content, "html.parser").get_text(separator=" ", strip=True)
            )
    return ""


def _link_from_entry_blob(blob: str) -> str:
    link_text = _first_tag_text(blob, ["link"])
    if link_text:
        return link_text
    match = re.search(
        r"<link\b[^>]*\bhref=[\"']([^\"']+)[\"'][^>]*/?>",
        blob,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return normalize_whitespace(match.group(1)) if match else ""


def _recover_rss_entries(html_or_xml: str, source: Metasource, fetched_at: str) -> list[RawItem]:
    items: list[RawItem] = []
    entry_blobs = [
        match.group(2)
        for match in re.finditer(
            r"<(item|entry)\b[^>]*>(.*?)</\1>",
            html_or_xml,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]
    for blob in entry_blobs:
        title = _first_tag_text(blob, ["title"])
        url = _link_from_entry_blob(blob)
        if not url:
            continue
        raw_text = _first_tag_text(blob, ["description", "summary", "content"])
        date_text = _first_tag_text(blob, ["pubdate", "published", "updated", "dc:date"])
        published_at = _parse_date_text_to_iso(date_text)
        items.append(
            RawItem(
                id=_make_id(source.id, url, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=url,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata={"extraction": "rss_recovery"},
            )
        )
    return items


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


def _extract_eltiempo_colombia_section(
    html_text: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    soup = BeautifulSoup(html_text, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for article in soup.find_all("article"):
        link = (
            article.select_one("a.c-articulo__titulo__txt[href]")
            or article.select_one("a.page-link[href]")
            or article.find("a", href=True)
        )
        if link is None:
            continue
        resolved = _same_site_url(str(link["href"]), base_url)
        if resolved is None or not urlsplit(resolved).path.startswith("/colombia/"):
            continue

        title = normalize_whitespace(link.get_text(separator=" ", strip=True))
        if not title:
            title = normalize_whitespace(str(article.get("data-name") or ""))
        if len(title) < MIN_ANCHOR_TEXT or title.lower() in NAV_TEXT:
            continue

        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)

        published_at = _parse_date_text_to_iso(str(article.get("data-publicacion") or ""))
        if not published_at:
            date_node = article.select_one(".c-articulo__fecha")
            if date_node is not None:
                published_at = _parse_date_text_to_iso(
                    date_node.get_text(separator=" ", strip=True)
                )

        summary_node = article.select_one(".c-articulo__resumen")
        raw_text = (
            normalize_whitespace(summary_node.get_text(separator=" ", strip=True))
            if summary_node is not None
            else ""
        )
        if not raw_text:
            raw_text = normalize_whitespace(article.get_text(separator=" ", strip=True))

        items.append(
            RawItem(
                id=_make_id(source.id, resolved, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=resolved,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata={
                    "extraction": ELTIEMPO_SECTION_EXTRACTION,
                    "article_id": str(article.get("data-id") or ""),
                    "category": str(article.get("data-category") or ""),
                    "section": "colombia",
                },
            )
        )
    return items


def _augment_eltiempo_colombia_rss_items(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
    base_items: list[RawItem],
) -> list[RawItem]:
    try:
        response = _http_get(client, ELTIEMPO_COLOMBIA_SECTION_URL)
        marker = _detect_bot_block(response.text)
        if marker:
            raise BotBlockError(f"bot block detected: {marker}")
        section_items = _extract_eltiempo_colombia_section(
            response.text,
            str(response.url),
            source,
            fetched_at,
        )
    except Exception:  # noqa: BLE001 - augmentation must not hide a healthy RSS feed.
        if base_items:
            logger.warning(
                "El Tiempo Colombia section augmentation failed; keeping RSS items",
                exc_info=True,
            )
            return _sort_raw_items_newest_first(_dedupe_raw_items_by_url(base_items))
        raise

    return _sort_raw_items_newest_first(
        _dedupe_raw_items_by_url([*base_items, *section_items])
    )


def _extract_anchors(html: str, base_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("main") or soup.find("article") or soup.body or soup
    base_host = urlsplit(base_url).netloc.lower()
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for a in container.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        resolved = urljoin(base_url, href)
        parts = urlsplit(resolved)
        if parts.scheme not in ("http", "https"):
            continue
        if parts.netloc and parts.netloc.lower() != base_host:
            continue
        text = normalize_whitespace(a.get_text(separator=" ", strip=True))
        if len(text) < MIN_ANCHOR_TEXT:
            continue
        if text.lower() in NAV_TEXT:
            continue
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        out.append((text, resolved))
        if len(out) >= ANCHORS_PER_SOURCE:
            break
    return out


def _same_site_url(url: str, base_url: str) -> str | None:
    resolved = urljoin(base_url, url.strip())
    parts = urlsplit(resolved)
    if parts.scheme not in ("http", "https"):
        return None
    base_host = urlsplit(base_url).netloc.lower()
    if parts.netloc and parts.netloc.lower() != base_host:
        return None
    return resolved


def _extract_dated_anchors(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
    extraction: str,
    require_date: bool = True,
) -> list[RawItem]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("main") or soup.find("article") or soup.body or soup
    seen: set[str] = set()
    items: list[RawItem] = []
    for a in container.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        resolved = _same_site_url(href, base_url)
        if resolved is None:
            continue
        title = normalize_whitespace(a.get_text(separator=" ", strip=True))
        if len(title) < MIN_ANCHOR_TEXT or title.lower() in NAV_TEXT:
            continue
        parent_text = normalize_whitespace(
            (a.find_parent(["tr", "li", "article", "div", "section"]) or a)
            .get_text(separator=" ", strip=True)
        )
        published_at = _parse_date_text_to_iso(parent_text[:DATE_CONTEXT_CHARS])
        if require_date and not published_at:
            continue
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        items.append(
            RawItem(
                id=_make_id(source.id, resolved, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=resolved,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=parent_text,
                metadata={"extraction": extraction},
            )
        )
        if len(items) >= ANCHORS_PER_SOURCE:
            break
    return items


def _extract_dane_comunicados(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    items: list[RawItem] = []
    seen: set[str] = set()
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        row_text = normalize_whitespace(row.get_text(separator=" ", strip=True))
        published_at = _parse_date_text_to_iso(row_text)
        if not published_at:
            continue
        link = row.find("a", href=True)
        if link is None:
            continue
        resolved = _same_site_url(link["href"], base_url) or urljoin(base_url, link["href"])
        title_candidates = [
            normalize_whitespace(c.get_text(separator=" ", strip=True))
            for c in cells
        ]
        title = next(
            (
                t
                for t in title_candidates
                if len(t) >= MIN_ANCHOR_TEXT and _parse_date_text_to_iso(t) is None
            ),
            normalize_whitespace(link.get_text(separator=" ", strip=True)),
        )
        if not title:
            continue
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        items.append(
            RawItem(
                id=_make_id(source.id, resolved, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=resolved,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=row_text,
                metadata={"extraction": "dane_comunicados_table"},
            )
        )
    if items:
        return items
    return _extract_dated_anchors(
        html, base_url, source, fetched_at, "dane_comunicados_dated_anchor"
    )


_MONTH_ABBR_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
}
_MONTH_NAME_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
    7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre",
    12: "diciembre",
}
_ICOCED_FILENAME_RE = re.compile(
    r"/anex-ICOCED-([a-z]{3})(\d{4})\.xlsx", re.IGNORECASE
)
_XLSX_MAIN_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_XLSX_REL_ID = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
_ICOCED_METRIC_SHEETS = {
    "total": "Anexo 1",
    "residential": "Anexo 2.1",
    "non_residential": "Anexo 2.2",
}


def _xlsx_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    shared: list[str] = []
    for si in root.findall("m:si", _XLSX_MAIN_NS):
        shared.append(
            "".join(t.text or "" for t in si.findall(".//m:t", _XLSX_MAIN_NS))
        )
    return shared


def _xlsx_sheet_paths(zf: zipfile.ZipFile) -> dict[str, str]:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rels = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels_root}
    paths: dict[str, str] = {}
    for sheet in workbook.findall(".//m:sheet", _XLSX_MAIN_NS):
        rel_id = sheet.attrib.get(_XLSX_REL_ID)
        target = rels.get(rel_id or "")
        if not target:
            continue
        paths[sheet.attrib["name"]] = "xl/" + target.lstrip("/")
    return paths


def _xlsx_cell_text(
    cell: ET.Element,
    shared_strings: list[str],
) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return normalize_whitespace(
            "".join(t.text or "" for t in cell.findall(".//m:t", _XLSX_MAIN_NS))
        )

    value = cell.find("m:v", _XLSX_MAIN_NS)
    if value is None or value.text is None:
        return ""
    if cell_type == "s":
        try:
            return shared_strings[int(value.text)]
        except (ValueError, IndexError):
            return ""
    return normalize_whitespace(value.text)


def _xlsx_rows(
    zf: zipfile.ZipFile,
    sheet_path: str,
    shared_strings: list[str],
) -> list[dict[str, str]]:
    root = ET.fromstring(zf.read(sheet_path))
    rows: list[dict[str, str]] = []
    for row in root.findall(".//m:row", _XLSX_MAIN_NS):
        values: dict[str, str] = {}
        for cell in row.findall("m:c", _XLSX_MAIN_NS):
            ref = cell.attrib.get("r", "")
            match = re.match(r"([A-Z]+)", ref)
            if not match:
                continue
            text = _xlsx_cell_text(cell, shared_strings)
            if text:
                values[match.group(1)] = text
        if values:
            rows.append(values)
    return rows


def _to_float(text: str | None) -> float | None:
    if text is None:
        return None
    cleaned = text.strip().replace(",", ".")
    if not cleaned:
        return None
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return None


def _find_icoced_period_row(
    rows: list[dict[str, str]],
    *,
    year: int,
    month_name: str,
) -> dict[str, str] | None:
    current_year: int | None = None
    target_month = fold_accents(month_name.lower())
    for row in rows:
        year_text = row.get("A")
        if year_text:
            try:
                current_year = int(float(year_text))
            except ValueError:
                pass
        month_text = fold_accents((row.get("B") or "").strip().lower())
        if current_year == year and month_text == target_month:
            return row
    return None


def _icoced_metrics_from_row(row: dict[str, str]) -> dict[str, float]:
    candidates = {
        "index": _to_float(row.get("C")),
        "monthly_variation_pct": _to_float(row.get("D")),
        "year_to_date_variation_pct": _to_float(row.get("E")),
        "annual_variation_pct": _to_float(row.get("F")),
    }
    return {k: v for k, v in candidates.items() if v is not None}


def _format_pct(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{value:.2f}".replace(".", ",") + "%"


def _format_decimal(value: float) -> str:
    return f"{value:.2f}".replace(".", ",")


def _parse_dane_icoced_xlsx(
    content: bytes,
    *,
    year: int,
    month: int,
) -> dict[str, Any] | None:
    month_name = _MONTH_NAME_ES[month]
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            shared_strings = _xlsx_shared_strings(zf)
            sheet_paths = _xlsx_sheet_paths(zf)
            metrics: dict[str, dict[str, float]] = {}
            for key, sheet_name in _ICOCED_METRIC_SHEETS.items():
                sheet_path = sheet_paths.get(sheet_name)
                if not sheet_path:
                    continue
                row = _find_icoced_period_row(
                    _xlsx_rows(zf, sheet_path, shared_strings),
                    year=year,
                    month_name=month_name,
                )
                if row:
                    metrics[key] = _icoced_metrics_from_row(row)
    except (KeyError, ET.ParseError, zipfile.BadZipFile):
        return None

    total = metrics.get("total")
    if not total:
        return None

    monthly = _format_pct(total.get("monthly_variation_pct"))
    ytd = _format_pct(total.get("year_to_date_variation_pct"))
    annual = _format_pct(total.get("annual_variation_pct"))
    index = total.get("index")
    period_label = f"{month_name} de {year}"

    clauses: list[str] = []
    if monthly:
        clauses.append(f"una variación mensual de {monthly}")
    if ytd:
        clauses.append(f"año corrido de {ytd}")
    if annual:
        clauses.append(f"anual de {annual}")

    if clauses:
        headline = (
            f"En {period_label}, el ICOCED total registró "
            + ", ".join(clauses)
            + "."
        )
    else:
        headline = f"En {period_label}, el ICOCED total fue publicado."
    if index is not None:
        headline += f" El número índice fue {_format_decimal(index)}."
    residential = metrics.get("residential", {})
    non_residential = metrics.get("non_residential", {})
    residential_monthly = _format_pct(residential.get("monthly_variation_pct"))
    non_residential_monthly = _format_pct(
        non_residential.get("monthly_variation_pct")
    )
    if residential_monthly or non_residential_monthly:
        comparison_parts = []
        if residential_monthly:
            comparison_parts.append(f"residenciales {residential_monthly}")
        if non_residential_monthly:
            comparison_parts.append(f"no residenciales {non_residential_monthly}")
        headline += (
            " Variación mensual por grupo: " + ", ".join(comparison_parts) + "."
        )

    return {
        "headline": headline,
        "metrics": metrics,
        "sheets": _ICOCED_METRIC_SHEETS,
    }


def _extract_dane_icoced(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    """Extract monthly Excel annexes from the DANE ICOCED page.

    The current ICOCED page includes the latest annex plus a release date in
    the same table row. The annex filename encodes the data period
    (/anex-ICOCED-{mes}{anio}.xlsx), so we keep that period in metadata while
    using the release date for published_at. That keeps the monthly source
    fresh when DANE publishes a new period after the period month ends.

    Items are returned newest-first so that source.max_items=1 in
    metasources.yaml picks the latest annex deterministically.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        match = _ICOCED_FILENAME_RE.search(href)
        if not match:
            continue
        month = _MONTH_ABBR_ES.get(match.group(1).lower())
        if month is None:
            continue
        try:
            year = int(match.group(2))
        except ValueError:
            continue
        period_start = _date_to_iso(year, month, 1)
        if not period_start:
            continue
        row = a.find_parent("tr")
        published_at = _parse_date_text_to_iso(
            row.get_text(" ", strip=True) if row else None
        )
        if not published_at:
            published_at = period_start
        resolved = urljoin(base_url, href)
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        title = f"DANE ICOCED — Anexo {_MONTH_NAME_ES[month]} {year}"
        raw_text = (
            f"{title}. Anexo estadístico mensual publicado por DANE "
            f"para el periodo {_MONTH_NAME_ES[month]} {year}."
        )
        items.append(
            RawItem(
                id=_make_id(source.id, resolved, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=resolved,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata={
                    "extraction": "dane_icoced_filename",
                    "annex_filename": href.rsplit("/", 1)[-1],
                    "period_start": period_start,
                    "period_year": year,
                    "period_month": month,
                },
            )
        )
    items.sort(key=lambda it: it.published_at or "", reverse=True)
    return items


def _is_banrep_minutas_item(item: RawItem) -> bool:
    folded = fold_accents(f"{item.title} {item.url}".lower())
    return "minutas" in folded and (
        "/minutas" in folded or "minutas-banrep" in folded
    )


def _extract_banrep_minutas_links(
    soup: BeautifulSoup,
    base_url: str,
) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        resolved = urljoin(base_url, href)
        parsed = urlsplit(resolved)
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc and parsed.netloc.lower() not in {
            urlsplit(base_url).netloc.lower(),
            "d1b4gd4m8561gs.cloudfront.net",
        }:
            continue
        text = normalize_whitespace(a.get_text(separator=" ", strip=True))
        href_folded = fold_accents(resolved.lower())
        text_folded = fold_accents(text.lower())
        if (
            ".pdf" not in href_folded
            and "print/pdf" not in href_folded
            and not any(
                marker in text_folded
                for marker in ("view pdf", "ver pdf", "anexo", "adjunto")
            )
        ):
            continue
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        links.append({"title": text or "Documento oficial", "url": resolved})
    return links[:10]


def _extract_banrep_minutas_metadata(
    html_text: str,
    base_url: str,
) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    h1 = soup.find("h1")
    container = (
        soup.select_one("[data-history-node-id].node--type-noticias")
        or soup.select_one(".node--type-noticias")
        or soup.find("article")
        or (h1.find_parent(["article", "main"]) if h1 else None)
        or soup.find("main")
        or soup.body
        or soup
    )
    title = normalize_whitespace(
        (h1 or container.find("h1") or soup.title or container).get_text(
            separator=" ", strip=True
        )
    )
    text = normalize_whitespace(
        f"{title} {container.get_text(separator=' ', strip=True)}"
    )
    if "minutas" not in fold_accents(f"{title} {text[:500]}".lower()):
        return {}

    published_at = None
    date_match = re.search(
        r"Fecha de publicaci[oó]n:?\s*([^*]{0,80}\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if date_match:
        published_at = _parse_date_text_to_iso(date_match.group(1))
    if published_at is None:
        published_at = _parse_date_text_to_iso(text)
    rate_match = re.search(
        r"tasa de inter[eé]s(?: de pol[ií]tica monetaria)?\s+(?:en|a)\s+"
        r"(\d{1,2}(?:[,.]\d{1,2})?)\s*%",
        text,
        flags=re.IGNORECASE,
    )
    change_match = re.search(
        r"(incrementar|aumentar|reducir|disminuir|mantener(?:la)?(?: inalterada)?)"
        r".{0,90}?(\d{1,3})\s+puntos?\s+b[aá]sicos|"
        r"\bmantener(?:la)?\s+inalterada\b",
        text,
        flags=re.IGNORECASE,
    )
    vote_match = re.search(
        r"((?:\w+\s+)?directores?\s+votaron?[^.]{0,260}\.)",
        text,
        flags=re.IGNORECASE,
    )
    unanimity_match = re.search(
        r"((?:La\s+)?decisi[oó]n adoptada por unanimidad[^.]{0,500}\.)",
        text,
        flags=re.IGNORECASE,
    ) or re.search(
        r"([^.]?\bpor unanimidad\b[^.]{0,220}\.)",
        text,
        flags=re.IGNORECASE,
    )
    next_match = re.search(
        r"([^.]*(?:sesi[oó]n de Junta del pr[oó]ximo)[^.]{0,500}\.)",
        text,
        flags=re.IGNORECASE,
    ) or re.search(
        r"([^.]*(?:siguiente sesi[oó]n decisoria|pr[oó]xima sesi[oó]n)"
        r"[^.]{0,500}\.)",
        text,
        flags=re.IGNORECASE,
    ) or re.search(
        r"((?:Pr[oó]ximas reuniones|minutas, informes y presentaciones).{0,220})",
        text,
        flags=re.IGNORECASE,
    )

    bullets = [
        normalize_whitespace(li.get_text(separator=" ", strip=True))
        for li in container.find_all("li")
    ]
    bullets = [
        bullet
        for bullet in bullets
        if (
            len(bullet) >= 50
            and "inflaci" in fold_accents(bullet.lower())
        )
        or len(bullet) >= 80
    ][:BANREP_MINUTAS_BULLET_LIMIT]

    paragraphs = [
        normalize_whitespace(p.get_text(separator=" ", strip=True))
        for p in container.find_all("p")
    ]
    paragraphs = [paragraph for paragraph in paragraphs if len(paragraph) >= 80]
    board_blocs: dict[str, str] = {}
    for paragraph in paragraphs:
        paragraph_folded = fold_accents(paragraph.lower())
        if "el grupo mayoritario" in paragraph_folded:
            board_blocs.setdefault("majority", paragraph[:BANREP_MINUTAS_BLOC_CHARS])
        elif paragraph_folded.startswith(
            "los directores que votaron por una reduccion"
        ):
            board_blocs.setdefault(
                "rate_cut_bloc", paragraph[:BANREP_MINUTAS_BLOC_CHARS]
            )
        elif paragraph_folded.startswith(
            "el miembro de la junta que voto por mantener"
        ):
            board_blocs.setdefault(
                "hold_bloc", paragraph[:BANREP_MINUTAS_BLOC_CHARS]
            )
        elif re.match(r"un grupo de .{1,40}directores?", paragraph_folded):
            board_blocs.setdefault(
                "hawkish_bloc", paragraph[:BANREP_MINUTAS_BLOC_CHARS]
            )
        elif re.match(
            r"los (?:dos|tres|cuatro|cinco|\d+) directores? que abogan",
            paragraph_folded,
        ):
            board_blocs.setdefault(
                "dovish_bloc", paragraph[:BANREP_MINUTAS_BLOC_CHARS]
            )
        elif paragraph_folded.startswith("otro miembro de la junta"):
            board_blocs.setdefault(
                "single_member_bloc", paragraph[:BANREP_MINUTAS_BLOC_CHARS]
            )

    bloc_patterns = {
        "majority": r"(El grupo mayoritario[^.]+(?:\.[^.]+){0,3})",
        "rate_cut_bloc": (
            r"(Los directores que votaron por una reducci[oó]n[^.]+"
            r"(?:\.[^.]+){0,3})"
        ),
        "hold_bloc": (
            r"(El miembro de la Junta que vot[oó] por mantener[^.]+"
            r"(?:\.[^.]+){0,3})"
        ),
        "hawkish_bloc": r"(Un grupo de [^.]+directores?[^.]+(?:\.[^.]+){0,5})",
        "dovish_bloc": (
            r"(Los (?:dos|tres|cuatro|cinco|\d+) directores? que abogan[^.]+"
            r"(?:\.[^.]+){0,5})"
        ),
        "single_member_bloc": r"(Otro miembro de la Junta[^.]+(?:\.[^.]+){0,5})",
    }
    for key, pattern in bloc_patterns.items():
        if key in board_blocs:
            continue
        if match := re.search(pattern, text, flags=re.IGNORECASE):
            board_blocs[key] = normalize_whitespace(match.group(1))[
                :BANREP_MINUTAS_BLOC_CHARS
            ]

    metadata: dict[str, Any] = {
        "content_extraction": "banrep_minutas_html",
        "document_title": title,
        "body_excerpt": text[:BANREP_MINUTAS_BODY_CHARS],
    }
    if published_at:
        metadata["publication_date"] = published_at
    if rate_match:
        metadata["policy_rate_pct"] = rate_match.group(1).replace(",", ".")
    if change_match:
        decision_summary = normalize_whitespace(change_match.group(0))
        decision_folded = fold_accents(decision_summary.lower())
        metadata["decision_summary"] = decision_summary
        if any(term in decision_folded for term in ("incrementar", "aumentar")):
            metadata["decision_action"] = "hike"
        elif any(term in decision_folded for term in ("reducir", "disminuir")):
            metadata["decision_action"] = "cut"
        elif "mantener" in decision_folded:
            metadata["decision_action"] = "hold"
        bps_match = re.search(
            r"(\d{1,3})\s+puntos?\s+b[aá]sicos",
            decision_summary,
            flags=re.IGNORECASE,
        )
        if bps_match:
            metadata["rate_change_bps"] = int(bps_match.group(1))
    if vote_match:
        metadata["vote_summary"] = normalize_whitespace(vote_match.group(1))
    elif unanimity_match:
        metadata["vote_summary"] = normalize_whitespace(unanimity_match.group(0))
    if re.search(r"\bpor unanimidad\b", text, flags=re.IGNORECASE):
        metadata["vote_result"] = "unanimous"
    elif vote_match or re.search(r"\bmayor[ií]a\b", text, flags=re.IGNORECASE):
        metadata["vote_result"] = "majority"
    if bullets:
        metadata["key_bullets"] = bullets
    if board_blocs:
        metadata["board_blocs"] = board_blocs
    if next_match:
        metadata["next_meeting_context"] = normalize_whitespace(next_match.group(1))
    official_links = _extract_banrep_minutas_links(soup, base_url)
    if official_links:
        metadata["official_links"] = official_links

    if not any(
        key in metadata for key in ("vote_summary", "key_bullets", "board_blocs")
    ):
        return {}
    return metadata


def _enrich_banrep_minutas_html(
    items: list[RawItem],
    client: httpx.Client,
    max_items: int = BANREP_MINUTAS_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        if parsed_count >= max_items or not _is_banrep_minutas_item(item):
            enriched.append(item)
            continue
        try:
            response = _http_get(client, item.url)
            marker = _detect_bot_block(response.text)
            if marker:
                enriched.append(item)
                continue
            metadata = _extract_banrep_minutas_metadata(
                response.text,
                str(response.url),
            )
        except Exception:
            enriched.append(item)
            continue
        if not metadata:
            enriched.append(item)
            continue
        parsed_count += 1
        merged_metadata = dict(item.metadata)
        merged_metadata.update(metadata)
        raw_text = normalize_whitespace(
            f"{item.raw_text} BanRep minutas detail: {metadata['body_excerpt']}"
        )
        enriched.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at or metadata.get("publication_date"),
                raw_text=raw_text,
                metadata=merged_metadata,
            )
        )
    return enriched


def _enrich_dane_icoced_xlsx(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int | None = None,
) -> list[RawItem]:
    if max_items is not None and max_items >= 0:
        parse_limit = max_items
    else:
        parse_limit = len(items)
    enriched: list[RawItem] = []
    for idx, item in enumerate(items):
        metadata = dict(item.metadata)
        year = metadata.get("period_year")
        month = metadata.get("period_month")
        if (
            idx >= parse_limit
            or not isinstance(year, int)
            or not isinstance(month, int)
        ):
            enriched.append(item)
            continue
        try:
            response = _http_get(client, item.url)
            parsed = _parse_dane_icoced_xlsx(
                response.content,
                year=year,
                month=month,
            )
        except Exception as exc:  # noqa: BLE001 - keep source usable as link-level
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            continue
        if not parsed:
            metadata["content_extraction_error"] = "unable to parse ICOCED XLSX"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            continue
        metadata.update(
            {
                "content_extraction": "dane_icoced_xlsx",
                "headline_metrics": parsed["metrics"],
                "metric_sheets": parsed["sheets"],
            }
        )
        enriched.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=f"{item.title}. {parsed['headline']}",
                metadata=metadata,
            )
        )
    return enriched


_PDF_STREAM_RE = re.compile(rb"stream\r?\n(.*?)\r?\nendstream", re.DOTALL)
_PDF_LITERAL_RE = re.compile(rb"\((?:\\.|[^\\()])*\)")
_PDF_TEXT_OBJECT_RE = re.compile(rb"\bBT\b(.*?)\bET\b", re.DOTALL)
_PDF_TEXT_TOKEN_RE = re.compile(
    rb"\((?:\\.|[^\\()])*\)|<\s*(?:[0-9A-Fa-f]{2}\s*)+>"
)
_PDF_ACTUAL_TEXT_RE = re.compile(
    rb"/ActualText\s*(\((?:\\.|[^\\()])*\)|<\s*(?:[0-9A-Fa-f]{2}\s*)+>)"
)
_PDF_TEXT_ALLOWED_PUNCT = set(".,;:!?¿¡()[]{}%$#/+-_=°'\"@&\n\r\t ")
_PDF_TEXT_ALLOWED_NONASCII = set("áéíóúÁÉÍÓÚñÑüÜ")
_PDF_COMMON_SPANISH_TERMS = {
    "con",
    "de",
    "del",
    "el",
    "en",
    "la",
    "las",
    "los",
    "para",
    "por",
    "que",
    "una",
}


def _decode_pdf_literal(raw: bytes) -> str:
    body = raw[1:-1]
    out = bytearray()
    i = 0
    while i < len(body):
        ch = body[i]
        if ch != 0x5C:
            out.append(ch)
            i += 1
            continue
        i += 1
        if i >= len(body):
            break
        esc = body[i]
        if esc in b"nrtbf":
            out.append({ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12}[esc])
            i += 1
            continue
        if esc in b"()\\":
            out.append(esc)
            i += 1
            continue
        if esc in b"\r\n":
            i += 1
            if esc == 13 and i < len(body) and body[i] == 10:
                i += 1
            continue
        if 48 <= esc <= 55:
            digits = bytes([esc])
            i += 1
            for _ in range(2):
                if i < len(body) and 48 <= body[i] <= 55:
                    digits += bytes([body[i]])
                    i += 1
                else:
                    break
            out.append(int(digits, 8) & 0xFF)
            continue
        out.append(esc)
        i += 1
    return bytes(out).decode("latin-1", errors="ignore")


def _decode_pdf_cmap_hex(cleaned: str, cmap: dict[str, str]) -> str:
    if not cmap:
        return ""
    for width in (4, 2):
        if len(cleaned) % width:
            continue
        chars: list[str] = []
        hits = 0
        for i in range(0, len(cleaned), width):
            code = cleaned[i : i + width].upper()
            value = cmap.get(code)
            if value:
                chars.append(value)
                hits += 1
            else:
                chars.append(" ")
        if hits:
            return normalize_whitespace("".join(chars))
    return ""


def _decode_pdf_hex(raw: bytes, cmap: dict[str, str] | None = None) -> str:
    cleaned = re.sub(rb"\s+", b"", raw[1:-1])
    if not cleaned or len(cleaned) % 2:
        return ""
    cleaned_text = cleaned.decode("ascii", errors="ignore").upper()
    if cmap:
        decoded = _decode_pdf_cmap_hex(cleaned_text, cmap)
        if decoded:
            return decoded
    try:
        content = bytes.fromhex(cleaned_text)
    except ValueError:
        return ""
    if content.startswith(b"\xfe\xff"):
        return content[2:].decode("utf-16-be", errors="ignore")

    candidates = [
        content.decode("utf-16-be", errors="ignore"),
        content.decode("utf-8", errors="ignore"),
        content.decode("latin-1", errors="ignore"),
    ]
    candidates = [normalize_whitespace(candidate) for candidate in candidates]
    return max(candidates, key=_text_signal_score, default="")


def _decode_pdf_text_token(
    raw: bytes, cmap: dict[str, str] | None = None
) -> str:
    if raw.startswith(b"("):
        return _decode_pdf_literal(raw)
    if raw.startswith(b"<"):
        return _decode_pdf_hex(raw, cmap)
    return ""


def _text_signal_score(text: str) -> int:
    return sum(
        1
        for ch in text
        if (ch.isascii() and ch.isalpha()) or ch in _PDF_TEXT_ALLOWED_NONASCII
    )


def _looks_like_text_fragment(text: str) -> bool:
    normalized = normalize_whitespace(text)
    if not normalized or "\x00" in normalized:
        return False
    if _is_pdf_noise_fragment(normalized):
        return False
    useful = sum(
        1
        for ch in normalized
        if (
            (ch.isascii() and (ch.isalnum() or ch.isspace()))
            or ch in _PDF_TEXT_ALLOWED_NONASCII
            or ch in _PDF_TEXT_ALLOWED_PUNCT
        )
    )
    return useful / len(normalized) >= 0.85 and _text_signal_score(normalized) > 0


def _is_pdf_noise_fragment(text: str) -> bool:
    folded = fold_accents(text.lower())
    return (
        "identityadobe" in folded
        or "microsoft sans" in folded
        or "segoe ui" in folded
        or ("arial" in folded and "proyecto" not in folded)
        or ("calibri" in folded and "proyecto" not in folded)
    )


def _extract_pdf_cmap(chunks: list[bytes]) -> dict[str, str]:
    cmap: dict[str, str] = {}
    for chunk in chunks:
        text = chunk.decode("latin-1", errors="ignore")
        for block in re.findall(r"beginbfchar(.*?)endbfchar", text, flags=re.DOTALL):
            for source, target in re.findall(
                r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", block
            ):
                try:
                    decoded = bytes.fromhex(target).decode("utf-16-be", errors="ignore")
                except ValueError:
                    continue
                if decoded:
                    cmap[source.upper()] = decoded
        for block in re.findall(r"beginbfrange(.*?)endbfrange", text, flags=re.DOTALL):
            for start, end, target in re.findall(
                r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>",
                block,
            ):
                try:
                    start_i = int(start, 16)
                    end_i = int(end, 16)
                    target_i = int(target, 16)
                except ValueError:
                    continue
                width = len(start)
                if end_i - start_i > 256:
                    continue
                for offset, code in enumerate(range(start_i, end_i + 1)):
                    try:
                        decoded = chr(target_i + offset)
                    except ValueError:
                        continue
                    cmap[f"{code:0{width}X}"] = decoded
    return cmap


def _extract_pdf_operator_lines(
    chunk: bytes,
    cmap: dict[str, str] | None = None,
) -> list[str]:
    lines: list[str] = []
    for text_object in _PDF_TEXT_OBJECT_RE.findall(chunk):
        fragments: list[str] = []
        for token in _PDF_TEXT_TOKEN_RE.findall(text_object):
            fragment = _decode_pdf_text_token(token, cmap)
            if _looks_like_text_fragment(fragment):
                fragments.append(fragment)
        line = normalize_whitespace("".join(fragments))
        if _looks_like_text_fragment(line):
            lines.append(line)
    return lines


def _extract_pdf_actual_text(
    chunk: bytes,
    cmap: dict[str, str] | None = None,
) -> list[str]:
    fragments: list[str] = []
    for token in _PDF_ACTUAL_TEXT_RE.findall(chunk):
        fragment = normalize_whitespace(_decode_pdf_text_token(token, cmap))
        if _looks_like_text_fragment(fragment):
            fragments.append(fragment)
    return fragments


def _looks_like_text(text: str) -> bool:
    normalized = normalize_whitespace(text)
    if len(normalized) < 8:
        return False
    useful = sum(
        1
        for ch in normalized
        if (
            (ch.isascii() and (ch.isalnum() or ch.isspace()))
            or ch in _PDF_TEXT_ALLOWED_NONASCII
            or ch in _PDF_TEXT_ALLOWED_PUNCT
        )
    )
    letters = sum(
        1
        for ch in normalized
        if (ch.isascii() and ch.isalpha()) or ch in _PDF_TEXT_ALLOWED_NONASCII
    )
    return useful / len(normalized) >= 0.85 and letters / len(normalized) >= 0.35


def _looks_like_pdf_excerpt(text: str) -> bool:
    if len(text) < PDF_TEXT_MIN_CHARS:
        return False
    folded_tokens = set(re.findall(r"\w+", fold_accents(text.lower())))
    if len(folded_tokens & _PDF_COMMON_SPANISH_TERMS) < 3:
        return False
    if text.count("Identity") > 1 or text.count("Segoe UI") > 1:
        return False
    if "endstream" in text or text.count("\\") > 2:
        return False
    return True


def _extract_pdf_text(content: bytes, *, max_chars: int = PDF_TEXT_EXCERPT_CHARS) -> str:
    """Best-effort PDF text extraction using only the standard library.

    This intentionally handles only common text streams. It is not a full PDF
    parser, but it gives M1 a useful excerpt when official PDFs expose readable
    literal strings without adding another production dependency.
    """
    chunks: list[bytes] = []
    for match in _PDF_STREAM_RE.finditer(content[:PDF_TEXT_MAX_BYTES]):
        stream = match.group(1).strip(b"\r\n")
        try:
            chunks.append(zlib.decompress(stream))
        except zlib.error:
            chunks.append(stream)
    if not chunks:
        chunks.append(content[:PDF_TEXT_MAX_BYTES])

    primary_texts: list[str] = []
    fallback_texts: list[str] = []
    cmap = _extract_pdf_cmap(chunks)
    for chunk in chunks:
        primary_texts.extend(_extract_pdf_operator_lines(chunk, cmap))
        primary_texts.extend(_extract_pdf_actual_text(chunk, cmap))
        for literal in _PDF_LITERAL_RE.findall(chunk):
            text = _decode_pdf_literal(literal)
            if _looks_like_text(text):
                fallback_texts.append(text)

    excerpt = normalize_whitespace(" ".join(primary_texts))
    if _looks_like_pdf_excerpt(excerpt):
        return excerpt[:max_chars]

    excerpt = normalize_whitespace(" ".join([*primary_texts, *fallback_texts]))
    if not _looks_like_pdf_excerpt(excerpt):
        return ""
    return excerpt[:max_chars]


_SENADO_AGENDA_PROJECT_RE = re.compile(
    r"\bProyecto\s+de\s+(?P<kind>Ley|Acto\s+Legislativo)\s+No\.?\s+"
    r"(?P<first_number>\d{1,4})\s+(?:de|del)\s+"
    r"(?P<first_year>\d{4})\s+(?P<first_chamber>Senado|C[aá]mara)"
    r"(?:[,;\s]+(?P<second_number>\d{1,4})\s+(?:de|del)\s+"
    r"(?P<second_year>\d{4})\s+(?P<second_chamber>Senado|C[aá]mara))?",
    re.IGNORECASE,
)
_SENADO_AGENDA_LOOSE_PROJECT_RE = re.compile(
    r"\bProyecto\s+de\s+Ley\s+(?:No\.?\s*)?"
    r"(?P<chamber>Senado|C[aá]mara)?\s*(?P<body>.{24,320}?)(?="
    r"\b(?:Autores?|Ponente|Publicaci[oó]n|Proyecto\s+de\s+Ley|Hora|"
    r"Lugar|Transmisi[oó]n|TEMA:|$))",
    re.IGNORECASE,
)
_SENADO_AGENDA_DAY_RE = re.compile(
    r"\b(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)"
    r"\s+(\d{1,2})\s+de\s+"
    r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|"
    r"setiembre|octubre|noviembre|diciembre)"
    r"(?:\s+de\s+(\d{4}))?",
    re.IGNORECASE,
)
_SENADO_TITLE_REPAIR_PATTERNS = (
    (r"\belacual\b", "el cual"),
    (r"\bmodificael\b", "modifica el"),
    (r"\blaleydeyse\b", "la ley de y se"),
    (r"\blaley\b", "la ley"),
    (r"\bdeyse\b", "de y se"),
    (r"\botrasdisposiciones\b", "otras disposiciones"),
)
_SENADO_LOSSY_TITLE_MARKERS = (
    "elacual",
    "modificael",
    "laley",
    "deyse",
    "otrasdisposiciones",
)
_GACETAS_CONGRESO_URL = "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml"


def _normalize_senado_agenda_text_for_matching(text: str) -> str:
    normalized = normalize_whitespace(text)
    replacements = (
        (r"Pr\s*oyecto", "Proyecto"),
        (r"Proyec\s*to", "Proyecto"),
        (r"ProyectodeLeyNode", "Proyecto de Ley No. "),
        (r"ProyectodeLeyNo", "Proyecto de Ley No. "),
        (r"Proyectode\s*Ley\s*Node", "Proyecto de Ley No. "),
        (r"Proyecto\s*de\s*Ley\s*Node", "Proyecto de Ley No. "),
        (r"Proyectode\s*Ley", "Proyecto de Ley"),
        (r"Proyecto\s*deLey", "Proyecto de Ley"),
        (r"No\.?\s*SENADO", "No. Senado"),
        (r"No\.?\s*Senado", "No. Senado"),
        (r"No\.?\s*de\s*C[aá]mara", "No. Cámara"),
        (r"No\.?\s*C[aá]mara", "No. Cámara"),
        (r"deC[aá]mara", "de Cámara"),
        (r"delC[aá]mara", "de Cámara"),
        (r"delSenado", "de Senado"),
        (r"\bSENADO\b", "Senado"),
        (r"\bCÁMARA\b", "Cámara"),
        (r"Autores", " Autores"),
        (r"Autor:", " Autor:"),
        (r"Ponente", " Ponente"),
        (r"Publicaci", " Publicaci"),
        (r"Transmisión", " Transmisión"),
        (r"Hora", " Hora"),
        (r"Lugar", " Lugar"),
        (r"TEMA:", " TEMA:"),
    )
    for pattern, replacement in replacements:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    return normalize_whitespace(normalized)


def _year_from_iso(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value[:4])
    except ValueError:
        return None


def _senado_project_label(match: re.Match[str]) -> str:
    records = _senado_project_records(match)
    kind = records[0]["kind"] if records else normalize_whitespace(match.group("kind"))
    labels = [
        f"{record['number']} de {record['year']} {record['chamber']}"
        for record in records
    ]
    return f"Proyecto de {kind} {' / '.join(labels)}"


def _senado_project_records(match: re.Match[str]) -> list[dict[str, str]]:
    kind = normalize_whitespace(match.group("kind"))
    records = [
        {
            "kind": kind,
            "number": match.group("first_number"),
            "year": match.group("first_year"),
            "chamber": _normalize_senado_chamber(match.group("first_chamber")),
        }
    ]
    second_number = match.group("second_number")
    if second_number:
        records.append(
            {
                "kind": kind,
                "number": second_number,
                "year": match.group("second_year"),
                "chamber": _normalize_senado_chamber(match.group("second_chamber")),
            }
        )
    return records


def _normalize_senado_chamber(value: str | None) -> str:
    folded = fold_accents((value or "").lower())
    return "Cámara" if "camara" in folded else "Senado"


def _senado_agenda_action(context: str) -> str:
    folded = fold_accents(context.lower())
    if "primer debate" in folded:
        return "primer debate"
    if "segundo debate" in folded:
        return "segundo debate"
    if "tercer debate" in folded:
        return "tercer debate"
    if "cuarto debate" in folded:
        return "cuarto debate"
    if "ponencia" in folded:
        return "ponencia"
    if "discusion" in folded or "discusion" in folded:
        return "discusion"
    return "agenda legislativa"


def _senado_document_title(context: str) -> str:
    quote_match = re.search(r"[“\"]([^”\"]{24,220})[”\"]", context)
    if quote_match:
        return _repair_senado_document_title(quote_match.group(1))
    tema_match = re.search(
        r"\bTEMA:\s*(.{24,260}?)(?:\bAutores?:|\bPublicaci[oó]n|$)",
        context,
        re.IGNORECASE,
    )
    if tema_match:
        return _repair_senado_document_title(tema_match.group(1))
    return ""


def _senado_loose_document_title(body: str) -> str:
    title = normalize_whitespace(body)
    title = re.sub(
        r"^(?:de\s+)?(?:No\.\s*)?(?:Senado|C[aá]mara)\b",
        "",
        title,
        flags=re.IGNORECASE,
    )
    title = re.sub(r"\bla\s*G\s*aceta\s*No.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(
        r"\b(?:HH\.?|H\.?\s*S\.?|H\.?\s*R\.?|Dr\.?)\b.*$",
        "",
        title,
        flags=re.IGNORECASE,
    )
    title = re.split(
        r"\b(?:Autor(?:es)?|Ponente(?:s)?|Publicaci[oó]n|Proyecto\s+de\s+Ley)\b",
        title,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return _repair_senado_document_title(title)[:220]


def _repair_senado_document_title(title: str) -> str:
    repaired = normalize_whitespace(title).strip(" .,:;-")
    for pattern, replacement in _SENADO_TITLE_REPAIR_PATTERNS:
        repaired = re.sub(pattern, replacement, repaired, flags=re.IGNORECASE)
    return normalize_whitespace(repaired).strip(" .,:;-")


def _senado_title_has_lossy_spacing(title: str) -> bool:
    folded = fold_accents(title.lower())
    return any(marker in folded for marker in _SENADO_LOSSY_TITLE_MARKERS)


def _senado_project_identity_status(
    label: str,
    document_title: str,
    *,
    original_title: str = "",
) -> str:
    has_project_number = bool(
        re.search(
            r"\b\d{1,4}\s+de\s+\d{4}\s+(?:Senado|C[aá]mara)\b",
            label,
            flags=re.IGNORECASE,
        )
    )
    if not has_project_number:
        return "missing_project_number"
    if len(document_title) < 24:
        return "missing_document_title"
    if _senado_title_has_lossy_spacing(original_title or document_title):
        return "lossy_document_title"
    return "clean_project_identity"


def _senado_follow_up_sources(
    agenda_item: RawItem,
    project_label: str,
) -> list[dict[str, str]]:
    search_hint = project_label
    return [
        {
            "source_id": "gacetas_congreso",
            "source_name": "Gacetas del Congreso — Imprenta Nacional",
            "url": _GACETAS_CONGRESO_URL,
            "search_hint": search_hint,
            "purpose": "Find ponencia, bill text, and later official publication records.",
        },
        {
            "source_id": "senado_agenda_legislativa",
            "source_name": agenda_item.source_name,
            "url": agenda_item.url,
            "search_hint": search_hint,
            "purpose": "Check whether the item reappears or advances in a later agenda window.",
        },
    ]


def _looks_like_senado_public_interest_title(title: str) -> bool:
    folded = fold_accents(title.lower())
    return any(
        term in folded
        for term in (
            "adopta",
            "aduanera",
            "codigo",
            "crea",
            "declara",
            "dictan",
            "establece",
            "expide",
            "modifica",
            "programa",
            "promueve",
            "reforma",
            "regimen",
            "salud",
            "sancionatorio",
            "sistema",
            "tributario",
            "violencia",
        )
    )


def _senado_scheduled_date(
    text: str, position: int, default_year: int | None
) -> str | None:
    latest: re.Match[str] | None = None
    for match in _SENADO_AGENDA_DAY_RE.finditer(text[:position]):
        latest = match
    if latest is None:
        return None
    year_text = latest.group(3)
    year = int(year_text) if year_text else default_year
    month = MONTHS_ES.get(fold_accents(latest.group(2).lower()))
    if year is None or month is None:
        return None
    return _date_to_iso(year, month, int(latest.group(1)))


def _extract_senado_agenda_entries_from_text(
    agenda_item: RawItem,
    text: str,
    *,
    max_entries: int = SENADO_AGENDA_ENTRY_LIMIT,
) -> list[RawItem]:
    match_text = _normalize_senado_agenda_text_for_matching(text)
    default_year = _year_from_iso(agenda_item.published_at) or _year_from_iso(
        _parse_date_text_to_iso(agenda_item.title)
    )
    entries: list[RawItem] = []
    seen_labels: set[str] = set()
    detailed_positions: list[int] = []
    for match in _SENADO_AGENDA_PROJECT_RE.finditer(match_text):
        label = _senado_project_label(match)
        project_records = _senado_project_records(match)
        if label in seen_labels:
            continue
        seen_labels.add(label)
        detailed_positions.append(match.start())
        context_start = max(0, match.start() - 180)
        context_end = min(len(text), match.end() + 520)
        context = normalize_whitespace(match_text[context_start:context_end])
        scheduled_at = _senado_scheduled_date(match_text, match.start(), default_year)
        action = _senado_agenda_action(context)
        document_title = _senado_document_title(context)
        original_document_title = normalize_whitespace(document_title)
        identity_status = _senado_project_identity_status(
            label,
            document_title,
            original_title=original_document_title,
        )
        agenda_start = agenda_item.published_at
        date_label = (scheduled_at or agenda_start or "")[:10] or "agenda window"
        title = f"Senado agenda {date_label} — {action}: {label}"
        if document_title:
            title = f"{title} — {document_title[:180]}"
        entry_url = f"{agenda_item.url}#project-{len(entries) + 1}"
        raw_text = (
            f"{title}. Extracted from official Senado agenda PDF. "
            f"Agenda excerpt: {context}. Follow-up sources: Gacetas del Congreso "
            f"and later Congreso/Senado agenda records; search hint: {label}."
        )
        entries.append(
            RawItem(
                id=_make_id(agenda_item.source_id, entry_url, title),
                source_id=agenda_item.source_id,
                source_name=agenda_item.source_name,
                source_type=agenda_item.source_type,
                url=entry_url,
                title=title,
                fetched_at=agenda_item.fetched_at,
                published_at=scheduled_at or agenda_item.published_at,
                raw_text=raw_text,
                metadata={
                    **dict(agenda_item.metadata),
                    "extraction": "senado_agenda_pdf_entry",
                    "content_extraction": "senado_agenda_pdf",
                    "agenda_source_url": agenda_item.url,
                    "agenda_title": agenda_item.title,
                    "agenda_window_start": agenda_start,
                    "scheduled_date": scheduled_at,
                    "agenda_action_type": action,
                    "project_label": label,
                    "project_records": project_records,
                    "document_title": document_title,
                    "project_identity_status": identity_status,
                    "has_clean_project_identity": (
                        identity_status == "clean_project_identity"
                    ),
                    "follow_up_sources": _senado_follow_up_sources(
                        agenda_item,
                        label,
                    ),
                    "pdf_text_chars": len(text),
                },
            )
        )
        if len(entries) >= max_entries:
            break

    for match in _SENADO_AGENDA_LOOSE_PROJECT_RE.finditer(match_text):
        if len(entries) >= max_entries:
            break
        if any(abs(match.start() - position) < 80 for position in detailed_positions):
            continue
        body = normalize_whitespace(match.group("body"))
        if re.match(r"\d{1,4}\s+(?:de|del)\s+\d{4}\b", body, re.IGNORECASE):
            continue
        raw_document_title = _senado_loose_document_title(body)
        document_title = _repair_senado_document_title(raw_document_title)
        if len(document_title) < 20 or not _looks_like_senado_public_interest_title(
            document_title
        ):
            continue
        chamber = normalize_whitespace(match.group("chamber") or "Senado")
        label = f"Proyecto de Ley {chamber}"
        identity_status = _senado_project_identity_status(
            label,
            document_title,
            original_title=raw_document_title,
        )
        seen_key = f"{label}|{document_title[:80]}"
        if seen_key in seen_labels:
            continue
        seen_labels.add(seen_key)
        context_start = max(0, match.start() - 180)
        context_end = min(len(match_text), match.end() + 240)
        context = normalize_whitespace(match_text[context_start:context_end])
        scheduled_at = _senado_scheduled_date(match_text, match.start(), default_year)
        action = _senado_agenda_action(context)
        agenda_start = agenda_item.published_at
        date_label = (scheduled_at or agenda_start or "")[:10] or "agenda window"
        title = (
            f"Senado agenda {date_label} — {action}: {label} — "
            f"{document_title}"
        )
        entry_url = f"{agenda_item.url}#project-{len(entries) + 1}"
        raw_text = (
            f"{title}. Extracted from official Senado agenda PDF. "
            f"Agenda excerpt: {context}. Follow-up sources: Gacetas del Congreso "
            f"and later Congreso/Senado agenda records; search hint: {label}."
        )
        entries.append(
            RawItem(
                id=_make_id(agenda_item.source_id, entry_url, title),
                source_id=agenda_item.source_id,
                source_name=agenda_item.source_name,
                source_type=agenda_item.source_type,
                url=entry_url,
                title=title,
                fetched_at=agenda_item.fetched_at,
                published_at=scheduled_at or agenda_item.published_at,
                raw_text=raw_text,
                metadata={
                    **dict(agenda_item.metadata),
                    "extraction": "senado_agenda_pdf_entry",
                    "content_extraction": "senado_agenda_pdf",
                    "agenda_source_url": agenda_item.url,
                    "agenda_title": agenda_item.title,
                    "agenda_window_start": agenda_start,
                    "scheduled_date": scheduled_at,
                    "agenda_action_type": action,
                    "project_label": label,
                    "document_title": document_title,
                    "project_records": [],
                    "project_identity_status": identity_status,
                    "has_clean_project_identity": False,
                    "follow_up_sources": _senado_follow_up_sources(
                        agenda_item,
                        label,
                    ),
                    "pdf_text_chars": len(text),
                },
            )
        )
    return entries


def _is_senado_agenda_pdf_item(item: RawItem) -> bool:
    title = fold_accents(item.title.lower())
    return "agenda legislativa" in title and (
        "pdf" in title or item.url.endswith("/file")
    )


def _enrich_senado_agenda_pdfs(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int = SENADO_AGENDA_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        if not _is_senado_agenda_pdf_item(item) or parsed_count >= max_items:
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            text = _extract_pdf_text(response.content, max_chars=PDF_TEXT_FULL_CHARS)
        except Exception as exc:  # noqa: BLE001 - preserve link-level item
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        entries = _extract_senado_agenda_entries_from_text(item, text)
        if entries:
            enriched.extend(entries)
        else:
            metadata.update(
                {
                    "content_extraction_error": "no legislative project entries found",
                    "pdf_text_chars": len(text),
                }
            )
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
        parsed_count += 1
    return enriched


def _enrich_pdf_text(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int = PDF_TEXT_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        path = urlsplit(item.url).path.lower()
        if parsed_count >= max_items or ".pdf" not in path:
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            excerpt = _extract_pdf_text(response.content)
        except Exception as exc:  # noqa: BLE001 - preserve link-level item
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        if len(excerpt) < PDF_TEXT_MIN_CHARS:
            metadata["content_extraction_error"] = "pdf text excerpt too short"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        metadata.update(
            {
                "content_extraction": "pdf_text_best_effort",
                "pdf_text_chars": len(excerpt),
            }
        )
        enriched.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=f"{item.raw_text} PDF text excerpt: {excerpt}",
                metadata=metadata,
            )
        )
        parsed_count += 1
    return enriched


def _extract_pdf_text_with_pdfplumber(
    content: bytes,
    *,
    max_chars: int = PDF_TEXT_FULL_CHARS,
) -> str:
    """Extract PDF text with layout when pdfplumber is available.

    MinHacienda TES auction reports are numeric tables; preserving spaces and
    line breaks is materially better than the generic no-dependency extractor.
    The fallback keeps the source fail-closed in environments that have not
    synced the dependency yet.
    """
    try:
        import pdfplumber
    except ImportError:
        return _extract_pdf_text_objects_text(content, max_chars=max_chars)

    pages: list[str] = []
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(layout=True) or page.extract_text() or ""
                if page_text:
                    pages.append(page_text)
    except Exception:  # noqa: BLE001 - keep best-effort PDF extraction fail-closed
        return _extract_pdf_text_objects_text(content, max_chars=max_chars)
    return "\n".join(pages)[:max_chars]


def _money_text_to_cop_billions(text: str | None) -> float | None:
    if not text:
        return None
    match = re.search(
        r"\$?\s*([\d.,]+)\s*(billones|mil\s+millones)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    value = _to_float(match.group(1))
    if value is None:
        return None
    unit = fold_accents(match.group(2).lower())
    if "mil millones" in unit:
        value = value / 1000
    return round(value, 3)


def _tes_decimal(text: str | None) -> float | None:
    if text is None:
        return None
    cleaned = text.strip().replace(",", ".")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _money_amounts_to_cop_billions(text: str) -> list[float]:
    amounts: list[float] = []
    for match in re.finditer(
        r"\$\s*[\d.,]+\s*(?:billones|mil\s+millones)",
        text,
        flags=re.IGNORECASE,
    ):
        value = _money_text_to_cop_billions(match.group(0))
        if value is not None:
            amounts.append(value)
    return amounts


def _large_cop_amounts_to_billions(text: str) -> list[float]:
    values: list[float] = []
    for match in re.finditer(r"\b\d{1,3}(?:\.\d{3}){2,}\b", text):
        amount = int(match.group(0).replace(".", ""))
        values.append(round(amount / 1_000_000_000_000, 3))
    return values


def _pct_values(text: str) -> list[float]:
    values: list[float] = []
    for match in re.finditer(r"\b(\d{1,2}(?:[,.]\d{1,3})?)\s*%", text):
        value = _tes_decimal(match.group(1))
        if value is not None:
            values.append(value)
    return values


def _minhacienda_tes_row_segment(
    normalized: str,
    label: str,
    next_label: str,
) -> str:
    match = re.search(
        rf"{label}\s+(.+?)\s+{next_label}",
        normalized,
        flags=re.IGNORECASE,
    )
    return normalize_whitespace(match.group(1)) if match else ""


def _extract_minhacienda_tes_auction_rows_from_text(
    text: str,
) -> list[dict[str, Any]]:
    normalized = normalize_whitespace(text)
    tenors = [
        int(match.group(1))
        for match in re.finditer(
            r"\b(\d{1,2})\s+a[ñn]os?\b",
            _minhacienda_tes_row_segment(
                normalized,
                r"Plazo\s+al\s+vencimiento",
                r"Fecha\s+de\s+Vencimiento",
            ),
            flags=re.IGNORECASE,
        )
    ]
    maturity_dates = re.findall(
        r"\b\d{1,2}-[a-z]{3}-\d{2}\b",
        _minhacienda_tes_row_segment(
            normalized,
            r"Fecha\s+de\s+Vencimiento",
            r"Tasa\s+cup[oó]n",
        ),
        flags=re.IGNORECASE,
    )
    coupon_rates = _pct_values(
        _minhacienda_tes_row_segment(
            normalized,
            r"Tasa\s+cup[oó]n",
            r"Tasa\s+de\s+corte",
        )
    )
    cutoff_rates = _pct_values(
        _minhacienda_tes_row_segment(
            normalized,
            r"Tasa\s+de\s+corte",
            r"Ofertas\s+Recibidas",
        )
    )
    demand = _money_amounts_to_cop_billions(
        _minhacienda_tes_row_segment(
            normalized,
            r"Ofertas\s+Recibidas",
            r"Monto\s+Aprobado",
        )
    )
    approved = _money_amounts_to_cop_billions(
        _minhacienda_tes_row_segment(
            normalized,
            r"Monto\s+Aprobado",
            r"(?:\(Fin\)|$)",
        )
    )

    row_count = min(
        len(tenors),
        len(maturity_dates),
        len(coupon_rates),
        len(cutoff_rates),
        len(demand),
        len(approved),
    )
    rows: list[dict[str, Any]] = []
    for index in range(row_count):
        maturity_date = maturity_dates[index]
        maturity_year = 2000 + int(maturity_date.rsplit("-", 1)[-1])
        rows.append(
            {
                "tenor_years": tenors[index],
                "maturity_date": maturity_date,
                "maturity_year": maturity_year,
                "coupon_rate_pct": coupon_rates[index],
                "cutoff_rate_pct": cutoff_rates[index],
                "demand_cop_billions": demand[index],
                "approved_cop_billions": approved[index],
            }
        )
    return rows


def _extract_irc_tes_auction_rows_from_text(text: str) -> list[dict[str, Any]]:
    rate_rows: dict[str, dict[str, Any]] = {}
    amount_rows: dict[str, dict[str, Any]] = {}
    in_amounts = False
    for line in text.splitlines():
        normalized = normalize_whitespace(line)
        if not normalized:
            continue
        if "MONTOS" in normalized:
            in_amounts = True
            continue
        maturity_match = re.match(r"^(\d{1,2}-[a-z]{3}-\d{2})\s+(.+)$", normalized)
        if not maturity_match:
            continue
        maturity_date = maturity_match.group(1)
        remainder = maturity_match.group(2)
        maturity_year = 2000 + int(maturity_date.rsplit("-", 1)[-1])
        if not in_amounts:
            percent_values = _pct_values(remainder)
            if len(percent_values) < 4:
                continue
            tenor_match = re.match(r"(?P<tenor>\d+)(?P<unit>[YD]?)\b", remainder)
            row: dict[str, Any] = {
                "maturity_date": maturity_date,
                "maturity_year": maturity_year,
                "cutoff_rate_pct": percent_values[3],
            }
            if tenor_match:
                unit = tenor_match.group("unit")
                tenor = int(tenor_match.group("tenor"))
                if unit == "D":
                    row["tenor_days"] = tenor
                else:
                    row["tenor_years"] = tenor
            if len(percent_values) >= 7:
                row["coupon_rate_pct"] = percent_values[6]
            rate_rows[maturity_date] = row
            continue
        amounts = _large_cop_amounts_to_billions(remainder)
        if len(amounts) >= 4:
            amount_rows[maturity_date] = {
                "demand_cop_billions": amounts[1],
                "approved_cop_billions": amounts[3],
                "offered_nominal_cop_billions": amounts[0],
                "approved_nominal_cop_billions": amounts[2],
            }
    rows: list[dict[str, Any]] = []
    for maturity_date, row in rate_rows.items():
        amount_row = amount_rows.get(maturity_date)
        if amount_row:
            row = {**row, **amount_row}
        rows.append(row)
    return rows


def _extract_irc_tes_total_facts(text: str) -> dict[str, Any]:
    lines = [normalize_whitespace(line) for line in text.splitlines()]
    for line in lines:
        if not line.startswith("TOTAL"):
            continue
        amounts = _large_cop_amounts_to_billions(line)
        bid_to_cover_values = [
            value
            for value in re.findall(r"\b\d{1,2},\d\b", line)
            if value not in {"00,0", "10,0"}
        ]
        if len(amounts) >= 4:
            return {
                "total_demand_cop_billions": amounts[1],
                "total_issued_cop_billions": amounts[3],
                "total_demand_nominal_cop_billions": amounts[0],
                "total_issued_nominal_cop_billions": amounts[2],
                "bid_to_cover": _to_float(bid_to_cover_values[-1])
                if bid_to_cover_values
                else None,
            }
    for line in lines:
        maturity_match = re.match(r"^\d{1,2}-[a-z]{3}-\d{2}\s+(.+)$", line)
        if not maturity_match:
            continue
        amounts = _large_cop_amounts_to_billions(maturity_match.group(1))
        bid_to_cover_match = re.search(r"\b(\d{1,2},\d)\b\s*$", line)
        if len(amounts) >= 4 and bid_to_cover_match:
            return {
                "total_demand_cop_billions": amounts[1],
                "total_issued_cop_billions": amounts[3],
                "total_demand_nominal_cop_billions": amounts[0],
                "total_issued_nominal_cop_billions": amounts[2],
                "bid_to_cover": _to_float(bid_to_cover_match.group(1)),
            }
    return {}


def _extract_irc_tes_auction_facts(
    text: str,
    *,
    title: str,
    pdf_url: str,
) -> dict[str, Any] | None:
    rows = _extract_irc_tes_auction_rows_from_text(text)
    if not rows or not all(row.get("cutoff_rate_pct") for row in rows):
        return None
    title_match = re.search(
        r"\bSubasta\s+0*(\d+)\s+(COP|UVR|TCO)\b",
        title,
        flags=re.IGNORECASE,
    )
    auction_number = title_match.group(1) if title_match else ""
    auction_type = title_match.group(2).upper() if title_match else "TES"
    auction_date = _parse_date_text_to_iso(title) or _parse_date_text_to_iso(text)
    totals = _extract_irc_tes_total_facts(text)
    total_issued = totals.get("total_issued_cop_billions")
    total_demand = totals.get("total_demand_cop_billions")
    bid_to_cover = totals.get("bid_to_cover")
    if auction_date is None or total_issued is None or total_demand is None:
        return None
    longest = max(rows, key=lambda row: int(row["maturity_year"]))
    max_cutoff = max(float(row["cutoff_rate_pct"]) for row in rows)
    return {
        "content_extraction": "minhacienda_tes_auction_pdf",
        "auction_date": auction_date,
        "auction_type": auction_type,
        "auction_number": auction_number,
        "currency": "COP" if auction_type in {"COP", "TCO"} else auction_type,
        "security_type": "TES",
        "total_issued_cop_billions": total_issued,
        "total_demand_cop_billions": total_demand,
        "bid_to_cover": bid_to_cover,
        "maturity_rows": rows,
        "maturity_years": [row["maturity_year"] for row in rows],
        "max_cutoff_rate_pct": round(max_cutoff, 3),
        "long_cutoff_rate_pct": longest["cutoff_rate_pct"],
        "long_maturity_year": longest["maturity_year"],
        "source_pdf_url": pdf_url,
        "pdf_text_chars": len(text),
        **{key: value for key, value in totals.items() if value is not None},
    }


def _extract_minhacienda_tes_auction_facts(
    text: str,
    *,
    title: str,
    pdf_url: str,
) -> dict[str, Any] | None:
    if "RESUMEN SUBASTA TES" in text.upper():
        return _extract_irc_tes_auction_facts(text, title=title, pdf_url=pdf_url)
    rows = _extract_minhacienda_tes_auction_rows_from_text(text)
    if not rows or not all(row.get("cutoff_rate_pct") for row in rows):
        return None

    normalized = normalize_whitespace(text)
    folded = fold_accents(normalized.lower())
    title_match = re.search(
        r"Informe\s+TES\s+subasta\s+([A-Z]+)\s+No\.?\s*(\d+)",
        title,
        flags=re.IGNORECASE,
    )
    auction_type = title_match.group(1).upper() if title_match else "TES"
    auction_number = title_match.group(2) if title_match else ""

    issued_match = re.search(
        r"(?:emitio|emitio hoy|subasto hoy).*?(\$\s*[\d.,]+\s*(?:billones|mil\s+millones))",
        folded,
        flags=re.IGNORECASE,
    )
    demand_match = re.search(
        r"(?:ordenes|ofertas)\s+de\s+compra\s+por\s+"
        r"(\$\s*[\d.,]+\s*(?:billones|mil\s+millones))",
        folded,
        flags=re.IGNORECASE,
    )
    bid_to_cover_match = re.search(
        r"\b(\d{1,2}(?:[,.]\d+)?)\s+veces\b",
        folded,
        flags=re.IGNORECASE,
    )
    auction_date = _parse_date_text_to_iso(normalized)
    total_issued = _money_text_to_cop_billions(
        issued_match.group(1) if issued_match else None
    )
    total_demand = _money_text_to_cop_billions(
        demand_match.group(1) if demand_match else None
    )
    bid_to_cover = _to_float(bid_to_cover_match.group(1)) if bid_to_cover_match else None
    if (
        auction_date is None
        or total_issued is None
        or total_demand is None
        or bid_to_cover is None
    ):
        return None
    longest = max(rows, key=lambda row: int(row["maturity_year"]))
    max_cutoff = max(float(row["cutoff_rate_pct"]) for row in rows)
    return {
        "content_extraction": "minhacienda_tes_auction_pdf",
        "auction_date": auction_date,
        "auction_type": auction_type,
        "auction_number": auction_number,
        "currency": "COP" if auction_type == "COP" else auction_type,
        "security_type": "TES",
        "total_issued_cop_billions": total_issued,
        "total_demand_cop_billions": total_demand,
        "bid_to_cover": bid_to_cover,
        "maturity_rows": rows,
        "maturity_years": [row["maturity_year"] for row in rows],
        "max_cutoff_rate_pct": round(max_cutoff, 3),
        "long_cutoff_rate_pct": longest["cutoff_rate_pct"],
        "long_maturity_year": longest["maturity_year"],
        "source_pdf_url": pdf_url,
        "pdf_text_chars": len(text),
    }


def _minhacienda_tes_pdf_url(url: str, *, title: str = "", base_url: str = "") -> str:
    if "download=" in url:
        return url
    if "/document_library/" in url or "view_file" in url:
        slug = _minhacienda_tes_title_slug(title)
        if slug:
            return urljoin(base_url or url, f"/documents/d/portal/{slug}?download=true")
    if "/documents/" in url:
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}download=true"
    return url


def _minhacienda_tes_item_with_facts(
    item: RawItem,
    facts: dict[str, Any],
) -> RawItem:
    metadata = dict(item.metadata)
    metadata.update(facts)
    issued = facts.get("total_issued_cop_billions")
    demand = facts.get("total_demand_cop_billions")
    btc = facts.get("bid_to_cover")
    maturities = "/".join(str(year) for year in facts["maturity_years"])
    raw_text = (
        f"{item.title}. Official MinHacienda TES auction report. "
        f"Auction date: {(facts.get('auction_date') or '')[:10] or 'unknown'}; "
        f"type: TES {facts.get('currency')}; issued: COP {issued} billones; "
        f"demand: COP {demand} billones; bid-to-cover: {btc}x; "
        f"maturities: {maturities}; max cutoff rate: "
        f"{facts.get('max_cutoff_rate_pct')}%; long cutoff rate: "
        f"{facts.get('long_cutoff_rate_pct')}% for {facts.get('long_maturity_year')}. "
        f"Source PDF: {item.url}."
    )
    return RawItem(
        id=item.id,
        source_id=item.source_id,
        source_name=item.source_name,
        source_type=item.source_type,
        url=item.url,
        title=item.title,
        fetched_at=item.fetched_at,
        published_at=facts.get("auction_date") or item.published_at,
        raw_text=raw_text,
        metadata=metadata,
    )


def _extract_minhacienda_tes_reports(
    html_text: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    if "irc.gov.co" in base_url:
        return _extract_irc_tes_reports(html_text, base_url, source, fetched_at)
    soup = BeautifulSoup(html_text, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        title = normalize_whitespace(link.get_text(" ", strip=True))
        if "informe tes subasta" not in fold_accents(title.lower()):
            continue
        resolved = _minhacienda_tes_pdf_url(
            urljoin(base_url, link["href"].strip()),
            title=title,
            base_url=base_url,
        )
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        row = link.find_parent(["tr", "li", "article", "div"]) or link
        row_text = normalize_whitespace(row.get_text(" ", strip=True))
        items.append(
            RawItem(
                id=_make_id(source.id, resolved, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=resolved,
                title=title,
                fetched_at=fetched_at,
                published_at=_parse_date_text_to_iso(row_text),
                raw_text=row_text or title,
                metadata={"extraction": "minhacienda_tes_report_index"},
            )
        )
        if len(items) >= (source.max_items or ANCHORS_PER_SOURCE):
            break
    return items


def _extract_irc_tes_reports(
    html_text: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    soup = BeautifulSoup(html_text, "html.parser")
    title_links: list[tuple[str, str, str]] = []
    for link in soup.find_all("a", href=True):
        title = normalize_whitespace(link.get_text(" ", strip=True))
        if not re.search(r"\bSubasta\s+\d+\s+(?:COP|UVR|TCO)\b", title):
            continue
        row = link.find_parent(["tr", "li", "article", "div"]) or link
        row_text = normalize_whitespace(row.get_text(" ", strip=True))
        title_links.append((title, urljoin(base_url, link["href"].strip()), row_text))

    download_links = [
        urljoin(base_url, link["href"].strip())
        for link in soup.find_all("a", href=True)
        if "/documents/d/guest/" in link["href"] and "download=true" in link["href"]
    ]
    items: list[RawItem] = []
    seen: set[str] = set()
    for index, (title, detail_url, row_text) in enumerate(title_links):
        url = download_links[index] if index < len(download_links) else detail_url
        canon = canonicalize_url(url)
        if canon in seen:
            continue
        seen.add(canon)
        items.append(
            RawItem(
                id=_make_id(source.id, url, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=url,
                title=title,
                fetched_at=fetched_at,
                published_at=_parse_date_text_to_iso(title) or _parse_date_text_to_iso(row_text),
                raw_text=row_text or title,
                metadata={
                    "extraction": "irc_tes_auction_index",
                    "detail_url": detail_url,
                },
            )
        )
        if len(items) >= (source.max_items or ANCHORS_PER_SOURCE):
            break
    return items


def _enrich_minhacienda_tes_reports(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int = MINHACIENDA_TES_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        metadata = dict(item.metadata)
        if parsed_count >= max_items:
            enriched.append(item)
            continue
        try:
            response = _http_get(client, item.url)
            text = _extract_pdf_text_with_pdfplumber(
                response.content,
                max_chars=MINHACIENDA_TES_TEXT_MAX_CHARS,
            )
            facts = _extract_minhacienda_tes_auction_facts(
                text,
                title=item.title,
                pdf_url=item.url,
            )
        except Exception as exc:  # noqa: BLE001 - preserve link-level item
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        if facts is None:
            metadata.update(
                {
                    "content_extraction_error": "unable to parse TES auction table/rates",
                    "pdf_text_chars": len(text),
                }
            )
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        enriched.append(_minhacienda_tes_item_with_facts(item, facts))
        parsed_count += 1
    return enriched


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


def _fetch_minhacienda_tes_reports_with_browser(
    source: Metasource,
    fetched_at: str,
    *,
    max_items: int = MINHACIENDA_TES_PARSE_LIMIT,
) -> list[RawItem]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise DynamicShellError(
            "MinHacienda TES is Radware-protected and requires the optional "
            "Playwright browser fetch path."
        ) from exc

    enriched: list[RawItem] = []
    with sync_playwright() as playwright:
        launch_kwargs: dict[str, Any] = {"headless": True}
        chrome_path = _chrome_executable_path()
        if chrome_path:
            launch_kwargs["executable_path"] = chrome_path
        browser = playwright.chromium.launch(**launch_kwargs)
        try:
            context = browser.new_context()
            page = context.new_page()
            page_urls = [source.url]
            if "irc.gov.co" in source.url:
                page_urls = list(dict.fromkeys([*MINHACIENDA_TES_IRC_PAGES, source.url]))
            per_page_limit = (
                max(1, max_items // len(page_urls))
                if "irc.gov.co" in source.url
                else max_items
            )
            items: list[RawItem] = []
            for page_url in page_urls:
                page.goto(
                    page_url,
                    wait_until="domcontentloaded",
                    timeout=MINHACIENDA_TES_BROWSER_TIMEOUT_MS,
                )
                try:
                    page.wait_for_load_state(
                        "networkidle",
                        timeout=MINHACIENDA_TES_BROWSER_NETWORK_IDLE_MS,
                    )
                except PlaywrightTimeoutError:
                    pass
                html_text = page.content()
                marker = _detect_bot_block(html_text)
                if marker:
                    raise BotBlockError(f"browser fetch still bot-blocked: {marker}")
                items.extend(
                    _extract_minhacienda_tes_reports(
                        html_text,
                        page.url,
                        source,
                        fetched_at,
                    )[:per_page_limit]
                )
                if len(items) >= max_items:
                    break
            items = items[:max_items]
            for item in items:
                metadata = dict(item.metadata)
                try:
                    response = context.request.get(
                        item.url,
                        timeout=MINHACIENDA_TES_BROWSER_TIMEOUT_MS,
                    )
                    if not response.ok:
                        raise httpx.HTTPStatusError(
                            f"browser download returned {response.status}",
                            request=httpx.Request("GET", item.url),
                            response=httpx.Response(response.status),
                        )
                    text = _extract_pdf_text_with_pdfplumber(
                        response.body(),
                        max_chars=MINHACIENDA_TES_TEXT_MAX_CHARS,
                    )
                    facts = _extract_minhacienda_tes_auction_facts(
                        text,
                        title=item.title,
                        pdf_url=item.url,
                    )
                except Exception as exc:  # noqa: BLE001 - preserve link-level item
                    metadata["content_extraction_error"] = (
                        f"browser_pdf_download: {exc.__class__.__name__}: {exc}"
                    )
                    enriched.append(
                        RawItem(
                            id=item.id,
                            source_id=item.source_id,
                            source_name=item.source_name,
                            source_type=item.source_type,
                            url=item.url,
                            title=item.title,
                            fetched_at=item.fetched_at,
                            published_at=item.published_at,
                            raw_text=item.raw_text,
                            metadata=metadata,
                        )
                    )
                    continue
                if facts is None:
                    metadata.update(
                        {
                            "content_extraction_error": (
                                "unable to parse TES auction table/rates"
                            ),
                            "pdf_text_chars": len(text),
                        }
                    )
                    enriched.append(
                        RawItem(
                            id=item.id,
                            source_id=item.source_id,
                            source_name=item.source_name,
                            source_type=item.source_type,
                            url=item.url,
                            title=item.title,
                            fetched_at=item.fetched_at,
                            published_at=item.published_at,
                            raw_text=item.raw_text,
                            metadata=metadata,
                        )
                    )
                    continue
                enriched.append(_minhacienda_tes_item_with_facts(item, facts))
        finally:
            browser.close()
    return enriched


MINCIT_ZF_APPROVED_REGISTRY = "mincit_zonas_francas_aprobadas"
MINCIT_ZF_APPROVED_EXTRACTION = "mincit_zonas_francas_approved_pdf"
MINCIT_ZF_TEXT_MAX_CHARS = 120_000
_MINCIT_ZF_APPROVED_TITLE_RE = re.compile(
    r"\bzonas\s+francas\s+aprobadas\b",
    re.IGNORECASE,
)
_MINCIT_ZF_SNAPSHOT_DATE_RE = re.compile(
    r"\bFECHA\s*:?\s*(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+DE\s+(\d{4})\b",
    re.IGNORECASE,
)
_MINCIT_ZF_SOURCE_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
_MINCIT_ZF_RESOLUTION_RE = re.compile(
    r"\bR(?:es|e)\.?\s*(?:No\.?\s*)?\d+.*?\b\d{4}\b",
    re.IGNORECASE,
)
_MINCIT_ZF_CLASS_RE = re.compile(
    r"\b(Permanente\s+Especial|Permanente)\b",
    re.IGNORECASE,
)
_MINCIT_ZF_DEPARTMENTS = (
    "Archipiélago de San Andrés, Providencia y Santa Catalina",
    "Archipielago de San Andres, Providencia y Santa Catalina",
    "Norte de Santander",
    "Valle del Cauca",
    "Cundinamarca",
    "Bogotá",
    "Bogota",
    "Antioquia",
    "Atlántico",
    "Atlantico",
    "Bolívar",
    "Bolivar",
    "Boyacá",
    "Boyaca",
    "Caldas",
    "Caquetá",
    "Caqueta",
    "Casanare",
    "Cauca",
    "Cesar",
    "Chocó",
    "Choco",
    "Córdoba",
    "Cordoba",
    "Guainía",
    "Guainia",
    "Guaviare",
    "Huila",
    "La Guajira",
    "Magdalena",
    "Meta",
    "Nariño",
    "Narino",
    "Quindío",
    "Quindio",
    "Risaralda",
    "Santander",
    "Sucre",
    "Tolima",
    "Arauca",
    "Amazonas",
    "Putumayo",
    "Vaupés",
    "Vaupes",
    "Vichada",
)
_MINCIT_ZF_DEPARTMENT_PATTERNS = tuple(
    (
        dep,
        re.compile(rf"\s({re.escape(dep)})(?=\s+)", re.IGNORECASE),
    )
    for dep in sorted(_MINCIT_ZF_DEPARTMENTS, key=len, reverse=True)
)
_MINCIT_DIARIO_OFICIAL_URL = "https://svrpubindc.imprenta.gov.co/diario/index.xhtml"
_MINCIT_PRESS_URL = "https://www.mincit.gov.co/prensa/noticias"
_SUIN_URL = "https://www.suin-juriscol.gov.co/"
_GESTOR_NORMATIVO_URL = "https://www.funcionpublica.gov.co/eva/gestornormativo/"


def _extract_pdf_text_objects_text(
    content: bytes,
    *,
    max_chars: int = PDF_TEXT_FULL_CHARS,
) -> str:
    """Extract PDF text-object content without dropping numeric-only fragments.

    The generic PDF excerpt parser intentionally rejects many numeric fragments
    to avoid false positives. MinCIT's approved-zones PDF is a numeric table, so
    the source-specific parser needs the text object stream with NITs,
    resolution numbers, dates, and CIIU codes intact.
    """
    chunks: list[bytes] = []
    for match in _PDF_STREAM_RE.finditer(content[:PDF_TEXT_MAX_BYTES]):
        stream = match.group(1).strip(b"\r\n")
        try:
            chunks.append(zlib.decompress(stream))
        except zlib.error:
            chunks.append(stream)
    if not chunks:
        chunks.append(content[:PDF_TEXT_MAX_BYTES])

    lines: list[str] = []
    cmap = _extract_pdf_cmap(chunks)
    for chunk in chunks:
        for text_object in _PDF_TEXT_OBJECT_RE.findall(chunk):
            fragments = [
                _decode_pdf_text_token(token, cmap)
                for token in _PDF_TEXT_TOKEN_RE.findall(text_object)
            ]
            line = normalize_whitespace("".join(fragments))
            if line:
                lines.append(line)
    return normalize_whitespace(" ".join(lines))[:max_chars]


def _mincit_zf_is_approved_pdf_item(item: RawItem) -> bool:
    path = urlsplit(item.url).path.lower()
    title = fold_accents(item.title.lower())
    return ".pdf" in path and bool(_MINCIT_ZF_APPROVED_TITLE_RE.search(title))


def _mincit_zf_snapshot_date(text: str) -> str | None:
    match = _MINCIT_ZF_SNAPSHOT_DATE_RE.search(text)
    if match:
        day, month_name, year = match.groups()
        month = MONTHS_ES.get(fold_accents(month_name.lower()))
        if month is not None:
            return _date_to_iso(int(year), month, int(day))
    return None


def _mincit_zf_source_report_date(text: str) -> str | None:
    match = _MINCIT_ZF_SOURCE_DATE_RE.search(text)
    if not match:
        return None
    day, month, year = (int(part) for part in match.groups())
    return _date_to_iso(year, month, day)


def _normalize_mincit_zf_text(text: str) -> str:
    normalized = normalize_whitespace(text)
    # The PDF occasionally splits 9-digit NITs as "90113504 8" at cell breaks.
    normalized = re.sub(
        r"\b(\d{8})\s+(\d)\s+(?=(?:Zona|Centro|ZFB)\b)",
        r"\1\2 ",
        normalized,
    )
    return normalized


def _mincit_zf_row_slices(text: str) -> list[tuple[str, str]]:
    normalized = _normalize_mincit_zf_text(text)
    start = normalized.find("NIT NOMBRE")
    if start != -1:
        normalized = normalized[start:]
    matches = list(re.finditer(r"\b(?P<nit>\d{8,10})\s+", normalized))
    rows: list[tuple[str, str]] = []
    for idx, match in enumerate(matches):
        body_start = match.end()
        body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(normalized)
        body = normalize_whitespace(normalized[body_start:body_end])
        body = re.split(
            r"\s+(?:Fuente:\s*Ministerio|SESI[ÓO]N|RESUMEN\s+ZONAS\s+FRANCAS|"
            r"ZONAS\s+FRANCAS\s+PERMANENTES\s+ESPECIALES)",
            body,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        if "Res." not in body and "Re." not in body:
            continue
        rows.append((match.group("nit"), body))
    return rows


def _parse_mincit_zf_prefix(prefix: str) -> dict[str, str] | None:
    for department, pattern in _MINCIT_ZF_DEPARTMENT_PATTERNS:
        matches = list(pattern.finditer(prefix))
        for dep_match in reversed(matches):
            head = normalize_whitespace(prefix[: dep_match.start()])
            class_matches = list(_MINCIT_ZF_CLASS_RE.finditer(head))
            if not class_matches:
                continue
            municipality = normalize_whitespace(prefix[dep_match.end() :])
            for class_match in reversed(class_matches):
                name = normalize_whitespace(head[: class_match.start()])
                zone_class = normalize_whitespace(class_match.group(1)).title()
                user_type = normalize_whitespace(head[class_match.end() :])
                if len(name) < 8 or not user_type or not municipality:
                    continue
                return {
                    "zona_franca_name": name,
                    "zone_class": zone_class,
                    "class": zone_class,
                    "user_type": user_type,
                    "department": normalize_whitespace(department),
                    "municipality": municipality,
                }
    return None


def _parse_mincit_zf_row_body(nit: str, body: str) -> dict[str, str] | None:
    ciiu_match = re.search(r"\s(?P<ciiu>\d{3,4})\s*$", body)
    if not ciiu_match:
        return None
    ciiu = ciiu_match.group("ciiu").zfill(4)
    without_ciiu = normalize_whitespace(body[: ciiu_match.start()])
    resolution_matches = list(_MINCIT_ZF_RESOLUTION_RE.finditer(without_ciiu))
    if not resolution_matches:
        return None
    first_resolution = resolution_matches[0]
    fields = _parse_mincit_zf_prefix(
        normalize_whitespace(without_ciiu[: first_resolution.start()])
    )
    if not fields:
        return None
    declaratory_resolution = normalize_whitespace(first_resolution.group(0))
    extension_resolution = ""
    if len(resolution_matches) > 1:
        extension_resolution = normalize_whitespace(resolution_matches[1].group(0))
    elif "Vacía" in without_ciiu[first_resolution.end() :]:
        extension_resolution = "Vacía"
    return {
        "nit": nit,
        **fields,
        "declaratory_resolution": declaratory_resolution,
        "extension_resolution": extension_resolution,
        "ciiu": ciiu,
    }


def _mincit_zf_follow_up_sources(row: Mapping[str, str]) -> list[dict[str, str]]:
    zone_name = row.get("zona_franca_name", "")
    resolution = row.get("declaratory_resolution", "")
    search_hint = normalize_whitespace(f"{zone_name} {resolution}")
    return [
        {
            "source_id": "mincit_prensa",
            "source_name": "MinCIT — Noticias",
            "url": _MINCIT_PRESS_URL,
            "search_hint": search_hint,
            "purpose": "Check whether MinCIT published a public-interest note about the named zone.",
        },
        {
            "source_id": "diario_oficial",
            "source_name": "Diario Oficial — Imprenta Nacional",
            "url": _MINCIT_DIARIO_OFICIAL_URL,
            "search_hint": search_hint,
            "purpose": "Verify official publication of the declaratory or extension resolution.",
        },
        {
            "source_id": "suin_juriscol",
            "source_name": "SUIN Juriscol",
            "url": _SUIN_URL,
            "search_hint": search_hint,
            "purpose": "Search for the final legal act by resolution number and zone name.",
        },
        {
            "source_id": "gestor_normativo_fp",
            "source_name": "Función Pública — Gestor Normativo",
            "url": _GESTOR_NORMATIVO_URL,
            "search_hint": search_hint,
            "purpose": "Secondary legal-resolution search by resolution number and zone name.",
        },
    ]


def _extract_mincit_zonas_francas_approved_rows_from_text(
    registry_item: RawItem,
    text: str,
) -> list[RawItem]:
    normalized = _normalize_mincit_zf_text(text)
    if "NIT NOMBRE" not in normalized or "ZONA FRANCA" not in normalized:
        return []
    snapshot_date = _mincit_zf_snapshot_date(normalized)
    source_report_date = _mincit_zf_source_report_date(normalized)
    published_at = registry_item.published_at or source_report_date or snapshot_date
    rows: list[RawItem] = []
    for index, (nit, body) in enumerate(_mincit_zf_row_slices(normalized), start=1):
        parsed = _parse_mincit_zf_row_body(nit, body)
        if not parsed:
            continue
        zone_name = parsed["zona_franca_name"]
        title = (
            "MinCIT Zonas Francas aprobadas — "
            f"{zone_name} — {parsed['municipality']}, {parsed['department']}"
        )
        if parsed.get("declaratory_resolution"):
            title = f"{title} — {parsed['declaratory_resolution']}"
        entry_url = f"{registry_item.url}#zf-{index}"
        follow_up_sources = _mincit_zf_follow_up_sources(parsed)
        metadata = {
            **dict(registry_item.metadata),
            "extraction": "mincit_zonas_francas_approved_pdf_row",
            "content_extraction": MINCIT_ZF_APPROVED_EXTRACTION,
            "registry": MINCIT_ZF_APPROVED_REGISTRY,
            "registry_row_type": "approved_zone",
            "registry_key": nit,
            "registry_pdf_url": registry_item.url,
            "registry_pdf_title": registry_item.title,
            "snapshot_date": snapshot_date,
            "source_report_date": source_report_date,
            "source_update_date": registry_item.published_at,
            "follow_up_sources": follow_up_sources,
            **parsed,
        }
        raw_text = (
            f"{title}. Official MinCIT approved-zones registry row. "
            f"Snapshot date: {(snapshot_date or '')[:10] or 'unknown'}. "
            f"Class: {parsed['zone_class']}; user type: {parsed['user_type']}; "
            f"NIT: {nit}; extension resolution: "
            f"{parsed.get('extension_resolution') or 'not listed'}; "
            f"CIIU: {parsed['ciiu']}."
        )
        rows.append(
            RawItem(
                id=_make_id(registry_item.source_id, entry_url, title),
                source_id=registry_item.source_id,
                source_name=registry_item.source_name,
                source_type=registry_item.source_type,
                url=entry_url,
                title=title,
                fetched_at=registry_item.fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata=metadata,
            )
        )
    return rows


def _enrich_mincit_zonas_francas(
    items: list[RawItem],
    client: httpx.Client,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    generic_pdf_items: list[RawItem] = []
    for item in items:
        if not _mincit_zf_is_approved_pdf_item(item):
            generic_pdf_items.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            text = _extract_pdf_text_objects_text(
                response.content,
                max_chars=MINCIT_ZF_TEXT_MAX_CHARS,
            )
            rows = _extract_mincit_zonas_francas_approved_rows_from_text(item, text)
        except Exception as exc:  # noqa: BLE001 - preserve link-level item
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            continue
        if not rows:
            metadata["content_extraction_error"] = "unable to parse approved-zones rows"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            continue
        enriched.extend(rows)
    if generic_pdf_items:
        enriched.extend(_enrich_pdf_text(generic_pdf_items, client))
    return enriched


_DATE_DDMMYYYY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_GACETA_PROJECT_RE = re.compile(
    r"\bPROYECTO\s+DE\s+(?P<kind>LEY|ACTO\s+LEGISLATIVO)\s+"
    r"N[ÚU]MERO\s+(?P<label>.{5,180}?)(?=\s+por\s+(?:la|el|medio)|"
    r"\s+P[aá]gina|\s+Gaceta|\.|$)",
    re.IGNORECASE,
)
_GACETA_PROJECT_RECORD_RE = re.compile(
    r"\b(?:N(?:o|ro)?\.?\s*)?(?P<number>\d{1,4})\s+de\s+"
    r"(?P<year>\d{4})\s+(?P<chamber>C[ÁA]MARA|CAMARA|SENADO)\b",
    re.IGNORECASE,
)
_GACETA_TITLE_RE = re.compile(
    r"\b(por\s+(?:la|el|medio)\s+(?:cual\s+)?(?:se\s+)?"
    r".{24,280}?)(?:\.|\s+P[aá]gina|\s+Gaceta|$)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ImprentaDownloadContext:
    url: str
    hidden_fields: dict[str, str]


def _extract_imprenta_download_context(
    html: str,
    current_url: str,
) -> ImprentaDownloadContext | None:
    soup = BeautifulSoup(html, "html.parser")
    form = (
        soup.find("form", {"id": "formResumen"})
        or soup.find("form", {"id": "frmConDiario"})
        or soup.find("form")
    )
    if form is None:
        return None
    action = form.get("action") or current_url
    hidden_fields: dict[str, str] = {}
    for field in form.find_all("input"):
        name = field.get("name")
        if not name:
            continue
        field_type = (field.get("type") or "").lower()
        if field_type == "hidden" or name == "javax.faces.ViewState":
            hidden_fields[name] = field.get("value") or ""
    form_id = str(form.get("id") or form.get("name") or "")
    if form_id:
        hidden_fields.setdefault(form_id, form_id)
    if "javax.faces.ViewState" not in hidden_fields:
        return None
    return ImprentaDownloadContext(
        url=urljoin(current_url, action),
        hidden_fields=hidden_fields,
    )


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


def _download_imprenta_pdf(
    client: httpx.Client,
    context: ImprentaDownloadContext,
    button_name: str,
) -> httpx.Response:
    data = dict(context.hidden_fields)
    data[button_name] = button_name
    return _http_post_form(client, context.url, data=data)


def _annotate_legal_identity_items(
    items: list[RawItem],
    *,
    max_records: int = 30,
) -> list[RawItem]:
    annotated: list[RawItem] = []
    for item in items:
        metadata = annotate_legal_identity(
            dict(item.metadata),
            item.title,
            item.raw_text,
            max_records=max_records,
        )
        if metadata == item.metadata:
            annotated.append(item)
            continue
        annotated.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=item.raw_text,
                metadata=metadata,
            )
        )
    return annotated


def _parse_diario_oficial_pdf_text(text: str) -> dict[str, Any] | None:
    clean_text = normalize_whitespace(text)
    if len(clean_text) < PDF_TEXT_MIN_CHARS:
        return None
    records = parse_legal_act_records(clean_text, max_records=40)
    return {
        "legal_act_records": records,
        "excerpt": clean_text[:PDF_TEXT_EXCERPT_CHARS],
        "parse_status": "legal_act_identities_found"
        if records
        else "parsed_no_legal_act_identities",
    }


def _imprenta_fragment(prefix: str, *parts: str) -> str:
    text = " ".join(part for part in parts if part)
    slug = re.sub(r"[^a-z0-9]+", "-", fold_accents(text.lower())).strip("-")
    if not slug:
        return prefix
    return f"{prefix}-{slug[:90].strip('-')}"


def _diario_publication_year(item: RawItem) -> str:
    return (item.published_at or item.fetched_at or "")[:4]


def _is_diario_published_act(
    record: dict[str, str],
    *,
    publication_year: str,
) -> bool:
    if not publication_year or str(record.get("year") or "") != publication_year:
        return False
    matched = str(record.get("matched_text") or "")
    letters = [ch for ch in matched if ch.isalpha()]
    if not letters:
        return False
    uppercase_ratio = sum(1 for ch in letters if ch.isupper()) / len(letters)
    return uppercase_ratio >= 0.8


def _diario_act_excerpt(text: str, record: dict[str, str]) -> str:
    matched = str(record.get("matched_text") or "")
    start = text.find(matched)
    if start < 0:
        label = str(record.get("label") or "")
        start = text.find(label)
    if start < 0:
        return text[:PDF_TEXT_EXCERPT_CHARS]
    context_start = max(0, start - 180)
    context_end = min(len(text), start + PDF_TEXT_EXCERPT_CHARS)
    return text[context_start:context_end]


def _diario_act_items(
    item: RawItem,
    metadata: dict[str, Any],
    parsed: dict[str, Any],
    text: str,
) -> list[RawItem]:
    clean_text = normalize_whitespace(text)
    records = [
        record
        for record in parsed["legal_act_records"]
        if _is_diario_published_act(
            record,
            publication_year=_diario_publication_year(item),
        )
    ]
    if not records:
        return []

    rows: list[RawItem] = []
    for index, record in enumerate(records, start=1):
        label = str(record.get("label") or "").strip()
        fragment = _imprenta_fragment("act", label or str(index))
        row_url = f"{item.url}#{fragment}"
        title = f"{item.title} — {label}" if label else item.title
        row_metadata = {
            **metadata,
            "document_row_type": "diario_legal_act",
            "parent_edition_url": item.url,
            "parent_item_id": item.id,
            "legal_act_record": record,
            "legal_act_records": [record],
            "legal_act_record_count": 1,
            "published_legal_act_record_count": len(records),
            "referenced_legal_act_records": parsed["legal_act_records"],
            "referenced_legal_act_record_count": len(parsed["legal_act_records"]),
        }
        excerpt = _diario_act_excerpt(clean_text, record)
        rows.append(
            RawItem(
                id=_make_id(item.source_id, row_url, title),
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=row_url,
                title=title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=(
                    f"{title}. Published in {item.title}. "
                    f"Official Diario act excerpt: {excerpt}"
                ),
                metadata=row_metadata,
            )
        )
    return rows


def _extract_embedded_pdf_url(html_text: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    for tag_name, attr in (("object", "data"), ("embed", "src"), ("iframe", "src")):
        for tag in soup.find_all(tag_name):
            value = str(tag.get(attr) or "").strip()
            if not value:
                continue
            folded = fold_accents(value.lower())
            if "pdf" in folded or "dynamiccontent" in folded:
                return urljoin(base_url, value)
    return None


def _enrich_diario_oficial_pdfs(
    items: list[RawItem],
    client: httpx.Client,
    html: str,
    current_url: str,
    *,
    max_items: int = PDF_TEXT_PARSE_LIMIT,
) -> list[RawItem]:
    context = _extract_imprenta_download_context(html, current_url)
    if context is None:
        return items
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        button_name = item.metadata.get("download_button_name")
        if (
            parsed_count >= max_items
            or item.source_id != "diario_oficial"
            or not isinstance(button_name, str)
            or not button_name
        ):
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _download_imprenta_pdf(client, context, button_name)
            content_type = response.headers.get("content-type", "")
            if (
                "pdf" not in content_type.lower()
                and not response.content.startswith(b"%PDF")
                and "html" in content_type.lower()
            ):
                embedded_url = _extract_embedded_pdf_url(
                    response.text,
                    str(response.url),
                )
                if embedded_url:
                    metadata["pdf_viewer_url"] = str(response.url)
                    metadata["pdf_embedded_url"] = embedded_url
                    response = _http_get(client, embedded_url)
                    content_type = response.headers.get("content-type", "")
            if (
                "pdf" not in content_type.lower()
                and not response.content.startswith(b"%PDF")
            ):
                raise ValueError(f"download did not return a PDF: {content_type}")
            text = _extract_pdf_text_with_pdfplumber(
                response.content,
                max_chars=IMPRENTA_PDF_TEXT_FULL_CHARS,
            )
        except Exception as exc:  # noqa: BLE001 - preserve edition-level row
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        parsed = _parse_diario_oficial_pdf_text(text)
        if parsed is None:
            metadata.update(
                {
                    "content_extraction_error": "no readable Diario PDF text found",
                    "pdf_text_chars": len(text),
                }
            )
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        metadata.update(
            {
                "content_extraction": "diario_oficial_pdf_text",
                "legal_act_records": parsed["legal_act_records"],
                "legal_act_record_count": len(parsed["legal_act_records"]),
                "pdf_parse_status": parsed["parse_status"],
                "pdf_text_chars": len(text),
            }
        )
        record_labels = ", ".join(
            record["label"] for record in parsed["legal_act_records"][:5]
        )
        act_rows = _diario_act_items(item, metadata, parsed, text)
        if act_rows:
            enriched.extend(act_rows)
            parsed_count += 1
            continue
        if parsed["legal_act_records"]:
            metadata.update(
                {
                    "legal_act_records": [],
                    "legal_act_record_count": 0,
                    "referenced_legal_act_records": parsed["legal_act_records"],
                    "referenced_legal_act_record_count": len(
                        parsed["legal_act_records"]
                    ),
                    "pdf_parse_status": "parsed_no_published_legal_act_headings",
                }
            )
            record_labels = ""
        identity_text = (
            f"Diario Oficial legal-act identities: {record_labels}. "
            if record_labels
            else "Diario Oficial PDF parsed; no legal-act identities found. "
        )
        enriched.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=(
                    f"{item.raw_text} {identity_text}"
                    f"PDF text excerpt: {parsed['excerpt']}"
                ),
                metadata=metadata,
            )
        )
        parsed_count += 1
    return enriched


def _gaceta_action_type(text: str) -> str:
    folded = fold_accents(text.lower())
    if "conciliacion" in folded:
        return "conciliacion"
    if "comisiones conjuntas" in folded or "conjuntas de la camara" in folded:
        return "comisiones conjuntas"
    if "informe de ponencia" in folded or "ponencia" in folded:
        return "ponencia"
    if "texto aprobado" in folded:
        return "texto aprobado"
    return "publicacion de gaceta"


def _normalize_gaceta_identity_text(text: str) -> str:
    clean = normalize_whitespace(text)
    replacements = (
        (r"\bDELEY\b", "DE LEY"),
        (r"\bDEACTOLEGISLATIVO\b", "DE ACTO LEGISLATIVO"),
        (r"\bN[ÚU]MERO\s*DELEY\b", "NÚMERO DE LEY"),
        (r"\bDESENADO\b", "DE SENADO"),
        (r"\bDEC[ÁA]MARA\b", "DE CÁMARA"),
        (r"\bSENADODE\b", "SENADO DE"),
        (r"\bC[ÁA]MARAPOR\b", "CÁMARA por"),
        (r"\bSENADOPOR\b", "SENADO por"),
        (r"\bp\s+or\b", "por"),
        (r"\bdelacual\b", "de la cual"),
    )
    for pattern, replacement in replacements:
        clean = re.sub(pattern, replacement, clean, flags=re.IGNORECASE)
    clean = re.sub(
        r"\b(20\d{2})(Senado|C[áa]mara|Camara)\b",
        r"\1 \2",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"\b(Senado|C[áa]mara|Camara)(por)\b",
        r"\1 \2",
        clean,
        flags=re.IGNORECASE,
    )
    return normalize_whitespace(clean)


def _normalize_gaceta_project_label(kind: str, label: str) -> str:
    clean = normalize_whitespace(label)
    clean = re.sub(r"\bC\s*[ÁA]\s*M\s*A\s*R\s*A\b", "Cámara", clean, flags=re.I)
    clean = re.sub(r"\bS\s*E\s*N\s*A\s*D\s*O\b", "Senado", clean, flags=re.I)
    clean = re.sub(r"\s+y\s+", " y ", clean, flags=re.I)
    kind_clean = "Acto Legislativo" if "ACTO" in fold_accents(kind.upper()) else "Ley"
    return f"Proyecto de {kind_clean} {clean}".strip()


def _gaceta_project_kind(text: str) -> str:
    folded = fold_accents(text.lower())
    return "Acto Legislativo" if "proyecto de acto legislativo" in folded else "Ley"


def _gaceta_project_records_from_text(text: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for match in _GACETA_PROJECT_RECORD_RE.finditer(text):
        record = {
            "number": match.group("number").lstrip("0") or "0",
            "year": match.group("year"),
            "chamber": _normalize_senado_chamber(match.group("chamber")),
        }
        key = (record["number"], record["year"], record["chamber"])
        if key in seen:
            continue
        seen.add(key)
        records.append(record)
    return records


def _gaceta_project_label_from_records(
    kind: str,
    records: list[dict[str, str]],
) -> str:
    labels = [
        f"{record['number']} DE {record['year']} {record['chamber']}"
        for record in records
    ]
    return f"Proyecto de {kind} {' y '.join(labels)}".strip()


def _gaceta_project_records(project_label: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    joint_match = re.search(
        r"\b(\d{1,4})\s+DE\s+(\d{4})\s+C[ÁA]MARA\s+Y\s+SENADO\b",
        project_label,
        flags=re.IGNORECASE,
    )
    if joint_match:
        return [
            {
                "number": joint_match.group(1),
                "year": joint_match.group(2),
                "chamber": "Cámara/Senado",
            }
        ]
    for match in re.finditer(
        r"\b(\d{1,4})\s+DE\s+(\d{4})\s+(C[ÁA]MARA|SENADO)\b",
        project_label,
        flags=re.IGNORECASE,
    ):
        records.append(
            {
                "number": match.group(1),
                "year": match.group(2),
                "chamber": _normalize_senado_chamber(match.group(3)),
            }
        )
    return records


def _has_usable_gaceta_document_title(document_title: str) -> bool:
    folded_title = fold_accents(document_title.lower()).strip()
    if len(folded_title) < 24:
        return False
    if folded_title.endswith((" de", " del", " la", " el", " fiscal de")):
        return False
    return True


def _has_usable_gaceta_identity(
    project_label: str,
    document_title: str,
    project_records: list[dict[str, str]] | None = None,
) -> bool:
    records = (
        project_records
        if project_records is not None
        else _gaceta_project_records(project_label)
    )
    if not records:
        return False
    folded_label = fold_accents(project_label.lower())
    if re.search(r"\bproyecto de (?:ley|acto legislativo)\s+de\s+\d{4}", folded_label):
        return False
    return _has_usable_gaceta_document_title(document_title)


def _parse_gaceta_pdf_text(item: RawItem, text: str) -> dict[str, Any] | None:
    clean_text = _normalize_gaceta_identity_text(text)
    if len(clean_text) < PDF_TEXT_MIN_CHARS:
        return None
    project_label = ""
    project_records: list[dict[str, str]] = []
    project_match = _GACETA_PROJECT_RE.search(clean_text)
    if project_match:
        project_label = _normalize_gaceta_project_label(
            project_match.group("kind"),
            project_match.group("label"),
        )
        project_records = _gaceta_project_records(project_label)
    if not project_records:
        project_records = _gaceta_project_records_from_text(clean_text)
        if project_records:
            project_label = _gaceta_project_label_from_records(
                _gaceta_project_kind(clean_text),
                project_records,
            )
    title_match = _GACETA_TITLE_RE.search(clean_text)
    document_title = ""
    if title_match:
        document_title = normalize_whitespace(title_match.group(1)).strip(" .,:;-")
    elif item.metadata.get("document_title"):
        document_title = normalize_whitespace(str(item.metadata["document_title"]))

    if not document_title:
        return None
    if not project_records:
        if not _has_usable_gaceta_document_title(document_title):
            return None
        return {
            "project_label": "",
            "project_records": [],
            "document_title": document_title,
            "action_type": _gaceta_action_type(clean_text[:1500]),
            "identity_quality": "document_title_only",
            "excerpt": clean_text[:PDF_TEXT_EXCERPT_CHARS],
        }
    if not _has_usable_gaceta_identity(
        project_label,
        document_title,
        project_records,
    ):
        return None
    return {
        "project_label": project_label,
        "project_records": project_records,
        "document_title": document_title,
        "action_type": _gaceta_action_type(clean_text[:1500]),
        "identity_quality": "project_and_title",
        "excerpt": clean_text[:PDF_TEXT_EXCERPT_CHARS],
    }


def _enrich_gaceta_pdfs(
    items: list[RawItem],
    client: httpx.Client,
    html: str,
    current_url: str,
    *,
    max_items: int = GACETA_PDF_PARSE_LIMIT,
) -> list[RawItem]:
    context = _extract_imprenta_download_context(html, current_url)
    if context is None:
        return items
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        button_name = item.metadata.get("download_button_name")
        if (
            parsed_count >= max_items
            or item.source_id != "gacetas_congreso"
            or not isinstance(button_name, str)
            or not button_name
        ):
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _download_imprenta_pdf(client, context, button_name)
            content_type = response.headers.get("content-type", "")
            if (
                "pdf" not in content_type.lower()
                and not response.content.startswith(b"%PDF")
            ):
                raise ValueError(f"download did not return a PDF: {content_type}")
            text = _extract_pdf_text(response.content, max_chars=PDF_TEXT_FULL_CHARS)
        except Exception as exc:  # noqa: BLE001 - preserve link-level row
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        parsed = _parse_gaceta_pdf_text(item, text)
        if parsed is None:
            metadata.update(
                {
                    "content_extraction_error": "no usable Gaceta project/title text found",
                    "pdf_text_chars": len(text),
                }
            )
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        title_parts = [item.title]
        if parsed["project_label"]:
            title_parts.append(parsed["project_label"])
        if parsed["document_title"]:
            title_parts.append(parsed["document_title"][:180])
        title = " — ".join(title_parts)
        fragment_prefix = "project" if parsed["project_label"] else "title"
        fragment = _imprenta_fragment(
            fragment_prefix,
            parsed["project_label"],
            parsed["document_title"],
        )
        row_url = f"{item.url}#{fragment}"
        metadata.update(
            {
                "content_extraction": "gaceta_pdf_text",
                "document_row_type": "gaceta_bill_item",
                "parent_edition_url": item.url,
                "parent_item_id": item.id,
                "document_title": parsed["document_title"],
                "project_label": parsed["project_label"],
                "project_records": parsed["project_records"],
                "agenda_action_type": parsed["action_type"],
                "gaceta_identity_quality": parsed["identity_quality"],
                "matched_project_labels": [parsed["project_label"]]
                if parsed["project_label"]
                else [],
                "pdf_text_chars": len(text),
            }
        )
        enriched.append(
            RawItem(
                id=_make_id(item.source_id, row_url, title),
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=row_url,
                title=title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=(
                    f"{title}. Extracted from official Gaceta PDF. "
                    f"PDF text excerpt: {parsed['excerpt']}"
                ),
                metadata=metadata,
            )
        )
        parsed_count += 1
    return enriched


def _extract_imprenta_jsf_table(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
    edition_label: str,
    query_param: str,
) -> list[RawItem]:
    """Parse the PrimeFaces datatable used by Imprenta Nacional sites.

    Diario Oficial and Gacetas del Congreso both render their listings as a
    JSF/PrimeFaces datatable: each data row has Number | Type-or-Entity | Date
    (DD/MM/YYYY) | … | JSF download button. The download buttons trigger
    postbacks rather than direct links, so we synthesize a stable query-string
    URL per edition (e.g. `?edicion=53.475`) so each row dedupes distinctly.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for tr in soup.find_all("tr"):
        cells = [
            normalize_whitespace(td.get_text(separator=" ", strip=True))
            for td in tr.find_all("td")
        ]
        date_match = None
        date_idx = None
        for i, cell in enumerate(cells):
            match = _DATE_DDMMYYYY_RE.match(cell)
            if match:
                date_match = match
                date_idx = i
                break
        if date_match is None or date_idx is None or date_idx == 0:
            continue
        # Real data rows have date_idx == 2 (Diario) or == 2 (Gacetas), with a
        # short number in cells[0]. JSF wrapper rows concatenate the whole
        # datatable into a single first cell — skip those.
        if date_idx > 3:
            continue
        number = cells[0]
        if not number or len(number) > 20:
            continue
        kind = cells[1] if date_idx >= 2 else ""
        day, month, year = (int(x) for x in date_match.groups())
        published_at = _date_to_iso(year, month, day)
        if not published_at:
            continue
        document_title = ""
        if len(cells) > date_idx + 1:
            candidate = cells[date_idx + 1]
            if candidate and fold_accents(candidate.lower()) not in {"ui-button"}:
                document_title = candidate
        title_parts = [f"{edition_label} {number}", kind, document_title[:140]]
        title = " — ".join(part for part in title_parts if part)
        synthetic_url = f"{base_url.rstrip('/')}?{query_param}={number}"
        canon = canonicalize_url(synthetic_url)
        if canon in seen:
            continue
        seen.add(canon)
        metadata = {
            "extraction": "imprenta_nacional_jsf_table",
            "edition_number": number,
        }
        button = tr.find("button", attrs={"name": True})
        if button is not None:
            metadata["download_button_name"] = str(button.get("name"))
            metadata["download_mechanism"] = "jsf_postback"
        if kind:
            metadata["entity_or_type"] = kind
        if document_title:
            metadata["document_title"] = document_title
        items.append(
            RawItem(
                id=_make_id(source.id, synthetic_url, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=synthetic_url,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=" | ".join(cells),
                metadata=metadata,
            )
        )
    return items


def _extract_corte_comunicados(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    items = _extract_dated_anchors(
        html, base_url, source, fetched_at, "corte_comunicados_dated_anchor"
    )
    if items:
        return [
            it
            for it in items
            if "comunicado" in fold_accents((it.title + " " + it.url).lower())
        ] or items
    return []


_DIAN_REGULATORY_LINK_MARKERS = (
    "agenda-reglamentaria",
    "proyectosnormas",
)


def _extract_dian_regulatory_project_links(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, str(anchor.get("href") or ""))
        anchor_text = normalize_whitespace(anchor.get_text(" ", strip=True))
        searchable = fold_accents(" ".join([anchor_text, href]).lower())
        if not any(marker in searchable for marker in _DIAN_REGULATORY_LINK_MARKERS):
            continue
        canon = canonicalize_url(href)
        if canon in seen:
            continue
        seen.add(canon)
        title_text = anchor_text or "DIAN regulatory project index"
        title = f"DIAN regulatory project index — {title_text[:140]}"
        metadata = {
            "extraction": "dian_regulatory_project_index_link",
            "parser_status": "dynamic_or_undated_index",
            "target_url": href,
        }
        items.append(
            RawItem(
                id=_make_id(source.id, href, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=href,
                title=title,
                fetched_at=fetched_at,
                published_at=None,
                raw_text=(
                    f"{title}. DIAN exposes this regulatory-project lead from "
                    "the normativity landing page, but the static HTML does not "
                    "include dated project rows. Treat as parser feasibility "
                    "metadata, not rankable evidence."
                ),
                metadata=metadata,
            )
        )
    return items


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


SOCRATA_FRESHNESS_DAYS = 14
SOCRATA_DEFAULT_LIMIT = 30
_SOCRATA_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})")


@dataclass(frozen=True, slots=True)
class SocrataAdapter:
    """Per-dataset configuration for the Socrata API fetcher.

    Each entry on `datos.gov.co` has its own column names, so we keep the
    column-to-RawItem mapping in code rather than YAML — same dispatch style
    we use for source-specific HTML extractors.
    """

    date_field: str
    title_field: str
    id_field: str
    label: str
    entity_field: str | None = None
    title_max_chars: int = 160


SOCRATA_ADAPTERS: dict[str, SocrataAdapter] = {
    "secop_ii_procesos": SocrataAdapter(
        date_field="fecha_de_publicacion_del",
        title_field="nombre_del_procedimiento",
        id_field="id_del_proceso",
        entity_field="entidad",
        label="SECOP II Proceso",
    ),
    "secop_ii_contratos": SocrataAdapter(
        date_field="fecha_de_firma",
        title_field="descripcion_del_proceso",
        id_field="id_contrato",
        entity_field="nombre_entidad",
        label="SECOP II Contrato",
    ),
    "secop_i_procesos": SocrataAdapter(
        date_field="fecha_de_cargue_en_el_secop",
        title_field="detalle_del_objeto_a_contratar",
        id_field="uid",
        entity_field="nombre_entidad",
        label="SECOP I Proceso",
    ),
    "secop_ii_adiciones": SocrataAdapter(
        date_field="fecharegistro",
        title_field="descripcion",
        id_field="identificador",
        label="SECOP II Adición",
    ),
    "secop_multas_sanciones": SocrataAdapter(
        date_field="fecha_de_publicacion",
        title_field="nombre_contratista",
        id_field="numero_de_resolucion",
        entity_field="nombre_entidad",
        label="Multa/Sanción SECOP I",
    ),
}


def _parse_socrata_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = _SOCRATA_DATE_RE.match(value)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(x) for x in match.groups())
    try:
        return datetime(
            year, month, day, hour, minute, second, tzinfo=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _socrata_row_to_item(
    row: Mapping[str, Any],
    source: Metasource,
    fetched_at: str,
    adapter: SocrataAdapter,
) -> RawItem | None:
    published_at = _parse_socrata_date(row.get(adapter.date_field))
    if not published_at:
        return None
    title_raw = normalize_whitespace((row.get(adapter.title_field) or "")).strip()
    if not title_raw:
        return None
    id_value = normalize_whitespace((row.get(adapter.id_field) or "")).strip()
    if not id_value:
        return None
    entity = ""
    if adapter.entity_field:
        entity = normalize_whitespace((row.get(adapter.entity_field) or "")).strip()
    title_body = title_raw[: adapter.title_max_chars]
    title_parts = [adapter.label, title_body]
    if entity:
        title_parts.append(entity[:80])
    title = " — ".join(p for p in title_parts if p)
    synthetic_url = f"{source.url}?id={id_value}"
    raw_text = " | ".join(p for p in [title_raw, entity] if p)
    metadata = {
        "extraction": "socrata_api",
        "dataset_url": source.url,
        "id_value": id_value,
        "date_field": adapter.date_field,
        "title_field": adapter.title_field,
    }
    if entity:
        metadata["entity"] = entity
    return RawItem(
        id=_make_id(source.id, synthetic_url, title),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=synthetic_url,
        title=title,
        fetched_at=fetched_at,
        published_at=published_at,
        raw_text=raw_text,
        metadata=metadata,
    )


def _socrata_params(
    adapter: SocrataAdapter,
    *,
    cutoff: datetime,
    limit: int,
) -> dict[str, str]:
    select_cols = {
        adapter.date_field,
        adapter.title_field,
        adapter.id_field,
    }
    if adapter.entity_field:
        select_cols.add(adapter.entity_field)
    cutoff_text = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000")
    return {
        "$select": ",".join(sorted(select_cols)),
        "$where": f"{adapter.date_field} >= '{cutoff_text}'",
        "$order": f"{adapter.date_field} DESC",
        "$limit": str(limit),
    }


def fetch_api(source: Metasource, client: httpx.Client) -> list[RawItem]:
    adapter = SOCRATA_ADAPTERS.get(source.id)
    if adapter is None:
        raise ValueError(
            f"no Socrata adapter configured for source.id={source.id!r}; "
            "add an entry in SOCRATA_ADAPTERS"
        )
    fetched_at = _now_iso()
    cutoff = datetime.now(timezone.utc) - timedelta(days=SOCRATA_FRESHNESS_DAYS)
    limit = source.max_items if source.max_items and source.max_items > 0 else SOCRATA_DEFAULT_LIMIT
    params = _socrata_params(adapter, cutoff=cutoff, limit=limit)
    response = _http_get(client, source.url, params=params)
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(
            f"unexpected Socrata payload for {source.id}: "
            f"expected list, got {type(payload).__name__}"
        )
    items: list[RawItem] = []
    seen: set[str] = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        item = _socrata_row_to_item(row, source, fetched_at, adapter)
        if item is None:
            continue
        canon = canonicalize_url(item.url)
        if canon in seen:
            continue
        seen.add(canon)
        items.append(item)
    return items


def _field_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", fold_accents(label.lower())).strip("_")


def _parse_detail_datetime_to_iso(value: str | None) -> str | None:
    if not value:
        return None
    clean = normalize_whitespace(value)
    try:
        parsed = datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except ValueError:
        return _parse_date_text_to_iso(clean)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _registry_year(value: str) -> str:
    year = int(value)
    if year < 100:
        year += 2000
    return str(year)


_REGISTRY_PROJECT_RE = re.compile(
    r"\b(?P<number>\d{1,4})\s*/\s*(?P<year>\d{2,4})(?:\s*(?P<suffix>[CS]))?",
    re.IGNORECASE,
)


def _normalize_project_number(value: str) -> str:
    stripped = value.lstrip("0")
    return stripped or "0"


def _registry_project_records(
    *,
    numero_senado: str | None = None,
    numero_camara: str | None = None,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for text, default_chamber in (
        (numero_senado or "", "Senado"),
        (numero_camara or "", "Cámara"),
    ):
        for match in _REGISTRY_PROJECT_RE.finditer(text):
            suffix = (match.group("suffix") or "").upper()
            chamber = (
                "Senado"
                if suffix == "S"
                else "Cámara"
                if suffix == "C"
                else default_chamber
            )
            record = {
                "number": _normalize_project_number(match.group("number")),
                "year": _registry_year(match.group("year")),
                "chamber": chamber,
            }
            key = (record["number"], record["year"], record["chamber"])
            if key in seen:
                continue
            seen.add(key)
            records.append(record)
    return records


def _registry_project_kind(value: str | None) -> str:
    folded = fold_accents((value or "").lower())
    return "Acto Legislativo" if "acto legislativo" in folded else "Ley"


def _registry_project_label(
    records: list[dict[str, str]],
    *,
    kind: str = "Ley",
) -> str:
    if not records:
        return ""
    record = records[0]
    return (
        f"Proyecto de {kind} {record['number']} de {record['year']} "
        f"{record['chamber']}"
    )


def _extract_detail_label_values(html_fragment: str) -> dict[str, str]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    values: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        for idx in range(0, len(cells) - 1, 2):
            label = normalize_whitespace(cells[idx]).rstrip(":")
            value = normalize_whitespace(cells[idx + 1])
            if label and value:
                values[_field_key(label)] = value
    return values


def _extract_senado_publication_links(
    html_fragment: str,
    base_url: str,
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    links: list[dict[str, str]] = []
    for label_cell in soup.select("td.celda-etiqueta"):
        value_cell = label_cell.find_next_sibling("td")
        if value_cell is None:
            continue
        link = value_cell.find("a")
        title = normalize_whitespace(
            link.get_text(" ", strip=True) if link else value_cell.get_text(" ", strip=True)
        )
        if not title:
            continue
        label = normalize_whitespace(label_cell.get_text(" ", strip=True)).rstrip(":")
        href = link.get("href") if link else ""
        links.append(
            {
                "type": label,
                "title": title,
                "url": urljoin(base_url, href) if href else "",
            }
        )
    return links


def _extract_senado_text_radicado_url(html_fragment: str, base_url: str) -> str:
    soup = BeautifulSoup(html_fragment, "html.parser")
    button = soup.find(id="textoRadicadoBtn")
    if button is None:
        return ""
    link = button.get("data-link") or ""
    return urljoin(base_url, link) if link else ""


def _senado_registry_row_to_item(
    row: Mapping[str, Any],
    detail_html: str,
    *,
    source: Metasource,
    fetched_at: str,
    detail_url: str,
) -> RawItem | None:
    title = normalize_whitespace(str(row.get("titulo") or ""))
    numero_senado = normalize_whitespace(str(row.get("numero_senado") or ""))
    numero_camara = normalize_whitespace(str(row.get("numero_camara") or ""))
    if not title or not (numero_senado or numero_camara):
        return None
    fields = _extract_detail_label_values(detail_html)
    kind = _registry_project_kind(fields.get("tipo_de_ley") or "Ley")
    records = _registry_project_records(
        numero_senado=numero_senado,
        numero_camara=numero_camara,
    )
    project_label = _registry_project_label(records, kind=kind)
    if not project_label:
        return None
    status = normalize_whitespace(
        fields.get("estado") or str(row.get("estado") or "")
    )
    commission = normalize_whitespace(
        fields.get("comision") or str(row.get("comision") or "")
    )
    filing_date = fields.get("fecha_de_presentacion")
    published_at = _parse_date_text_to_iso(filing_date) if filing_date else None
    publication_links = _extract_senado_publication_links(detail_html, source.url)
    text_radicado_url = _extract_senado_text_radicado_url(detail_html, source.url)
    evidence_parts = [
        project_label,
        title,
        f"Estado: {status}" if status else "",
        f"Comisión: {commission}" if commission else "",
        f"Fecha de presentación: {filing_date}" if filing_date else "",
    ]
    if publication_links:
        evidence_parts.append(
            "Publicaciones: "
            + "; ".join(link["title"] for link in publication_links[:4])
        )
    metadata: dict[str, Any] = {
        "content_extraction": "senado_leyes_registry",
        "parsed_content": True,
        "legislative_registry": "senado_leyes",
        "registry_detail_url": detail_url,
        "project_label": project_label,
        "project_records": records,
        "project_identity_status": "clean",
        "has_clean_project_identity": True,
        "bill_title": title,
        "status": status,
        "commission": commission,
        "author": normalize_whitespace(str(row.get("autor") or "")),
        "legislature": fields.get("legislatura") or _current_legislature_label(),
        "cuatrenio": fields.get("cuatrenio") or str(row.get("cuatrenio") or ""),
        "source_row_id": str(row.get("id") or ""),
    }
    if publication_links:
        metadata["publication_links"] = publication_links
    if text_radicado_url:
        metadata["text_radicado_url"] = text_radicado_url
    return RawItem(
        id=_make_id(source.id, detail_url, project_label),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=detail_url,
        title=f"Senado registry — {project_label} — {title}",
        fetched_at=fetched_at,
        published_at=published_at,
        raw_text=". ".join(part for part in evidence_parts if part),
        metadata=metadata,
    )


def _fetch_senado_leyes_registry(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
) -> list[RawItem]:
    search_url = urljoin(source.url, "api/search_pdly.php")
    detail_base = urljoin(source.url, "api/get_detalle_pdly.php")
    response = _http_post_form(
        client,
        search_url,
        {"legislatura": _current_legislature_label()},
    )
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError("unexpected Senado registry payload")
    limit = source.max_items or LEGISLATIVE_REGISTRY_DEFAULT_LIMIT
    def row_sort_key(row: object) -> int:
        if not isinstance(row, dict):
            return -1
        try:
            return int(str(row.get("id") or "0"))
        except ValueError:
            return -1

    items: list[RawItem] = []
    for row in sorted(rows, key=row_sort_key, reverse=True)[:limit]:
        if not isinstance(row, dict):
            continue
        row_id = row.get("id")
        if row_id is None:
            continue
        detail_url = f"{detail_base}?id={row_id}"
        detail = _http_get(client, detail_base, params={"id": str(row_id)})
        item = _senado_registry_row_to_item(
            row,
            detail.text,
            source=source,
            fetched_at=fetched_at,
            detail_url=detail_url,
        )
        if item is not None:
            items.append(item)
    return items


def _extract_camara_pl_nonce(html_text: str) -> str:
    match = re.search(r"PL_NONCE\s*:\s*['\"]([^'\"]+)['\"]", html_text)
    return match.group(1) if match else ""


def _extract_camara_legislature_id(html_text: str, label: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    select = soup.find("select", id="legislaturaField")
    if select is None:
        return "All"
    for option in select.find_all("option"):
        text = normalize_whitespace(option.get_text(" ", strip=True))
        if text == label:
            return option.get("value") or "All"
    return "All"


def _camara_pack_names(pack: str | None) -> str:
    if not pack:
        return ""
    names: list[str] = []
    for entry in str(pack).split("::"):
        parts = entry.split("||")
        if len(parts) >= 2 and parts[1].strip():
            names.append(normalize_whitespace(parts[1]))
    return ", ".join(names)


def _extract_camara_detail_fields(html_text: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    fields: dict[str, Any] = {}
    match = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html_text)
    if match:
        fields["date_published"] = match.group(1)
    for card in soup.select(".pl-card"):
        title_el = card.select_one(".pl-title")
        body_el = card.select_one(".pl-body")
        if title_el is None or body_el is None:
            continue
        key = _field_key(title_el.get_text(" ", strip=True))
        fields[key] = normalize_whitespace(body_el.get_text(" ", strip=True))
        if key == "publicacion":
            fields["publication_links"] = [
                {
                    "title": normalize_whitespace(a.get_text(" ", strip=True)),
                    "url": a.get("href") or "",
                }
                for a in body_el.find_all("a")
                if a.get("href")
            ]
    for title_el in soup.select(".pl-nums-title"):
        if "fecha de radicacion" not in fold_accents(
            title_el.get_text(" ", strip=True).lower()
        ):
            continue
        parent = title_el.find_parent(class_="pl-nums-group")
        if parent is None:
            continue
        for card in parent.select(".pl-kpi-card"):
            label_el = card.select_one(".pl-kpi-label")
            value_el = card.select_one(".pl-kpi-value")
            if label_el is None or value_el is None:
                continue
            value = normalize_whitespace(value_el.get_text(" ", strip=True))
            if value and value not in {"-", "—"}:
                fields["fecha_de_radicacion"] = value
                break
    return fields


def _camara_registry_row_to_item(
    row: Mapping[str, Any],
    detail_html: str,
    *,
    source: Metasource,
    fetched_at: str,
    detail_url: str,
) -> RawItem | None:
    title = normalize_whitespace(str(row.get("titulo") or ""))
    short_title = normalize_whitespace(str(row.get("proyecto") or ""))
    numero_senado = normalize_whitespace(str(row.get("nro_senado") or ""))
    numero_camara = normalize_whitespace(str(row.get("nro_camara") or ""))
    if not title or not (numero_senado or numero_camara):
        return None
    fields = _extract_camara_detail_fields(detail_html)
    kind = _registry_project_kind(str(row.get("tipo") or fields.get("tipo_de_ley") or ""))
    records = _registry_project_records(
        numero_senado=numero_senado,
        numero_camara=numero_camara,
    )
    project_label = _registry_project_label(records, kind=kind)
    if not project_label:
        return None
    published_at = _parse_date_text_to_iso(str(fields.get("fecha_de_radicacion") or ""))
    if not published_at:
        published_at = _parse_detail_datetime_to_iso(str(fields.get("date_published") or ""))
    status = normalize_whitespace(str(row.get("estado") or ""))
    commission = _camara_pack_names(str(row.get("comisiones_pack") or ""))
    authors = _camara_pack_names(str(row.get("autores_pack") or ""))
    other_authors = normalize_whitespace(str(row.get("otros_autores") or ""))
    object_text = normalize_whitespace(str(fields.get("objeto_del_proyecto") or ""))
    publication_links = [
        {
            "title": str(link.get("title") or ""),
            "url": urljoin(detail_url, str(link.get("url") or "")),
        }
        for link in (fields.get("publication_links") or [])
        if isinstance(link, dict) and link.get("url")
    ]
    display_title = short_title or title
    evidence_parts = [
        project_label,
        display_title,
        title,
        f"Estado: {status}" if status else "",
        f"Comisión: {commission}" if commission else "",
        f"Fecha de radicación: {fields.get('fecha_de_radicacion')}"
        if fields.get("fecha_de_radicacion")
        else "",
        f"Objeto: {object_text}" if object_text else "",
    ]
    metadata: dict[str, Any] = {
        "content_extraction": "camara_proyectos_ley_registry",
        "parsed_content": True,
        "legislative_registry": "camara_proyectos_ley",
        "registry_detail_url": detail_url,
        "project_label": project_label,
        "project_records": records,
        "project_identity_status": "clean",
        "has_clean_project_identity": True,
        "bill_title": title,
        "short_title": short_title,
        "status": status,
        "commission": commission,
        "authors": ", ".join(p for p in [authors, other_authors] if p),
        "legislature": str(row.get("vigencia") or ""),
        "origin": str(row.get("origen") or ""),
        "bill_type": str(row.get("tipo") or ""),
    }
    if object_text:
        metadata["object"] = object_text
    if publication_links:
        metadata["publication_links"] = publication_links
    return RawItem(
        id=_make_id(source.id, detail_url, project_label),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=detail_url,
        title=f"Cámara registry — {project_label} — {display_title}",
        fetched_at=fetched_at,
        published_at=published_at,
        raw_text=". ".join(part for part in evidence_parts if part),
        metadata=metadata,
    )


def _fetch_camara_proyectos_ley_registry(
    source: Metasource,
    client: httpx.Client,
    home_html: str,
    fetched_at: str,
) -> list[RawItem]:
    nonce = _extract_camara_pl_nonce(home_html)
    if not nonce:
        raise ValueError("Camara proyectos page missing PL_NONCE")
    legislature = _extract_camara_legislature_id(
        home_html,
        _current_legislature_label(),
    )
    limit = source.max_items or LEGISLATIVE_REGISTRY_DEFAULT_LIMIT
    ajax_url = urljoin(source.url, "/wp-admin/admin-ajax.php")
    response = _http_post_form(
        client,
        ajax_url,
        {
            "action": "get_proyectos_ley_page",
            "_ajax_nonce": nonce,
            "page": "1",
            "per_page": str(limit),
            "term": "",
            "comision": "",
            "tipo": "All",
            "estado": "All",
            "origen": "All",
            "legislatura": legislature,
            "ley_numero": "",
            "ley_fecha": "",
            "comision_adv": "All",
        },
    )
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    rows = data.get("items") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("unexpected Camara proyectos payload")
    items: list[RawItem] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        link_web = normalize_whitespace(str(row.get("link_web") or ""))
        if not link_web:
            continue
        split = urlsplit(source.url)
        site_root = f"{split.scheme}://{split.netloc}/"
        detail_url = urljoin(site_root, link_web)
        detail = _http_get(client, detail_url)
        item = _camara_registry_row_to_item(
            row,
            detail.text,
            source=source,
            fetched_at=fetched_at,
            detail_url=detail_url,
        )
        if item is not None:
            items.append(item)
    return items


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


def fetch_rss(source: Metasource, client: httpx.Client) -> list[RawItem]:
    fetched_at = _now_iso()
    response = _http_get(client, source.url)
    marker = _detect_bot_block(response.text)
    if marker:
        raise BotBlockError(f"bot block detected: {marker}")
    parsed = feedparser.parse(response.content)
    items = _parse_rss_entries(parsed, source, fetched_at)
    if not items:
        items = _recover_rss_entries(response.text, source, fetched_at)

    if source.id == "eltiempo_colombia":
        augmented = _augment_eltiempo_colombia_rss_items(
            source,
            client,
            fetched_at,
            items,
        )
        if augmented:
            return augmented

    if items:
        return items

    html_fallback = _extract_dated_anchors(
        response.text,
        str(response.url),
        source,
        fetched_at,
        "rss_html_dated_anchor_fallback",
    )
    if html_fallback:
        return html_fallback

    if getattr(parsed, "bozo", False):
        raise RssParseError(
            f"feed parse error: {getattr(parsed, 'bozo_exception', 'unknown')}"
        )
    return []


def fetch_html(source: Metasource, client: httpx.Client) -> list[RawItem]:
    fetched_at = _now_iso()
    if source.id == "minhacienda_tes_reports" and "irc.gov.co" in source.url:
        return _fetch_minhacienda_tes_reports_with_browser(
            source,
            fetched_at,
            max_items=source.max_items
            if source.max_items is not None
            else MINHACIENDA_TES_PARSE_LIMIT,
        )
    response = _http_get(client, source.url)
    marker = _detect_bot_block(response.text)
    if marker:
        if source.id == "minhacienda_tes_reports":
            browser_items = _fetch_minhacienda_tes_reports_with_browser(
                source,
                fetched_at,
                max_items=source.max_items
                if source.max_items is not None
                else MINHACIENDA_TES_PARSE_LIMIT,
            )
            if browser_items:
                return browser_items
        raise BotBlockError(f"bot block detected: {marker}")
    if _detect_spa_shell(response.text):
        raise DynamicShellError(
            "page is a JS app shell with no static content; "
            "needs a headless renderer or a different URL"
        )
    if source.id == "senado_leyes_registry":
        return _fetch_senado_leyes_registry(source, client, fetched_at)
    if source.id == "camara_proyectos_ley_registry":
        return _fetch_camara_proyectos_ley_registry(
            source,
            client,
            response.text,
            fetched_at,
        )
    if source.id == "dane_comunicados_prensa":
        items = _extract_dane_comunicados(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return _enrich_pdf_text(items, client)
    if source.id == "mincit_zonas_francas":
        items = _extract_dated_anchors(
            response.text,
            str(response.url),
            source,
            fetched_at,
            "anchor",
            require_date=False,
        )
        if items:
            return _enrich_mincit_zonas_francas(items, client)
    if source.id == "senado_agenda_legislativa":
        items = _extract_dated_anchors(
            response.text,
            str(response.url),
            source,
            fetched_at,
            "anchor",
            require_date=False,
        )
        if items:
            return _enrich_senado_agenda_pdfs(items, client)
    if source.id == "dane_icoced":
        items = _extract_dane_icoced(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return _enrich_dane_icoced_xlsx(
                items,
                client,
                max_items=source.max_items,
            )
    if source.id == "corte_constitucional_comunicados":
        items = _extract_corte_comunicados(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return items
    if source.id == "dian_proyectos_normas":
        items = _extract_dian_regulatory_project_links(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return items
    if source.id == "minhacienda_tes_reports":
        items = _extract_minhacienda_tes_reports(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return _enrich_minhacienda_tes_reports(
                items,
                client,
                max_items=source.max_items
                if source.max_items is not None
                else MINHACIENDA_TES_PARSE_LIMIT,
            )
    if source.id == "banrep_junta_comunicados":
        items = _extract_dated_anchors(
            response.text,
            str(response.url),
            source,
            fetched_at,
            "anchor",
            require_date=False,
        )
        if items:
            return _enrich_banrep_minutas_html(items, client)
    if source.id == "diario_oficial":
        items = _extract_imprenta_jsf_table(
            response.text,
            str(response.url),
            source,
            fetched_at,
            edition_label="Diario Oficial",
            query_param="edicion",
        )
        if items:
            return _enrich_diario_oficial_pdfs(
                items,
                client,
                response.text,
                str(response.url),
                max_items=source.max_items
                if source.max_items is not None
                else PDF_TEXT_PARSE_LIMIT,
            )
    if source.id == "gacetas_congreso":
        items = _extract_imprenta_jsf_table(
            response.text,
            str(response.url),
            source,
            fetched_at,
            edition_label="Gaceta del Congreso",
            query_param="gaceta",
        )
        if items:
            return _enrich_gaceta_pdfs(items, client, response.text, str(response.url))
    items = _extract_dated_anchors(
        response.text,
        str(response.url),
        source,
        fetched_at,
        "anchor",
        require_date=False,
    )
    if source.id in {"gestor_normativo_fp", "suin_juriscol", "suin_juriscol_normas"}:
        return _annotate_legal_identity_items(items)
    return items


def _cap_items(source: Metasource, items: list[RawItem]) -> list[RawItem]:
    if source.max_items is None or source.max_items < 0:
        return items
    if len(items) <= source.max_items:
        return items
    logger.info("Capping %s items: %d -> %d", source.id, len(items), source.max_items)
    return items[: source.max_items]


def _fetch_one(source: Metasource, client: httpx.Client) -> list[RawItem]:
    if source.fetch_method == "rss":
        return fetch_rss(source, client)
    if source.fetch_method == "html":
        return fetch_html(source, client)
    if source.fetch_method == "api":
        return fetch_api(source, client)
    raise ValueError(f"unsupported fetch_method: {source.fetch_method}")


def fetch_all(
    sources: list[Metasource],
    client: httpx.Client | None = None,
) -> tuple[list[RawItem], list[SourceFailure]]:
    items: list[RawItem] = []
    failures: list[SourceFailure] = []

    owns_client = client is None
    if client is None:
        client = httpx.Client(
            timeout=HTTP_TIMEOUT,
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
        )

    try:
        for source in sources:
            source_client = client
            source_owns_client = False
            if source.verify_ssl is False:
                source_client = httpx.Client(
                    timeout=HTTP_TIMEOUT,
                    follow_redirects=True,
                    verify=False,
                    headers=DEFAULT_HEADERS,
                )
                source_owns_client = True
            try:
                fetched = _cap_items(source, _fetch_one(source, source_client))
                items.extend(fetched)
                logger.info("Fetched %d items from %s", len(fetched), source.id)
            except Exception as exc:  # noqa: BLE001 — boundary catch by design
                failures.append(
                    SourceFailure(
                        source_id=source.id,
                        source_name=source.name,
                        url=source.url,
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        occurred_at=_now_iso(),
                    )
                )
                logger.warning(
                    "FAILED %s: %s: %s",
                    source.id,
                    exc.__class__.__name__,
                    exc,
                )
            finally:
                if source_owns_client:
                    source_client.close()
    finally:
        if owns_client:
            client.close()

    return items, failures
