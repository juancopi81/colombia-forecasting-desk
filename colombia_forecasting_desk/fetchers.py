from __future__ import annotations

import hashlib
import html
import logging
import re
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from urllib.parse import urljoin, urlsplit

import feedparser
import httpx
from bs4 import BeautifulSoup

from .cleaner import fold_accents, normalize_whitespace
from .dedupe import canonicalize_url
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
BOT_BLOCK_MARKERS = (
    "Radware Bot Manager",
    "validate.perfdrive.com",
    "Access denied",
    "Bot Manager Block",
)
SPA_SHELL_MARKERS = ("<app-root></app-root>", "<app-root>")

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

    match = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", compact)
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


_DATE_DDMMYYYY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


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
        title = f"{edition_label} {number}" + (f" — {kind}" if kind else "")
        synthetic_url = f"{base_url.rstrip('/')}?{query_param}={number}"
        canon = canonicalize_url(synthetic_url)
        if canon in seen:
            continue
        seen.add(canon)
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
                metadata={"extraction": "imprenta_nacional_jsf_table"},
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
        metadata={
            "extraction": "socrata_api",
            "dataset_url": source.url,
            "id_value": id_value,
        },
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
    if items:
        return items

    recovered = _recover_rss_entries(response.text, source, fetched_at)
    if recovered:
        return recovered

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
    response = _http_get(client, source.url)
    marker = _detect_bot_block(response.text)
    if marker:
        raise BotBlockError(f"bot block detected: {marker}")
    if _detect_spa_shell(response.text):
        raise DynamicShellError(
            "page is a JS app shell with no static content; "
            "needs a headless renderer or a different URL"
        )
    if source.id == "dane_comunicados_prensa":
        items = _extract_dane_comunicados(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return items
    if source.id == "corte_constitucional_comunicados":
        items = _extract_corte_comunicados(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return items
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
            return items
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
            return items
    return _extract_dated_anchors(
        response.text,
        str(response.url),
        source,
        fetched_at,
        "anchor",
        require_date=False,
    )


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
