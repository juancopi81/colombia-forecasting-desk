from __future__ import annotations

import hashlib
import html
import logging
import re
import time
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlsplit

import feedparser
import httpx
from bs4 import BeautifulSoup

from .cleaner import fold_accents, normalize_whitespace
from .dedupe import canonicalize_url
from .models import Metasource, RawItem, SourceFailure

logger = logging.getLogger(__name__)

USER_AGENT = "colombia-forecasting-desk/0.1"
HTTP_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
MAX_RETRIES = 2
BACKOFF_SECONDS = 1.0
ANCHORS_PER_SOURCE = 30
MIN_ANCHOR_TEXT = 10
DATE_CONTEXT_CHARS = 500

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


def _http_get(client: httpx.Client, url: str) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.get(url)
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


def fetch_rss(source: Metasource, client: httpx.Client) -> list[RawItem]:
    fetched_at = _now_iso()
    response = _http_get(client, source.url)
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
            headers={"User-Agent": USER_AGENT},
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
                    headers={"User-Agent": USER_AGENT},
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
