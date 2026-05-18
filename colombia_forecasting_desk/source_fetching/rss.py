from __future__ import annotations

from .common import *
from .html import *

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




__all__ = [name for name in globals() if not name.startswith("__")]
