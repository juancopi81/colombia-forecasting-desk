from __future__ import annotations

from dataclasses import replace
from urllib.parse import parse_qsl, urlencode, urlunsplit

from .common import *
from .html import *


DANE_PUBLICATION_CALENDAR_SOURCE_ID = "dane_publication_calendar"
DANE_PUBLICATION_CALENDAR_PAGE_URL = "https://www.dane.gov.co/index.php/calendario"
DANE_PUBLICATION_CALENDAR_HORIZON_DAYS = 60
DANE_PUBLICATION_CALENDAR_TIMEZONE = "America/Bogota"
DANE_PUBLICATION_CALENDAR_OFFSET = timezone(timedelta(hours=-5))
DANE_PUBLICATION_CALENDAR_MONTHS = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}
DANE_PUBLICATION_CALENDAR_TITLE_RE = re.compile(
    r"^\s*(\d{1,2})\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+(\d{4})\s+"
    r"(\d{1,2}):(\d{2})\s*:\s*(.+?)\s*$"
)
DANE_PUBLICATION_CALENDAR_RELEASE_PATTERNS = (
    ("ipc", r"\b(?:ipc|indice de precios al consumidor)\b"),
    ("pib", r"\b(?:pib|producto interno bruto)\b"),
    ("ise", r"\b(?:ise|indicador de seguimiento a la economia)\b"),
    (
        "labor",
        r"\b(?:geih|mercado laboral|gran encuesta integrada de hogares)\b",
    ),
    (
        "icoced",
        r"\b(?:icoced|indice de costos de la construccion de edificaciones)\b",
    ),
    (
        "emmet",
        r"\b(?:emmet|encuesta mensual manufacturera con enfoque territorial)\b",
    ),
    (
        "services_trade",
        r"\b(?:emces|encuesta mensual de comercio exterior de servicios)\b",
    ),
    (
        "retail",
        r"\b(?:emc\b|encuesta mensual de comercio\b(?!\s+exterior)|"
        r"comercio minorista\b|"
        r"ventas (?:reales )?del comercio minorista\b)",
    ),
    ("imports", r"\bimportaciones\b"),
    ("exports", r"\bexportaciones\b"),
    (
        "construction_licenses",
        r"\b(?:elic|licencias? de construccion|"
        r"estadisticas de edificacion licencias de construccion)\b",
    ),
)


def _parse_dane_calendar_title(title: str) -> tuple[datetime, str] | None:
    match = DANE_PUBLICATION_CALENDAR_TITLE_RE.match(title)
    if match is None:
        return None
    day_s, month_s, year_s, hour_s, minute_s, release_title = match.groups()
    month = DANE_PUBLICATION_CALENDAR_MONTHS.get(fold_accents(month_s.lower()))
    if month is None:
        return None
    try:
        scheduled_at = datetime(
            int(year_s),
            month,
            int(day_s),
            int(hour_s),
            int(minute_s),
            tzinfo=DANE_PUBLICATION_CALENDAR_OFFSET,
        )
    except ValueError:
        return None
    return scheduled_at, normalize_whitespace(release_title)


def _dane_calendar_release_family(release_title: str) -> str | None:
    folded = fold_accents(release_title.lower())
    for family, pattern in DANE_PUBLICATION_CALENDAR_RELEASE_PATTERNS:
        if re.search(pattern, folded):
            return family
    return None


def _parse_dane_calendar_reference_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _dane_calendar_operation_url(
    description: str,
    calendar_event_url: str,
) -> str | None:
    soup = BeautifulSoup(description, "html.parser")
    for link in soup.find_all("a", href=True):
        resolved = urljoin(calendar_event_url, str(link["href"]).strip())
        parsed = urlsplit(resolved)
        hostname = (parsed.hostname or "").lower()
        if hostname != "dane.gov.co" and not hostname.endswith(".dane.gov.co"):
            continue
        label = fold_accents(
            normalize_whitespace(
                " ".join(
                    [
                        str(link.get("title") or ""),
                        link.get_text(separator=" ", strip=True),
                    ]
                )
            ).lower()
        )
        if (
            "consultar operacion estadistica" in label
            or "/estadisticas-por-tema/" in parsed.path.lower()
        ):
            return resolved
    return None


def _transform_dane_publication_calendar_items(
    items: list[RawItem],
    source: Metasource,
    fetched_at: str,
    *,
    apply_limit: bool = True,
    include_past: bool = False,
) -> list[RawItem]:
    fetched_at_dt = _parse_dane_calendar_reference_time(fetched_at)
    if fetched_at_dt is None:
        return []

    calendar_items: list[RawItem] = []
    for item in items:
        parsed_title = _parse_dane_calendar_title(item.title)
        if parsed_title is None:
            continue
        scheduled_at, release_title = parsed_title
        release_family = _dane_calendar_release_family(release_title)
        if release_family is None or (not include_past and scheduled_at < fetched_at_dt):
            continue

        scheduled_at_iso = scheduled_at.isoformat()
        metadata = {
            **item.metadata,
            "content_extraction": item.metadata.get("content_extraction", "dane_publication_calendar_rss"),
            "event_type": "dane_economic_release",
            "calendar": DANE_PUBLICATION_CALENDAR_SOURCE_ID,
            "calendar_event_url": item.url,
            "release_family": release_family,
            "scheduled_date": scheduled_at.date().isoformat(),
            "scheduled_at_local": scheduled_at_iso,
            "timezone": DANE_PUBLICATION_CALENDAR_TIMEZONE,
            "feed_published_at": item.published_at,
        }
        operation_url = _dane_calendar_operation_url(item.raw_text, item.url)
        if operation_url is not None:
            metadata["operation_url"] = operation_url
        calendar_items.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=scheduled_at_iso,
                raw_text=item.raw_text,
                metadata=metadata,
            )
        )

    calendar_items.sort(key=lambda item: item.published_at or "")
    if apply_limit and source.max_items is not None and source.max_items >= 0:
        return calendar_items[: source.max_items]
    return calendar_items


def _dane_annual_link(soup: BeautifulSoup, base_url: str, year: int) -> str | None:
    for link in soup.select("a[href]"):
        url = urljoin(base_url, link["href"])
        parts = urlsplit(url)
        if parts.hostname not in {"www.dane.gov.co", "dane.gov.co"}:
            continue
        if re.fullmatch(rf"/index.php/calendario/year\.listevents/{year}/\d{{2}}/\d{{2}}/-", parts.path):
            # The official annual page advertises limit=0 as its Todas option.
            query = [(k, v) for k, v in parse_qsl(parts.query) if k not in {"limit", "start"}]
            return urlunsplit(parts._replace(query=urlencode(query + [("limit", "0")])))
    return None


def _parse_dane_annual_calendar(
    html_text: str, url: str, source: Metasource, fetched_at: str,
) -> tuple[list[RawItem], BeautifulSoup, list[str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    body = soup.select_one("#jevents_body")
    if body is None or _detect_bot_block(html_text):
        raise RssParseError("DANE annual calendar body unavailable")
    event_links = body.select('a.ev_link_row[href*="icalrepeat.detail"]')
    if not event_links:
        raise RssParseError("DANE annual calendar has no verifiable event tables; coverage is unknown")
    notices: list[str] = []
    if any(re.fullmatch(r"[1-9]\d*", dict(parse_qsl(urlsplit(a["href"]).query)).get("start", "0"))
           for a in body.select('a[href*="year.listevents"]')):
        notices.append("DANE annual calendar is still paginated; coverage is incomplete.")
    items = []
    month_labels = {v: k for k, v in DANE_PUBLICATION_CALENDAR_MONTHS.items() if k != "sept"}
    for link in event_links:
        title = normalize_whitespace(link.get_text(" ", strip=True))
        if _dane_calendar_release_family(title) is None:
            continue
        event_url = urljoin(url, link["href"])
        parts = urlsplit(event_url)
        date_match = re.search(r"/icalrepeat\.detail/(\d{4})/(\d{2})/(\d{2})/", parts.path)
        table = link.find_parent("table")
        row = table.find("tr") if table else None
        caption = normalize_whitespace(row.get_text(" ", strip=True)) if row else ""
        clock = re.search(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*(am|pm)\b", fold_accents(caption), re.I)
        if parts.hostname not in {"www.dane.gov.co", "dane.gov.co"} or not date_match or not clock:
            notices.append("DANE annual calendar contains an economic event without a verified local clock.")
            continue
        day, month_name, year, hour, minute, meridiem = clock.groups()
        month = MONTHS_ES.get(month_name.lower())
        try:
            if not month or not 1 <= int(hour) <= 12:
                raise ValueError("invalid local clock")
            scheduled = datetime(int(year), month, int(day), int(hour) % 12 + (12 if meridiem.lower() == "pm" else 0), int(minute), tzinfo=DANE_PUBLICATION_CALENDAR_OFFSET)
            if (scheduled.year, scheduled.month, scheduled.day) != tuple(map(int, date_match.groups())):
                raise ValueError("calendar clock and event URL disagree")
        except ValueError:
            notices.append("DANE annual calendar has a conflicting or invalid economic-event date.")
            continue
        rss_title = f"{scheduled.day:02} {month_labels[month].title()} {scheduled.year} {scheduled:%H:%M} : {title}"
        items.append(RawItem(
            id=_make_id(source.id, event_url, rss_title), source_id=source.id,
            source_name=source.name, source_type=source.type, url=event_url,
            title=rss_title, fetched_at=fetched_at, published_at=None,
            raw_text=f"{caption}: {title}. Official DANE annual publication calendar.",
            metadata={"content_extraction": "dane_publication_calendar_annual_html", "calendar_list_url": url},
        ))
    return items, soup, notices


def _fetch_dane_publication_calendar(source: Metasource, client: httpx.Client) -> list[RawItem]:
    fetched_at = _now_iso()
    now = _parse_dane_calendar_reference_time(fetched_at)
    if now is None:
        raise RssParseError("DANE calendar retrieval clock unavailable")
    local_now = now.astimezone(DANE_PUBLICATION_CALENDAR_OFFSET)
    horizon = local_now.date() + timedelta(days=DANE_PUBLICATION_CALENDAR_HORIZON_DAYS)
    notices: list[str] = []
    items: list[RawItem] = []
    checked: list[str] = []
    try:
        response = _http_get(client, source.url)
        if _detect_bot_block(response.text):
            raise BotBlockError("DANE RSS bot block")
        parsed = feedparser.parse(response.content)
        items = _parse_rss_entries(parsed, source, fetched_at) or _recover_rss_entries(response.text, source, fetched_at)
        if not items:
            notices.append("DANE RSS returned no event entries; calendar-list evidence is used separately.")
    except (httpx.HTTPError, BotBlockError, RssParseError) as exc:
        notices.append(f"DANE RSS unavailable ({type(exc).__name__}); calendar-list evidence is used separately.")
    try:
        response = _http_get(client, DANE_PUBLICATION_CALENDAR_PAGE_URL)
        annual_soup = BeautifulSoup(response.text, "html.parser")
        base_url = str(response.url)
        for year in range(local_now.year, horizon.year + 1):
            annual_url = _dane_annual_link(annual_soup, base_url, year)
            if annual_url is None:
                raise RssParseError(f"official annual navigation missing for {year}")
            response = _http_get(client, annual_url)
            if len(response.content) > 2_000_000:
                raise RssParseError("annual calendar exceeds bounded response size")
            annual_items, annual_soup, annual_notices = _parse_dane_annual_calendar(response.text, str(response.url), source, fetched_at)
            items.extend(annual_items)
            notices.extend(annual_notices)
            checked.append(str(response.url))
            base_url = str(response.url)
    except (httpx.HTTPError, BotBlockError, RssParseError) as exc:
        notices.append(f"DANE future-calendar collection incomplete ({type(exc).__name__}: {exc}). Missing events remain unknown.")

    transformed = _transform_dane_publication_calendar_items(items, source, fetched_at, apply_limit=False, include_past=True)
    merged: dict[str, RawItem] = {}
    conflicted: set[str] = set()
    for item in transformed:
        identity = canonicalize_url(item.url)
        if identity in conflicted:
            continue
        prior = merged.get(identity)
        if prior is not None:
            prior_title = _parse_dane_calendar_title(prior.title)[1]
            title = _parse_dane_calendar_title(item.title)[1]
            if prior.published_at != item.published_at or fold_accents(prior_title.lower()) != fold_accents(title.lower()):
                conflicted.add(identity)
                del merged[identity]
                notices.append(f"DANE calendar sources disagree for {identity}; event withheld.")
            # RSS is first and retains its operation URL and existing item ID.
            continue
        merged[identity] = item
    result = sorted((item for item in merged.values()
                     if datetime.fromisoformat(item.published_at) >= now
                     and item.metadata["scheduled_date"] <= horizon.isoformat()),
                    key=lambda item: (item.published_at or "", item.url))
    found = len(result)
    if source.max_items is not None and source.max_items >= 0 and found > source.max_items:
        result = result[:source.max_items]
        notices.append(f"DANE calendar output truncated to {source.max_items} of {found} economic events in the {DANE_PUBLICATION_CALENDAR_HORIZON_DAYS}-day window; coverage is incomplete.")
    if not result and notices:
        raise RssParseError("; ".join(sorted(set(notices))))
    coverage = {"calendar_coverage": "incomplete" if notices else "bounded_official_calendar",
                "calendar_coverage_notices": sorted(set(notices)),
                "calendar_checked_urls": checked, "calendar_horizon_end": horizon.isoformat(),
                "calendar_events_before_limit": found}
    return [replace(item, metadata={**item.metadata, **coverage}) for item in result]


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
    if source.id == DANE_PUBLICATION_CALENDAR_SOURCE_ID:
        return _fetch_dane_publication_calendar(source, client)
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
