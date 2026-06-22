from __future__ import annotations

from .common import *


def _registraduria_news_archive_url(source: Metasource) -> str:
    if "-Noticias-" in source.url or "-noticias-" in source.url:
        return REGISTRADURIA_NEWS_ARCHIVE_URL
    return source.url


def _extract_registraduria_news_cards(
    html_text: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
    *,
    source_access: str,
    max_items: int = REGISTRADURIA_NEWS_PARSE_LIMIT,
) -> list[RawItem]:
    soup = BeautifulSoup(html_text, "html.parser")
    cards = soup.select("li.newsmodule")
    items: list[RawItem] = []
    seen: set[str] = set()
    for card in cards:
        link = card.select_one("a.seemorenew[href]") or card.find("a", href=True)
        title_node = card.select_one(".titlepreview")
        date_node = card.select_one(".datenew")
        excerpt_node = card.select_one(".captionnew")
        if link is None or title_node is None:
            continue
        url = urljoin(base_url, link.get("href", "").strip())
        title = normalize_whitespace(title_node.get_text(" ", strip=True))
        if not title or len(title) < MIN_ANCHOR_TEXT:
            continue
        canon = canonicalize_url(url)
        if canon in seen:
            continue
        seen.add(canon)

        date_text = (
            normalize_whitespace(date_node.get_text(" ", strip=True))
            if date_node is not None
            else ""
        )
        excerpt = (
            normalize_whitespace(excerpt_node.get_text(" ", strip=True))
            if excerpt_node is not None
            else ""
        )
        number = normalize_whitespace(
            (card.select_one(".num-comunicado") or card).get_text(" ", strip=True)
        )
        number_match = re.search(r"\bNo\.\s*\d+\b", number, flags=re.IGNORECASE)
        comunicado_number = number_match.group(0) if number_match else ""
        published_at = _parse_date_text_to_iso(date_text)
        card_text = normalize_whitespace(
            " ".join(
                part
                for part in (comunicado_number, title, date_text, excerpt)
                if part
            )
        )
        metadata: dict[str, Any] = {
            "extraction": "registraduria_news_card",
            "content_extraction": "registraduria_news_card",
            "source_access": source_access,
            "source_page_url": base_url,
            "article_url": url,
        }
        if comunicado_number:
            metadata["comunicado_number"] = comunicado_number
        if excerpt:
            metadata["excerpt"] = excerpt
        if date_text:
            metadata["date_text"] = date_text

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
                raw_text=card_text or title,
                metadata=metadata,
            )
        )
        if len(items) >= max_items:
            break
    return items


def _registraduria_news_unexpected_page_reason(
    html_text: str,
    rendered_url: str,
) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    title_text = normalize_whitespace(
        " ".join(
            node.get_text(" ", strip=True)
            for node in soup.find_all(["title", "h1"], limit=3)
        )
    )
    folded_title = fold_accents(title_text.lower())
    if (
        "eleccion de presidente y vicepresidente" in folded_title
        and "2026" in folded_title
    ):
        return (
            "browser rendered Registraduria 2026 election microsite instead "
            f"of news archive: {rendered_url}"
        )
    return None


def _extract_registraduria_news_article_detail(
    html_text: str,
) -> dict[str, Any] | None:
    soup = BeautifulSoup(html_text, "html.parser")
    container = (
        soup.select_one(".maincollumn")
        or soup.find("article")
        or soup.find("main")
        or soup.body
        or soup
    )
    title_node = container.find(["h1", "h2"])
    title = (
        normalize_whitespace(title_node.get_text(" ", strip=True))
        if title_node is not None
        else ""
    )
    date_node = container.select_one(".date-news")
    date_text = (
        normalize_whitespace(date_node.get_text(" ", strip=True))
        if date_node is not None
        else ""
    )
    publication_date = _parse_date_text_to_iso(date_text)
    body_parts: list[str] = []
    seen_parts: set[str] = set()
    for node in container.find_all(["blockquote", "p"]):
        if node is date_node:
            continue
        text = normalize_whitespace(node.get_text(" ", strip=True))
        if not text or text == title or text == date_text:
            continue
        folded = fold_accents(text.lower())
        if folded in {"siguenos para mas noticias en google news"}:
            continue
        if folded in seen_parts:
            continue
        seen_parts.add(folded)
        body_parts.append(text)
    body_excerpt = normalize_whitespace(" ".join(body_parts))[
        :REGISTRADURIA_NEWS_BODY_CHARS
    ]
    if not title and not body_excerpt:
        return None
    detail: dict[str, Any] = {
        "content_extraction": "registraduria_news_article_html",
    }
    if title:
        detail["detail_title"] = title
    if date_text:
        detail["detail_date_text"] = date_text
    if publication_date:
        detail["publication_date"] = publication_date
    if body_excerpt:
        detail["body_excerpt"] = body_excerpt
    return detail


def _registraduria_item_with_detail(
    item: RawItem,
    detail: dict[str, Any],
) -> RawItem:
    metadata = dict(item.metadata)
    metadata.update(detail)
    detail_text = detail.get("body_excerpt")
    raw_text = item.raw_text
    if isinstance(detail_text, str) and detail_text:
        raw_text = normalize_whitespace(f"{item.raw_text} Article detail: {detail_text}")
    title = item.title
    detail_title = detail.get("detail_title")
    if isinstance(detail_title, str) and len(detail_title) > len(title):
        title = detail_title
    return RawItem(
        id=item.id,
        source_id=item.source_id,
        source_name=item.source_name,
        source_type=item.source_type,
        url=item.url,
        title=title,
        fetched_at=item.fetched_at,
        published_at=item.published_at or detail.get("publication_date"),
        raw_text=raw_text,
        metadata=metadata,
    )


def _enrich_registraduria_news_details_with_browser(
    items: list[RawItem],
    page: Any,
    *,
    max_items: int = REGISTRADURIA_NEWS_DETAIL_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        if parsed_count >= max_items:
            enriched.append(item)
            continue
        try:
            page.goto(
                item.url,
                wait_until="domcontentloaded",
                timeout=REGISTRADURIA_NEWS_BROWSER_TIMEOUT_MS,
            )
            try:
                from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

                try:
                    page.wait_for_load_state(
                        "networkidle",
                        timeout=REGISTRADURIA_NEWS_BROWSER_NETWORK_IDLE_MS,
                    )
                except PlaywrightTimeoutError:
                    pass
            except ImportError:
                pass
            html_text = page.content()
            marker = _detect_bot_block(html_text)
            if marker:
                enriched.append(item)
                continue
            detail = _extract_registraduria_news_article_detail(html_text)
        except Exception:
            enriched.append(item)
            continue
        if not detail:
            enriched.append(item)
            continue
        parsed_count += 1
        enriched.append(_registraduria_item_with_detail(item, detail))
    return enriched


def _fetch_registraduria_noticias_with_browser(
    source: Metasource,
    fetched_at: str,
    *,
    max_items: int = REGISTRADURIA_NEWS_PARSE_LIMIT,
) -> list[RawItem]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise DynamicShellError(
            "Registraduria news is Cloudflare-protected and requires the "
            "optional Playwright browser fetch path."
        ) from exc

    with sync_playwright() as playwright:
        launch_kwargs: dict[str, Any] = {
            "headless": True,
        }
        chrome_path = _chrome_executable_path()
        if chrome_path:
            launch_kwargs["executable_path"] = chrome_path
        browser = playwright.chromium.launch(**launch_kwargs)
        try:
            context = browser.new_context(
                locale="es-CO",
                user_agent=BROWSER_USER_AGENT,
            )
            page = context.new_page()
            archive_url = _registraduria_news_archive_url(source)
            page.goto(
                archive_url,
                wait_until="domcontentloaded",
                timeout=REGISTRADURIA_NEWS_BROWSER_TIMEOUT_MS,
            )
            try:
                page.wait_for_selector(
                    "li.newsmodule",
                    timeout=REGISTRADURIA_NEWS_BROWSER_SELECTOR_TIMEOUT_MS,
                )
            except PlaywrightTimeoutError:
                pass
            try:
                page.wait_for_load_state(
                    "networkidle",
                    timeout=REGISTRADURIA_NEWS_BROWSER_NETWORK_IDLE_MS,
                )
            except PlaywrightTimeoutError:
                pass
            html_text = page.content()
            marker = _detect_bot_block(html_text)
            if marker:
                raise BotBlockError(f"browser fetch still bot-blocked: {marker}")
            items = _extract_registraduria_news_cards(
                html_text,
                page.url,
                source,
                fetched_at,
                source_access="browser_official_html",
                max_items=max_items,
            )
            if not items:
                reason = _registraduria_news_unexpected_page_reason(
                    html_text,
                    page.url,
                )
                if reason:
                    raise DynamicShellError(reason)
                raise DynamicShellError("browser rendered no Registraduria news cards")
            return _enrich_registraduria_news_details_with_browser(items, page)
        finally:
            browser.close()


def _fetch_registraduria_noticias(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
    *,
    max_items: int = REGISTRADURIA_NEWS_PARSE_LIMIT,
) -> list[RawItem]:
    archive_url = _registraduria_news_archive_url(source)
    try:
        response = _http_get(client, archive_url)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in {403, 404}:
            return _fetch_registraduria_noticias_with_browser(
                source,
                fetched_at,
                max_items=max_items,
            )
        raise
    marker = _detect_bot_block(response.text)
    if marker:
        return _fetch_registraduria_noticias_with_browser(
            source,
            fetched_at,
            max_items=max_items,
        )
    items = _extract_registraduria_news_cards(
        response.text,
        str(response.url),
        source,
        fetched_at,
        source_access="official_html",
        max_items=max_items,
    )
    if items:
        return items
    return _fetch_registraduria_noticias_with_browser(
        source,
        fetched_at,
        max_items=max_items,
    )


__all__ = [name for name in globals() if not name.startswith("__")]
