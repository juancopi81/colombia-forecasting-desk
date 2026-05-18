from __future__ import annotations

from .common import *
from .dane import _to_float
from .pdf import *

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




__all__ = [name for name in globals() if not name.startswith("__")]
