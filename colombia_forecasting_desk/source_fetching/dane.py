from __future__ import annotations

from .common import *
from .html import *

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




__all__ = [name for name in globals() if not name.startswith("__")]
