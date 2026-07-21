from __future__ import annotations

from urllib.parse import parse_qs

from .common import *
from .pdf import *


_CAMARA_AGENDA_PROJECT_RE = re.compile(
    r"\bProyecto\s+de\s+(?P<kind>Ley|Acto\s+Legislativo)\s+"
    r"(?:No\.?|N[°ºo]\.?)\s*"
    r"(?P<first_number>\d{1,4})\s+(?:de|del)\s+"
    r"(?P<first_year>\d{4})\s+(?P<first_chamber>Senado|C[aá]mara)"
    r"(?:\s*(?:[-–—]|,|;)?\s*(?P<second_number>\d{1,4})\s+"
    r"(?:de|del)\s+"
    r"(?P<second_year>\d{4})\s+(?P<second_chamber>Senado|C[aá]mara))?",
    re.IGNORECASE,
)


def _normalize_camara_agenda_text_for_matching(text: str) -> str:
    normalized = normalize_whitespace(text)
    replacements = (
        (r"Pr\s*oyecto", "Proyecto"),
        (r"Proyec\s*to", "Proyecto"),
        (r"ProyectodeLeyNo", "Proyecto de Ley No. "),
        (r"Proyectode\s*Ley", "Proyecto de Ley"),
        (r"Proyecto\s*deLey", "Proyecto de Ley"),
        (r"No\.?\s*C[aá]mara", "No. Cámara"),
        (r"No\.?\s*de\s*C[aá]mara", "No. Cámara"),
        (r"No\.?\s*SENADO", "No. Senado"),
        (r"No\.?\s*Senado", "No. Senado"),
        (r"deC[aá]mara", "de Cámara"),
        (r"delC[aá]mara", "de Cámara"),
        (r"delSenado", "de Senado"),
        (r"\bCAMARA\b", "Cámara"),
        (r"\bCÁMARA\b", "Cámara"),
        (r"\bSENADO\b", "Senado"),
        (r"Autores", " Autores"),
        (r"Autor:", " Autor:"),
        (r"Ponente", " Ponente"),
        (r"Publicaci", " Publicaci"),
        (r"Hora", " Hora"),
        (r"Lugar", " Lugar"),
        (r"TEMA:", " TEMA:"),
    )
    for pattern, replacement in replacements:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    return normalize_whitespace(normalized)


def _camara_year_from_iso(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value[:4])
    except ValueError:
        return None


def _normalize_camara_chamber(value: str | None) -> str:
    folded = fold_accents((value or "").lower())
    return "Cámara" if "camara" in folded else "Senado"


def _camara_project_records(match: re.Match[str]) -> list[dict[str, str]]:
    kind = normalize_whitespace(match.group("kind"))
    records = [
        {
            "kind": kind,
            "number": match.group("first_number"),
            "year": match.group("first_year"),
            "chamber": _normalize_camara_chamber(match.group("first_chamber")),
        }
    ]
    second_number = match.group("second_number")
    if second_number:
        records.append(
            {
                "kind": kind,
                "number": second_number,
                "year": match.group("second_year"),
                "chamber": _normalize_camara_chamber(match.group("second_chamber")),
            }
        )
    return records


def _camara_project_label(match: re.Match[str]) -> str:
    records = _camara_project_records(match)
    kind = records[0]["kind"] if records else normalize_whitespace(match.group("kind"))
    labels = [
        f"{record['number']} de {record['year']} {record['chamber']}"
        for record in records
    ]
    return f"Proyecto de {kind} {' / '.join(labels)}"


def _camara_agenda_action(context: str) -> str:
    folded = fold_accents(context.lower())
    if "primer debate" in folded:
        return "primer debate"
    if "segundo debate" in folded:
        return "segundo debate"
    if "tercer debate" in folded:
        return "tercer debate"
    if "cuarto debate" in folded:
        return "cuarto debate"
    if "votacion" in folded:
        return "votacion"
    if "ponencia" in folded:
        return "ponencia"
    if "audiencia" in folded:
        return "audiencia"
    if "discusion" in folded:
        return "discusion"
    return "agenda legislativa"


def _camara_document_title(context: str) -> str:
    quote_match = re.search(r"[“\"]([^”\"]{24,220})[”\"]", context)
    if quote_match:
        return normalize_whitespace(quote_match.group(1)).strip(" .,:;-")
    tema_match = re.search(
        r"\bTEMA:\s*(.{24,260}?)(?:\bAutores?:|\bPublicaci[oó]n|$)",
        context,
        re.IGNORECASE,
    )
    if tema_match:
        title = re.split(
            r"\bProyecto\s+de\s+(?:Ley|Acto\s+Legislativo)\b",
            tema_match.group(1),
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        return normalize_whitespace(title).strip(" .,:;-")
    return ""


def _camara_project_identity_status(label: str, document_title: str) -> str:
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
    return "clean_project_identity"


def _camara_scheduled_date(
    text: str,
    position: int,
    default_year: int | None,
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


def _camara_follow_up_sources(
    agenda_item: RawItem,
    project_label: str,
) -> list[dict[str, str]]:
    return [
        {
            "source_id": "camara_proyectos_ley_registry",
            "source_name": "Cámara — Proyectos de Ley",
            "url": "https://www.camara.gov.co/proyectos-de-ley/",
            "search_hint": project_label,
            "purpose": "Check official bill status, commission, authors, and publication links.",
        },
        {
            "source_id": "gacetas_congreso",
            "source_name": "Gacetas del Congreso — Imprenta Nacional",
            "url": "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
            "search_hint": project_label,
            "purpose": "Find ponencia, bill text, and later official publication records.",
        },
        {
            "source_id": agenda_item.source_id,
            "source_name": agenda_item.source_name,
            "url": agenda_item.metadata.get("source_page_url") or agenda_item.url,
            "search_hint": project_label,
            "purpose": "Check whether the item reappears or advances in a later agenda window.",
        },
    ]


def _camara_agenda_pdf_url_from_viewer_url(
    viewer_url: str,
    base_url: str,
) -> str | None:
    params = parse_qs(urlsplit(viewer_url).query)
    file_values = params.get("file")
    if not file_values:
        return None
    pdf_url = urljoin(base_url, file_values[0].strip())
    parts = urlsplit(pdf_url)
    if parts.scheme not in ("http", "https"):
        return None
    if parts.netloc.lower() != urlsplit(base_url).netloc.lower():
        return None
    if not parts.path.lower().endswith(".pdf"):
        return None
    return pdf_url


def _camara_agenda_pdf_title(iframe: Any, pdf_url: str) -> tuple[str, str]:
    agenda_title = normalize_whitespace(iframe.get("title", ""))
    if agenda_title:
        return f"Cámara agenda PDF — {agenda_title}", agenda_title
    filename = urlsplit(pdf_url).path.rsplit("/", 1)[-1].removesuffix(".pdf")
    agenda_title = normalize_whitespace(filename.replace("-", " "))
    if agenda_title:
        return f"Cámara agenda PDF — {agenda_title}", agenda_title
    return "Cámara agenda PDF", ""


def _extract_camara_agenda_pdf_links(
    html_text: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    soup = BeautifulSoup(html_text, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for iframe in soup.find_all("iframe"):
        viewer_url = iframe.get("data-src", "").strip()
        if not viewer_url:
            continue
        pdf_url = _camara_agenda_pdf_url_from_viewer_url(viewer_url, base_url)
        if pdf_url is None:
            continue
        canon = canonicalize_url(pdf_url)
        if canon in seen:
            continue
        seen.add(canon)

        title, agenda_title = _camara_agenda_pdf_title(iframe, pdf_url)
        published_at = _parse_date_text_to_iso(agenda_title or title)
        raw_text = normalize_whitespace(
            " ".join(
                part
                for part in (
                    title,
                    (
                        "Discovered from Cámara EmbedPress iframe data-src "
                        "file parameter."
                    ),
                    f"Source page: {base_url}.",
                    "PDF body not parsed.",
                )
                if part
            )
        )
        items.append(
            RawItem(
                id=_make_id(source.id, pdf_url, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=pdf_url,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata={
                    "extraction": "camara_agenda_embedpress_pdf_link",
                    "document_link_type": "agenda_pdf",
                    "pdf_discovery": "embedpress_iframe_data_src_file_param",
                    "source_page_url": base_url,
                    "embedpress_viewer_url": viewer_url,
                    "agenda_title": agenda_title,
                    "pdf_parse_status": "not_parsed",
                },
            )
        )
        if len(items) >= ANCHORS_PER_SOURCE:
            break
    return items


def _extract_camara_agenda_entries_from_text(
    agenda_item: RawItem,
    text: str,
    *,
    max_entries: int = CAMARA_AGENDA_ENTRY_LIMIT,
) -> list[RawItem]:
    match_text = _normalize_camara_agenda_text_for_matching(text)
    default_year = _camara_year_from_iso(agenda_item.published_at) or (
        _camara_year_from_iso(_parse_date_text_to_iso(agenda_item.title))
    )
    agenda_start = agenda_item.published_at
    entries: list[RawItem] = []
    seen_labels: set[str] = set()
    for match in _CAMARA_AGENDA_PROJECT_RE.finditer(match_text):
        label = _camara_project_label(match)
        if label in seen_labels:
            continue
        seen_labels.add(label)
        context_start = max(0, match.start() - 220)
        context_end = min(len(match_text), match.end() + 520)
        context = normalize_whitespace(match_text[context_start:context_end])
        scheduled_at = _camara_scheduled_date(
            match_text,
            match.start(),
            default_year,
        )
        action = _camara_agenda_action(context)
        document_title = _camara_document_title(context)
        identity_status = _camara_project_identity_status(label, document_title)
        date_label = (scheduled_at or agenda_start or "")[:10] or "agenda window"
        title = f"Cámara agenda {date_label} — {action}: {label}"
        if document_title:
            title = f"{title} — {document_title[:180]}"
        entry_url = f"{agenda_item.url}#project-{len(entries) + 1}"
        metadata = {
            key: value
            for key, value in dict(agenda_item.metadata).items()
            if key not in {"content_extraction_error", "pdf_parse_status"}
        }
        metadata.update(
            {
                "extraction": "camara_agenda_pdf_entry",
                "content_extraction": "camara_agenda_pdf",
                "document_row_type": "camara_agenda_item",
                "agenda_source_url": agenda_item.metadata.get("source_page_url"),
                "source_pdf_url": agenda_item.url,
                "agenda_pdf_url": agenda_item.url,
                "agenda_title": agenda_item.metadata.get("agenda_title")
                or agenda_item.title,
                "agenda_window_start": agenda_start,
                "scheduled_date": scheduled_at,
                "agenda_action_type": action,
                "project_label": label,
                "project_records": _camara_project_records(match),
                "document_title": document_title,
                "project_identity_status": identity_status,
                "has_clean_project_identity": (
                    identity_status == "clean_project_identity"
                ),
                "follow_up_sources": _camara_follow_up_sources(agenda_item, label),
                "pdf_text_chars": len(text),
            }
        )
        raw_text = (
            f"{title}. Extracted from official Cámara agenda PDF. "
            f"Agenda excerpt: {context}. Follow-up sources: Cámara registry, "
            f"Gacetas del Congreso, and later Cámara agenda records; "
            f"search hint: {label}."
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
                metadata=metadata,
            )
        )
        if len(entries) >= max_entries:
            break
    return entries


def _is_camara_agenda_pdf_item(item: RawItem) -> bool:
    return (
        item.source_id == "camara_agenda_consolidada"
        and item.metadata.get("document_link_type") == "agenda_pdf"
        and urlsplit(item.url).path.lower().endswith(".pdf")
    )


def _camara_agenda_link_with_error(
    item: RawItem,
    metadata: Mapping[str, Any],
) -> RawItem:
    return RawItem(
        id=item.id,
        source_id=item.source_id,
        source_name=item.source_name,
        source_type=item.source_type,
        url=item.url,
        title=item.title,
        fetched_at=item.fetched_at,
        published_at=item.published_at,
        raw_text=item.raw_text,
        metadata=dict(metadata),
    )


def _camara_agenda_parsed_document_without_projects(
    item: RawItem,
    text: str,
) -> RawItem:
    metadata = {
        key: value
        for key, value in dict(item.metadata).items()
        if key != "content_extraction_error"
    }
    metadata.update(
        {
            "content_extraction": "camara_agenda_pdf",
            "document_row_type": "camara_agenda_document",
            "pdf_parse_status": "parsed_no_legislative_entries",
            "pdf_text_chars": len(text),
        }
    )
    excerpt = normalize_whitespace(text)[:PDF_TEXT_EXCERPT_CHARS]
    raw_text = normalize_whitespace(
        f"{item.title}. PDF body parsed; no legislative project entries found. "
        f"Document excerpt: {excerpt}"
    )
    return RawItem(
        id=item.id,
        source_id=item.source_id,
        source_name=item.source_name,
        source_type=item.source_type,
        url=item.url,
        title=item.title,
        fetched_at=item.fetched_at,
        published_at=item.published_at,
        raw_text=raw_text,
        metadata=metadata,
    )


def _enrich_camara_agenda_pdfs(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int = CAMARA_AGENDA_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        if not _is_camara_agenda_pdf_item(item) or parsed_count >= max_items:
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            text = _extract_pdf_text_with_pdfplumber(
                response.content,
                max_chars=PDF_TEXT_FULL_CHARS,
            )
        except Exception as exc:  # noqa: BLE001 - preserve link-level item
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(_camara_agenda_link_with_error(item, metadata))
            parsed_count += 1
            continue
        entries = _extract_camara_agenda_entries_from_text(item, text)
        if entries:
            enriched.extend(entries)
        else:
            enriched.append(_camara_agenda_parsed_document_without_projects(item, text))
        parsed_count += 1
    return enriched


__all__ = [name for name in globals() if not name.startswith("__")]
