from __future__ import annotations

from .common import *
from .pdf import *

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




__all__ = [name for name in globals() if not name.startswith("__")]
