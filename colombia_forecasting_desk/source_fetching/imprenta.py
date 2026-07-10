from __future__ import annotations

from .common import *
from .pdf import *
from .senado import _normalize_senado_chamber

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
_GACETA_PROJECT_START_RE = re.compile(
    r"\b(?:AL\s+)?PROYECTO\s+DE\s+(?:LEY|ACTO\s+LEGISLATIVO)\b",
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
        (r"pormediodelacual", "por medio de la cual "),
        (r"porlacual", "por la cual "),
        (r"porelcual", "por el cual "),
        (r"\bsemodifica\b", "se modifica"),
        (r"semodificalaley", "se modifica la Ley "),
        (r"\bysedictan\b", "y se dictan"),
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


def _gaceta_document_title_after_identity(text: str) -> str:
    identity = _GACETA_PROJECT_RECORD_RE.search(text)
    if identity is None:
        return ""
    trailing = text[identity.end() :]
    trailing = re.sub(r"^\s*(?:[,;:—-]+\s*)?", "", trailing)
    trailing = re.split(
        r"\s+P[aá]gina\b|\s+Gaceta\b",
        trailing,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    title = normalize_whitespace(trailing).strip(" .,:;-—")
    return title if _has_usable_gaceta_document_title(title) else ""


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
    elif project_records:
        document_title = _gaceta_document_title_after_identity(clean_text)
        if document_title:
            project_label = _gaceta_project_label_from_records(
                _gaceta_project_kind(clean_text),
                project_records,
            )

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


def _project_record_keys(parsed: dict[str, Any]) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    for record in parsed.get("project_records") or []:
        if not isinstance(record, dict):
            continue
        key = (
            str(record.get("number") or ""),
            str(record.get("year") or ""),
            str(record.get("chamber") or ""),
        )
        if all(key):
            keys.add(key)
    return keys


def _parse_gaceta_pdf_documents(item: RawItem, text: str) -> list[dict[str, Any]]:
    """Split unrelated project sections while preserving linked identities."""
    parsed = _parse_gaceta_pdf_text(item, text)
    if parsed is None:
        return []

    clean_text = _normalize_gaceta_identity_text(text)
    starts = list(_GACETA_PROJECT_START_RE.finditer(clean_text))
    if len(starts) <= 1:
        return [parsed]

    sections_by_keys: dict[
        tuple[tuple[str, str, str], ...],
        dict[str, Any],
    ] = {}
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(clean_text)
        section = _parse_gaceta_pdf_text(item, clean_text[start.start() : end])
        if section is None:
            continue
        keys = _project_record_keys(section)
        if not keys:
            continue
        sections_by_keys.setdefault(tuple(sorted(keys)), section)

    covered_keys: set[tuple[str, str, str]] = set()
    for keys in sections_by_keys:
        key_set = set(keys)
        if covered_keys & key_set:
            return [parsed]
        covered_keys.update(key_set)

    sections = list(sections_by_keys.values())
    if len(sections) < 2:
        return [parsed]
    return sections


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
        parsed_documents = _parse_gaceta_pdf_documents(item, text)
        if not parsed_documents:
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
        for split_index, parsed in enumerate(parsed_documents, start=1):
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
            child_metadata = dict(metadata)
            child_metadata.update(
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
            if len(parsed_documents) > 1:
                child_metadata.update(
                    {
                        "project_split_index": split_index,
                        "project_split_count": len(parsed_documents),
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
                    metadata=child_metadata,
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




__all__ = [name for name in globals() if not name.startswith("__")]
