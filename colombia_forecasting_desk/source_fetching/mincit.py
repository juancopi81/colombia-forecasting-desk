from __future__ import annotations

from .common import *
from .html import *
from .pdf import *

MINCIT_ZF_APPROVED_REGISTRY = "mincit_zonas_francas_aprobadas"
MINCIT_ZF_APPROVED_EXTRACTION = "mincit_zonas_francas_approved_pdf"
MINCIT_ZF_TEXT_MAX_CHARS = 120_000
_MINCIT_ZF_APPROVED_TITLE_RE = re.compile(
    r"\bzonas\s+francas\s+aprobadas\b",
    re.IGNORECASE,
)
_MINCIT_ZF_SNAPSHOT_DATE_RE = re.compile(
    r"\bFECHA\s*:?\s*(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+DE\s+(\d{4})\b",
    re.IGNORECASE,
)
_MINCIT_ZF_SOURCE_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
_MINCIT_ZF_RESOLUTION_RE = re.compile(
    r"\bR(?:es|e)\.?\s*(?:No\.?\s*)?\d+.*?\b\d{4}\b",
    re.IGNORECASE,
)
_MINCIT_ZF_CLASS_RE = re.compile(
    r"\b(Permanente\s+Especial|Permanente)\b",
    re.IGNORECASE,
)
_MINCIT_ZF_DEPARTMENTS = (
    "Archipiélago de San Andrés, Providencia y Santa Catalina",
    "Archipielago de San Andres, Providencia y Santa Catalina",
    "Norte de Santander",
    "Valle del Cauca",
    "Cundinamarca",
    "Bogotá",
    "Bogota",
    "Antioquia",
    "Atlántico",
    "Atlantico",
    "Bolívar",
    "Bolivar",
    "Boyacá",
    "Boyaca",
    "Caldas",
    "Caquetá",
    "Caqueta",
    "Casanare",
    "Cauca",
    "Cesar",
    "Chocó",
    "Choco",
    "Córdoba",
    "Cordoba",
    "Guainía",
    "Guainia",
    "Guaviare",
    "Huila",
    "La Guajira",
    "Magdalena",
    "Meta",
    "Nariño",
    "Narino",
    "Quindío",
    "Quindio",
    "Risaralda",
    "Santander",
    "Sucre",
    "Tolima",
    "Arauca",
    "Amazonas",
    "Putumayo",
    "Vaupés",
    "Vaupes",
    "Vichada",
)
_MINCIT_ZF_DEPARTMENT_PATTERNS = tuple(
    (
        dep,
        re.compile(rf"\s({re.escape(dep)})(?=\s+)", re.IGNORECASE),
    )
    for dep in sorted(_MINCIT_ZF_DEPARTMENTS, key=len, reverse=True)
)
_MINCIT_DIARIO_OFICIAL_URL = "https://svrpubindc.imprenta.gov.co/diario/index.xhtml"
_MINCIT_PRESS_URL = "https://www.mincit.gov.co/prensa/noticias"
_SUIN_URL = "https://www.suin-juriscol.gov.co/"
_GESTOR_NORMATIVO_URL = "https://www.funcionpublica.gov.co/eva/gestornormativo/"



def _mincit_zf_is_approved_pdf_item(item: RawItem) -> bool:
    path = urlsplit(item.url).path.lower()
    title = fold_accents(item.title.lower())
    return ".pdf" in path and bool(_MINCIT_ZF_APPROVED_TITLE_RE.search(title))


def _mincit_zf_snapshot_date(text: str) -> str | None:
    match = _MINCIT_ZF_SNAPSHOT_DATE_RE.search(text)
    if match:
        day, month_name, year = match.groups()
        month = MONTHS_ES.get(fold_accents(month_name.lower()))
        if month is not None:
            return _date_to_iso(int(year), month, int(day))
    return None


def _mincit_zf_source_report_date(text: str) -> str | None:
    match = _MINCIT_ZF_SOURCE_DATE_RE.search(text)
    if not match:
        return None
    day, month, year = (int(part) for part in match.groups())
    return _date_to_iso(year, month, day)


def _normalize_mincit_zf_text(text: str) -> str:
    normalized = normalize_whitespace(text)
    # The PDF occasionally splits 9-digit NITs as "90113504 8" at cell breaks.
    normalized = re.sub(
        r"\b(\d{8})\s+(\d)\s+(?=(?:Zona|Centro|ZFB)\b)",
        r"\1\2 ",
        normalized,
    )
    return normalized


def _mincit_zf_row_slices(text: str) -> list[tuple[str, str]]:
    normalized = _normalize_mincit_zf_text(text)
    start = normalized.find("NIT NOMBRE")
    if start != -1:
        normalized = normalized[start:]
    matches = list(re.finditer(r"\b(?P<nit>\d{8,10})\s+", normalized))
    rows: list[tuple[str, str]] = []
    for idx, match in enumerate(matches):
        body_start = match.end()
        body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(normalized)
        body = normalize_whitespace(normalized[body_start:body_end])
        body = re.split(
            r"\s+(?:Fuente:\s*Ministerio|SESI[ÓO]N|RESUMEN\s+ZONAS\s+FRANCAS|"
            r"ZONAS\s+FRANCAS\s+PERMANENTES\s+ESPECIALES)",
            body,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        if "Res." not in body and "Re." not in body:
            continue
        rows.append((match.group("nit"), body))
    return rows


def _parse_mincit_zf_prefix(prefix: str) -> dict[str, str] | None:
    for department, pattern in _MINCIT_ZF_DEPARTMENT_PATTERNS:
        matches = list(pattern.finditer(prefix))
        for dep_match in reversed(matches):
            head = normalize_whitespace(prefix[: dep_match.start()])
            class_matches = list(_MINCIT_ZF_CLASS_RE.finditer(head))
            if not class_matches:
                continue
            municipality = normalize_whitespace(prefix[dep_match.end() :])
            for class_match in reversed(class_matches):
                name = normalize_whitespace(head[: class_match.start()])
                zone_class = normalize_whitespace(class_match.group(1)).title()
                user_type = normalize_whitespace(head[class_match.end() :])
                if len(name) < 8 or not user_type or not municipality:
                    continue
                return {
                    "zona_franca_name": name,
                    "zone_class": zone_class,
                    "class": zone_class,
                    "user_type": user_type,
                    "department": normalize_whitespace(department),
                    "municipality": municipality,
                }
    return None


def _parse_mincit_zf_row_body(nit: str, body: str) -> dict[str, str] | None:
    ciiu_match = re.search(r"\s(?P<ciiu>\d{3,4})\s*$", body)
    if not ciiu_match:
        return None
    ciiu = ciiu_match.group("ciiu").zfill(4)
    without_ciiu = normalize_whitespace(body[: ciiu_match.start()])
    resolution_matches = list(_MINCIT_ZF_RESOLUTION_RE.finditer(without_ciiu))
    if not resolution_matches:
        return None
    first_resolution = resolution_matches[0]
    fields = _parse_mincit_zf_prefix(
        normalize_whitespace(without_ciiu[: first_resolution.start()])
    )
    if not fields:
        return None
    declaratory_resolution = normalize_whitespace(first_resolution.group(0))
    extension_resolution = ""
    if len(resolution_matches) > 1:
        extension_resolution = normalize_whitespace(resolution_matches[1].group(0))
    elif "Vacía" in without_ciiu[first_resolution.end() :]:
        extension_resolution = "Vacía"
    return {
        "nit": nit,
        **fields,
        "declaratory_resolution": declaratory_resolution,
        "extension_resolution": extension_resolution,
        "ciiu": ciiu,
    }


def _mincit_zf_follow_up_sources(row: Mapping[str, str]) -> list[dict[str, str]]:
    zone_name = row.get("zona_franca_name", "")
    resolution = row.get("declaratory_resolution", "")
    search_hint = normalize_whitespace(f"{zone_name} {resolution}")
    return [
        {
            "source_id": "mincit_prensa",
            "source_name": "MinCIT — Noticias",
            "url": _MINCIT_PRESS_URL,
            "search_hint": search_hint,
            "purpose": "Check whether MinCIT published a public-interest note about the named zone.",
        },
        {
            "source_id": "diario_oficial",
            "source_name": "Diario Oficial — Imprenta Nacional",
            "url": _MINCIT_DIARIO_OFICIAL_URL,
            "search_hint": search_hint,
            "purpose": "Verify official publication of the declaratory or extension resolution.",
        },
        {
            "source_id": "suin_juriscol",
            "source_name": "SUIN Juriscol",
            "url": _SUIN_URL,
            "search_hint": search_hint,
            "purpose": "Search for the final legal act by resolution number and zone name.",
        },
        {
            "source_id": "gestor_normativo_fp",
            "source_name": "Función Pública — Gestor Normativo",
            "url": _GESTOR_NORMATIVO_URL,
            "search_hint": search_hint,
            "purpose": "Secondary legal-resolution search by resolution number and zone name.",
        },
    ]


def _extract_mincit_zonas_francas_approved_rows_from_text(
    registry_item: RawItem,
    text: str,
) -> list[RawItem]:
    normalized = _normalize_mincit_zf_text(text)
    if "NIT NOMBRE" not in normalized or "ZONA FRANCA" not in normalized:
        return []
    snapshot_date = _mincit_zf_snapshot_date(normalized)
    source_report_date = _mincit_zf_source_report_date(normalized)
    published_at = registry_item.published_at or source_report_date or snapshot_date
    rows: list[RawItem] = []
    for index, (nit, body) in enumerate(_mincit_zf_row_slices(normalized), start=1):
        parsed = _parse_mincit_zf_row_body(nit, body)
        if not parsed:
            continue
        zone_name = parsed["zona_franca_name"]
        title = (
            "MinCIT Zonas Francas aprobadas — "
            f"{zone_name} — {parsed['municipality']}, {parsed['department']}"
        )
        if parsed.get("declaratory_resolution"):
            title = f"{title} — {parsed['declaratory_resolution']}"
        entry_url = f"{registry_item.url}#zf-{index}"
        follow_up_sources = _mincit_zf_follow_up_sources(parsed)
        metadata = {
            **dict(registry_item.metadata),
            "extraction": "mincit_zonas_francas_approved_pdf_row",
            "content_extraction": MINCIT_ZF_APPROVED_EXTRACTION,
            "registry": MINCIT_ZF_APPROVED_REGISTRY,
            "registry_row_type": "approved_zone",
            "registry_key": nit,
            "registry_pdf_url": registry_item.url,
            "registry_pdf_title": registry_item.title,
            "snapshot_date": snapshot_date,
            "source_report_date": source_report_date,
            "source_update_date": registry_item.published_at,
            "follow_up_sources": follow_up_sources,
            **parsed,
        }
        raw_text = (
            f"{title}. Official MinCIT approved-zones registry row. "
            f"Snapshot date: {(snapshot_date or '')[:10] or 'unknown'}. "
            f"Class: {parsed['zone_class']}; user type: {parsed['user_type']}; "
            f"NIT: {nit}; extension resolution: "
            f"{parsed.get('extension_resolution') or 'not listed'}; "
            f"CIIU: {parsed['ciiu']}."
        )
        rows.append(
            RawItem(
                id=_make_id(registry_item.source_id, entry_url, title),
                source_id=registry_item.source_id,
                source_name=registry_item.source_name,
                source_type=registry_item.source_type,
                url=entry_url,
                title=title,
                fetched_at=registry_item.fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata=metadata,
            )
        )
    return rows


def _enrich_mincit_zonas_francas(
    items: list[RawItem],
    client: httpx.Client,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    generic_pdf_items: list[RawItem] = []
    for item in items:
        if not _mincit_zf_is_approved_pdf_item(item):
            generic_pdf_items.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            text = _extract_pdf_text_objects_text(
                response.content,
                max_chars=MINCIT_ZF_TEXT_MAX_CHARS,
            )
            rows = _extract_mincit_zonas_francas_approved_rows_from_text(item, text)
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
            continue
        if not rows:
            metadata["content_extraction_error"] = "unable to parse approved-zones rows"
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
        enriched.extend(rows)
    if generic_pdf_items:
        enriched.extend(_enrich_pdf_text(generic_pdf_items, client))
    return enriched




__all__ = [name for name in globals() if not name.startswith("__")]
