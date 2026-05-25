from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Iterable

from .cleaner import normalize_whitespace
from .models import CleanedItem, RawItem
from .tagger import fold_accents

MINCIT_ZONAS_FRANCAS_SOURCE_ID = "mincit_zonas_francas"
MINCIT_ZF_APPROVED_REGISTRY = "mincit_zonas_francas_aprobadas"
MINCIT_ZF_DIFF_CONTENT = "mincit_zonas_francas_approved_diff"
MAX_ZONA_FRANCA_LEADS = 2
MAX_EVIDENCE_ITEMS = 4


@dataclass(frozen=True, slots=True)
class ZonaFrancaRecord:
    item_id: str
    source_id: str
    source_name: str
    title: str
    url: str
    published_at: str
    nit: str
    zone_name: str
    zone_class: str
    user_type: str
    department: str
    municipality: str
    declaratory_resolution: str
    extension_resolution: str
    ciiu: str
    change_type: str
    changed_fields: list[str]
    official_resolution_matches: list[dict[str, Any]]
    follow_up_sources: list[dict[str, Any]]


def build_zona_franca_land_use_leads(
    raw_items: Iterable[RawItem],
    cleaned_items: Iterable[CleanedItem],
    *,
    max_leads: int = MAX_ZONA_FRANCA_LEADS,
) -> list[dict[str, Any]]:
    """Build land-use/economic-development leads from MinCIT registry diffs."""
    records = _zona_franca_change_records(raw_items, cleaned_items)
    records = sorted(records, key=_record_sort_key, reverse=True)
    return [_lead(record) for record in records[:max_leads]]


def _zona_franca_change_records(
    raw_items: Iterable[RawItem],
    cleaned_items: Iterable[CleanedItem],
) -> list[ZonaFrancaRecord]:
    raw_by_id = {item.id: item for item in raw_items}
    records: list[ZonaFrancaRecord] = []
    for item in cleaned_items:
        if item.source_id != MINCIT_ZONAS_FRANCAS_SOURCE_ID or item.quality_notes:
            continue
        raw = raw_by_id.get(item.id)
        metadata = raw.metadata if raw else item.metadata
        if not isinstance(metadata, dict):
            continue
        if metadata.get("registry") != MINCIT_ZF_APPROVED_REGISTRY:
            continue
        if metadata.get("content_extraction") != MINCIT_ZF_DIFF_CONTENT:
            continue

        change_type = _first_text(metadata.get("registry_change_type"))
        zone_name = _first_text(metadata.get("zona_franca_name")) or item.title
        municipality = _first_text(metadata.get("municipality"))
        department = _first_text(metadata.get("department"))
        declaratory_resolution = _first_text(metadata.get("declaratory_resolution"))
        if not change_type or not zone_name or not municipality or not department:
            continue
        records.append(
            ZonaFrancaRecord(
                item_id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                title=item.title,
                url=item.url,
                published_at=item.published_at or "",
                nit=_first_text(metadata.get("nit"), metadata.get("registry_key")),
                zone_name=zone_name,
                zone_class=_first_text(metadata.get("zone_class")),
                user_type=_first_text(metadata.get("user_type")),
                department=department,
                municipality=municipality,
                declaratory_resolution=declaratory_resolution,
                extension_resolution=_first_text(metadata.get("extension_resolution")),
                ciiu=_first_text(metadata.get("ciiu")),
                change_type=change_type,
                changed_fields=[
                    _first_text(field)
                    for field in metadata.get("changed_fields") or []
                    if _first_text(field)
                ],
                official_resolution_matches=[
                    match
                    for match in metadata.get("official_resolution_matches") or []
                    if isinstance(match, dict)
                ],
                follow_up_sources=[
                    source
                    for source in metadata.get("follow_up_sources") or []
                    if isinstance(source, dict)
                ],
            )
        )
    return records


def _record_sort_key(record: ZonaFrancaRecord) -> tuple[int, int, str, str]:
    change_score = 2 if record.change_type == "new_registry_row" else 1
    match_score = 1 if record.official_resolution_matches else 0
    return (change_score, match_score, record.published_at, record.zone_name)


def _lead(record: ZonaFrancaRecord) -> dict[str, Any]:
    pattern = (
        "new_zona_franca_registry_row"
        if record.change_type == "new_registry_row"
        else "updated_zona_franca_registry_row"
    )
    item_ids = [record.item_id]
    urls = _unique(
        [
            record.url,
            *[
                _first_text(match.get("url"))
                for match in record.official_resolution_matches
            ],
        ]
    )
    return {
        "lead_id": _lead_id(
            "analyst_insight",
            f"{pattern}:{record.zone_name}:{record.nit}:{record.published_at}",
        ),
        "lead_type": "analyst_insight",
        "title": f"Zona franca land-use signal — {_trim(record.zone_name, 70)}",
        "claim_or_question": _claim(record),
        "disposition": "monitor_or_research",
        "evidence": _evidence(record),
        "caveats": _caveats(record),
        "next_check": _next_check(record),
        "source_refs": {
            "artifact_refs": [
                {
                    "artifact": "raw_items.json",
                    "key": "source_id",
                    "value": MINCIT_ZONAS_FRANCAS_SOURCE_ID,
                }
            ],
            "source_item_ids": item_ids,
            "source_urls": urls,
        },
        "review_context": {
            "family": "land_use_zona_franca",
            "pattern": pattern,
            "change_type": record.change_type,
            "changed_fields": record.changed_fields,
            "municipality": record.municipality,
            "department": record.department,
            "nit": record.nit,
            "official_resolution_match_count": len(record.official_resolution_matches),
        },
    }


def _claim(record: ZonaFrancaRecord) -> str:
    location = f"{record.municipality}, {record.department}"
    if record.change_type == "new_registry_row":
        return (
            "MinCIT's approved-zones registry added "
            f"{record.zone_name} in {location}, creating a named "
            "zona-franca land-use/economic-development signal worth tracking."
        )
    changed = ", ".join(record.changed_fields) or "registry fields"
    return (
        "MinCIT's approved-zones registry changed "
        f"{changed} for {record.zone_name} in {location}, which may alter the "
        "legal or economic context for that named zona franca."
    )


def _evidence(record: ZonaFrancaRecord) -> list[dict[str, str]]:
    evidence = [_registry_evidence(record)]
    for match in record.official_resolution_matches[: MAX_EVIDENCE_ITEMS - 1]:
        evidence.append(
            {
                "label": _trim(
                    _first_text(match.get("legal_act_label"), match.get("title")),
                    120,
                ),
                "value": "Official legal-resolution match by number/year and MinCIT or zone-name context.",
                "source": _first_text(match.get("source_name"), match.get("source_id")),
                "url": _first_text(match.get("url")),
                "item_id": "",
                "content_kind": "official_legal_resolution_match",
            }
        )
    return evidence


def _registry_evidence(record: ZonaFrancaRecord) -> dict[str, str]:
    details = [
        f"change: {_change_label(record.change_type)}",
        f"location: {record.municipality}, {record.department}",
    ]
    if record.declaratory_resolution:
        details.append(f"declaratory resolution: {record.declaratory_resolution}")
    if record.extension_resolution:
        details.append(f"extension resolution: {record.extension_resolution}")
    if record.zone_class:
        details.append(f"class: {record.zone_class}")
    if record.user_type:
        details.append(f"user type: {record.user_type}")
    if record.nit:
        details.append(f"NIT: {record.nit}")
    if record.ciiu:
        details.append(f"CIIU: {record.ciiu}")
    return {
        "label": _trim(record.title, 120),
        "value": "; ".join(details),
        "source": record.source_name,
        "url": record.url,
        "item_id": record.item_id,
        "content_kind": "structured_zona_franca_registry_change",
    }


def _caveats(record: ZonaFrancaRecord) -> list[str]:
    caveats = [
        "This is a land-use/economic-development signal, not investment advice.",
        "The v0 tracker only surfaces MinCIT approved-registry changes, not all pending local land-use decisions.",
        "Confirm the underlying resolution and any local planning or licensing implications before publishing a forecast.",
    ]
    if not record.official_resolution_matches:
        caveats.append(
            "No deterministic Diario Oficial, SUIN, or Gestor Normativo match was attached in this run."
        )
    return caveats


def _next_check(record: ZonaFrancaRecord) -> str:
    if record.official_resolution_matches:
        return (
            "Open the matched official legal record, verify the resolution text, "
            "and decide whether the case is an insight, investigation lead, or "
            "forecastable follow-up."
        )
    hints = _follow_up_hints(record)
    if hints:
        return (
            "Search the listed official follow-up sources for "
            f"{hints[0]} and verify whether the resolution text is published."
        )
    return (
        "Search Diario Oficial, SUIN, Gestor Normativo, and MinCIT press for "
        f"{record.zone_name} and the listed resolution."
    )


def _follow_up_hints(record: ZonaFrancaRecord) -> list[str]:
    hints: list[str] = []
    for source in record.follow_up_sources:
        hint = _first_text(source.get("search_hint"))
        if hint:
            hints.append(hint)
    return _unique(hints)


def _change_label(change_type: str) -> str:
    if change_type == "new_registry_row":
        return "new approved-registry row"
    if change_type == "updated_registry_row":
        return "updated approved-registry row"
    return change_type.replace("_", " ")


def _first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = normalize_whitespace(str(value))
        if text:
            return text
    return ""


def _lead_id(lead_type: str, value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", fold_accents(value.lower())).strip("_")[:48]
    digest = hashlib.sha1(f"{lead_type}:{value}".encode("utf-8")).hexdigest()[:8]
    return f"{lead_type}:{slug or 'lead'}:{digest}"


def _trim(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


def _unique(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
