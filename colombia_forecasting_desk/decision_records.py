from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from .cleaner import fold_accents, normalize_whitespace
from .legal_identity import (
    OFFICIAL_LEGAL_SOURCE_IDS,
    best_shared_legal_act_record,
    parse_legal_act_records,
)
from .models import RawItem

LEGISLATIVE_IDENTITY_SOURCE_IDS = {
    "senado_agenda_legislativa",
    "senado_leyes_registry",
    "camara_proyectos_ley_registry",
}
MINCIT_ZONAS_FRANCAS_SOURCE_ID = "mincit_zonas_francas"
MINCIT_REGISTRY_ID = "mincit_zonas_francas_aprobadas"

_MINCIT_CONTEXT_TERMS = (
    "mincit",
    "ministerio de comercio",
    "comercio industria y turismo",
    "comercio, industria y turismo",
)
_ZONE_NAME_STOPWORDS = {
    "agroindustrial",
    "bienes",
    "colombia",
    "especial",
    "franca",
    "francas",
    "industrial",
    "permanente",
    "servicios",
    "zona",
}


def _record_keys(records: object) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    if not isinstance(records, list):
        return keys
    for record in records:
        if not isinstance(record, dict):
            continue
        number = str(record.get("number") or "").strip().lstrip("0") or "0"
        year = str(record.get("year") or "").strip()
        chamber = str(record.get("chamber") or "").strip().lower()
        if not number or not year:
            continue
        if chamber in {"cámara/senado", "camara/senado", "senado/camara", "senado/cámara"}:
            keys.add((number, year, "senado"))
            keys.add((number, year, "camara"))
        elif "senado" in chamber:
            keys.add((number, year, "senado"))
        elif "camara" in chamber or "cámara" in chamber:
            keys.add((number, year, "camara"))
        else:
            keys.add((number, year, "unknown"))
    return keys


def _followup_match(gaceta: RawItem) -> dict[str, Any]:
    metadata = gaceta.metadata or {}
    return {
        "source_id": gaceta.source_id,
        "source_name": gaceta.source_name,
        "url": gaceta.url,
        "title": gaceta.title,
        "published_at": gaceta.published_at or "",
        "gaceta_number": str(metadata.get("edition_number") or ""),
        "project_label": str(metadata.get("project_label") or ""),
        "document_title": str(metadata.get("document_title") or ""),
        "action_type": str(metadata.get("agenda_action_type") or ""),
        "match_basis": "project_number_year_chamber",
    }


def link_legislative_followups(items: list[RawItem]) -> list[RawItem]:
    """Attach parsed Gaceta follow-up matches to clean legislative records.

    This is intentionally conservative: only rows already marked as clean
    project identities can receive matches, and only Gaceta rows with parsed
    project records participate. Unmatched or lossy rows are preserved unchanged.
    """
    gacetas: list[tuple[RawItem, set[tuple[str, str, str]]]] = []
    for item in items:
        if item.source_id != "gacetas_congreso":
            continue
        if item.metadata.get("content_extraction") != "gaceta_pdf_text":
            continue
        keys = _record_keys(item.metadata.get("project_records"))
        if keys:
            gacetas.append((item, keys))

    if not gacetas:
        return items

    linked: list[RawItem] = []
    for item in items:
        if item.source_id not in LEGISLATIVE_IDENTITY_SOURCE_IDS:
            linked.append(item)
            continue
        if item.metadata.get("has_clean_project_identity") is not True:
            linked.append(item)
            continue
        senado_keys = _record_keys(item.metadata.get("project_records"))
        if not senado_keys:
            linked.append(item)
            continue

        matches = [
            _followup_match(gaceta)
            for gaceta, gaceta_keys in gacetas
            if senado_keys & gaceta_keys
        ]
        if not matches:
            linked.append(item)
            continue

        metadata = dict(item.metadata)
        metadata["official_followup_matches"] = matches
        metadata["official_followup_match_count"] = len(matches)
        metadata["matched_followup_source_ids"] = sorted(
            {match["source_id"] for match in matches}
        )
        metadata["resolution_source_status"] = "official_followup_matched"
        match_text = "; ".join(
            f"{match['source_name']} {match['gaceta_number']}: {match['title']}"
            for match in matches[:3]
        )
        linked.append(
            replace(
                item,
                raw_text=(
                    f"{item.raw_text} Official follow-up matches by project "
                    f"identity: {match_text}."
                ),
                metadata=metadata,
            )
        )
    return linked


def _legal_records_for(item: RawItem) -> list[dict[str, Any]]:
    records = item.metadata.get("legal_act_records")
    if isinstance(records, list):
        return [record for record in records if isinstance(record, dict)]
    return parse_legal_act_records(item.title, item.raw_text)


def _official_legal_match(
    source_item: RawItem,
    shared_record: dict[str, str],
    *,
    match_basis: str,
) -> dict[str, str]:
    metadata = source_item.metadata or {}
    return {
        "source_id": source_item.source_id,
        "source_name": source_item.source_name,
        "url": source_item.url,
        "title": source_item.title,
        "published_at": source_item.published_at or "",
        "edition_number": str(metadata.get("edition_number") or ""),
        "legal_act_label": shared_record.get("label", ""),
        "legal_act_kind": shared_record.get("kind", ""),
        "legal_act_number": shared_record.get("number", ""),
        "legal_act_year": shared_record.get("year", ""),
        "match_basis": match_basis,
    }


def _mincit_target_records(item: RawItem) -> list[dict[str, str]]:
    metadata = item.metadata or {}
    if (
        item.source_id != MINCIT_ZONAS_FRANCAS_SOURCE_ID
        or metadata.get("registry") != MINCIT_REGISTRY_ID
    ):
        return []
    texts = [
        str(metadata.get("declaratory_resolution") or ""),
        str(metadata.get("extension_resolution") or ""),
    ]
    texts = [text for text in texts if text and fold_accents(text.lower()) != "vacia"]
    return parse_legal_act_records(*texts)


def _zone_name_terms(zone_name: str) -> set[str]:
    folded = fold_accents(zone_name.lower())
    return {
        token
        for token in re.findall(r"[a-z0-9]{4,}", folded)
        if token not in _ZONE_NAME_STOPWORDS
    }


def _is_mincit_resolution_context_match(target: RawItem, source: RawItem) -> bool:
    source_text = fold_accents(
        normalize_whitespace(f"{source.title} {source.raw_text}").lower()
    )
    if any(term in source_text for term in _MINCIT_CONTEXT_TERMS):
        return True

    zone_name = str((target.metadata or {}).get("zona_franca_name") or "")
    zone_terms = _zone_name_terms(zone_name)
    if not zone_terms:
        return False
    source_terms = set(re.findall(r"[a-z0-9]{4,}", source_text))
    shared = zone_terms & source_terms
    if len(shared) >= 2:
        return True
    return len(shared) == 1 and any(len(term) >= 8 for term in shared)


def _link_mincit_resolution_matches(
    item: RawItem,
    legal_sources: list[tuple[RawItem, list[dict[str, Any]]]],
) -> RawItem:
    target_records = _mincit_target_records(item)
    if not target_records:
        return item

    matches: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for source_item, source_records in legal_sources:
        shared = best_shared_legal_act_record(target_records, source_records)
        if shared is None:
            continue
        if not _is_mincit_resolution_context_match(item, source_item):
            continue
        key = (
            source_item.source_id,
            source_item.url,
            shared.get("label", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        matches.append(
            _official_legal_match(
                source_item,
                shared,
                match_basis="legal_act_number_year_with_mincit_context",
            )
        )

    if not matches:
        return item

    metadata = dict(item.metadata)
    existing = [
        match
        for match in metadata.get("official_resolution_matches", [])
        if isinstance(match, dict)
    ]
    metadata["official_resolution_matches"] = [*existing, *matches]
    metadata["official_resolution_match_count"] = len(
        metadata["official_resolution_matches"]
    )
    metadata["matched_resolution_source_ids"] = sorted(
        {
            str(match["source_id"])
            for match in metadata["official_resolution_matches"]
            if match.get("source_id")
        }
    )
    metadata["resolution_source_status"] = "official_resolution_matched"
    match_text = "; ".join(
        f"{match['source_name']} {match['legal_act_label']}"
        for match in matches[:3]
    )
    return replace(
        item,
        raw_text=(
            f"{item.raw_text} Official resolution matches by legal-act "
            f"identity: {match_text}."
        ),
        metadata=metadata,
    )


def link_official_legal_records(items: list[RawItem]) -> list[RawItem]:
    """Attach deterministic Diario/SUIN/Gestor matches to source records.

    The bridge only matches MinCIT zona-franca rows when a source shares the
    same legal act kind/number/year and also contains MinCIT or zone-name
    context. That keeps same-number resolutions from unrelated entities out of
    M2-ready follow-up evidence.
    """
    legal_sources: list[tuple[RawItem, list[dict[str, Any]]]] = []
    normalized_items: list[RawItem] = []
    for item in items:
        if item.source_id not in OFFICIAL_LEGAL_SOURCE_IDS:
            normalized_items.append(item)
            continue
        records = _legal_records_for(item)
        if records and not item.metadata.get("legal_act_records"):
            item = replace(
                item,
                metadata={
                    **dict(item.metadata),
                    "legal_act_records": records,
                    "legal_act_record_count": len(records),
                },
            )
        if records:
            legal_sources.append((item, records))
        normalized_items.append(item)

    if not legal_sources:
        return normalized_items

    return [
        _link_mincit_resolution_matches(item, legal_sources)
        if item.source_id == MINCIT_ZONAS_FRANCAS_SOURCE_ID
        else item
        for item in normalized_items
    ]
