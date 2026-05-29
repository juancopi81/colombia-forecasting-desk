from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

from .cleaner import normalize_whitespace
from .models import CleanedItem, RawItem
from .tagger import fold_accents

SECOP_SOURCE_IDS = frozenset(
    {
        "secop_ii_procesos",
        "secop_ii_contratos",
        "secop_i_procesos",
        "secop_ii_adiciones",
        "secop_multas_sanciones",
    }
)
MAX_PROCUREMENT_LEADS = 2
MAX_EVIDENCE_ITEMS = 4
MIN_REPEATED_PAIR_RECORDS = 2
MIN_ENTITY_RECORDS = 3
MIN_DIRECT_RECORDS = 2
MIN_DIRECT_SHARE = 0.60
MIN_LOW_COMPETITION_RECORDS = 2
MIN_CANCELLED_RECORDS = 2


@dataclass(frozen=True, slots=True)
class ProcurementRecord:
    item_id: str
    source_id: str
    source_name: str
    title: str
    url: str
    published_at: str
    entity: str
    supplier: str
    supplier_id: str
    modality: str
    status: str
    value_cop: float | None
    response_count: int | None


def build_procurement_concentration_leads(
    raw_items: Iterable[RawItem],
    cleaned_items: Iterable[CleanedItem],
    *,
    max_leads: int = MAX_PROCUREMENT_LEADS,
) -> list[dict[str, Any]]:
    """Build conservative analyst-insight leads from recent SECOP rows."""
    records = _procurement_records(raw_items, cleaned_items)
    if not records:
        return []

    candidates = [
        _repeated_supplier_entity_lead(records),
        _direct_contracting_concentration_lead(records),
        _low_competition_lead(records),
        _cancelled_processes_lead(records),
    ]
    leads: list[dict[str, Any]] = []
    seen_patterns: set[str] = set()
    for lead in candidates:
        if not lead:
            continue
        pattern = str(lead.get("review_context", {}).get("pattern") or "")
        if pattern and pattern in seen_patterns:
            continue
        leads.append(lead)
        if pattern:
            seen_patterns.add(pattern)
        if len(leads) >= max_leads:
            break
    return leads


def _procurement_records(
    raw_items: Iterable[RawItem],
    cleaned_items: Iterable[CleanedItem],
) -> list[ProcurementRecord]:
    raw_by_id = {item.id: item for item in raw_items}
    records: list[ProcurementRecord] = []
    for item in cleaned_items:
        if item.source_id not in SECOP_SOURCE_IDS or item.quality_notes:
            continue
        raw = raw_by_id.get(item.id)
        metadata = raw.metadata if raw else item.metadata
        fields = metadata.get("socrata_fields") if isinstance(metadata, dict) else {}
        if not isinstance(fields, dict):
            fields = {}
        entity = _first_text(metadata.get("entity") if isinstance(metadata, dict) else "")
        if not entity:
            entity = _entity_from_text(item)
        supplier = _first_text(
            fields.get("proveedor_adjudicado"),
            fields.get("nombre_del_proveedor"),
            fields.get("nom_raz_social_contratista"),
            fields.get("nombre_contratista"),
        )
        supplier_id = _first_text(
            fields.get("documento_proveedor"),
            fields.get("nit_del_proveedor_adjudicado"),
            fields.get("nit_del_contratista"),
        )
        modality = _first_text(
            fields.get("modalidad_de_contratacion"),
            fields.get("modalidad"),
        )
        status = _first_text(
            fields.get("estado_del_procedimiento"),
            fields.get("estado_del_proceso"),
            fields.get("estado_contrato"),
        )
        value_cop = _number(
            fields.get("valor_del_contrato"),
            fields.get("valor_total_adjudicacion"),
            fields.get("cuantia_proceso"),
            fields.get("precio_base"),
        )
        response_count = _integer(
            fields.get("proveedores_unicos_con"),
            fields.get("conteo_de_respuestas_a_ofertas"),
            fields.get("respuestas_al_procedimiento"),
        )
        url = _url_text(fields.get("urlproceso")) or item.url
        if not entity and not supplier:
            continue
        records.append(
            ProcurementRecord(
                item_id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                title=item.title,
                url=url,
                published_at=item.published_at or "",
                entity=entity,
                supplier=supplier,
                supplier_id=supplier_id,
                modality=modality,
                status=status,
                value_cop=value_cop,
                response_count=response_count,
            )
        )
    return records


def _repeated_supplier_entity_lead(
    records: list[ProcurementRecord],
) -> dict[str, Any] | None:
    groups: dict[tuple[str, str], list[ProcurementRecord]] = defaultdict(list)
    for record in records:
        if record.entity and record.supplier:
            groups[(_key(record.entity), _key(record.supplier))].append(record)
    eligible = [
        group for group in groups.values() if len(group) >= MIN_REPEATED_PAIR_RECORDS
    ]
    if not eligible:
        return None
    group = max(eligible, key=lambda rows: (len(rows), _value_sum(rows), rows[0].entity))
    entity = group[0].entity
    supplier = group[0].supplier
    return _lead(
        pattern="repeated_supplier_entity_pair",
        title=f"SECOP repeated supplier-entity pair — {_trim(entity, 55)}",
        claim=(
            f"Recent SECOP rows show {len(group)} records involving {supplier} "
            f"and {entity}, which may deserve review as a supplier-entity "
            "concentration pattern."
        ),
        next_check=(
            "Open the underlying SECOP rows, compare against the entity's normal "
            "supplier base, and verify whether the records are related procedures "
            "or independent awards."
        ),
        records=group,
        caveats=[
            "This is a concentration screen, not evidence of wrongdoing.",
            "Supplier and entity matching is string-based in v0.",
            "The datos.gov.co SECOP mirror can lag the live platform.",
            "Daily rows are capped, so this is not a full historical baseline.",
        ],
    )


def _direct_contracting_concentration_lead(
    records: list[ProcurementRecord],
) -> dict[str, Any] | None:
    by_entity: dict[str, list[ProcurementRecord]] = defaultdict(list)
    for record in records:
        if record.entity:
            by_entity[_key(record.entity)].append(record)
    eligible: list[tuple[list[ProcurementRecord], list[ProcurementRecord], float]] = []
    for group in by_entity.values():
        direct = [record for record in group if _is_direct_contracting(record.modality)]
        if len(group) < MIN_ENTITY_RECORDS or len(direct) < MIN_DIRECT_RECORDS:
            continue
        share = len(direct) / len(group)
        if share >= MIN_DIRECT_SHARE:
            eligible.append((group, direct, share))
    if not eligible:
        return None
    group, direct, share = max(
        eligible,
        key=lambda entry: (entry[2], len(entry[1]), _value_sum(entry[1])),
    )
    entity = group[0].entity
    return _lead(
        pattern="direct_contracting_concentration",
        title=f"SECOP direct-contracting concentration — {_trim(entity, 55)}",
        claim=(
            f"Recent SECOP rows for {entity} include {len(direct)} direct-contracting "
            f"records out of {len(group)} rankable procurement records "
            f"({share:.0%})."
        ),
        next_check=(
            "Check whether the direct-contracting records share supplier, object, "
            "urgency/legal basis, or budget line before treating this as an insight."
        ),
        records=direct,
        caveats=[
            "Direct contracting can be legal and routine depending on the object and legal basis.",
            "This screen does not evaluate compliance with procurement rules.",
            "The denominator is the recent rankable sample, not all entity procurement.",
        ],
    )


def _low_competition_lead(records: list[ProcurementRecord]) -> dict[str, Any] | None:
    by_entity: dict[str, list[ProcurementRecord]] = defaultdict(list)
    for record in records:
        if (
            record.entity
            and record.response_count is not None
            and record.response_count <= 1
            and _has_competition_window_closed(record.status)
        ):
            by_entity[_key(record.entity)].append(record)
    eligible = [
        group for group in by_entity.values() if len(group) >= MIN_LOW_COMPETITION_RECORDS
    ]
    if not eligible:
        return None
    group = max(eligible, key=lambda rows: (len(rows), _value_sum(rows), rows[0].entity))
    entity = group[0].entity
    return _lead(
        pattern="low_competition_processes",
        title=f"SECOP low-competition process cluster — {_trim(entity, 55)}",
        claim=(
            f"Recent SECOP rows for {entity} include {len(group)} processes with "
            "one or zero recorded supplier responses."
        ),
        next_check=(
            "Verify the process pages and compare expected competition for the "
            "contracting modality and object."
        ),
        records=group,
        caveats=[
            "Response-count fields are dataset fields and can be missing or delayed.",
            "Low response counts are not automatically irregular.",
            "Review process object, modality, publication time, and market size.",
        ],
    )


def _cancelled_processes_lead(records: list[ProcurementRecord]) -> dict[str, Any] | None:
    by_entity: dict[str, list[ProcurementRecord]] = defaultdict(list)
    for record in records:
        if record.entity and _is_cancelled(record.status):
            by_entity[_key(record.entity)].append(record)
    eligible = [group for group in by_entity.values() if len(group) >= MIN_CANCELLED_RECORDS]
    if not eligible:
        return None
    group = max(eligible, key=lambda rows: (len(rows), _value_sum(rows), rows[0].entity))
    entity = group[0].entity
    return _lead(
        pattern="cancelled_process_cluster",
        title=f"SECOP cancelled process cluster — {_trim(entity, 55)}",
        claim=(
            f"Recent SECOP rows for {entity} include {len(group)} cancelled or "
            "terminated procurement records."
        ),
        next_check=(
            "Check cancellation reasons and whether replacement processes or "
            "contract additions appear shortly afterward."
        ),
        records=group,
        caveats=[
            "Cancellation can reflect ordinary planning changes or data corrections.",
            "Use the official process pages before drawing a governance conclusion.",
        ],
    )


def _lead(
    *,
    pattern: str,
    title: str,
    claim: str,
    next_check: str,
    records: list[ProcurementRecord],
    caveats: list[str],
) -> dict[str, Any]:
    evidence = [_evidence(record) for record in records[:MAX_EVIDENCE_ITEMS]]
    urls = _unique([record.url for record in records if record.url])
    item_ids = [record.item_id for record in records]
    key = f"{pattern}:{title}:{','.join(item_ids)}"
    return {
        "lead_id": _lead_id("analyst_insight", key),
        "lead_type": "analyst_insight",
        "title": title,
        "claim_or_question": claim,
        "disposition": "monitor_or_research",
        "evidence": evidence,
        "caveats": caveats,
        "next_check": next_check,
        "source_refs": {
            "artifact_refs": [
                {
                    "artifact": "raw_items.json",
                    "key": "source_id",
                    "value": "secop_*",
                }
            ],
            "source_item_ids": item_ids,
            "source_urls": urls,
        },
        "review_context": {
            "family": "procurement_concentration",
            "pattern": pattern,
            "record_count": len(records),
            "sample_window": "SECOP freshness window",
        },
    }


def _evidence(record: ProcurementRecord) -> dict[str, str]:
    details = []
    if record.modality:
        details.append(f"modality: {record.modality}")
    if record.supplier:
        details.append(f"supplier: {record.supplier}")
    if record.status:
        details.append(f"status: {record.status}")
    if record.value_cop is not None:
        details.append(f"value: COP {record.value_cop:,.0f}")
    if record.response_count is not None:
        details.append(f"responses: {record.response_count}")
    value = "; ".join(details) or record.title
    return {
        "label": _trim(record.title, 120),
        "value": value,
        "source": record.source_name,
        "url": record.url,
        "item_id": record.item_id,
        "content_kind": "structured_procurement",
    }


def _entity_from_text(item: CleanedItem) -> str:
    parts = item.clean_text.split("|")
    if len(parts) >= 2:
        return normalize_whitespace(parts[-1])
    parts = item.title.split(" — ")
    if len(parts) >= 3:
        return normalize_whitespace(parts[-1])
    return ""


def _first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = normalize_whitespace(str(value))
        if text:
            return text
    return ""


def _url_text(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("url")
    text = _first_text(value)
    if text.startswith(("http://", "https://")):
        return text
    return ""


def _number(*values: Any) -> float | None:
    for value in values:
        text = _first_text(value)
        if not text:
            continue
        text = re.sub(r"[^\d,.\-]", "", text)
        if not text:
            continue
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            decimals = len(text.rsplit(",", 1)[-1])
            text = text.replace(",", ".") if decimals <= 2 else text.replace(",", "")
        try:
            return float(text)
        except ValueError:
            continue
    return None


def _integer(*values: Any) -> int | None:
    number = _number(*values)
    if number is None:
        return None
    return int(number)


def _is_direct_contracting(value: str) -> bool:
    folded = fold_accents(value.lower())
    return "contratacion directa" in folded or folded == "directa"


def _is_cancelled(value: str) -> bool:
    folded = fold_accents(value.lower())
    return any(term in folded for term in ("cancel", "terminad", "revocad"))


def _has_competition_window_closed(value: str) -> bool:
    folded = fold_accents(value.lower())
    if not folded:
        return False
    open_terms = (
        "publicado",
        "abierto",
        "presentacion de oferta",
        "presentacion de observaciones",
        "borrador",
    )
    return not any(term in folded for term in open_terms)


def _value_sum(records: Iterable[ProcurementRecord]) -> float:
    return sum(record.value_cop or 0.0 for record in records)


def _key(value: str) -> str:
    return fold_accents(normalize_whitespace(value).lower())


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
