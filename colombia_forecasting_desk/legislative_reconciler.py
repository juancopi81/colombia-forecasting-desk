from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .cleaner import fold_accents, normalize_whitespace
from .legal_identity import (
    OFFICIAL_LEGAL_SOURCE_IDS,
    legal_act_label,
    parse_legal_act_records,
)
from .models import RawItem

SCHEMA_VERSION = "legislative_reconciler.v1"
RESOLVED_STATUS_OVERRIDES_SCHEMA_VERSION = "resolved_status_overrides.v1"
DEFAULT_RESOLVED_STATUS_OVERRIDES_PATH = (
    Path(__file__).resolve().parent / "data" / "resolved_status_overrides.json"
)

LEGISLATIVE_SOURCE_IDS = frozenset(
    {
        "senado_leyes_registry",
        "camara_proyectos_ley_registry",
        "senado_agenda_legislativa",
        "gacetas_congreso",
    }
)
REGISTRY_SOURCE_IDS = frozenset(
    {"senado_leyes_registry", "camara_proyectos_ley_registry"}
)
MOVEMENT_SOURCE_IDS = frozenset({"senado_agenda_legislativa", "gacetas_congreso"})
LEGAL_FINAL_KINDS = frozenset({"ley", "acto legislativo"})

SOURCE_PRIORITY = {
    "camara_proyectos_ley_registry": 0,
    "senado_leyes_registry": 1,
    "senado_agenda_legislativa": 2,
    "gacetas_congreso": 3,
    "diario_oficial": 4,
}

ProjectKey = tuple[str, str, str]  # year, chamber, number


@dataclass
class _UnionFind:
    parent: dict[ProjectKey, ProjectKey] = field(default_factory=dict)

    def add(self, key: ProjectKey) -> None:
        self.parent.setdefault(key, key)

    def find(self, key: ProjectKey) -> ProjectKey:
        self.add(key)
        parent = self.parent[key]
        if parent != key:
            self.parent[key] = self.find(parent)
        return self.parent[key]

    def union(self, first: ProjectKey, second: ProjectKey) -> None:
        first_root = self.find(first)
        second_root = self.find(second)
        if first_root == second_root:
            return
        self.parent[second_root] = first_root


def load_resolved_status_overrides(
    path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Load manual resolved-status overrides keyed by canonical bill id."""
    override_path = path or DEFAULT_RESOLVED_STATUS_OVERRIDES_PATH
    if not override_path.exists():
        return {}

    payload = json.loads(override_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("resolved status overrides must be a JSON object")
    schema_version = payload.get("schema_version")
    if schema_version != RESOLVED_STATUS_OVERRIDES_SCHEMA_VERSION:
        raise ValueError(
            "resolved status overrides schema_version must be "
            f"{RESOLVED_STATUS_OVERRIDES_SCHEMA_VERSION!r}"
        )

    rows = payload.get("overrides")
    if not isinstance(rows, list):
        raise ValueError("resolved status overrides must contain an overrides list")

    output: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"resolved status override at index {index} is not an object")
        canonical_bill_id = normalize_whitespace(str(row.get("canonical_bill_id") or ""))
        if not canonical_bill_id:
            raise ValueError(
                f"resolved status override at index {index} is missing canonical_bill_id"
            )
        output[canonical_bill_id] = row
    return output


def build_legislative_reconciliations(
    raw_items: list[RawItem],
    *,
    resolved_status_overrides: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build one conservative bill-status record per reconciled identity."""
    resolved_status_overrides = resolved_status_overrides or {}
    exact_items: dict[ProjectKey, list[RawItem]] = {}
    title_only: dict[str, list[RawItem]] = {}
    final_acts: dict[tuple[str, str, str], list[tuple[RawItem, dict[str, Any]]]] = {}
    uf = _UnionFind()

    for item in raw_items:
        keys = _project_keys(item)
        if keys:
            for key in keys:
                uf.add(key)
                exact_items.setdefault(key, []).append(item)
            for key in keys[1:]:
                uf.union(keys[0], key)
            continue

        for record in _final_legal_act_records(item):
            key = _legal_act_key(record)
            final_acts.setdefault(key, []).append((item, record))

        if item.source_id in LEGISLATIVE_SOURCE_IDS:
            title = _best_title([item])
            normalized = _title_normalized(title)
            if normalized:
                title_only.setdefault(normalized, []).append(item)

    reconciliations: list[dict[str, Any]] = []
    grouped_keys: dict[ProjectKey, set[ProjectKey]] = {}
    for key in exact_items:
        grouped_keys.setdefault(uf.find(key), set()).add(key)

    for keys in grouped_keys.values():
        items = _unique_items(
            item for key in sorted(keys, key=_project_sort_key) for item in exact_items[key]
        )
        reconciliations.append(
            _build_project_record(keys, items, resolved_status_overrides)
        )

    for normalized_title, items in sorted(title_only.items()):
        reconciliations.append(_build_title_lead_record(normalized_title, items))

    for act_key, rows in sorted(final_acts.items()):
        reconciliations.append(_build_final_act_record(act_key, rows))

    return sorted(
        reconciliations,
        key=lambda record: (
            _readiness_rank(record),
            str(record.get("canonical_bill_id") or ""),
        ),
    )


def _project_keys(item: RawItem) -> list[ProjectKey]:
    records = item.metadata.get("project_records")
    if not isinstance(records, list):
        return []
    keys: list[ProjectKey] = []
    seen: set[ProjectKey] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        number = _normalize_project_number(record.get("number"))
        year = str(record.get("year") or "").strip()
        chamber = _normalize_chamber(record.get("chamber"))
        if not number or not year or not chamber:
            continue
        for chamber_value in chamber:
            key = (year, chamber_value, number)
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    return keys


def _normalize_chamber(value: object) -> list[str]:
    folded = fold_accents(str(value or "").lower())
    if "camara" in folded and "senado" in folded:
        return ["camara", "senado"]
    if "camara" in folded:
        return ["camara"]
    if "senado" in folded:
        return ["senado"]
    return []


def _normalize_project_number(value: object) -> str:
    clean = re.sub(r"\D+", "", str(value or ""))
    return clean.lstrip("0") or clean


def _project_sort_key(key: ProjectKey) -> tuple[str, int, int]:
    year, chamber, number = key
    chamber_rank = 0 if chamber == "camara" else 1
    try:
        number_rank = int(number)
    except ValueError:
        number_rank = 999999
    return year, chamber_rank, number_rank


def _build_project_record(
    keys: set[ProjectKey],
    items: list[RawItem],
    resolved_status_overrides: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    origin_key = _origin_project_key(keys, items)
    title = _best_title(items)
    status = _latest_status(items)
    movements = _movement_records(items)
    latest_movement = _latest_movement(movements)
    contradiction = _contradiction(items, status, movements)
    decision_state = _decision_state(status, contradiction)
    readiness = _m2_readiness(origin_key, title, status, latest_movement, contradiction)

    display_title = _display_title(origin_key, title)
    record = {
        "schema_version": SCHEMA_VERSION,
        "canonical_bill_id": _canonical_project_id(origin_key),
        "display_title": display_title,
        "title_normalized": _title_normalized(title or display_title),
        "origin_project": _project_dict(origin_key),
        "linked_projects": _linked_projects(keys, items),
        "status": status or _unknown_status(items),
        "latest_movement": latest_movement,
        "source_evidence": _source_evidence(items),
        "contradiction": contradiction,
        "decision_state": decision_state,
        "m2_readiness": readiness,
    }
    return _apply_resolved_status_override(record, resolved_status_overrides)


def _build_title_lead_record(
    normalized_title: str,
    items: list[RawItem],
) -> dict[str, Any]:
    title = _best_title(items) or next((item.title for item in items if item.title), "")
    digest = hashlib.sha1(normalized_title.encode("utf-8")).hexdigest()[:12]
    return {
        "schema_version": SCHEMA_VERSION,
        "canonical_bill_id": f"bill:research:{digest}",
        "display_title": title,
        "title_normalized": normalized_title,
        "origin_project": None,
        "linked_projects": [],
        "status": _unknown_status(items),
        "latest_movement": _latest_movement(_movement_records(items)),
        "source_evidence": _source_evidence(items),
        "contradiction": {
            "has_contradiction": False,
            "severity": "none",
            "fields": [],
            "summary": "",
        },
        "decision_state": "unknown",
        "m2_readiness": {
            "state": "research_lead",
            "reason": (
                "Parsed official legislative item has a title but no clean "
                "project number, year, and chamber."
            ),
            "missing": ["clean project number/year/chamber", "current registry status"],
        },
    }


def _build_final_act_record(
    act_key: tuple[str, str, str],
    rows: list[tuple[RawItem, dict[str, Any]]],
) -> dict[str, Any]:
    kind, number, year = act_key
    items = _unique_items(item for item, _ in rows)
    record = rows[0][1]
    label = str(record.get("label") or legal_act_label(record))
    movement = _legal_act_movement(items[0], record)
    return {
        "schema_version": SCHEMA_VERSION,
        "canonical_bill_id": f"legal_act:{kind}:{year}:{number}",
        "display_title": label,
        "title_normalized": _title_normalized(label),
        "origin_project": None,
        "linked_projects": [],
        "status": {
            "stage": "resolved",
            "label": label,
            "as_of": _item_date(items[0]),
            "source_id": items[0].source_id,
            "url": items[0].url,
        },
        "latest_movement": movement,
        "source_evidence": _source_evidence(items),
        "contradiction": {
            "has_contradiction": False,
            "severity": "none",
            "fields": [],
            "summary": "",
        },
        "decision_state": "resolved",
        "m2_readiness": {
            "state": "resolved",
            "reason": "Official legal source already published the final act.",
            "missing": [],
        },
    }


def _origin_project_key(keys: set[ProjectKey], items: list[RawItem]) -> ProjectKey:
    item_positions = {item.id: index for index, item in enumerate(items)}

    def score(key: ProjectKey) -> tuple[int, int, tuple[str, int, int]]:
        best_source = 999
        best_position = 999
        for item in items:
            if key not in _project_keys(item):
                continue
            best_source = min(best_source, SOURCE_PRIORITY.get(item.source_id, 99))
            best_position = min(best_position, item_positions.get(item.id, 999))
        return best_source, best_position, _project_sort_key(key)

    return sorted(keys, key=score)[0]


def _project_dict(key: ProjectKey) -> dict[str, str]:
    year, chamber, number = key
    return {"chamber": chamber, "number": number, "year": year}


def _canonical_project_id(key: ProjectKey) -> str:
    year, chamber, number = key
    return f"bill:{year}:{chamber}:{number}"


def _linked_projects(keys: set[ProjectKey], items: list[RawItem]) -> list[dict[str, str]]:
    linked: list[dict[str, str]] = []
    for key in sorted(keys, key=_project_sort_key):
        item = _first_item_for_key(key, items)
        project = _project_dict(key)
        if item is not None:
            project["source_id"] = item.source_id
            project["url"] = item.url
        linked.append(project)
    return linked


def _first_item_for_key(key: ProjectKey, items: list[RawItem]) -> RawItem | None:
    matches = [item for item in items if key in _project_keys(item)]
    if not matches:
        return None
    return sorted(matches, key=lambda item: SOURCE_PRIORITY.get(item.source_id, 99))[0]


def _best_title(items: list[RawItem]) -> str:
    field_priority = {
        "bill_title": 0,
        "short_title": 1,
        "document_title": 2,
        "object": 3,
    }
    candidates: list[tuple[int, int, str]] = []
    for item in items:
        for field_name, priority in field_priority.items():
            value = normalize_whitespace(str(item.metadata.get(field_name) or ""))
            if _meaningful_title(value):
                source_score = SOURCE_PRIORITY.get(item.source_id, 99)
                candidates.append((source_score, priority, value))
        if not candidates:
            value = _strip_source_prefix(item.title)
            if _meaningful_title(value):
                candidates.append((SOURCE_PRIORITY.get(item.source_id, 99), 9, value))
    if not candidates:
        return ""
    return sorted(candidates, key=lambda row: (row[0], row[1], len(row[2])))[0][2]


def _strip_source_prefix(title: str) -> str:
    clean = normalize_whitespace(title)
    parts = [part.strip() for part in re.split(r"\s+[—-]\s+", clean) if part.strip()]
    for part in reversed(parts):
        if _meaningful_title(part) and not re.match(
            r"^(?:senado|camara|c[aá]mara|gaceta|diario)\b",
            part,
            flags=re.IGNORECASE,
        ):
            return part
    return clean


def _meaningful_title(title: str) -> bool:
    folded = fold_accents(title.lower())
    if len(folded) < 12:
        return False
    if folded in {"ui-button", "senado", "camara", "camara/senado"}:
        return False
    return True


def _title_normalized(title: str) -> str:
    folded = fold_accents(normalize_whitespace(title).lower())
    folded = re.sub(
        r"\bproyecto\s+de\s+(?:ley|acto\s+legislativo)\s+\d{1,4}\s+"
        r"de\s+\d{4}\s+(?:camara|senado)\b",
        " ",
        folded,
    )
    folded = re.sub(
        r"\bpor\s+(?:medio\s+de\s+)?(?:la\s+)?cual\s+se\b",
        " ",
        folded,
    )
    folded = re.sub(
        r"\bse\s+(?:adopta|crea|dictan|establece|expide|modifica)\b",
        " ",
        folded,
    )
    folded = re.sub(r"[^a-z0-9]+", " ", folded)
    return normalize_whitespace(folded)


def _display_title(origin_key: ProjectKey, title: str) -> str:
    label = _project_label(origin_key)
    if not title:
        return label
    return f"{label} - {title}"


def _project_label(key: ProjectKey) -> str:
    year, chamber, number = key
    chamber_label = "Cámara" if chamber == "camara" else "Senado"
    return f"Proyecto de Ley {number} de {year} {chamber_label}"


def _latest_status(items: list[RawItem]) -> dict[str, str] | None:
    statuses: list[dict[str, str]] = []
    for item in items:
        label = normalize_whitespace(str(item.metadata.get("status") or ""))
        if _is_final_legal_item(item):
            statuses.append(
                {
                    "stage": "resolved",
                    "label": "Final official publication",
                    "as_of": _item_date(item),
                    "source_id": item.source_id,
                    "url": item.url,
                }
            )
            continue
        if not label or item.source_id not in REGISTRY_SOURCE_IDS:
            continue
        statuses.append(
            {
                "stage": _status_stage(label),
                "label": label,
                "as_of": _item_date(item),
                "source_id": item.source_id,
                "url": item.url,
            }
        )
    if not statuses:
        return None
    return sorted(statuses, key=lambda status: _date_key(status.get("as_of")))[-1]


def _unknown_status(items: list[RawItem]) -> dict[str, str]:
    item = items[0] if items else None
    return {
        "stage": "unknown",
        "label": "Estado no reconciliado",
        "as_of": _item_date(item) if item is not None else "",
        "source_id": item.source_id if item is not None else "",
        "url": item.url if item is not None else "",
    }


def _status_stage(label: str) -> str:
    folded = fold_accents(label.lower())
    if any(
        term in folded
        for term in (
            "sancion",
            "promulgad",
            "ley de la republica",
            "publicad",
            "convertid",
        )
    ):
        return "resolved"
    if any(
        term in folded
        for term in (
            "archivad",
            "archivo",
            "retirad",
            "hundid",
            "negad",
            "rechazad",
            "desistid",
        )
    ):
        return "archived"
    if any(
        term in folded
        for term in (
            "tramite",
            "transito",
            "pendiente",
            "radicad",
            "debate",
            "ponencia",
            "comision",
            "plenaria",
            "conciliacion",
            "aprob",
        )
    ):
        return "active"
    return "unknown"


def _movement_records(items: list[RawItem]) -> list[dict[str, str]]:
    movements: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in items:
        for match in item.metadata.get("official_followup_matches") or []:
            if not isinstance(match, dict):
                continue
            movement = {
                "date": str(match.get("published_at") or ""),
                "action_type": _action_type(str(match.get("action_type") or "")),
                "label": _movement_label(
                    "gacetas_congreso",
                    str(match.get("action_type") or ""),
                    str(match.get("document_title") or match.get("title") or ""),
                ),
                "source_id": str(match.get("source_id") or ""),
                "source_name": str(match.get("source_name") or ""),
                "url": str(match.get("url") or ""),
                "gaceta_number": str(match.get("gaceta_number") or ""),
            }
            _append_unique_movement(movements, seen, movement)

        if item.source_id == "gacetas_congreso" and _project_keys(item):
            movement = {
                "date": item.published_at or "",
                "action_type": _action_type(str(item.metadata.get("agenda_action_type") or "")),
                "label": _movement_label(
                    item.source_id,
                    str(item.metadata.get("agenda_action_type") or ""),
                    str(item.metadata.get("document_title") or item.title),
                ),
                "source_id": item.source_id,
                "source_name": item.source_name,
                "url": item.url,
                "gaceta_number": str(item.metadata.get("edition_number") or ""),
            }
            _append_unique_movement(movements, seen, movement)

        if item.source_id == "senado_agenda_legislativa" and _project_keys(item):
            movement = {
                "date": str(item.metadata.get("scheduled_date") or item.published_at or ""),
                "action_type": _action_type(str(item.metadata.get("agenda_action_type") or "")),
                "label": _movement_label(
                    item.source_id,
                    str(item.metadata.get("agenda_action_type") or ""),
                    str(item.metadata.get("document_title") or item.title),
                ),
                "source_id": item.source_id,
                "source_name": item.source_name,
                "url": item.url,
            }
            _append_unique_movement(movements, seen, movement)

        if item.source_id in OFFICIAL_LEGAL_SOURCE_IDS and _is_final_legal_item(item):
            movement = {
                "date": item.published_at or item.fetched_at,
                "action_type": "final_act_published",
                "label": "Final official publication",
                "source_id": item.source_id,
                "source_name": item.source_name,
                "url": item.url,
            }
            _append_unique_movement(movements, seen, movement)

        publication_links = item.metadata.get("publication_links")
        if item.source_id in REGISTRY_SOURCE_IDS and isinstance(publication_links, list):
            movement = {
                "date": item.published_at or item.fetched_at,
                "action_type": "registry_publication",
                "label": "Publication or follow-up metadata listed in official registry",
                "source_id": item.source_id,
                "source_name": item.source_name,
                "url": item.url,
            }
            _append_unique_movement(movements, seen, movement)
    return movements


def _append_unique_movement(
    movements: list[dict[str, str]],
    seen: set[tuple[str, str, str, str]],
    movement: dict[str, str],
) -> None:
    key = (
        movement.get("source_id", ""),
        movement.get("url", ""),
        movement.get("action_type", ""),
        movement.get("date", ""),
    )
    if key in seen:
        return
    seen.add(key)
    movements.append(movement)


def _latest_movement(movements: list[dict[str, str]]) -> dict[str, str] | None:
    if not movements:
        return None
    return sorted(movements, key=lambda movement: _date_key(movement.get("date")))[-1]


def _action_type(action: str) -> str:
    folded = fold_accents(action.lower())
    if "ponencia" in folded:
        return "ponencia_publicada"
    if "texto aprobado" in folded:
        return "texto_aprobado_publicado"
    if "conciliacion" in folded:
        return "conciliacion_publicada"
    if "debate" in folded:
        return "agenda_debate"
    if "comisiones conjuntas" in folded:
        return "comisiones_conjuntas"
    if "agenda" in folded:
        return "agenda_legislativa"
    return re.sub(r"[^a-z0-9]+", "_", folded).strip("_") or "movimiento_oficial"


def _movement_label(source_id: str, action: str, title: str) -> str:
    folded = fold_accents(action.lower())
    title = normalize_whitespace(title)
    if source_id == "gacetas_congreso":
        if "ponencia" in folded:
            return "Ponencia publicada en Gaceta del Congreso"
        if "conciliacion" in folded:
            return "Conciliación publicada en Gaceta del Congreso"
        if "texto aprobado" in folded:
            return "Texto aprobado publicado en Gaceta del Congreso"
        return "Publicación en Gaceta del Congreso"
    if source_id == "senado_agenda_legislativa":
        action_label = normalize_whitespace(action) or "agenda legislativa"
        return f"Agenda legislativa: {action_label}"
    return title or "Movimiento oficial"


def _legal_act_movement(item: RawItem, record: dict[str, Any]) -> dict[str, str]:
    return {
        "date": item.published_at or item.fetched_at,
        "action_type": "final_act_published",
        "label": str(record.get("label") or legal_act_label(record)),
        "source_id": item.source_id,
        "source_name": item.source_name,
        "url": item.url,
    }


def _source_evidence(items: list[RawItem]) -> list[dict[str, str]]:
    evidence: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        role = _evidence_role(item)
        summary = _evidence_summary(item, role)
        key = (item.source_id, item.url, role)
        if key in seen:
            continue
        seen.add(key)
        evidence.append(
            {
                "source_id": item.source_id,
                "role": role,
                "date": _item_date(item),
                "url": item.url,
                "summary": summary,
            }
        )
    return evidence


def _evidence_role(item: RawItem) -> str:
    if item.source_id in REGISTRY_SOURCE_IDS and _project_keys(item):
        return "identity_status"
    if item.source_id in MOVEMENT_SOURCE_IDS:
        return "movement" if _project_keys(item) else "research_lead"
    if item.source_id in OFFICIAL_LEGAL_SOURCE_IDS:
        return "final_act"
    return "supporting"


def _evidence_summary(item: RawItem, role: str) -> str:
    if role == "identity_status":
        status = normalize_whitespace(str(item.metadata.get("status") or "unknown"))
        return f"Registry row with project number, title, chamber, and status: {status}."
    if item.source_id == "gacetas_congreso":
        action = str(item.metadata.get("agenda_action_type") or "publication")
        title = str(item.metadata.get("document_title") or item.title)
        return f"Parsed Gaceta item with {action} evidence: {title[:180]}."
    if item.source_id == "senado_agenda_legislativa":
        action = str(item.metadata.get("agenda_action_type") or "agenda")
        return f"Parsed Senado agenda item with official {action} listing."
    if role == "final_act":
        records = _final_legal_act_records(item)
        label = legal_act_label(records[0]) if records else item.title
        return f"Official legal source published final act: {label}."
    return normalize_whitespace(item.title)[:220]


def _contradiction(
    items: list[RawItem],
    status: dict[str, str] | None,
    movements: list[dict[str, str]],
) -> dict[str, Any]:
    status_stages = {
        _status_stage(str(item.metadata.get("status") or ""))
        for item in items
        if item.metadata.get("status")
    }
    status_stages.discard("unknown")
    fields: list[str] = []
    summary_parts: list[str] = []
    if "active" in status_stages and ({"resolved", "archived"} & status_stages):
        fields.append("status")
        summary_parts.append("Official registry statuses disagree on active vs final state.")

    final_items = [item for item in items if _is_final_legal_item(item)]
    if final_items and "active" in status_stages:
        fields.append("status")
        summary_parts.append("A final legal act appears alongside an active registry status.")

    if status and status.get("stage") in {"resolved", "archived"}:
        status_date = _date_key(status.get("as_of"))
        later_active = [
            movement
            for movement in movements
            if _date_key(movement.get("date")) > status_date
            and movement.get("action_type") not in {"final_act_published"}
        ]
        if later_active:
            fields.append("latest_movement")
            summary_parts.append(
                "A later official movement appears after a final or archived registry status."
            )

    fields = sorted(set(fields))
    return {
        "has_contradiction": bool(fields),
        "severity": "material" if fields else "none",
        "fields": fields,
        "summary": " ".join(summary_parts),
    }


def _decision_state(
    status: dict[str, str] | None,
    contradiction: dict[str, Any],
) -> str:
    if contradiction.get("has_contradiction"):
        return "unknown"
    stage = (status or {}).get("stage")
    if stage == "resolved":
        return "resolved"
    if stage == "archived":
        return "archived"
    if stage == "active":
        return "unresolved"
    return "unknown"


def _apply_resolved_status_override(
    record: dict[str, Any],
    resolved_status_overrides: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    override = resolved_status_overrides.get(str(record.get("canonical_bill_id") or ""))
    if not override or not _resolved_status_override_applies(record, override):
        return record

    reason = normalize_whitespace(
        str(override.get("reason") or "Manual resolved-status override applied.")
    )
    record = dict(record)
    record["contradiction"] = {
        "has_contradiction": False,
        "severity": "none",
        "fields": [],
        "summary": f"Manual resolved-status override applied: {reason}",
    }
    status_override = override.get("status_override")
    if isinstance(status_override, dict):
        record["status"] = {
            str(key): normalize_whitespace(str(value))
            for key, value in status_override.items()
            if value is not None
        }
    record["decision_state"] = normalize_whitespace(
        str(override.get("decision_state") or record.get("decision_state") or "archived")
    )
    record["m2_readiness"] = {
        "state": normalize_whitespace(
            str(override.get("m2_readiness_state") or "resolved")
        ),
        "reason": reason,
        "missing": [],
    }
    record["resolved_status_override"] = {
        "override_id": normalize_whitespace(str(override.get("override_id") or "")),
        "reason": reason,
        "source": normalize_whitespace(str(override.get("source") or "")),
    }
    return record


def _resolved_status_override_applies(
    record: dict[str, Any], override: dict[str, Any]
) -> bool:
    applies_when = override.get("applies_when")
    contradiction = record.get("contradiction")
    require_contradiction = not (
        isinstance(applies_when, dict)
        and applies_when.get("require_contradiction") is False
    )
    has_contradiction = isinstance(contradiction, dict) and bool(
        contradiction.get("has_contradiction")
    )
    if require_contradiction and not has_contradiction:
        return False

    if not isinstance(applies_when, dict):
        return True

    expected_stage = normalize_whitespace(str(applies_when.get("status_stage") or ""))
    status = record.get("status") if isinstance(record.get("status"), dict) else {}
    if expected_stage and status.get("stage") != expected_stage:
        return False

    expected_actions = applies_when.get("latest_movement_action_types")
    if isinstance(expected_actions, list) and expected_actions:
        latest = (
            record.get("latest_movement")
            if isinstance(record.get("latest_movement"), dict)
            else {}
        )
        action_type = str(latest.get("action_type") or "")
        allowed = {str(action or "") for action in expected_actions}
        if action_type not in allowed:
            return False

    return True


def _m2_readiness(
    origin_key: ProjectKey,
    title: str,
    status: dict[str, str] | None,
    latest_movement: dict[str, str] | None,
    contradiction: dict[str, Any],
) -> dict[str, Any]:
    if contradiction.get("has_contradiction"):
        return {
            "state": "blocked",
            "reason": "Official source evidence has a material contradiction.",
            "missing": ["human reconciliation of contradictory official records"],
        }

    stage = (status or {}).get("stage", "unknown")
    if stage in {"resolved", "archived"}:
        return {
            "state": "resolved",
            "reason": "The bill is final, archived, withdrawn, or already published.",
            "missing": [],
        }

    missing: list[str] = []
    if not all(origin_key):
        missing.append("clean project number/year/chamber")
    if not title:
        missing.append("meaningful bill title")
    if not status or stage == "unknown":
        missing.append("current official registry status")
    if latest_movement is None:
        missing.append("latest official movement or follow-up action")

    if missing:
        return {
            "state": "research_lead",
            "reason": "Bill identity exists, but promotion evidence is incomplete.",
            "missing": missing,
        }

    return {
        "state": "ready",
        "reason": (
            "Clean project number, title, active status, latest official movement, "
            "and plausible official resolution source are available."
        ),
        "missing": [],
    }


def _final_legal_act_records(item: RawItem) -> list[dict[str, Any]]:
    if item.source_id not in OFFICIAL_LEGAL_SOURCE_IDS:
        return []
    records = item.metadata.get("legal_act_records")
    if not isinstance(records, list):
        records = parse_legal_act_records(item.title, item.raw_text)
    output: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        kind = fold_accents(str(record.get("kind") or "").lower())
        if kind in LEGAL_FINAL_KINDS:
            output.append(record)
    return output


def _is_final_legal_item(item: RawItem) -> bool:
    if item.source_id not in OFFICIAL_LEGAL_SOURCE_IDS:
        return False
    if _final_legal_act_records(item):
        return True
    folded = fold_accents(f"{item.title} {item.raw_text}".lower())
    return bool(re.search(r"\bley\s+\d{1,4}\s+de\s+\d{4}\b", folded)) and any(
        term in folded
        for term in (
            "diario oficial",
            "sancion",
            "promulgad",
            "publicad",
            "ley de la republica",
        )
    )


def _legal_act_key(record: dict[str, Any]) -> tuple[str, str, str]:
    kind = fold_accents(str(record.get("kind") or "").lower()).replace(" ", "_")
    number = _normalize_project_number(record.get("number"))
    year = str(record.get("year") or "").strip()
    return kind, number, year


def _readiness_rank(record: dict[str, Any]) -> int:
    state = ((record.get("m2_readiness") or {}).get("state") or "").lower()
    return {"ready": 0, "research_lead": 1, "blocked": 2, "resolved": 3}.get(state, 9)


def _unique_items(items: Any) -> list[RawItem]:
    output: list[RawItem] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        key = (item.id, item.url)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _item_date(item: RawItem | None) -> str:
    if item is None:
        return ""
    return item.published_at or item.fetched_at or ""


def _date_key(value: object) -> str:
    text = str(value or "")
    return text[:19]
