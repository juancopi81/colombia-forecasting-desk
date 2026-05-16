from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .fetchers import MINCIT_ZF_APPROVED_REGISTRY
from .models import RawItem

MINCIT_ZF_DIFF_EXTRACTION = "mincit_zonas_francas_snapshot_change"
MINCIT_ZF_DIFF_CONTENT = "mincit_zonas_francas_approved_diff"
_RUN_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MINCIT_ZF_COMPARE_FIELDS = (
    "zona_franca_name",
    "zone_class",
    "user_type",
    "department",
    "municipality",
    "declaratory_resolution",
    "extension_resolution",
    "ciiu",
)


def add_mincit_zonas_francas_change_events(
    raw_items: list[RawItem],
    *,
    runs_root: str | Path,
    run_date: str,
    now: datetime | None = None,
) -> list[RawItem]:
    current_rows = [
        item
        for item in raw_items
        if item.metadata.get("registry") == MINCIT_ZF_APPROVED_REGISTRY
        and item.metadata.get("registry_row_type") == "approved_zone"
    ]
    if not current_rows:
        return raw_items

    previous_rows = _latest_previous_rows(Path(runs_root), run_date)
    if previous_rows is None:
        return raw_items

    previous_by_key = {
        str(row.get("metadata", {}).get("registry_key") or ""): row
        for row in previous_rows
    }
    previous_by_key = {key: row for key, row in previous_by_key.items() if key}

    events: list[RawItem] = []
    event_time = _event_time(run_date, now)
    for row in current_rows:
        key = str(row.metadata.get("registry_key") or "")
        if not key:
            continue
        previous = previous_by_key.get(key)
        if previous is None:
            events.append(
                _change_event(
                    row,
                    change_type="new_registry_row",
                    changed_fields=list(_MINCIT_ZF_COMPARE_FIELDS),
                    previous_metadata={},
                    event_time=event_time,
                )
            )
            continue
        previous_metadata = dict(previous.get("metadata", {}))
        changed_fields = _changed_fields(row.metadata, previous_metadata)
        if changed_fields:
            events.append(
                _change_event(
                    row,
                    change_type="updated_registry_row",
                    changed_fields=changed_fields,
                    previous_metadata=previous_metadata,
                    event_time=event_time,
                )
            )

    if not events:
        return raw_items
    return [*raw_items, *events]


def _event_time(run_date: str, now: datetime | None) -> str:
    if now is not None:
        return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"{run_date}T00:00:00Z"


def _latest_previous_rows(runs_root: Path, run_date: str) -> list[dict[str, Any]] | None:
    if not runs_root.exists():
        return None
    run_dirs = sorted(
        (
            child
            for child in runs_root.iterdir()
            if child.is_dir() and _RUN_DIR_RE.match(child.name) and child.name < run_date
        ),
        key=lambda path: path.name,
        reverse=True,
    )
    for run_dir in run_dirs:
        path = run_dir / "raw_items.json"
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, list):
            continue
        rows = [
            item
            for item in payload
            if isinstance(item, dict)
            and item.get("metadata", {}).get("registry") == MINCIT_ZF_APPROVED_REGISTRY
            and item.get("metadata", {}).get("registry_row_type") == "approved_zone"
        ]
        if rows:
            return rows
    return None


def _changed_fields(
    current_metadata: dict[str, Any],
    previous_metadata: dict[str, Any],
) -> list[str]:
    changed: list[str] = []
    for field in _MINCIT_ZF_COMPARE_FIELDS:
        if _clean_compare(current_metadata.get(field)) != _clean_compare(
            previous_metadata.get(field)
        ):
            changed.append(field)
    return changed


def _clean_compare(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def _event_id(row: RawItem, change_type: str, changed_fields: list[str]) -> str:
    digest = hashlib.sha1(
        "|".join(
            [
                row.id,
                change_type,
                ",".join(changed_fields),
                str(row.metadata.get("snapshot_date") or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"mincit-zf-change-{digest}"


def _change_event(
    row: RawItem,
    *,
    change_type: str,
    changed_fields: list[str],
    previous_metadata: dict[str, Any],
    event_time: str,
) -> RawItem:
    metadata = dict(row.metadata)
    zone_name = str(metadata.get("zona_franca_name") or row.title)
    title = f"MinCIT zona franca registry change — {zone_name}"
    change_label = (
        "new approved-registry row"
        if change_type == "new_registry_row"
        else "updated approved-registry row"
    )
    metadata.update(
        {
            "extraction": MINCIT_ZF_DIFF_EXTRACTION,
            "content_extraction": MINCIT_ZF_DIFF_CONTENT,
            "registry_change_type": change_type,
            "changed_fields": changed_fields,
            "source_registry_row_id": row.id,
            "previous_snapshot_date": previous_metadata.get("snapshot_date"),
            "previous_declaratory_resolution": previous_metadata.get(
                "declaratory_resolution"
            ),
            "previous_extension_resolution": previous_metadata.get(
                "extension_resolution"
            ),
        }
    )
    raw_text = (
        f"{title}. Official MinCIT approved-zones registry shows a "
        f"{change_label}. Changed fields: {', '.join(changed_fields)}. "
        f"Declaratory resolution: "
        f"{metadata.get('declaratory_resolution') or 'not listed'}; "
        f"extension resolution: {metadata.get('extension_resolution') or 'not listed'}. "
        "Follow up in Diario Oficial, SUIN/Gestor Normativo, and MinCIT press."
    )
    return RawItem(
        id=_event_id(row, change_type, changed_fields),
        source_id=row.source_id,
        source_name=row.source_name,
        source_type=row.source_type,
        url=row.url,
        title=title,
        fetched_at=row.fetched_at,
        published_at=event_time,
        raw_text=raw_text,
        metadata=metadata,
    )
