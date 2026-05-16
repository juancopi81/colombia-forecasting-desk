from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

from colombia_forecasting_desk.models import RawItem
from colombia_forecasting_desk.registry_changes import (
    add_mincit_zonas_francas_change_events,
)


def _row(
    *,
    nit: str = "901911193",
    extension_resolution: str = "Vacía",
    snapshot_date: str = "2025-12-31T00:00:00Z",
) -> RawItem:
    return RawItem(
        id=f"row-{nit}",
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas (Estadísticas)",
        source_type="regulatory",
        url=f"https://zf.mincit.gov.co/estadisticas#zf-{nit}",
        title="MinCIT Zonas Francas aprobadas — Rionegro MRO",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-02-18T00:00:00Z",
        raw_text="Official MinCIT approved-zones registry row.",
        metadata={
            "registry": "mincit_zonas_francas_aprobadas",
            "registry_row_type": "approved_zone",
            "registry_key": nit,
            "content_extraction": "mincit_zonas_francas_approved_pdf",
            "snapshot_date": snapshot_date,
            "zona_franca_name": "Zona Franca Permanente Especial De Servicios Rionegro MRO",
            "zone_class": "Permanente Especial",
            "user_type": "Servicios",
            "department": "Antioquia",
            "municipality": "Rionegro",
            "declaratory_resolution": "Res. No. 2118 del 26 de diciembre de 2025",
            "extension_resolution": extension_resolution,
            "ciiu": "3315",
            "follow_up_sources": [
                {
                    "source_id": "diario_oficial",
                    "source_name": "Diario Oficial",
                    "url": "https://svrpubindc.imprenta.gov.co/diario/index.xhtml",
                    "search_hint": "Rionegro MRO Res. No. 2118",
                    "purpose": "Verify official publication.",
                }
            ],
        },
    )


def _write_previous(tmp_path: Path, row: RawItem) -> None:
    run_dir = tmp_path / "2026-05-14"
    run_dir.mkdir()
    (run_dir / "raw_items.json").write_text(
        json.dumps([asdict(row)], ensure_ascii=False),
        encoding="utf-8",
    )


def test_mincit_zonas_francas_change_events_wait_for_prior_snapshot(
    tmp_path: Path,
) -> None:
    current = [_row()]

    out = add_mincit_zonas_francas_change_events(
        current,
        runs_root=tmp_path,
        run_date="2026-05-15",
    )

    assert out == current


def test_mincit_zonas_francas_change_events_detect_changed_resolution(
    tmp_path: Path,
) -> None:
    previous = _row(extension_resolution="Vacía")
    _write_previous(tmp_path, previous)
    current = replace(previous, metadata={**previous.metadata})
    current.metadata["extension_resolution"] = "Res. 101 de 2 de mayo de 2026"

    out = add_mincit_zonas_francas_change_events(
        [current],
        runs_root=tmp_path,
        run_date="2026-05-15",
    )

    assert len(out) == 2
    event = out[1]
    assert event.metadata["content_extraction"] == (
        "mincit_zonas_francas_approved_diff"
    )
    assert event.metadata["registry_change_type"] == "updated_registry_row"
    assert event.metadata["changed_fields"] == ["extension_resolution"]
    assert event.metadata["previous_extension_resolution"] == "Vacía"
    assert event.published_at == "2026-05-15T00:00:00Z"
    assert "Diario Oficial" in event.raw_text


def test_mincit_zonas_francas_change_events_detect_new_row(
    tmp_path: Path,
) -> None:
    _write_previous(tmp_path, _row(nit="800178052"))

    out = add_mincit_zonas_francas_change_events(
        [_row(nit="901911193")],
        runs_root=tmp_path,
        run_date="2026-05-15",
    )

    assert len(out) == 2
    assert out[1].metadata["registry_change_type"] == "new_registry_row"
