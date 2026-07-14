from __future__ import annotations

from colombia_forecasting_desk.legislative_reconciler import (
    build_legislative_reconciliations,
    load_resolved_status_overrides,
)
from colombia_forecasting_desk.models import RawItem


def _raw(
    source_id: str,
    title: str,
    *,
    metadata: dict,
    url: str | None = None,
    raw_text: str | None = None,
    published_at: str = "2026-05-18T00:00:00Z",
) -> RawItem:
    return RawItem(
        id=f"{source_id}-{abs(hash((source_id, title))) % 100000}",
        source_id=source_id,
        source_name=source_id,
        source_type="official_updates",
        url=url or f"https://example.com/{source_id}",
        title=title,
        fetched_at="2026-05-18T12:00:00Z",
        published_at=published_at,
        raw_text=raw_text if raw_text is not None else title,
        metadata=metadata,
    )


def _project(number: str, year: str, chamber: str) -> dict[str, str]:
    return {"number": number, "year": year, "chamber": chamber}


def test_clean_camara_registry_plus_gaceta_is_m2_ready() -> None:
    camara = _raw(
        "camara_proyectos_ley_registry",
        "Cámara registry — Proyecto de Ley 560 de 2025 Cámara — Subsidio GLP",
        metadata={
            "legislative_registry": "camara_proyectos_ley",
            "registry_detail_url": "https://example.com/camara/560",
            "project_label": "Proyecto de Ley 560 de 2025 Cámara",
            "project_records": [_project("560", "2025", "Cámara")],
            "has_clean_project_identity": True,
            "bill_title": "Subsidio al transporte de GLP para San Andrés",
            "status": "En trámite",
        },
        url="https://example.com/camara/560",
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 485 — Proyecto de Ley 560 de 2025 Cámara",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "485",
            "project_label": "Proyecto de Ley 560 de 2025 Cámara",
            "project_records": [_project("560", "2025", "Cámara")],
            "document_title": "Ponencia para primer debate al subsidio GLP",
            "agenda_action_type": "ponencia",
        },
        url="https://example.com/gaceta/485",
        published_at="2026-05-19T00:00:00Z",
    )

    records = build_legislative_reconciliations([camara, gaceta])

    assert len(records) == 1
    record = records[0]
    assert record["canonical_bill_id"] == "bill:2025:camara:560"
    assert record["status"]["stage"] == "active"
    assert record["latest_movement"]["action_type"] == "ponencia_publicada"
    assert record["latest_movement"]["gaceta_number"] == "485"
    assert record["m2_readiness"]["state"] == "ready"
    assert record["contradiction"]["has_contradiction"] is False


def test_clean_senado_registry_plus_gaceta_is_m2_ready() -> None:
    senado = _raw(
        "senado_leyes_registry",
        "Senado registry — Proyecto de Ley 1 de 2026 Senado — Reforma",
        metadata={
            "legislative_registry": "senado_leyes",
            "registry_detail_url": "https://example.com/senado/1",
            "project_label": "Proyecto de Ley 1 de 2026 Senado",
            "project_records": [_project("001", "2026", "Senado")],
            "has_clean_project_identity": True,
            "bill_title": "Reforma institucional",
            "status": "Pendiente ponencia para primer debate",
        },
        url="https://example.com/senado/1",
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 476 — Proyecto de Ley 1 de 2026 Senado",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "476",
            "project_label": "Proyecto de Ley 1 de 2026 Senado",
            "project_records": [_project("1", "2026", "Senado")],
            "document_title": "Texto de ponencia para primer debate",
            "agenda_action_type": "ponencia",
        },
        url="https://example.com/gaceta/476",
        published_at="2026-05-19T00:00:00Z",
    )

    record = build_legislative_reconciliations([senado, gaceta])[0]

    assert record["origin_project"] == {
        "chamber": "senado",
        "number": "1",
        "year": "2026",
    }
    assert record["status"]["source_id"] == "senado_leyes_registry"
    assert record["m2_readiness"]["state"] == "ready"


def test_lossy_agenda_row_is_research_lead_not_ready() -> None:
    agenda = _raw(
        "senado_agenda_legislativa",
        "Senado agenda — Proyecto de Ley Senado — reforma",
        metadata={
            "content_extraction": "senado_agenda_pdf",
            "has_clean_project_identity": False,
            "project_records": [],
            "bill_title": "Reforma sin número limpio",
            "agenda_action_type": "discusión",
        },
    )

    record = build_legislative_reconciliations([agenda])[0]

    assert record["canonical_bill_id"].startswith("bill:research:")
    assert record["m2_readiness"]["state"] == "research_lead"
    assert "clean project number/year/chamber" in record["m2_readiness"]["missing"]


def test_title_only_fuzzy_match_does_not_become_ready() -> None:
    agenda = _raw(
        "senado_agenda_legislativa",
        "Proyecto de Ley sobre transporte de GLP en San Andrés",
        metadata={
            "has_clean_project_identity": False,
            "project_records": [],
            "bill_title": "Transporte de GLP en San Andrés",
        },
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta — transporte de GLP en San Andrés",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "project_records": [],
            "document_title": "Transporte de GLP en San Andrés",
        },
    )

    records = build_legislative_reconciliations([agenda, gaceta])

    assert records
    assert all(record["m2_readiness"]["state"] != "ready" for record in records)
    assert all(record["linked_projects"] == [] for record in records)


def test_registry_gaceta_contradiction_is_blocked() -> None:
    registry = _raw(
        "camara_proyectos_ley_registry",
        "Cámara registry — Proyecto de Ley 560 de 2025 Cámara — Subsidio GLP",
        metadata={
            "legislative_registry": "camara_proyectos_ley",
            "project_records": [_project("560", "2025", "Cámara")],
            "has_clean_project_identity": True,
            "bill_title": "Subsidio al transporte de GLP para San Andrés",
            "status": "En trámite",
        },
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 780 — Proyecto de Ley 560 de 2025 Cámara — informe de archivo",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "780",
            "project_label": "Proyecto de Ley 560 de 2025 Cámara",
            "project_records": [_project("560", "2025", "Cámara")],
            "document_title": "Informe de archivo del proyecto",
            "agenda_action_type": "archivo",
            "status": "Archivado",
        },
        raw_text="Gaceta del Congreso. Informe de archivo del proyecto.",
        published_at="2026-05-19T00:00:00Z",
    )

    record = build_legislative_reconciliations([registry, gaceta])[0]

    assert record["contradiction"]["has_contradiction"] is True
    assert record["contradiction"]["fields"] == ["status"]
    assert record["m2_readiness"]["state"] == "blocked"


def test_resolved_status_override_suppresses_archived_project_text_artifact() -> None:
    registry = _raw(
        "camara_proyectos_ley_registry",
        "Cámara registry — Proyecto de Ley 566 de 2026 Cámara — Obras",
        metadata={
            "legislative_registry": "camara_proyectos_ley",
            "project_records": [_project("566", "2026", "Cámara")],
            "has_clean_project_identity": True,
            "bill_title": "Obras por impuestos",
            "status": "Archivado",
        },
        published_at="2026-06-16T00:00:00Z",
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 800 — Proyecto de Ley 566 de 2026 Cámara",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "800",
            "project_label": "Proyecto de Ley 566 de 2026 Cámara",
            "project_records": [_project("566", "2026", "Cámara")],
            "document_title": "Proyecto de Ley 566 de 2026 Cámara",
            "agenda_action_type": "publicacion de gaceta",
        },
        published_at="2026-07-01T00:00:00Z",
    )
    overrides = {
        "bill:2026:camara:566": {
            "override_id": "pl566_archived_gaceta800_project_text",
            "decision_state": "archived",
            "m2_readiness_state": "resolved",
            "reason": "Manual reconciliation found project-text publication only.",
            "source": "runs/2026-07-06/pl572_566_reconciliation.md",
            "applies_when": {
                "status_stage": "archived",
                "latest_movement_action_types": ["publicacion_de_gaceta"],
            },
        }
    }

    record = build_legislative_reconciliations(
        [registry, gaceta],
        resolved_status_overrides=overrides,
    )[0]

    assert record["canonical_bill_id"] == "bill:2026:camara:566"
    assert record["contradiction"]["has_contradiction"] is False
    assert record["decision_state"] == "archived"
    assert record["m2_readiness"]["state"] == "resolved"
    assert record["resolved_status_override"]["override_id"] == (
        "pl566_archived_gaceta800_project_text"
    )


def test_resolved_status_override_does_not_hide_substantive_later_movement() -> None:
    registry = _raw(
        "camara_proyectos_ley_registry",
        "Cámara registry — Proyecto de Ley 566 de 2026 Cámara — Obras",
        metadata={
            "legislative_registry": "camara_proyectos_ley",
            "project_records": [_project("566", "2026", "Cámara")],
            "has_clean_project_identity": True,
            "bill_title": "Obras por impuestos",
            "status": "Archivado",
        },
        published_at="2026-06-16T00:00:00Z",
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 801 — Ponencia Proyecto de Ley 566 de 2026 Cámara",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "801",
            "project_label": "Proyecto de Ley 566 de 2026 Cámara",
            "project_records": [_project("566", "2026", "Cámara")],
            "document_title": "Ponencia para primer debate",
            "agenda_action_type": "ponencia",
        },
        published_at="2026-07-02T00:00:00Z",
    )
    overrides = {
        "bill:2026:camara:566": {
            "override_id": "pl566_archived_gaceta800_project_text",
            "decision_state": "archived",
            "m2_readiness_state": "resolved",
            "reason": "Manual reconciliation found project-text publication only.",
            "applies_when": {
                "status_stage": "archived",
                "latest_movement_action_types": ["publicacion_de_gaceta"],
            },
        }
    }

    record = build_legislative_reconciliations(
        [registry, gaceta],
        resolved_status_overrides=overrides,
    )[0]

    assert record["latest_movement"]["action_type"] == "ponencia_publicada"
    assert record["contradiction"]["has_contradiction"] is True
    assert record["m2_readiness"]["state"] == "blocked"
    assert "resolved_status_override" not in record


def test_verified_archive_override_closes_gaceta_only_unknown_status() -> None:
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 819 — Proyecto de Ley 041 de 2025 Cámara",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "819",
            "project_label": "Proyecto de Ley 041 de 2025 Cámara",
            "project_records": [_project("041", "2025", "Cámara")],
            "document_title": "Proyecto de Ley 041 de 2025 Cámara",
            "agenda_action_type": "publicacion de gaceta",
        },
        published_at="2026-07-07T00:00:00Z",
    )
    overrides = {
        "bill:2025:camara:41": {
            "override_id": "pl041_archived_verified_registry",
            "decision_state": "archived",
            "m2_readiness_state": "resolved",
            "reason": "Official Cámara registry verified the project as archived.",
            "status_override": {
                "stage": "archived",
                "label": "Archivado",
                "as_of": "2026-07-14",
                "source_id": "camara_proyectos_ley_registry",
                "url": "https://www.camara.gov.co/maquinaria-amarilla-320/",
            },
            "applies_when": {
                "require_contradiction": False,
                "status_stage": "unknown",
                "latest_movement_action_types": ["publicacion_de_gaceta"],
            },
        }
    }

    record = build_legislative_reconciliations(
        [gaceta],
        resolved_status_overrides=overrides,
    )[0]

    assert record["canonical_bill_id"] == "bill:2025:camara:41"
    assert record["status"]["stage"] == "archived"
    assert record["status"]["source_id"] == "camara_proyectos_ley_registry"
    assert record["decision_state"] == "archived"
    assert record["m2_readiness"]["state"] == "resolved"
    assert record["resolved_status_override"]["override_id"] == (
        "pl041_archived_verified_registry"
    )


def test_verified_archive_override_does_not_hide_later_ponencia() -> None:
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta 900 — Ponencia Proyecto de Ley 041 de 2025 Cámara",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "900",
            "project_label": "Proyecto de Ley 041 de 2025 Cámara",
            "project_records": [_project("041", "2025", "Cámara")],
            "document_title": "Ponencia para segundo debate",
            "agenda_action_type": "ponencia",
        },
        published_at="2026-07-15T00:00:00Z",
    )
    overrides = {
        "bill:2025:camara:41": {
            "override_id": "pl041_archived_verified_registry",
            "decision_state": "archived",
            "status_override": {"stage": "archived"},
            "applies_when": {
                "require_contradiction": False,
                "status_stage": "unknown",
                "latest_movement_action_types": ["publicacion_de_gaceta"],
            },
        }
    }

    record = build_legislative_reconciliations(
        [gaceta],
        resolved_status_overrides=overrides,
    )[0]

    assert record["latest_movement"]["action_type"] == "ponencia_publicada"
    assert record["decision_state"] == "unknown"
    assert record["m2_readiness"]["state"] == "research_lead"
    assert "resolved_status_override" not in record


def test_load_resolved_status_overrides_from_json(tmp_path) -> None:
    path = tmp_path / "resolved_status_overrides.json"
    path.write_text(
        """
{
  "schema_version": "resolved_status_overrides.v1",
  "overrides": [
    {
      "canonical_bill_id": "bill:2026:camara:566",
      "reason": "done"
    }
  ]
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    overrides = load_resolved_status_overrides(path)

    assert list(overrides) == ["bill:2026:camara:566"]
    assert overrides["bill:2026:camara:566"]["reason"] == "done"


def test_already_final_act_is_resolved_not_ready() -> None:
    diario = _raw(
        "diario_oficial",
        "Diario Oficial — Ley 560 de 2025",
        metadata={
            "content_extraction": "diario_oficial_pdf_text",
            "project_records": [_project("560", "2025", "Cámara")],
        },
        raw_text="Diario Oficial. Ley 560 de 2025 sancionada por el Presidente.",
    )

    record = build_legislative_reconciliations([diario])[0]

    assert record["decision_state"] == "resolved"
    assert record["m2_readiness"]["state"] == "resolved"


def test_cross_chamber_identity_preserves_linked_projects() -> None:
    camara = _raw(
        "camara_proyectos_ley_registry",
        "Cámara registry — Proyecto de Ley 560 de 2025 Cámara — Subsidio GLP",
        metadata={
            "legislative_registry": "camara_proyectos_ley",
            "project_records": [
                _project("560", "2025", "Cámara"),
                _project("123", "2026", "Senado"),
            ],
            "has_clean_project_identity": True,
            "bill_title": "Subsidio al transporte de GLP para San Andrés",
            "status": "En trámite",
        },
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta — Proyecto de Ley 123 de 2026 Senado",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "700",
            "project_label": "Proyecto de Ley 123 de 2026 Senado",
            "project_records": [_project("123", "2026", "Senado")],
            "agenda_action_type": "ponencia",
        },
        published_at="2026-05-19T00:00:00Z",
    )

    record = build_legislative_reconciliations([camara, gaceta])[0]

    linked = {
        (project["chamber"], project["number"], project["year"])
        for project in record["linked_projects"]
    }
    assert linked == {("camara", "560", "2025"), ("senado", "123", "2026")}
    assert record["contradiction"]["has_contradiction"] is False
    assert record["m2_readiness"]["state"] == "ready"
