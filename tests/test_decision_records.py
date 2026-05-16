from __future__ import annotations

from colombia_forecasting_desk.decision_records import (
    link_legislative_followups,
    link_official_legal_records,
)
from colombia_forecasting_desk.models import RawItem


def _raw(
    source_id: str,
    title: str,
    *,
    metadata: dict,
    url: str = "https://example.com/item",
    raw_text: str | None = None,
) -> RawItem:
    return RawItem(
        id=f"{source_id}-{title.lower().replace(' ', '-')[:32]}",
        source_id=source_id,
        source_name=source_id,
        source_type="legal",
        url=url,
        title=title,
        fetched_at="2026-05-15T12:00:00Z",
        published_at="2026-05-15T00:00:00Z",
        raw_text=raw_text if raw_text is not None else title,
        metadata=metadata,
    )


def test_link_legislative_followups_matches_clean_senado_to_gaceta() -> None:
    senado = _raw(
        "senado_agenda_legislativa",
        "Senado agenda — Proyecto de Ley 550 de 2026 Senado — reforma",
        metadata={
            "has_clean_project_identity": True,
            "project_records": [
                {"number": "550", "year": "2026", "chamber": "Senado"}
            ],
        },
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta del Congreso 476 — Proyecto de Ley 550 DE 2026 Cámara y Senado",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "476",
            "project_label": "Proyecto de Ley 550 DE 2026 Cámara y Senado",
            "document_title": "por medio de la cual se adopta una reforma",
            "agenda_action_type": "ponencia",
            "project_records": [
                {"number": "550", "year": "2026", "chamber": "Cámara/Senado"}
            ],
        },
        url="https://example.com/gaceta-476",
    )

    linked = link_legislative_followups([senado, gaceta])

    assert linked[0].metadata["official_followup_match_count"] == 1
    assert linked[0].metadata["official_followup_matches"][0]["gaceta_number"] == "476"
    assert linked[0].metadata["resolution_source_status"] == "official_followup_matched"
    assert "Official follow-up matches" in linked[0].raw_text


def test_link_legislative_followups_matches_registry_to_gaceta() -> None:
    registry = _raw(
        "senado_leyes_registry",
        "Senado registry — Proyecto de Ley 001 de 2026 Senado — reforma",
        metadata={
            "has_clean_project_identity": True,
            "project_records": [
                {"number": "001", "year": "2026", "chamber": "Senado"}
            ],
        },
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta del Congreso 476 — Proyecto de Ley 1 DE 2026 Senado",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "476",
            "project_label": "Proyecto de Ley 1 DE 2026 Senado",
            "document_title": "por medio de la cual se adopta una reforma",
            "agenda_action_type": "ponencia",
            "project_records": [
                {"number": "1", "year": "2026", "chamber": "Senado"}
            ],
        },
        url="https://example.com/gaceta-476",
    )

    linked = link_legislative_followups([registry, gaceta])

    assert linked[0].metadata["official_followup_match_count"] == 1
    assert linked[0].metadata["official_followup_matches"][0]["gaceta_number"] == "476"


def test_link_legislative_followups_ignores_lossy_senado_identity() -> None:
    senado = _raw(
        "senado_agenda_legislativa",
        "Senado agenda — Proyecto de Ley Senado — reforma",
        metadata={
            "has_clean_project_identity": False,
            "project_records": [],
        },
    )
    gaceta = _raw(
        "gacetas_congreso",
        "Gaceta del Congreso 476 — Proyecto de Ley 550 DE 2026 Cámara y Senado",
        metadata={
            "content_extraction": "gaceta_pdf_text",
            "edition_number": "476",
            "project_records": [
                {"number": "550", "year": "2026", "chamber": "Cámara/Senado"}
            ],
        },
    )

    linked = link_legislative_followups([senado, gaceta])

    assert "official_followup_matches" not in linked[0].metadata


def test_link_official_legal_records_matches_mincit_resolution_context() -> None:
    mincit = _raw(
        "mincit_zonas_francas",
        "MinCIT Zonas Francas aprobadas — Rionegro MRO",
        metadata={
            "registry": "mincit_zonas_francas_aprobadas",
            "zona_franca_name": (
                "Zona Franca Permanente Especial De Servicios Rionegro MRO"
            ),
            "declaratory_resolution": (
                "Res. No. 2118 del 26 de diciembre de 2025"
            ),
            "extension_resolution": "Vacía",
        },
    )
    diario = _raw(
        "diario_oficial",
        "Diario Oficial 53.490 — Resolución 2118 de 2025",
        metadata={
            "content_extraction": "diario_oficial_pdf_text",
            "legal_act_records": [
                {
                    "kind": "Resolución",
                    "number": "2118",
                    "year": "2025",
                    "label": "Resolución 2118 de 2025",
                }
            ],
        },
        raw_text=(
            "Ministerio de Comercio, Industria y Turismo. Resolución 2118 "
            "de 2025 por la cual se declara la Zona Franca Permanente "
            "Especial De Servicios Rionegro MRO."
        ),
        url="https://example.com/diario-53490",
    )

    linked = link_official_legal_records([mincit, diario])

    assert linked[0].metadata["official_resolution_match_count"] == 1
    match = linked[0].metadata["official_resolution_matches"][0]
    assert match["source_id"] == "diario_oficial"
    assert match["legal_act_label"] == "Resolución 2118 de 2025"
    assert linked[0].metadata["resolution_source_status"] == (
        "official_resolution_matched"
    )
    assert "Official resolution matches" in linked[0].raw_text


def test_link_official_legal_records_rejects_same_resolution_without_context() -> None:
    mincit = _raw(
        "mincit_zonas_francas",
        "MinCIT Zonas Francas aprobadas — Rionegro MRO",
        metadata={
            "registry": "mincit_zonas_francas_aprobadas",
            "zona_franca_name": (
                "Zona Franca Permanente Especial De Servicios Rionegro MRO"
            ),
            "declaratory_resolution": (
                "Res. No. 2118 del 26 de diciembre de 2025"
            ),
            "extension_resolution": "Vacía",
        },
    )
    unrelated = _raw(
        "diario_oficial",
        "Diario Oficial 53.490 — Resolución 2118 de 2025",
        metadata={
            "content_extraction": "diario_oficial_pdf_text",
            "legal_act_records": [
                {
                    "kind": "Resolución",
                    "number": "2118",
                    "year": "2025",
                    "label": "Resolución 2118 de 2025",
                }
            ],
        },
        raw_text=(
            "Resolución 2118 de 2025 de una entidad distinta sobre un trámite "
            "administrativo sin relación con zonas francas."
        ),
        url="https://example.com/diario-53490",
    )

    linked = link_official_legal_records([mincit, unrelated])

    assert "official_resolution_matches" not in linked[0].metadata
