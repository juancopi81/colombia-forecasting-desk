from __future__ import annotations

from colombia_forecasting_desk.models import CleanedItem, RawItem
from colombia_forecasting_desk.zona_franca_leads import (
    build_zona_franca_land_use_leads,
)


def _raw(
    *,
    content_extraction: str = "mincit_zonas_francas_approved_diff",
    registry_change_type: str = "new_registry_row",
    official_match: bool = False,
) -> RawItem:
    metadata = {
        "registry": "mincit_zonas_francas_aprobadas",
        "registry_row_type": "approved_zone",
        "registry_key": "901911193",
        "content_extraction": content_extraction,
        "registry_change_type": registry_change_type,
        "changed_fields": ["zona_franca_name", "municipality"],
        "nit": "901911193",
        "zona_franca_name": "Zona Franca Permanente Especial De Servicios Rionegro MRO",
        "zone_class": "Permanente Especial",
        "user_type": "Servicios",
        "department": "Antioquia",
        "municipality": "Rionegro",
        "declaratory_resolution": "Res. No. 2118 del 26 de diciembre de 2025",
        "extension_resolution": "Vacía",
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
    }
    if official_match:
        metadata["official_resolution_matches"] = [
            {
                "source_id": "diario_oficial",
                "source_name": "Diario Oficial",
                "url": "https://example.com/diario-53490",
                "title": "Diario Oficial 53.490",
                "legal_act_label": "Resolución 2118 de 2025",
            }
        ]
    return RawItem(
        id="mincit-zf-change-1",
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas (Estadísticas)",
        source_type="regulatory",
        url="https://zf.mincit.gov.co/estadisticas#zf-1",
        title=(
            "MinCIT zona franca registry change — Zona Franca Permanente "
            "Especial De Servicios Rionegro MRO"
        ),
        fetched_at="2026-05-25T15:00:00Z",
        published_at="2026-05-25T00:00:00Z",
        raw_text="Official MinCIT approved-zones registry shows a new row.",
        metadata=metadata,
    )


def _cleaned(raw: RawItem, *, quality_notes: str = "") -> CleanedItem:
    return CleanedItem(
        id=raw.id,
        source_id=raw.source_id,
        source_name=raw.source_name,
        source_type=raw.source_type,
        url=raw.url,
        title=raw.title,
        fetched_at=raw.fetched_at,
        published_at=raw.published_at,
        clean_text=raw.raw_text,
        summary=raw.raw_text,
        signal_type="court_or_regulatory_movement",
        country_relevance="high",
        quality_notes=quality_notes,
        detected_entities=["MinCIT"],
        detected_topics=["zona franca"],
        trust_role="regulatory_signal",
        priority="high",
        metadata=raw.metadata,
    )


def test_zona_franca_land_use_lead_from_new_registry_row() -> None:
    raw = _raw()

    leads = build_zona_franca_land_use_leads([raw], [_cleaned(raw)])

    assert len(leads) == 1
    lead = leads[0]
    assert lead["lead_type"] == "analyst_insight"
    assert lead["review_context"]["family"] == "land_use_zona_franca"
    assert lead["review_context"]["pattern"] == "new_zona_franca_registry_row"
    assert lead["review_context"]["municipality"] == "Rionegro"
    assert "Rionegro, Antioquia" in lead["claim_or_question"]
    assert "investment advice" in lead["caveats"][0]
    assert lead["evidence"][0]["content_kind"] == (
        "structured_zona_franca_registry_change"
    )
    assert "Res. No. 2118" in lead["evidence"][0]["value"]


def test_zona_franca_lead_includes_official_resolution_match() -> None:
    raw = _raw(registry_change_type="updated_registry_row", official_match=True)

    leads = build_zona_franca_land_use_leads([raw], [_cleaned(raw)])

    assert len(leads) == 1
    lead = leads[0]
    assert lead["review_context"]["pattern"] == "updated_zona_franca_registry_row"
    assert lead["review_context"]["official_resolution_match_count"] == 1
    assert lead["evidence"][1]["content_kind"] == "official_legal_resolution_match"
    assert "Open the matched official legal record" in lead["next_check"]
    assert all(
        "No deterministic Diario Oficial" not in caveat
        for caveat in lead["caveats"]
    )


def test_zona_franca_leads_ignore_historical_rows_without_diff() -> None:
    raw = _raw(
        content_extraction="mincit_zonas_francas_approved_pdf",
        registry_change_type="",
    )

    leads = build_zona_franca_land_use_leads([raw], [_cleaned(raw)])

    assert leads == []


def test_zona_franca_leads_ignore_low_quality_cleaned_items() -> None:
    raw = _raw()

    leads = build_zona_franca_land_use_leads(
        [raw],
        [_cleaned(raw, quality_notes="missing date")],
    )

    assert leads == []
