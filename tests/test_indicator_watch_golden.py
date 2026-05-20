from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from colombia_forecasting_desk.indicator_watch import build_indicator_watch
from colombia_forecasting_desk.models import CleanedItem, IndicatorObservation, RawItem

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "indicator_watch"
GOLDEN_NOW = datetime(2026, 5, 19, tzinfo=timezone.utc)


def _load_fixture(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text())


def _catalog_summary(watch: list[IndicatorObservation]) -> list[dict[str, object]]:
    return [
        {
            "indicator_id": item.indicator_id,
            "name": item.name,
            "category": item.category,
            "frequency": item.frequency,
            "source_name": item.source_name,
            "source_url": item.source_url,
            "status": item.status,
            "freshness_status": item.freshness_status,
            "why_it_matters": item.why_it_matters,
            "correlations": item.correlations,
            "next_step": item.next_step,
            "components": [
                {
                    "component_id": component.component_id,
                    "name": component.name,
                    "status": component.status,
                    "source_name": component.source_name,
                    "source_url": component.source_url,
                    "freshness_status": component.freshness_status,
                    "next_step": component.next_step,
                }
                for component in item.components
            ],
        }
        for item in watch
    ]


def _runtime_summary(watch: list[IndicatorObservation]) -> list[dict[str, object]]:
    wanted = {"construction_bundle", "secop_procurement", "fiscal_tax_pulse"}
    return [
        {
            "indicator_id": item.indicator_id,
            "status": item.status,
            "period": item.period,
            "release_date": item.release_date,
            "headline": item.headline,
            "values": item.values,
            "freshness_status": item.freshness_status,
            "components": [
                {
                    "component_id": component.component_id,
                    "status": component.status,
                    "period": component.period,
                    "release_date": component.release_date,
                    "headline": component.headline,
                    "values": component.values,
                    "freshness_status": component.freshness_status,
                }
                for component in item.components
            ],
        }
        for item in watch
        if item.indicator_id in wanted
    ]


def _mixed_runtime_inputs() -> tuple[list[RawItem], list[CleanedItem]]:
    raw_icoced = RawItem(
        id="icoced-2026-03",
        source_id="dane_icoced",
        source_name="DANE ICOCED",
        source_type="economic_indicator",
        url=(
            "https://www.dane.gov.co/files/operaciones/ICOCED/"
            "anex-ICOCED-mar2026.xlsx"
        ),
        title="DANE ICOCED - Anexo marzo 2026",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-04-30T00:00:00Z",
        raw_text="DANE ICOCED headline.",
        metadata={
            "content_extraction": "dane_icoced_xlsx",
            "period_year": 2026,
            "period_month": 3,
            "headline_metrics": {
                "total": {
                    "index": 135.44,
                    "monthly_variation_pct": 0.75,
                    "year_to_date_variation_pct": 6.47,
                    "annual_variation_pct": 6.33,
                },
                "residential": {"monthly_variation_pct": 0.77},
                "non_residential": {"monthly_variation_pct": 0.72},
            },
        },
    )
    raw_secop_1 = RawItem(
        id="secop-1",
        source_id="secop_ii_contratos",
        source_name="SECOP II Contratos",
        source_type="procurement",
        url="https://example.com/secop/1",
        title="Contrato 1",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-18T00:00:00Z",
        raw_text="Contrato 1",
        metadata={"entity": "Alcaldia de Cali"},
    )
    raw_secop_2 = RawItem(
        id="secop-2",
        source_id="secop_ii_adiciones",
        source_name="SECOP II Adiciones",
        source_type="procurement",
        url="https://example.com/secop/2",
        title="Adicion 1",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-18T00:00:00Z",
        raw_text="Adicion 1",
        metadata={"entity": "Gobernacion del Meta"},
    )
    cleaned = [
        CleanedItem(
            id="secop-1",
            source_id="secop_ii_contratos",
            source_name="SECOP II Contratos",
            source_type="procurement",
            url="https://example.com/secop/1",
            title="Contrato 1",
            fetched_at="2026-05-19T00:00:00Z",
            published_at="2026-05-18T00:00:00Z",
            clean_text="Contrato 1",
            summary="Contrato 1",
            signal_type="procurement",
            country_relevance="high",
            quality_notes="",
        ),
        CleanedItem(
            id="secop-2",
            source_id="secop_ii_adiciones",
            source_name="SECOP II Adiciones",
            source_type="procurement",
            url="https://example.com/secop/2",
            title="Adicion 1",
            fetched_at="2026-05-19T00:00:00Z",
            published_at="2026-05-18T00:00:00Z",
            clean_text="Adicion 1",
            summary="Adicion 1",
            signal_type="procurement",
            country_relevance="high",
            quality_notes="",
        ),
    ]
    return [raw_icoced, raw_secop_1, raw_secop_2], cleaned


def test_indicator_watch_pending_catalog_matches_golden_fixture() -> None:
    watch = build_indicator_watch([], [], now=GOLDEN_NOW)

    assert _catalog_summary(watch) == _load_fixture("pending_catalog_summary.json")


def test_indicator_watch_mixed_runtime_matches_golden_fixture() -> None:
    raw_items, cleaned_items = _mixed_runtime_inputs()
    watch = build_indicator_watch(raw_items, cleaned_items, now=GOLDEN_NOW)

    assert _runtime_summary(watch) == _load_fixture("mixed_runtime_summary.json")
