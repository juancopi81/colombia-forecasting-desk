from __future__ import annotations

from colombia_forecasting_desk.indicator_watch import build_indicator_watch


def test_indicator_watch_registers_all_core_indicators() -> None:
    watch = build_indicator_watch([], [])

    assert len(watch) == 12
    assert {item.indicator_id for item in watch} == {
        "ipc_inflation",
        "trm_usd_cop",
        "policy_rate_ibr",
        "labor_market",
        "retail_sales",
        "manufacturing",
        "construction_bundle",
        "secop_procurement",
        "energy_system",
        "external_trade",
        "oil_gas_production",
        "fiscal_tax_pulse",
    }
    assert all(item.status == "pending_source" for item in watch)


def test_indicator_watch_extracts_icoced_observation(make_raw) -> None:
    raw = make_raw(
        source_id="dane_icoced",
        source_name="DANE ICOCED",
        source_type="economic_indicator",
        url="https://www.dane.gov.co/files/operaciones/ICOCED/anex-ICOCED-mar2026.xlsx",
        title="DANE ICOCED — Anexo marzo 2026",
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

    construction = next(
        item
        for item in build_indicator_watch([raw], [])
        if item.indicator_id == "construction_bundle"
    )

    assert construction.status == "observed"
    assert construction.period == "2026-03"
    assert construction.release_date == "2026-04-30T00:00:00Z"
    assert construction.values["icoced_total_index"] == 135.44
    assert construction.values["icoced_residential_monthly_variation_pct"] == 0.77


def test_indicator_watch_extracts_secop_pulse(make_cleaned) -> None:
    cleaned = [
        make_cleaned(source_id="secop_ii_contratos", published_at="2026-05-01T00:00:00Z"),
        make_cleaned(source_id="secop_ii_contratos", published_at="2026-05-02T00:00:00Z"),
        make_cleaned(source_id="secop_ii_adiciones", published_at="2026-05-02T00:00:00Z"),
        make_cleaned(source_id="eltiempo_colombia", published_at="2026-05-02T00:00:00Z"),
    ]

    secop = next(
        item
        for item in build_indicator_watch([], cleaned)
        if item.indicator_id == "secop_procurement"
    )

    assert secop.status == "observed"
    assert secop.values["rankable_records"] == 3
    assert secop.values["records_by_source"] == {
        "secop_ii_adiciones": 1,
        "secop_ii_contratos": 2,
    }
