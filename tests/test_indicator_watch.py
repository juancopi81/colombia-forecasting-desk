from __future__ import annotations

from colombia_forecasting_desk.indicator_watch import (
    build_indicator_watch,
    trm_observation_from_rows,
)


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


def test_indicator_watch_extracts_secop_pulse(make_cleaned, make_raw) -> None:
    raw = [
        make_raw(
            id="s1",
            source_id="secop_ii_contratos",
            metadata={"entity": "Alcaldia de Cali"},
        ),
        make_raw(
            id="s2",
            source_id="secop_ii_contratos",
            metadata={"entity": "Alcaldia de Cali"},
        ),
        make_raw(
            id="s3",
            source_id="secop_ii_adiciones",
            metadata={"entity": "Gobernacion del Meta"},
        ),
    ]
    cleaned = [
        make_cleaned(
            id="s1",
            source_id="secop_ii_contratos",
            published_at="2026-05-01T00:00:00Z",
        ),
        make_cleaned(
            id="s2",
            source_id="secop_ii_contratos",
            published_at="2026-05-02T00:00:00Z",
        ),
        make_cleaned(
            id="s3",
            source_id="secop_ii_adiciones",
            published_at="2026-05-02T00:00:00Z",
        ),
        make_cleaned(
            id="n1",
            source_id="eltiempo_colombia",
            published_at="2026-05-02T00:00:00Z",
        ),
    ]

    secop = next(
        item
        for item in build_indicator_watch(raw, cleaned)
        if item.indicator_id == "secop_procurement"
    )

    assert secop.status == "observed"
    assert secop.values["rankable_records"] == 3
    assert secop.values["records_by_source"] == {
        "secop_ii_adiciones": 1,
        "secop_ii_contratos": 2,
    }
    assert secop.values["records_by_day"] == {
        "2026-05-01": 1,
        "2026-05-02": 2,
    }
    assert secop.values["records_by_process_type"] == {
        "secop_ii_additions": 1,
        "secop_ii_contracts": 2,
    }
    assert secop.values["top_entities"][0] == {
        "name": "Alcaldia de Cali",
        "records": 2,
    }


def test_trm_observation_from_rows_computes_changes() -> None:
    observation = trm_observation_from_rows(
        [
            {
                "valor": "3723.33",
                "unidad": "COP",
                "vigenciadesde": "2026-05-06T00:00:00.000",
                "vigenciahasta": "2026-05-06T00:00:00.000",
            },
            {
                "valor": "3707.58",
                "unidad": "COP",
                "vigenciadesde": "2026-05-05T00:00:00.000",
            },
            {
                "valor": "3633.76",
                "unidad": "COP",
                "vigenciadesde": "2026-04-29T00:00:00.000",
            },
            {
                "valor": "3600.00",
                "unidad": "COP",
                "vigenciadesde": "2026-04-06T00:00:00.000",
            },
        ]
    )

    assert observation is not None
    assert observation.status == "observed"
    assert observation.period == "2026-05-06"
    assert observation.values["trm_cop_per_usd"] == 3723.33
    assert observation.values["daily_change_cop"] == 15.75
    assert observation.values["seven_day_change_cop"] == 89.57
    assert observation.values["thirty_day_change_pct"] == 3.43
