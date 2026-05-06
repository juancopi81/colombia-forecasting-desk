from __future__ import annotations

from colombia_forecasting_desk.indicator_watch import (
    build_indicator_watch,
    ipc_observation_from_html,
    labor_market_observation_from_html,
    manufacturing_observation_from_html,
    retail_sales_observation_from_html,
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


def test_ipc_observation_from_html_extracts_dane_headline() -> None:
    observation = ipc_observation_from_html(
        """
        <main>
          <p>Boletín técnico 9/04/2026</p>
          <p>En marzo de 2026 la variación mensual del IPC fue 0,78%,
          la variación año corrido fue 3,07% y la anual 5,56%.</p>
          <p>En marzo de 2026 la variación anual del IPC fue 5,56%, es decir,
          0,47 puntos porcentuales mayor que la reportada en el mismo periodo
          del año anterior, cuando fue de (5,09%).</p>
          <p>Las mayores variaciones se presentaron en las divisiones
          Información y comunicación (2,96%) y Alimentos y bebidas no
          alcohólicas (1,27%).</p>
        </main>
        """
    )

    assert observation is not None
    assert observation.status == "observed"
    assert observation.period == "2026-03"
    assert observation.release_date == "2026-04-09T00:00:00Z"
    assert observation.values["monthly_variation_pct"] == 0.78
    assert observation.values["year_to_date_variation_pct"] == 3.07
    assert observation.values["annual_variation_pct"] == 5.56
    assert observation.values["annual_previous_year_pct"] == 5.09
    assert observation.values["largest_monthly_divisions"][0] == {
        "name": "Información y comunicación",
        "monthly_variation_pct": 2.96,
    }


def test_retail_sales_observation_from_html_extracts_dane_headline() -> None:
    observation = retail_sales_observation_from_html(
        """
        <main>
          <p>Boletín técnico 16-abr-2026</p>
          <p>En febrero de 2026, las ventas reales del comercio minorista
          aumentaron 10,9% y el personal ocupado creció 1,8% en relación
          con el mismo mes de 2025. Excluyendo el comercio de combustibles,
          la variación de las ventas reales del sector fue de 13,7%.</p>
        </main>
        """
    )

    assert observation is not None
    assert observation.indicator_id == "retail_sales"
    assert observation.period == "2026-02"
    assert observation.release_date == "2026-04-16T00:00:00Z"
    assert observation.values["real_retail_sales_annual_variation_pct"] == 10.9
    assert observation.values["employment_annual_variation_pct"] == 1.8
    assert observation.values["real_retail_sales_ex_fuel_annual_variation_pct"] == 13.7


def test_manufacturing_observation_from_html_extracts_dane_headline() -> None:
    observation = manufacturing_observation_from_html(
        """
        <main>
          <p>Anexos 16-abr-2026</p>
          <p>En febrero de 2026 frente a febrero de 2025, la producción real
          de la industria manufacturera presentó una variación de 1,4%, las
          ventas reales de -2,5% y el personal ocupado de -0,4%.</p>
          <p>De las 39 actividades industriales representadas por la encuesta,
          un total de 20 registraron variaciones positivas en su producción
          real, contribuyendo con 4,4 puntos porcentuales a la variación total
          anual y 19 subsectores presentaron variaciones negativas con una
          contribución de -3,0 puntos porcentuales.</p>
        </main>
        """
    )

    assert observation is not None
    assert observation.indicator_id == "manufacturing"
    assert observation.period == "2026-02"
    assert observation.values["real_production_annual_variation_pct"] == 1.4
    assert observation.values["real_sales_annual_variation_pct"] == -2.5
    assert observation.values["employment_annual_variation_pct"] == -0.4
    assert observation.values["activities_positive_count"] == 20
    assert observation.values["activities_negative_count"] == 19
    assert observation.values["positive_contribution_pp"] == 4.4
    assert observation.values["negative_contribution_pp"] == -3.0


def test_labor_market_observation_from_html_extracts_dane_headline() -> None:
    observation = labor_market_observation_from_html(
        """
        <main>
          <p>Comunicado 30/04/2026</p>
          <p>Para marzo de 2026, la tasa de desocupación del total nacional
          fue 8,8%, lo que representó una disminución de 0,8 puntos porcentuales
          respecto al mismo mes de 2025 (9,6%). La tasa global de participación
          se ubicó en 65,0% y la tasa de ocupación en 59,3%, en marzo de 2025
          estas tasas fueron 64,7% y 58,5%, respectivamente.</p>
          <p>Para las 13 ciudades y áreas metropolitanas, la tasa de
          desocupación fue 9,4%, en comparación con 9,3% observado en marzo
          de 2025.</p>
        </main>
        """
    )

    assert observation is not None
    assert observation.indicator_id == "labor_market"
    assert observation.period == "2026-03"
    assert observation.release_date == "2026-04-30T00:00:00Z"
    assert observation.values["national_unemployment_rate_pct"] == 8.8
    assert observation.values["national_unemployment_annual_change_pp"] == -0.8
    assert observation.values["national_unemployment_previous_year_pct"] == 9.6
    assert observation.values["national_participation_rate_pct"] == 65.0
    assert observation.values["national_occupation_rate_pct"] == 59.3
    assert observation.values["thirteen_cities_unemployment_rate_pct"] == 9.4
