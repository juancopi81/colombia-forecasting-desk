from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone

from colombia_forecasting_desk.indicator_watch import (
    build_indicator_watch,
    cement_component_from_html,
    construction_bundle_observation_from_components,
    construction_licenses_component_from_html,
    crude_oil_component_from_anh_rows,
    electricity_demand_component_from_xm_response,
    energy_system_observation_from_components,
    exports_component_from_html,
    external_trade_observation_from_components,
    fiscal_tax_observation_from_dian_xlsx,
    fiscalized_gas_component_from_anh_rows,
    housing_finance_component_from_html,
    imports_component_from_html,
    ipc_observation_from_html,
    labor_market_observation_from_html,
    latest_complete_anh_period,
    manufacturing_observation_from_html,
    oil_gas_observation_from_components,
    policy_rate_ibr_observation_from_rows,
    reservoir_component_from_xm_response,
    retail_sales_observation_from_html,
    spot_price_component_from_xm_response,
    trm_observation_from_rows,
)
from colombia_forecasting_desk.models import IndicatorObservation


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
    assert all(item.freshness_status == "pending" for item in watch)
    construction = next(
        item for item in watch if item.indicator_id == "construction_bundle"
    )
    assert [component.component_id for component in construction.components] == [
        "icoced",
        "cement",
        "licenses",
        "housing_finance",
    ]
    energy = next(item for item in watch if item.indicator_id == "energy_system")
    assert [component.component_id for component in energy.components] == [
        "electricity_demand",
        "reservoir_useful_volume",
        "spot_price",
    ]
    oil_gas = next(
        item for item in watch if item.indicator_id == "oil_gas_production"
    )
    assert [component.component_id for component in oil_gas.components] == [
        "oil_production",
        "gas_production",
    ]
    external_trade = next(
        item for item in watch if item.indicator_id == "external_trade"
    )
    assert [component.component_id for component in external_trade.components] == [
        "exports",
        "imports",
    ]


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
        for item in build_indicator_watch(
            [raw],
            [],
            now=datetime(2026, 5, 6, tzinfo=timezone.utc),
        )
        if item.indicator_id == "construction_bundle"
    )

    assert construction.status == "observed"
    assert construction.period == "2026-03"
    assert construction.release_date == "2026-04-30T00:00:00Z"
    assert construction.freshness_status == "current"
    assert construction.values["icoced_total_index"] == 135.44
    assert construction.values["icoced_residential_monthly_variation_pct"] == 0.77
    assert construction.components[0].component_id == "icoced"
    assert construction.components[0].status == "observed"
    assert construction.components[1].component_id == "cement"
    assert construction.components[1].status == "pending_source"


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


def test_policy_rate_ibr_observation_from_banrep_rows() -> None:
    observation = policy_rate_ibr_observation_from_rows(
        [
            {
                "fecha": "05/05/2026",
                "valor": 11.25,
            }
        ],
        [
            {
                "fecha": "05/05/2026",
                "valor": 10.505,
            }
        ],
    )

    assert observation is not None
    assert observation.indicator_id == "policy_rate_ibr"
    assert observation.status == "observed"
    assert observation.period == "2026-05-05"
    assert observation.release_date == "2026-05-05T00:00:00Z"
    assert observation.values["policy_rate_pct"] == 11.25
    assert observation.values["ibr_overnight_nominal_pct"] == 10.505
    assert observation.values["ibr_policy_spread_pp"] == -0.745


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


def test_construction_components_from_html_extract_dane_headlines() -> None:
    cement = cement_component_from_html(
        """
        <main>
          <p>En marzo de 2026, la producción de cemento gris a nivel nacional
          fue de 1.246,7 miles de toneladas, lo que representó una variación
          de 3,8% con relación al mismo mes de 2025. En el mes de análisis se
          despacharon al mercado nacional 1.149,7 miles de toneladas de cemento
          gris, lo que significó un crecimiento del 6,0% frente a marzo de
          2025.</p>
          <p>En el período enero – marzo 2026 la producción de cemento gris
          alcanzó los 3.319,0 miles de toneladas, presentando un aumento de
          2,8% con relación al mismo periodo del año anterior. Los despachos al
          mercado nacional acumularon 3.125,4 miles de toneladas dando como
          resultado una variación positiva de 5,7%.</p>
          <p>Información actualizada el 30 de abril de 2026</p>
        </main>
        """
    )
    licenses = construction_licenses_component_from_html(
        """
        <main>
          <p>En febrero de 2026 se licenciaron 2.016.426 m² para construcción,
          cifra superior en 44.476 m² a la registrada en el mismo mes de 2025
          (1.971.950 m²). Esto se traduce en un crecimiento anual de 2,3% en el
          área licenciada. El comportamiento del total licenciado se explica
          por el aumento de 30,7% en el área aprobada para destinos no
          habitacionales. Por otra parte, el área aprobada para vivienda
          disminuyó 4,4%.</p>
          <p>Durante el mes de referencia se aprobaron 1.527.141 m² para
          vivienda. Por su parte, el área aprobada para destinos no
          habitacionales alcanzó 489.285 m².</p>
          <p>Información actualizada el 15 de abril de 2026</p>
        </main>
        """
    )
    housing = housing_finance_component_from_html(
        """
        <main>
          <p>Durante el cuarto trimestre de 2025, se desembolsaron $8.656.077
          millones de pesos corrientes para compra de vivienda, de los cuales
          $7.189.602 millones fueron créditos de vivienda y $1.466.476 millones
          fueron leasing habitacional.</p>
          <p>En el cuarto trimestre de 2025, los desembolsos para compra de
          vivienda a precios constantes sumaron $3.799.272 millones, con una
          variación anual de 10,3%.</p>
          <p>Información actualizada 16 de febrero de 2026</p>
        </main>
        """
    )

    assert cement is not None
    assert cement.period == "2026-03"
    assert cement.values["production_thousand_tons"] == 1246.7
    assert cement.values["domestic_shipments_annual_variation_pct"] == 6.0
    assert licenses is not None
    assert licenses.period == "2026-02"
    assert licenses.values["licensed_area_m2"] == 2016426.0
    assert licenses.values["housing_area_annual_variation_pct"] == -4.4
    assert licenses.values["non_residential_area_m2"] == 489285.0
    assert housing is not None
    assert housing.period == "2025-Q4"
    assert housing.values["purchase_disbursements_cop_millions"] == 8656077.0
    assert housing.values["real_purchase_disbursements_annual_variation_pct"] == 10.3


def test_construction_bundle_merges_components_with_icoced(make_raw) -> None:
    cement = cement_component_from_html(
        """
        <p>En marzo de 2026, la producción de cemento gris a nivel nacional fue
        de 1.246,7 miles de toneladas, lo que representó una variación de 3,8%
        con relación al mismo mes de 2025. En el mes de análisis se despacharon
        al mercado nacional 1.149,7 miles de toneladas de cemento gris, lo que
        significó un crecimiento del 6,0% frente a marzo de 2025.</p>
        <p>Información actualizada el 30 de abril de 2026</p>
        """
    )
    assert cement is not None
    extra = construction_bundle_observation_from_components([cement])
    raw = make_raw(
        source_id="dane_icoced",
        source_name="DANE ICOCED",
        url="https://www.dane.gov.co/files/operaciones/ICOCED/anex-ICOCED-mar2026.xlsx",
        title="DANE ICOCED — Anexo marzo 2026",
        published_at="2026-04-30T00:00:00Z",
        metadata={
            "period_year": 2026,
            "period_month": 3,
            "headline_metrics": {"total": {"index": 135.44}},
        },
    )

    construction = next(
        item
        for item in build_indicator_watch(
            [raw],
            [],
            [extra] if extra else [],
            now=datetime(2026, 5, 6, tzinfo=timezone.utc),
        )
        if item.indicator_id == "construction_bundle"
    )

    by_id = {component.component_id: component for component in construction.components}
    assert construction.status == "observed"
    assert construction.freshness_status == "current"
    assert construction.values["observed_components"] == 2
    assert by_id["icoced"].status == "observed"
    assert by_id["cement"].status == "observed"
    assert by_id["licenses"].status == "pending_source"


def test_external_trade_components_from_dane_html_build_bundle() -> None:
    exports = exports_component_from_html(
        """
        <main>
          <p>Información marzo de 2026</p>
          <p>De acuerdo con la información de exportaciones procesada por el
          DANE y la DIAN, en marzo de 2026 las ventas externas del país fueron
          US$5.315,9 millones FOB y presentaron un crecimiento de 20,9% en
          relación con marzo de 2025; este resultado se debió principalmente al
          aumento del 149,2% en las ventas externas del grupo de Otros
          Sectores.</p>
          <p>En el mes de referencia, las exportaciones de Combustibles y
          productos de industrias extractivas participaron con 41,5% del valor
          FOB total de las exportaciones; así mismo, Agropecuarios, alimentos y
          bebidas con 24,4%, Manufacturas con 17,9% y Otros sectores con 16,1%.</p>
          <p>Información actualizada el 5 de mayo de 2026</p>
        </main>
        """
    )
    imports = imports_component_from_html(
        """
        <main>
          <p>Información marzo 2026</p>
          <p>De acuerdo con las declaraciones de importación registradas ante
          la DIAN en marzo de 2026, las importaciones fueron US$5.100,0
          millones CIF y presentaron un crecimiento de 7,8% con relación al
          mismo mes de 2025. Este comportamiento obedeció principalmente al
          aumento de 13,2% en el grupo de Manufacturas.</p>
          <p>En marzo de 2026, las importaciones de Manufacturas participaron
          con 75,6% del valor CIF total de las importaciones, seguido por
          Agropecuarios, alimentos y bebidas con 13,7%, Combustibles y
          productos de las industrias extractivas con 10,7% y Otros sectores
          con 0,1%.</p>
          <p>Información actualizada el 21 de abril de 2026</p>
        </main>
        """
    )

    assert exports is not None
    assert exports.period == "2026-03"
    assert exports.values["exports_usd_millions_fob"] == 5315.9
    assert exports.values["export_group_shares_pct"]["fuels_and_extractives"] == 41.5
    assert imports is not None
    assert imports.values["imports_usd_millions_cif"] == 5100.0
    assert imports.values["import_group_shares_pct"]["manufacturing"] == 75.6

    bundle = external_trade_observation_from_components([exports, imports])

    assert bundle is not None
    assert bundle.status == "observed"
    assert bundle.period == "2026-03"
    assert bundle.values["observed_components"] == 2
    assert bundle.values["goods_trade_balance_usd_millions"] == 215.9


def test_energy_components_from_xm_responses_build_bundle() -> None:
    demand = electricity_demand_component_from_xm_response(
        {
            "Items": [
                {
                    "Date": "2026-05-03",
                    "HourlyEntities": [
                        {
                            "Id": "Sistema",
                            "Values": {
                                "code": "Sistema",
                                **{
                                    f"Hour{hour:02d}": "1000000"
                                    for hour in range(1, 25)
                                },
                            },
                        }
                    ],
                },
                {
                    "Date": "2026-05-02",
                    "HourlyEntities": [
                        {
                            "Id": "Sistema",
                            "Values": {
                                "code": "Sistema",
                                **{
                                    f"Hour{hour:02d}": "10000000"
                                    for hour in range(1, 25)
                                },
                            },
                        }
                    ],
                }
            ]
        }
    )
    reservoir = reservoir_component_from_xm_response(
        {
            "Items": [
                {
                    "Date": "2026-05-03",
                    "DailyEntities": [{"Id": "Sistema", "Value": "0.65009"}],
                }
            ]
        }
    )
    spot = spot_price_component_from_xm_response(
        {
            "Items": [
                {
                    "Date": "2026-05-02",
                    "DailyEntities": [{"Id": "Sistema", "Value": "179.10883"}],
                }
            ]
        }
    )

    assert demand is not None
    assert demand.period == "2026-05-02"
    assert demand.values["demand_gwh"] == 240.0
    assert demand.values["peak_hourly_mw"] == 10000.0
    assert reservoir is not None
    assert reservoir.values["reservoir_useful_volume_pct"] == 65.01
    assert spot is not None
    assert spot.values["spot_price_cop_per_kwh"] == 179.11

    bundle = energy_system_observation_from_components([demand, reservoir, spot])

    assert bundle is not None
    assert bundle.status == "observed"
    assert bundle.period == "2026-05-03"
    assert bundle.values["observed_components"] == 3
    assert bundle.values["components"]["electricity_demand"]["demand_gwh"] == 240.0


def test_anh_components_choose_latest_complete_period_and_build_bundle() -> None:
    assert latest_complete_anh_period(
        [
            {"vigencia": "2025", "mes": "11", "count": "100"},
            {"vigencia": "2025", "mes": "10", "count": "330"},
            {"vigencia": "2025", "mes": "9", "count": "340"},
        ]
    ) == (2025, 10)
    oil = crude_oil_component_from_anh_rows(
        [
            {"departamento": "CASANARE", "produccion_bls": "11,955.00"},
            {"departamento": "META", "produccion_bls": "73,862.00"},
        ],
        year=2025,
        month=10,
        release_date="2026-03-02T00:00:00Z",
    )
    gas = fiscalized_gas_component_from_anh_rows(
        [
            {"departamento": "CASANARE", "produccionkpc": "6,209.00"},
            {"departamento": "META", "produccionkpc": "93,791.00"},
        ],
        year=2025,
        month=10,
        release_date="2026-03-02T00:00:00Z",
    )

    assert oil is not None
    assert oil.period == "2025-10"
    assert oil.values["total_barrels"] == 85817.0
    assert oil.values["average_barrels_per_day"] == 2768.29
    assert oil.values["top_departments_by_barrels"][0] == {
        "name": "META",
        "value": 73862.0,
    }
    assert gas is not None
    assert gas.values["total_kpc"] == 100000.0
    assert gas.values["average_million_cubic_feet_per_day"] == 3.23

    bundle = oil_gas_observation_from_components([oil, gas])

    assert bundle is not None
    assert bundle.status == "observed"
    assert bundle.values["observed_components"] == 2
    assert bundle.values["components"]["gas_production"]["total_kpc"] == 100000.0


def _minimal_xlsx(rows: list[dict[str, str]]) -> bytes:
    def cell_xml(ref: str, value: str) -> str:
        if value.replace(".", "", 1).isdigit():
            return f'<c r="{ref}"><v>{value}</v></c>'
        return (
            f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'
        )

    sheet_rows = []
    for row_number, cells in enumerate(rows, start=1):
        sheet_rows.append(
            f'<row r="{row_number}">'
            + "".join(cell_xml(ref, value) for ref, value in cells.items())
            + "</row>"
        )
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        "<sheetData>"
        + "".join(sheet_rows)
        + "</sheetData></worksheet>"
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return output.getvalue()


def test_fiscal_tax_observation_from_dian_xlsx() -> None:
    observation = fiscal_tax_observation_from_dian_xlsx(
        _minimal_xlsx(
            [
                {
                    "B": (
                        "Actualizado a marzo de 2026 "
                        "(Fecha de corte: 23 de abril de 2026)."
                    )
                },
                {
                    "B": "Año",
                    "C": "Mes",
                    "D": "A. Internos (1+...+18)",
                    "E": "1. Renta (1.1.+1.2)",
                    "H": "2. IVA interno (2.1+2.2)",
                    "AC": "B. Externos (19+20)",
                    "AD": "19.Arancel",
                    "AE": "20. IVA Externo ",
                    "AG": "Total (A+B+C)",
                },
                {
                    "B": "2025",
                    "C": "Marzo",
                    "D": "18890",
                    "E": "7231",
                    "H": "8000",
                    "AC": "3745",
                    "AD": "613",
                    "AE": "3132",
                    "AG": "22653",
                },
                {
                    "B": "2026",
                    "C": "Marzo",
                    "D": "19411",
                    "E": "7725",
                    "H": "8100",
                    "AC": "3555",
                    "AD": "195",
                    "AE": "3359",
                    "AG": "22979",
                },
            ]
        )
    )

    assert observation is not None
    assert observation.indicator_id == "fiscal_tax_pulse"
    assert observation.status == "observed"
    assert observation.period == "2026-03"
    assert observation.release_date == "2026-04-23T00:00:00Z"
    assert observation.values["gross_tax_revenue_cop_millions"] == 22979.0
    assert observation.values["gross_tax_revenue_annual_variation_pct"] == 1.44
    assert observation.values["income_tax_cop_millions"] == 7725.0
    assert observation.values["external_tax_revenue_cop_millions"] == 3555.0


def test_indicator_watch_marks_stale_observation_but_keeps_it_visible() -> None:
    stale = IndicatorObservation(
        indicator_id="ipc_inflation",
        name="IPC / inflation",
        category="prices",
        status="observed",
        frequency="monthly",
        source_name="DANE",
        source_url="https://www.dane.gov.co/",
        period="2025-01",
        release_date="2025-02-10T00:00:00Z",
        headline="Old IPC value.",
        values={"annual_variation_pct": 5.0},
    )

    ipc = next(
        item
        for item in build_indicator_watch(
            [],
            [],
            [stale],
            now=datetime(2026, 5, 6, tzinfo=timezone.utc),
        )
        if item.indicator_id == "ipc_inflation"
    )

    assert ipc.status == "observed"
    assert ipc.freshness_status == "stale"
    assert ipc.values["annual_variation_pct"] == 5.0
