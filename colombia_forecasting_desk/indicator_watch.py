from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .models import CleanedItem, IndicatorObservation, RawItem


IPC_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "precios-y-costos/indice-de-precios-al-consumidor-ipc/"
    "ipc-informacion-tecnica"
)
EMC_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "comercio-interno/encuesta-mensual-de-comercio-emc"
)
EMMET_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/industria/"
    "encuesta-mensual-manufacturera-con-enfoque-territorial-emmet"
)
ANH_PRODUCTION_URL = (
    "https://www.anh.gov.co/es/operaciones-y-regal%C3%ADas/"
    "sistemas-integrados-operaciones/estad%C3%ADsticas-de-producci%C3%B3n/"
)


@dataclass(frozen=True, slots=True)
class IndicatorDefinition:
    indicator_id: str
    name: str
    category: str
    frequency: str
    source_name: str
    source_url: str
    why_it_matters: str
    correlations: tuple[str, ...]
    next_step: str


INDICATOR_DEFINITIONS: tuple[IndicatorDefinition, ...] = (
    IndicatorDefinition(
        indicator_id="ipc_inflation",
        name="IPC / inflation",
        category="prices",
        frequency="monthly",
        source_name="DANE",
        source_url=IPC_URL,
        why_it_matters=(
            "Inflation pressure drives BanRep policy, wages, household stress, "
            "and political salience."
        ),
        correlations=(
            "food IPC + TRM + fuel prices can reveal imported and logistics "
            "inflation before the headline narrative catches up",
            "rent/utilities IPC + labor income helps separate demand pressure "
            "from regulated-price pressure",
        ),
        next_step=(
            "Prefer the DANE IPC technical page/XLSX annex or an easier "
            "structured DANE endpoint if available."
        ),
    ),
    IndicatorDefinition(
        indicator_id="trm_usd_cop",
        name="TRM / USD-COP",
        category="markets",
        frequency="daily",
        source_name="Banco de la República / Superfinanciera",
        source_url="https://www.banrep.gov.co/es/glosario/tasa-cambio-trm",
        why_it_matters=(
            "The exchange rate transmits into imports, fuel, food, external "
            "debt, and fiscal oil revenue."
        ),
        correlations=(
            "TRM + Brent/oil production separates commodity-revenue support "
            "from domestic currency pressure",
            "TRM + IPC imported baskets helps identify inflation pressure "
            "that may not look domestic",
        ),
        next_step=(
            "Use a structured BanRep/Superfinanciera statistics endpoint "
            "before scraping the glossary page."
        ),
    ),
    IndicatorDefinition(
        indicator_id="policy_rate_ibr",
        name="Policy rate + IBR",
        category="monetary",
        frequency="daily/monthly",
        source_name="Banco de la República",
        source_url="https://www.banrep.gov.co/es/glosario/ibr",
        why_it_matters="Shows monetary stance and short-term peso liquidity.",
        correlations=(
            "IBR-policy spread can flag liquidity stress or market repricing",
            "policy rate + inflation surprise frames likelihood of cuts or pauses",
        ),
        next_step=(
            "Wire BanRep statistics for policy rate and IBR tenors; avoid "
            "parsing meeting PDFs for M1."
        ),
    ),
    IndicatorDefinition(
        indicator_id="labor_market",
        name="Labor market",
        category="labor",
        frequency="monthly",
        source_name="DANE",
        source_url="https://www.dane.gov.co/index.php/indicadores-relevantes",
        why_it_matters=(
            "Employment, participation, informality, and youth unemployment "
            "drive household pressure and politics."
        ),
        correlations=(
            "unemployment + participation distinguishes real labor improvement "
            "from discouraged-worker effects",
            "informality + IPC reveals purchasing-power stress hidden by "
            "headline employment",
        ),
        next_step=(
            "Find the easiest DANE structured endpoint for GEIH headline "
            "indicators before parsing bulletins."
        ),
    ),
    IndicatorDefinition(
        indicator_id="retail_sales",
        name="Retail sales",
        category="activity",
        frequency="monthly",
        source_name="DANE",
        source_url=EMC_URL,
        why_it_matters=(
            "Retail sales are a timely read on household demand, credit stress, "
            "and consumption momentum."
        ),
        correlations=(
            "retail ex-fuel + employment shows whether consumption is broad-based",
            "vehicle sales + rates gives an early stress signal for durable goods demand",
        ),
        next_step=(
            "Use DANE EMC headline HTML/table if stable; otherwise parse only "
            "the latest annex headline rows."
        ),
    ),
    IndicatorDefinition(
        indicator_id="manufacturing",
        name="Manufacturing",
        category="activity",
        frequency="monthly",
        source_name="DANE",
        source_url=EMMET_URL,
        why_it_matters=(
            "Manufacturing production, sales, and employment give an early read "
            "on industrial momentum."
        ),
        correlations=(
            "manufacturing sales + electricity demand can nowcast real activity before GDP",
            "production down + employment stable can indicate margin pressure before layoffs",
        ),
        next_step=(
            "Prefer the EMMET page headline text or an accessible DANE data "
            "endpoint over full annex parsing."
        ),
    ),
    IndicatorDefinition(
        indicator_id="construction_bundle",
        name="Construction bundle",
        category="construction",
        frequency="monthly",
        source_name="DANE",
        source_url="https://www.dane.gov.co/index.php/estadisticas-por-tema-2/construccion",
        why_it_matters=(
            "Costs, licenses, cement, housing finance, and prices reveal "
            "building-cycle stress."
        ),
        correlations=(
            "ICOCED + cement dispatches + licenses detects slowdown or margin "
            "squeeze earlier than one metric alone",
            "ICOCED + SECOP infrastructure contracts flags public-works budget pressure",
        ),
        next_step=(
            "ICOCED is wired; add cement/licenses/housing finance through DANE "
            "structured pages next."
        ),
    ),
    IndicatorDefinition(
        indicator_id="secop_procurement",
        name="SECOP public procurement pulse",
        category="fiscal_governance",
        frequency="daily",
        source_name="SECOP / Colombia Compra Eficiente",
        source_url="https://operaciones.colombiacompra.gov.co/datos-abiertos",
        why_it_matters=(
            "Procurement volume, amendments, and direct contracting can reveal "
            "fiscal impulse and execution risk."
        ),
        correlations=(
            "contract additions + construction costs can flag budget pressure in public works",
            "direct contracting concentration + electoral calendar can flag governance risk",
        ),
        next_step="Aggregate existing Socrata adapters by day, entity, sector, and process type.",
    ),
    IndicatorDefinition(
        indicator_id="energy_system",
        name="Energy demand / reservoirs / spot price",
        category="energy",
        frequency="daily",
        source_name="XM / UPME",
        source_url="https://portalxm-cal.xm.com.co/",
        why_it_matters=(
            "Electricity demand is an activity proxy; reservoirs and spot prices "
            "reveal drought and reliability stress."
        ),
        correlations=(
            "energy demand + manufacturing + retail sales can nowcast real activity",
            "reservoirs + spot price + weather signals power-system stress "
            "before policy announcements",
        ),
        next_step=(
            "Look for XM/UPME API or downloadable structured time series before "
            "attempting portal scraping."
        ),
    ),
    IndicatorDefinition(
        indicator_id="external_trade",
        name="External trade",
        category="external",
        frequency="monthly",
        source_name="DANE / DIAN",
        source_url="https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional",
        why_it_matters=(
            "Imports, exports, and trade balance connect domestic demand, FX "
            "pressure, and industrial investment."
        ),
        correlations=(
            "capital goods imports + manufacturing predicts investment and production capacity",
            "fuel exports + TRM frames external-account and fiscal sensitivity",
        ),
        next_step=(
            "Find DANE/DIAN structured tables for imports, exports, and trade "
            "balance before PDF parsing."
        ),
    ),
    IndicatorDefinition(
        indicator_id="oil_gas_production",
        name="Oil and gas production",
        category="energy_fiscal",
        frequency="monthly",
        source_name="Agencia Nacional de Hidrocarburos",
        source_url=ANH_PRODUCTION_URL,
        why_it_matters=(
            "Hydrocarbon production affects exports, fiscal revenue, royalties, "
            "and energy security."
        ),
        correlations=(
            "oil production + Brent + TRM estimates fiscal and external-account cushion",
            "gas production + reservoir/energy demand flags reliability and import needs",
        ),
        next_step=(
            "Use ANH downloadable production files or datos.gov.co mirrors if "
            "available."
        ),
    ),
    IndicatorDefinition(
        indicator_id="fiscal_tax_pulse",
        name="Fiscal / tax pulse",
        category="fiscal",
        frequency="monthly",
        source_name="DIAN / Minhacienda",
        source_url="https://www.dian.gov.co/",
        why_it_matters=(
            "Tax collection, deficit, debt, and TES conditions reveal spending "
            "capacity and fiscal stress."
        ),
        correlations=(
            "tax collection + retail/manufacturing separates nominal inflation "
            "lift from real activity",
            "TES yields + fiscal deficit flags market concern before budget headlines",
        ),
        next_step=(
            "Look for DIAN/Minhacienda structured releases; keep PDF budget "
            "documents for later parser work."
        ),
    ),
)


def _definition_map() -> dict[str, IndicatorDefinition]:
    return {definition.indicator_id: definition for definition in INDICATOR_DEFINITIONS}


def _format_period(year: Any, month: Any) -> str:
    if isinstance(year, int) and isinstance(month, int):
        return f"{year}-{month:02d}"
    return ""


def _icoced_observation(item: RawItem) -> IndicatorObservation | None:
    metadata = item.metadata or {}
    metrics = metadata.get("headline_metrics")
    if not isinstance(metrics, dict):
        return None
    total = metrics.get("total")
    if not isinstance(total, dict):
        return None
    definition = _definition_map()["construction_bundle"]
    values: dict[str, Any] = {
        "icoced_total_index": total.get("index"),
        "icoced_total_monthly_variation_pct": total.get("monthly_variation_pct"),
        "icoced_total_year_to_date_variation_pct": total.get(
            "year_to_date_variation_pct"
        ),
        "icoced_total_annual_variation_pct": total.get("annual_variation_pct"),
    }
    residential = metrics.get("residential")
    if isinstance(residential, dict):
        values["icoced_residential_monthly_variation_pct"] = residential.get(
            "monthly_variation_pct"
        )
    non_residential = metrics.get("non_residential")
    if isinstance(non_residential, dict):
        values["icoced_non_residential_monthly_variation_pct"] = non_residential.get(
            "monthly_variation_pct"
        )

    return IndicatorObservation(
        indicator_id=definition.indicator_id,
        name=definition.name,
        category=definition.category,
        status="observed",
        frequency=definition.frequency,
        source_name=item.source_name,
        source_url=item.url,
        period=_format_period(metadata.get("period_year"), metadata.get("period_month")),
        release_date=item.published_at,
        headline=item.raw_text,
        values={k: v for k, v in values.items() if v is not None},
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


def _secop_observation(
    cleaned_items: Iterable[CleanedItem],
) -> IndicatorObservation | None:
    secop_items = [
        item
        for item in cleaned_items
        if item.source_id
        in {
            "secop_ii_procesos",
            "secop_ii_contratos",
            "secop_i_procesos",
            "secop_ii_adiciones",
            "secop_multas_sanciones",
        }
        and not item.quality_notes
        and item.published_at
    ]
    if not secop_items:
        return None

    by_source: dict[str, int] = {}
    latest: str | None = None
    for item in secop_items:
        by_source[item.source_id] = by_source.get(item.source_id, 0) + 1
        if latest is None or item.published_at > latest:
            latest = item.published_at

    definition = _definition_map()["secop_procurement"]
    parts = [f"{count} {source_id}" for source_id, count in sorted(by_source.items())]
    headline = (
        f"SECOP pulse captured {len(secop_items)} rankable procurement records"
        f" in the freshness window ({', '.join(parts)})."
    )
    return IndicatorObservation(
        indicator_id=definition.indicator_id,
        name=definition.name,
        category=definition.category,
        status="observed",
        frequency=definition.frequency,
        source_name=definition.source_name,
        source_url=definition.source_url,
        period="freshness_window",
        release_date=latest,
        headline=headline,
        values={
            "rankable_records": len(secop_items),
            "records_by_source": by_source,
        },
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


def _pending_observation(definition: IndicatorDefinition) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id=definition.indicator_id,
        name=definition.name,
        category=definition.category,
        status="pending_source",
        frequency=definition.frequency,
        source_name=definition.source_name,
        source_url=definition.source_url,
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


def build_indicator_watch(
    raw_items: list[RawItem],
    cleaned_items: list[CleanedItem],
) -> list[IndicatorObservation]:
    observed: dict[str, IndicatorObservation] = {}
    for item in raw_items:
        if item.source_id == "dane_icoced":
            observation = _icoced_observation(item)
            if observation:
                observed[observation.indicator_id] = observation

    secop = _secop_observation(cleaned_items)
    if secop:
        observed[secop.indicator_id] = secop

    watch: list[IndicatorObservation] = []
    for definition in INDICATOR_DEFINITIONS:
        watch.append(
            observed.get(definition.indicator_id) or _pending_observation(definition)
        )
    return watch
