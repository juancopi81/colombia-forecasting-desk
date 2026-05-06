from __future__ import annotations

import io
import re
import zipfile
from calendar import monthrange
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup

from .cleaner import fold_accents, normalize_whitespace
from .models import CleanedItem, IndicatorComponent, IndicatorObservation, RawItem


STRUCTURED_INDICATOR_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
BANREP_SERIES_API_BASE = (
    "https://suameca.banrep.gov.co/estadisticas-economicas-back/rest/"
    "estadisticaEconomicaRestService"
)
BANREP_POLICY_RATE_SERIES_ID = 59
BANREP_IBR_OVERNIGHT_SERIES_ID = 241
BANREP_POLICY_RATE_SOURCE_URL = (
    "https://suameca.banrep.gov.co/estadisticas-economicas/informacionSerie/"
    "59/tasas_interes_politica_monetaria/"
)
BANREP_IBR_SOURCE_URL = (
    "https://suameca.banrep.gov.co/estadisticas-economicas/informacionSerie/"
    "241/tasas_interes_indicador_bancario_referencia_ibr/"
)
TRM_API_URL = "https://www.datos.gov.co/resource/32sa-8pi3.json"
TRM_SOURCE_URL = (
    "https://www.datos.gov.co/Econom-a-y-Finanzas/"
    "Tasa-de-Cambio-Representativa-del-Mercado-TRM/32sa-8pi3"
)
TRM_ROWS_LIMIT = 45
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
LABOR_MARKET_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "mercado-laboral/empleo-y-desempleo"
)
CEMENT_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "construccion/estadisticas-de-cemento-gris"
)
CONSTRUCTION_LICENSES_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "construccion/licencias-de-construccion"
)
HOUSING_FINANCE_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "construccion/financiacion-de-vivienda"
)
EXPORTS_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "comercio-internacional/exportaciones"
)
IMPORTS_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "comercio-internacional/importaciones"
)
ANH_PRODUCTION_URL = (
    "https://www.anh.gov.co/es/operaciones-y-regal%C3%ADas/"
    "sistemas-integrados-operaciones/estad%C3%ADsticas-de-producci%C3%B3n/"
)
XM_API_SOURCE_URL = "https://github.com/EquipoAnaliticaXM/API_XM"
XM_HOURLY_API_URL = "https://servapibi.xm.com.co/hourly"
XM_DAILY_API_URL = "https://servapibi.xm.com.co/daily"
XM_LOOKBACK_DAYS = 10
XM_MIN_FULL_DAY_DEMAND_GWH = 100
ANH_CRUDE_RESOURCE_ID = "fdvb-hsrf"
ANH_GAS_RESOURCE_ID = "5dux-bfvx"
ANH_CRUDE_API_URL = (
    f"https://www.datos.gov.co/resource/{ANH_CRUDE_RESOURCE_ID}.json"
)
ANH_GAS_API_URL = f"https://www.datos.gov.co/resource/{ANH_GAS_RESOURCE_ID}.json"
ANH_CRUDE_SOURCE_URL = (
    "https://www.datos.gov.co/Minas-y-Energ-a/"
    f"Produccion-Fiscalizada-Crudo-Consolidada/{ANH_CRUDE_RESOURCE_ID}"
)
ANH_GAS_SOURCE_URL = (
    "https://www.datos.gov.co/Minas-y-Energ-a/"
    f"Produccion-Fiscalizada-Gas-Consolidada/{ANH_GAS_RESOURCE_ID}"
)
ANH_PERIOD_COUNT_LIMIT = 18
ANH_COMPLETENESS_RATIO = 0.75
DIAN_TAX_REVENUE_PAGE_URL = (
    "https://www.dian.gov.co/dian/cifras/Paginas/EstadisticasRecaudo.aspx"
)
DIAN_MONTHLY_TAX_ZIP_URL = (
    "https://www.dian.gov.co/dian/cifras/EstadisticasRecaudo/"
    "Estadisticas-de-recaudo-mensual-por-tipo-de-impuesto-2000-2026.zip"
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
            "Headline HTML is wired; add category/city annex drivers when the "
            "watch needs deeper inflation decomposition."
        ),
    ),
    IndicatorDefinition(
        indicator_id="trm_usd_cop",
        name="TRM / USD-COP",
        category="markets",
        frequency="daily",
        source_name="Superintendencia Financiera de Colombia",
        source_url=TRM_SOURCE_URL,
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
            "Observed from datos.gov.co TRM dataset; add intraday market proxies "
            "only if needed later."
        ),
    ),
    IndicatorDefinition(
        indicator_id="policy_rate_ibr",
        name="Policy rate + IBR",
        category="monetary",
        frequency="daily/monthly",
        source_name="Banco de la República",
        source_url=BANREP_POLICY_RATE_SOURCE_URL,
        why_it_matters="Shows monetary stance and short-term peso liquidity.",
        correlations=(
            "IBR-policy spread can flag liquidity stress or market repricing",
            "policy rate + inflation surprise frames likelihood of cuts or pauses",
        ),
        next_step=(
            "Observed from BanRep SUAMECA policy-rate and IBR series; add "
            "IBR term structure if liquidity stress needs more depth."
        ),
    ),
    IndicatorDefinition(
        indicator_id="labor_market",
        name="Labor market",
        category="labor",
        frequency="monthly",
        source_name="DANE",
        source_url=LABOR_MARKET_URL,
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
            "Headline HTML is wired; add informality, youth, and city details "
            "from GEIH annexes when needed."
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
            "Headline HTML is wired; add vehicle, department, and ecommerce "
            "annex drivers when needed."
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
            "Headline HTML is wired; add subsector and territory contribution "
            "annex drivers when needed."
        ),
    ),
    IndicatorDefinition(
        indicator_id="construction_bundle",
        name="Construction bundle",
        category="construction",
        frequency="monthly",
        source_name="DANE",
        source_url=CEMENT_URL,
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
            "ICOCED, cement, licenses, and housing finance headline HTML are "
            "wired; add deeper annex drivers when needed."
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
        next_step=(
            "Day/entity/process-type aggregation is wired; add sector fields by "
            "extending the Socrata column selects."
        ),
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
            "XM demand, useful reservoir volume, and spot-price API components "
            "are wired; add thermal generation and non-regulated demand if the "
            "watch needs a stress decomposition."
        ),
    ),
    IndicatorDefinition(
        indicator_id="external_trade",
        name="External trade",
        category="external",
        frequency="monthly",
        source_name="DANE / DIAN",
        source_url=EXPORTS_URL,
        why_it_matters=(
            "Imports, exports, and trade balance connect domestic demand, FX "
            "pressure, and industrial investment."
        ),
        correlations=(
            "capital goods imports + manufacturing predicts investment and production capacity",
            "fuel exports + TRM frames external-account and fiscal sensitivity",
        ),
        next_step=(
            "DANE headline exports and imports pages are wired; add annex "
            "country/product drivers when needed."
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
            "ANH datos.gov.co crude and gas production components are wired; "
            "add royalties and Brent when fiscal sensitivity needs more depth."
        ),
    ),
    IndicatorDefinition(
        indicator_id="fiscal_tax_pulse",
        name="Fiscal / tax pulse",
        category="fiscal",
        frequency="monthly",
        source_name="DIAN / Minhacienda",
        source_url=DIAN_TAX_REVENUE_PAGE_URL,
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
            "DIAN monthly tax-collection XLSX is wired; add deficit, debt, "
            "and TES components when a broader fiscal-stress card is needed."
        ),
    ),
)


def _definition_map() -> dict[str, IndicatorDefinition]:
    return {definition.indicator_id: definition for definition in INDICATOR_DEFINITIONS}


def _format_period(year: Any, month: Any) -> str:
    if isinstance(year, int) and isinstance(month, int):
        return f"{year}-{month:02d}"
    return ""


def _parse_socrata_date(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_float_unrounded(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        text = str(value).strip().replace(" ", "")
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            text = text.replace(",", ".")
        elif text.count(".") > 1:
            text = text.replace(".", "")
        elif "." in text:
            before, after = text.split(".", 1)
            if len(after) == 3 and 1 <= len(before.lstrip("-")) <= 3:
                text = before + after
        return float(text)
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    parsed = _to_float_unrounded(value)
    if parsed is None:
        return None
    return round(parsed, 2)


_MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

_MONTH_ABBR_ES = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "set": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}

_NUMBER = r"(-?[\d.,]+)"
_PERCENT = rf"{_NUMBER}\s*%"
_MONTH_NAMES = "|".join(_MONTHS_ES)


def _text_from_html(html: str) -> str:
    return normalize_whitespace(
        BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    )


def _folded_text_from_html(html: str) -> str:
    return fold_accents(_text_from_html(html).lower())


def _month_period(month_name: str, year: str) -> str:
    month = _MONTHS_ES.get(fold_accents(month_name.lower()))
    if month is None:
        return ""
    return f"{int(year)}-{month:02d}"


def _iso_date(year: int, month: int, day: int) -> str | None:
    try:
        return datetime(year, month, day, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT00:00:00Z"
        )
    except ValueError:
        return None


def _release_date_from_text(text: str) -> str:
    folded = fold_accents(text.lower())
    dates: list[datetime] = []
    for match in re.finditer(r"\b(\d{1,2})[/-](\d{1,2})[/-](20\d{2})\b", folded):
        try:
            dates.append(
                datetime(
                    int(match.group(3)),
                    int(match.group(2)),
                    int(match.group(1)),
                    tzinfo=timezone.utc,
                )
            )
        except ValueError:
            continue
    month_words = "|".join((*_MONTHS_ES, *_MONTH_ABBR_ES))
    date_patterns = (
        rf"\b(\d{{1,2}})\s+de\s+({month_words})\s+de\s+(20\d{{2}})\b",
        rf"\b(\d{{1,2}})[-\s]({month_words})[-\s](20\d{{2}})\b",
    )
    for pattern in date_patterns:
        for match in re.finditer(pattern, folded):
            month = _MONTHS_ES.get(match.group(2)) or _MONTH_ABBR_ES.get(
                match.group(2)[:3]
            )
            if month is None:
                continue
            try:
                dates.append(
                    datetime(
                        int(match.group(3)),
                        month,
                        int(match.group(1)),
                        tzinfo=timezone.utc,
                    )
                )
            except ValueError:
                continue
    if not dates:
        return ""
    return max(dates).strftime("%Y-%m-%dT00:00:00Z")


def _latest_release_date(html: str) -> str:
    return _release_date_from_text(_text_from_html(html))


_FRESHNESS_DAYS_BY_FREQUENCY = {
    "daily": 7,
    "monthly": 95,
    "quarterly": 150,
    "daily/monthly": 95,
}


_BUNDLE_COMPONENTS = {
    "construction_bundle": (
        {
            "component_id": "icoced",
            "name": "ICOCED costs",
            "source_name": "DANE",
            "source_url": (
                "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
                "precios-y-costos/indice-de-costos-de-la-construccion-de-"
                "edificaciones-icoced"
            ),
            "next_step": "Parsed from the latest XLSX annex.",
        },
        {
            "component_id": "cement",
            "name": "Cement production and shipments",
            "source_name": "DANE",
            "source_url": CEMENT_URL,
            "next_step": "Headline HTML is wired; add regional/channel annex details later.",
        },
        {
            "component_id": "licenses",
            "name": "Construction licenses",
            "source_name": "DANE",
            "source_url": CONSTRUCTION_LICENSES_URL,
            "next_step": "Headline HTML is wired; add destination/municipality annex details later.",
        },
        {
            "component_id": "housing_finance",
            "name": "Housing finance",
            "source_name": "DANE",
            "source_url": HOUSING_FINANCE_URL,
            "next_step": "Headline HTML is wired; add credit type and geography annex details later.",
        },
    ),
    "energy_system": (
        {
            "component_id": "electricity_demand",
            "name": "Electricity demand",
            "source_name": "XM",
            "source_url": XM_API_SOURCE_URL,
            "next_step": "Public XM API is wired; add regulated/non-regulated split later.",
        },
        {
            "component_id": "reservoir_useful_volume",
            "name": "Reservoir useful volume",
            "source_name": "XM",
            "source_url": XM_API_SOURCE_URL,
            "next_step": "Public XM API is wired; add reservoir-level stress detail later.",
        },
        {
            "component_id": "spot_price",
            "name": "Spot price",
            "source_name": "XM",
            "source_url": XM_API_SOURCE_URL,
            "next_step": "Public XM API is wired; add scarcity price spread later.",
        },
    ),
    "oil_gas_production": (
        {
            "component_id": "oil_production",
            "name": "Crude oil production",
            "source_name": "ANH / datos.gov.co",
            "source_url": ANH_CRUDE_SOURCE_URL,
            "next_step": "Socrata aggregate is wired; add field/operator contribution shifts later.",
        },
        {
            "component_id": "gas_production",
            "name": "Fiscalized gas production",
            "source_name": "ANH / datos.gov.co",
            "source_url": ANH_GAS_SOURCE_URL,
            "next_step": "Socrata aggregate is wired; add commercialized gas and demand balance later.",
        },
    ),
    "external_trade": (
        {
            "component_id": "exports",
            "name": "Goods exports",
            "source_name": "DANE / DIAN",
            "source_url": EXPORTS_URL,
            "next_step": "Headline HTML is wired; add destination/product annex drivers later.",
        },
        {
            "component_id": "imports",
            "name": "Goods imports",
            "source_name": "DANE / DIAN",
            "source_url": IMPORTS_URL,
            "next_step": "Headline HTML is wired; add origin/CUODE/product annex drivers later.",
        },
    ),
}


def _parse_iso_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _freshness_status(
    status: str,
    release_date: str | None,
    frequency: str,
    now: datetime,
) -> str:
    if status in {"pending_source", "failed"}:
        return "pending" if status == "pending_source" else "failed"
    release = _parse_iso_date(release_date)
    if release is None:
        return "unknown"
    threshold_days = _FRESHNESS_DAYS_BY_FREQUENCY.get(frequency, 120)
    return "stale" if (now - release).days > threshold_days else "current"


def _component(
    *,
    component_id: str,
    name: str,
    status: str,
    source_name: str,
    source_url: str,
    period: str = "",
    release_date: str | None = None,
    headline: str = "",
    values: dict[str, Any] | None = None,
    freshness_status: str = "unknown",
    next_step: str = "",
) -> IndicatorComponent:
    return IndicatorComponent(
        component_id=component_id,
        name=name,
        status=status,
        source_name=source_name,
        source_url=source_url,
        period=period,
        release_date=release_date,
        headline=headline,
        values={
            key: value for key, value in (values or {}).items() if value is not None
        },
        freshness_status=freshness_status,
        next_step=next_step,
    )


def _pending_components(indicator_id: str) -> list[IndicatorComponent]:
    return [
        _component(
            component_id=str(component["component_id"]),
            name=str(component["name"]),
            status="pending_source",
            source_name=str(component["source_name"]),
            source_url=str(component["source_url"]),
            freshness_status="pending",
            next_step=str(component["next_step"]),
        )
        for component in _BUNDLE_COMPONENTS.get(indicator_id, ())
    ]


def _complete_bundle_components(
    indicator_id: str,
    components: list[IndicatorComponent],
) -> list[IndicatorComponent]:
    observed_by_id = {component.component_id: component for component in components}
    completed: list[IndicatorComponent] = []
    for component in _pending_components(indicator_id):
        completed.append(observed_by_id.get(component.component_id, component))
    for component in components:
        if component.component_id not in {item.component_id for item in completed}:
            completed.append(component)
    return completed


def _bundle_values(
    indicator_id: str,
    components: list[IndicatorComponent],
) -> dict[str, Any]:
    observed_components = [
        component for component in components if component.status == "observed"
    ]
    if not observed_components:
        return {}
    total_components = len(_BUNDLE_COMPONENTS.get(indicator_id, ()))
    return {
        "observed_components": len(observed_components),
        "total_components": total_components or len(components),
        "components": {
            component.component_id: component.values
            for component in observed_components
        },
    }


def _component_frequency(indicator_id: str) -> str:
    if indicator_id in {"construction_bundle", "oil_gas_production", "external_trade"}:
        return "monthly"
    if indicator_id == "energy_system":
        return "daily"
    return _definition_map()[indicator_id].frequency


def _apply_freshness(
    observation: IndicatorObservation,
    now: datetime,
) -> IndicatorObservation:
    component_frequency = _component_frequency(observation.indicator_id)
    components = [
        replace(
            component,
            freshness_status=_freshness_status(
                component.status,
                component.release_date,
                component_frequency,
                now,
            ),
        )
        for component in observation.components
    ]
    return replace(
        observation,
        freshness_status=_freshness_status(
            observation.status,
            observation.release_date,
            observation.frequency,
            now,
        ),
        components=components,
    )


def _headline_definition(
    definition: IndicatorDefinition,
    *,
    status: str,
    period: str,
    release_date: str,
    headline: str,
    values: dict[str, Any],
    components: list[IndicatorComponent] | None = None,
) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id=definition.indicator_id,
        name=definition.name,
        category=definition.category,
        status=status,
        frequency=definition.frequency,
        source_name=definition.source_name,
        source_url=definition.source_url,
        period=period,
        release_date=release_date,
        headline=headline,
        values={key: value for key, value in values.items() if value is not None},
        components=components or [],
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


def ipc_observation_from_html(html: str) -> IndicatorObservation | None:
    original_text = _text_from_html(html)
    text = _folded_text_from_html(html)
    match = re.search(
        rf"en\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}}).*?"
        rf"variacion mensual del ipc fue\s+{_PERCENT}.*?"
        rf"variacion ano corrido fue\s+{_PERCENT}.*?"
        rf"(?:variacion\s+)?anual\s+{_PERCENT}",
        text,
    )
    if not match:
        return None
    month_name, year = match.group(1), match.group(2)
    monthly = _to_float(match.group(3))
    year_to_date = _to_float(match.group(4))
    annual = _to_float(match.group(5))
    previous_match = re.search(
        rf"variacion anual del ipc fue\s+{_PERCENT}.*?"
        rf"periodo del ano anterior.*?cuando fue de\s+\(?{_PERCENT}\)?",
        text,
    )
    divisions_match = re.search(
        rf"mayores variaciones se presentaron en las divisiones "
        rf"(.+?)\s+\({_PERCENT}\)\s+y\s+(.+?)\s+\({_PERCENT}\)",
        original_text,
        re.IGNORECASE,
    )
    definition = _definition_map()["ipc_inflation"]
    values: dict[str, Any] = {
        "monthly_variation_pct": monthly,
        "year_to_date_variation_pct": year_to_date,
        "annual_variation_pct": annual,
    }
    if previous_match:
        values["annual_previous_year_pct"] = _to_float(previous_match.group(2))
    if divisions_match:
        values["largest_monthly_divisions"] = [
            {
                "name": normalize_whitespace(divisions_match.group(1)),
                "monthly_variation_pct": _to_float(divisions_match.group(2)),
            },
            {
                "name": normalize_whitespace(divisions_match.group(3)),
                "monthly_variation_pct": _to_float(divisions_match.group(4)),
            },
        ]
    period = _month_period(month_name, year)
    headline = (
        f"DANE IPC {period}: monthly variation {monthly:.2f}%, "
        f"year-to-date {year_to_date:.2f}%, annual {annual:.2f}%."
    )
    return _headline_definition(
        definition,
        status="observed",
        period=period,
        release_date=_latest_release_date(html),
        headline=headline,
        values=values,
    )


def retail_sales_observation_from_html(html: str) -> IndicatorObservation | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"en\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}}),\s+las ventas reales "
        rf"del comercio minorista aumentaron\s+{_PERCENT}\s+y el personal "
        rf"ocupado crecio\s+{_PERCENT}.*?"
        rf"excluyendo el comercio de combustibles.*?fue de\s+{_PERCENT}",
        text,
    )
    if not match:
        return None
    month_name, year = match.group(1), match.group(2)
    sales = _to_float(match.group(3))
    employment = _to_float(match.group(4))
    ex_fuel = _to_float(match.group(5))
    definition = _definition_map()["retail_sales"]
    period = _month_period(month_name, year)
    headline = (
        f"DANE EMC {period}: real retail sales {sales:+.2f}% y/y, "
        f"employment {employment:+.2f}% y/y, ex-fuel sales {ex_fuel:+.2f}% y/y."
    )
    return _headline_definition(
        definition,
        status="observed",
        period=period,
        release_date=_latest_release_date(html),
        headline=headline,
        values={
            "real_retail_sales_annual_variation_pct": sales,
            "employment_annual_variation_pct": employment,
            "real_retail_sales_ex_fuel_annual_variation_pct": ex_fuel,
        },
    )


def manufacturing_observation_from_html(html: str) -> IndicatorObservation | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"en\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}})\s+frente a\s+"
        rf"\1\s+de\s+20\d{{2}},\s+la produccion real de la industria "
        rf"manufacturera presento una variacion de\s+{_PERCENT},\s+"
        rf"las ventas reales de\s+{_PERCENT}\s+y el personal ocupado de\s+"
        rf"{_PERCENT}",
        text,
    )
    if not match:
        return None
    month_name, year = match.group(1), match.group(2)
    production = _to_float(match.group(3))
    sales = _to_float(match.group(4))
    employment = _to_float(match.group(5))
    activity_match = re.search(
        r"de las 39 actividades industriales representadas.*?un total de "
        r"(\d+) registraron variaciones positivas.*?(\d+) subsectores "
        r"(?:presentaron|con) variaciones negativas",
        text,
    )
    contribution_match = re.search(
        rf"(?:contribuyendo con|sumando)\s+{_NUMBER}\s+puntos porcentuales.*?"
        rf"(?:con(?: una)? contribucion de|restaron en conjunto)\s+{_NUMBER}"
        rf"\s+puntos porcentuales",
        text,
    )
    values: dict[str, Any] = {
        "real_production_annual_variation_pct": production,
        "real_sales_annual_variation_pct": sales,
        "employment_annual_variation_pct": employment,
    }
    if activity_match:
        values["activities_total_count"] = 39
        values["activities_positive_count"] = int(activity_match.group(1))
        values["activities_negative_count"] = int(activity_match.group(2))
    if contribution_match:
        values["positive_contribution_pp"] = _to_float(contribution_match.group(1))
        negative_contribution = _to_float(contribution_match.group(2))
        if (
            negative_contribution is not None
            and negative_contribution > 0
            and "restaron en conjunto" in contribution_match.group(0)
        ):
            negative_contribution = -negative_contribution
        values["negative_contribution_pp"] = negative_contribution
    definition = _definition_map()["manufacturing"]
    period = _month_period(month_name, year)
    headline = (
        f"DANE EMMET {period}: real production {production:+.2f}% y/y, "
        f"real sales {sales:+.2f}% y/y, employment {employment:+.2f}% y/y."
    )
    return _headline_definition(
        definition,
        status="observed",
        period=period,
        release_date=_latest_release_date(html),
        headline=headline,
        values=values,
    )


def labor_market_observation_from_html(html: str) -> IndicatorObservation | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"para\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}}),\s+la tasa de "
        rf"desocupacion del total nacional fue\s+{_PERCENT}.*?"
        rf"(disminucion|aumento) de\s+([\d,.]+)\s+puntos porcentuales.*?"
        rf"mismo mes de\s+20\d{{2}}\s+\({_PERCENT}\).*?"
        rf"tasa global de participacion se ubico en\s+{_PERCENT}\s+y la "
        rf"tasa de ocupacion en\s+{_PERCENT}.*?"
        rf"estas tasas fueron\s+{_PERCENT}\s+y\s+{_PERCENT}",
        text,
    )
    if not match:
        return None
    month_name, year = match.group(1), match.group(2)
    unemployment = _to_float(match.group(3))
    change = _to_float(match.group(5))
    if change is not None and match.group(4) == "disminucion":
        change = -change
    previous_unemployment = _to_float(match.group(6))
    participation = _to_float(match.group(7))
    occupation = _to_float(match.group(8))
    previous_participation = _to_float(match.group(9))
    previous_occupation = _to_float(match.group(10))
    cities_match = re.search(
        rf"(?:para|en)\s+({_MONTH_NAMES})\s+de\s+20\d{{2}},\s+la tasa de "
        rf"desocupacion en el total de las 13 ciudades.*?fue\s+{_PERCENT},"
        rf".*?mismo mes de 20\d{{2}} fue\s+{_PERCENT}.*?"
        rf"tasa global de participacion se ubico en\s+{_PERCENT}\s+y la "
        rf"tasa de ocupacion en\s+{_PERCENT}.*?"
        rf"estas tasas fueron\s+{_PERCENT}\s+y\s+{_PERCENT}",
        text,
    )
    if not cities_match:
        cities_match = re.search(
            rf"para las 13 ciudades.*?tasa de desocupacion fue\s+{_PERCENT},.*?"
            rf"comparacion con\s+{_PERCENT}\s+observado en\s+({_MONTH_NAMES}) "
            rf"de 20\d{{2}}",
            text,
        )
    values: dict[str, Any] = {
        "national_unemployment_rate_pct": unemployment,
        "national_unemployment_annual_change_pp": change,
        "national_unemployment_previous_year_pct": previous_unemployment,
        "national_participation_rate_pct": participation,
        "national_occupation_rate_pct": occupation,
        "national_participation_previous_year_pct": previous_participation,
        "national_occupation_previous_year_pct": previous_occupation,
    }
    if cities_match:
        offset = 1 if len(cities_match.groups()) == 7 else 0
        values["thirteen_cities_unemployment_rate_pct"] = _to_float(
            cities_match.group(1 + offset)
        )
        values["thirteen_cities_unemployment_previous_year_pct"] = _to_float(
            cities_match.group(2 + offset)
        )
        if len(cities_match.groups()) == 7:
            values["thirteen_cities_participation_rate_pct"] = _to_float(
                cities_match.group(4)
            )
            values["thirteen_cities_occupation_rate_pct"] = _to_float(
                cities_match.group(5)
            )
            values["thirteen_cities_participation_previous_year_pct"] = _to_float(
                cities_match.group(6)
            )
            values["thirteen_cities_occupation_previous_year_pct"] = _to_float(
                cities_match.group(7)
            )
    definition = _definition_map()["labor_market"]
    period = _month_period(month_name, year)
    headline = (
        f"DANE GEIH {period}: national unemployment {unemployment:.2f}%, "
        f"participation {participation:.2f}%, occupation {occupation:.2f}%."
    )
    return _headline_definition(
        definition,
        status="observed",
        period=period,
        release_date=_latest_release_date(html),
        headline=headline,
        values=values,
    )


def cement_component_from_html(html: str) -> IndicatorComponent | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"en\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}}),\s+la produccion de "
        rf"cemento gris a nivel nacional fue de\s+{_NUMBER}\s+miles de "
        rf"toneladas.*?variacion de\s+{_PERCENT}.*?"
        rf"se despacharon al mercado nacional\s+{_NUMBER}\s+miles de "
        rf"toneladas.*?crecimiento del\s+{_PERCENT}",
        text,
    )
    if not match:
        return None
    ytd_match = re.search(
        rf"en el periodo .*? la produccion de cemento gris alcanzo los "
        rf"{_NUMBER}\s+miles de toneladas.*?aumento de\s+{_PERCENT}.*?"
        rf"los despachos al mercado nacional acumularon\s+{_NUMBER}\s+miles "
        rf"de toneladas.*?variacion positiva de\s+{_PERCENT}",
        text,
    )
    month_name, year = match.group(1), match.group(2)
    period = _month_period(month_name, year)
    production = _to_float(match.group(3))
    production_change = _to_float(match.group(4))
    shipments = _to_float(match.group(5))
    shipments_change = _to_float(match.group(6))
    values: dict[str, Any] = {
        "production_thousand_tons": production,
        "production_annual_variation_pct": production_change,
        "domestic_shipments_thousand_tons": shipments,
        "domestic_shipments_annual_variation_pct": shipments_change,
    }
    if ytd_match:
        values.update(
            {
                "year_to_date_production_thousand_tons": _to_float(
                    ytd_match.group(1)
                ),
                "year_to_date_production_variation_pct": _to_float(
                    ytd_match.group(2)
                ),
                "year_to_date_shipments_thousand_tons": _to_float(
                    ytd_match.group(3)
                ),
                "year_to_date_shipments_variation_pct": _to_float(
                    ytd_match.group(4)
                ),
            }
        )
    return _component(
        component_id="cement",
        name="Cement production and shipments",
        status="observed",
        source_name="DANE",
        source_url=CEMENT_URL,
        period=period,
        release_date=_latest_release_date(html),
        headline=(
            f"DANE ECG {period}: cement production {production:.1f}k tons "
            f"({production_change:+.2f}% y/y), domestic shipments "
            f"{shipments:.1f}k tons ({shipments_change:+.2f}% y/y)."
        ),
        values=values,
        next_step="Add regional and channel-distribution annex detail when needed.",
    )


def construction_licenses_component_from_html(html: str) -> IndicatorComponent | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"en\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}})\s+se licenciaron "
        rf"{_NUMBER}\s*m2?\s+para construccion.*?registrada en el mismo mes "
        rf"de 20\d{{2}}\s+\({_NUMBER}\s*m2?\).*?crecimiento anual de "
        rf"{_PERCENT}.*?aumento de\s+{_PERCENT}\s+en el area aprobada para "
        rf"destinos no habitacionales.*?area aprobada para vivienda "
        rf"(aumento|disminuyo|disminucion|disminuyó)\s+{_PERCENT}",
        text,
    )
    if not match:
        return None
    area_match = re.search(
        rf"se aprobaron\s+{_NUMBER}\s*m2?\s+para vivienda.*?"
        rf"destinos no habitacionales alcanzo\s+{_NUMBER}\s*m2?",
        text,
    )
    month_name, year = match.group(1), match.group(2)
    period = _month_period(month_name, year)
    licensed_area = _to_float(match.group(3))
    prior_area = _to_float(match.group(4))
    total_growth = _to_float(match.group(5))
    non_res_growth = _to_float(match.group(6))
    housing_growth = _to_float(match.group(8))
    if housing_growth is not None and match.group(7).startswith("dismin"):
        housing_growth = -housing_growth
    values: dict[str, Any] = {
        "licensed_area_m2": licensed_area,
        "prior_year_licensed_area_m2": prior_area,
        "licensed_area_annual_variation_pct": total_growth,
        "housing_area_annual_variation_pct": housing_growth,
        "non_residential_area_annual_variation_pct": non_res_growth,
    }
    if area_match:
        values["housing_area_m2"] = _to_float(area_match.group(1))
        values["non_residential_area_m2"] = _to_float(area_match.group(2))
    return _component(
        component_id="licenses",
        name="Construction licenses",
        status="observed",
        source_name="DANE",
        source_url=CONSTRUCTION_LICENSES_URL,
        period=period,
        release_date=_latest_release_date(html),
        headline=(
            f"DANE ELIC {period}: licensed area {licensed_area:.0f} m2 "
            f"({total_growth:+.2f}% y/y); housing {housing_growth:+.2f}% "
            f"and non-residential {non_res_growth:+.2f}% y/y."
        ),
        values=values,
        next_step="Add destination, municipality, and VIS/non-VIS annex detail when needed.",
    )


def housing_finance_component_from_html(html: str) -> IndicatorComponent | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"durante el\s+(.+?)\s+de\s+(20\d{{2}}),\s+se desembolsaron "
        rf"\$\s*{_NUMBER}\s+millones de pesos corrientes para compra de "
        rf"vivienda.*?\$\s*{_NUMBER}\s+millones fueron creditos de vivienda "
        rf"y\s+\$\s*{_NUMBER}\s+millones fueron leasing habitacional",
        text,
    )
    if not match:
        return None
    real_match = re.search(
        rf"precios constantes.*?sumaron\s+\$\s*{_NUMBER}\s+millones.*?"
        rf"variacion anual de\s+{_PERCENT}",
        text,
    )
    quarter_text, year = match.group(1), match.group(2)
    quarter = _quarter_period(quarter_text, year)
    total = _to_float(match.group(3))
    credit = _to_float(match.group(4))
    leasing = _to_float(match.group(5))
    values: dict[str, Any] = {
        "purchase_disbursements_cop_millions": total,
        "housing_credit_disbursements_cop_millions": credit,
        "leasing_disbursements_cop_millions": leasing,
    }
    if real_match:
        values["real_purchase_disbursements_cop_millions"] = _to_float(
            real_match.group(1)
        )
        values["real_purchase_disbursements_annual_variation_pct"] = _to_float(
            real_match.group(2)
        )
    real_change = values.get("real_purchase_disbursements_annual_variation_pct")
    change_text = (
        f", real disbursements {real_change:+.2f}% y/y"
        if isinstance(real_change, float)
        else ""
    )
    return _component(
        component_id="housing_finance",
        name="Housing finance",
        status="observed",
        source_name="DANE",
        source_url=HOUSING_FINANCE_URL,
        period=quarter,
        release_date=_latest_release_date(html),
        headline=(
            f"DANE FIVI {quarter}: COP {total:.0f} million in purchase "
            f"disbursements{change_text}."
        ),
        values=values,
        next_step="Add credit-type, tenure, and geography annex detail when needed.",
    )


def exports_component_from_html(html: str) -> IndicatorComponent | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"informacion\s+({_MONTH_NAMES})\s+de\s+(20\d{{2}}).*?"
        rf"ventas externas del pais fueron us\$?{_NUMBER}\s+millones fob "
        rf"y presentaron un crecimiento de\s+{_PERCENT}.*?"
        rf"aumento del\s+{_PERCENT}\s+en las ventas externas del grupo de "
        rf"(.+?)\.",
        text,
    )
    if not match:
        return None
    shares_match = re.search(
        rf"combustibles y productos de industrias extractivas participaron "
        rf"con\s+{_PERCENT}.*?agropecuarios, alimentos y bebidas con\s+"
        rf"{_PERCENT}.*?manufacturas con\s+{_PERCENT}.*?otros sectores "
        rf"con\s+{_PERCENT}",
        text,
    )
    period = _month_period(match.group(1), match.group(2))
    exports = _to_float(match.group(3))
    growth = _to_float(match.group(4))
    driver_growth = _to_float(match.group(5))
    driver_group = normalize_whitespace(match.group(6))
    values: dict[str, Any] = {
        "exports_usd_millions_fob": exports,
        "exports_annual_variation_pct": growth,
        "main_driver_group": driver_group,
        "main_driver_annual_variation_pct": driver_growth,
    }
    if shares_match:
        values["export_group_shares_pct"] = {
            "fuels_and_extractives": _to_float(shares_match.group(1)),
            "agriculture_food_beverages": _to_float(shares_match.group(2)),
            "manufacturing": _to_float(shares_match.group(3)),
            "other_sectors": _to_float(shares_match.group(4)),
        }
    return _component(
        component_id="exports",
        name="Goods exports",
        status="observed",
        source_name="DANE / DIAN",
        source_url=EXPORTS_URL,
        period=period,
        release_date=_latest_release_date(html),
        headline=(
            f"DANE exports {period}: US${exports:,.1f}m FOB "
            f"({growth:+.2f}% y/y); main driver {driver_group} "
            f"({driver_growth:+.2f}% y/y)."
        ),
        values=values,
        next_step="Add destination and product annex drivers when needed.",
    )


def imports_component_from_html(html: str) -> IndicatorComponent | None:
    text = _folded_text_from_html(html)
    match = re.search(
        rf"informacion\s+({_MONTH_NAMES})\s+(?:de\s+)?(20\d{{2}}).*?"
        rf"importaciones fueron us\$?{_NUMBER}\s+millones cif y presentaron "
        rf"un crecimiento de\s+{_PERCENT}.*?aumento de\s+{_PERCENT}\s+en "
        rf"el grupo de\s+(.+?)\.",
        text,
    )
    if not match:
        return None
    shares_match = re.search(
        rf"importaciones de manufacturas participaron con\s+{_PERCENT}.*?"
        rf"agropecuarios, alimentos y bebidas con\s+{_PERCENT}.*?"
        rf"combustibles y productos de las industrias extractivas con\s+"
        rf"{_PERCENT}.*?otros sectores con\s+{_PERCENT}",
        text,
    )
    period = _month_period(match.group(1), match.group(2))
    imports = _to_float(match.group(3))
    growth = _to_float(match.group(4))
    driver_growth = _to_float(match.group(5))
    driver_group = normalize_whitespace(match.group(6))
    values: dict[str, Any] = {
        "imports_usd_millions_cif": imports,
        "imports_annual_variation_pct": growth,
        "main_driver_group": driver_group,
        "main_driver_annual_variation_pct": driver_growth,
    }
    if shares_match:
        values["import_group_shares_pct"] = {
            "manufacturing": _to_float(shares_match.group(1)),
            "agriculture_food_beverages": _to_float(shares_match.group(2)),
            "fuels_and_extractives": _to_float(shares_match.group(3)),
            "other_sectors": _to_float(shares_match.group(4)),
        }
    return _component(
        component_id="imports",
        name="Goods imports",
        status="observed",
        source_name="DANE / DIAN",
        source_url=IMPORTS_URL,
        period=period,
        release_date=_latest_release_date(html),
        headline=(
            f"DANE imports {period}: US${imports:,.1f}m CIF "
            f"({growth:+.2f}% y/y); main driver {driver_group} "
            f"({driver_growth:+.2f}% y/y)."
        ),
        values=values,
        next_step="Add origin, CUODE, and product annex drivers when needed.",
    )


def _quarter_period(quarter_text: str, year: str) -> str:
    folded = fold_accents(quarter_text.lower())
    mapping = {
        "primer trimestre": "Q1",
        "i trimestre": "Q1",
        "segundo trimestre": "Q2",
        "ii trimestre": "Q2",
        "tercer trimestre": "Q3",
        "iii trimestre": "Q3",
        "cuarto trimestre": "Q4",
        "iv trimestre": "Q4",
    }
    for key, quarter in mapping.items():
        if key in folded:
            return f"{year}-{quarter}"
    return year


def _bundle_observation_from_components(
    indicator_id: str,
    components: Iterable[IndicatorComponent],
    *,
    failed_headline: str,
    observed_headline_prefix: str,
) -> IndicatorObservation | None:
    component_list = list(components)
    observed_components = [
        component for component in component_list if component.status == "observed"
    ]
    failed_components = [
        component for component in component_list if component.status == "failed"
    ]
    if not observed_components:
        if not failed_components:
            return None
        definition = _definition_map()[indicator_id]
        return _headline_definition(
            definition,
            status="failed",
            period="",
            release_date="",
            headline=failed_headline,
            values={},
            components=_complete_bundle_components(indicator_id, component_list),
        )
    definition = _definition_map()[indicator_id]
    latest_component = max(
        observed_components,
        key=lambda component: component.release_date or "",
    )
    values = {
        component.component_id: component.values for component in observed_components
    }
    component_ids = ", ".join(component.component_id for component in observed_components)
    completed = _complete_bundle_components(indicator_id, component_list)
    return _headline_definition(
        definition,
        status="observed",
        period=latest_component.period,
        release_date=latest_component.release_date or "",
        headline=f"{observed_headline_prefix}: {component_ids}.",
        values={
            "observed_components": len(observed_components),
            "total_components": len(_BUNDLE_COMPONENTS[indicator_id]),
            "components": values,
        },
        components=completed,
    )


def construction_bundle_observation_from_components(
    components: Iterable[IndicatorComponent],
) -> IndicatorObservation | None:
    return _bundle_observation_from_components(
        "construction_bundle",
        components,
        failed_headline="Construction bundle components failed to fetch or parse.",
        observed_headline_prefix="Construction bundle has observed components",
    )


def energy_system_observation_from_components(
    components: Iterable[IndicatorComponent],
) -> IndicatorObservation | None:
    return _bundle_observation_from_components(
        "energy_system",
        components,
        failed_headline="Energy system components failed to fetch or parse.",
        observed_headline_prefix="Energy system has observed components",
    )


def oil_gas_observation_from_components(
    components: Iterable[IndicatorComponent],
) -> IndicatorObservation | None:
    return _bundle_observation_from_components(
        "oil_gas_production",
        components,
        failed_headline="Oil and gas production components failed to fetch or parse.",
        observed_headline_prefix="Oil and gas production has observed components",
    )


def external_trade_observation_from_components(
    components: Iterable[IndicatorComponent],
) -> IndicatorObservation | None:
    observation = _bundle_observation_from_components(
        "external_trade",
        components,
        failed_headline="External trade components failed to fetch or parse.",
        observed_headline_prefix="External trade has observed components",
    )
    if observation is None or observation.status != "observed":
        return observation
    by_id = {
        component.component_id: component
        for component in observation.components
        if component.status == "observed"
    }
    exports = by_id.get("exports")
    imports = by_id.get("imports")
    if (
        exports is None
        or imports is None
        or exports.period != imports.period
        or not isinstance(exports.values.get("exports_usd_millions_fob"), float)
        or not isinstance(imports.values.get("imports_usd_millions_cif"), float)
    ):
        return observation
    trade_balance = round(
        exports.values["exports_usd_millions_fob"]
        - imports.values["imports_usd_millions_cif"],
        2,
    )
    values = {
        **observation.values,
        "goods_trade_balance_usd_millions": trade_balance,
    }
    headline = (
        f"DANE external trade {exports.period}: exports "
        f"US${exports.values['exports_usd_millions_fob']:,.1f}m FOB, imports "
        f"US${imports.values['imports_usd_millions_cif']:,.1f}m CIF, "
        f"balance {trade_balance:+,.1f}m."
    )
    return replace(observation, period=exports.period, headline=headline, values=values)


@dataclass(frozen=True, slots=True)
class TrmPoint:
    value: float
    valid_from: datetime
    valid_to: datetime | None
    unit: str


def _trm_points(rows: Iterable[dict[str, Any]]) -> list[TrmPoint]:
    points: list[TrmPoint] = []
    for row in rows:
        value = _to_float(row.get("valor"))
        valid_from = _parse_socrata_date(row.get("vigenciadesde"))
        if value is None or valid_from is None:
            continue
        points.append(
            TrmPoint(
                value=value,
                valid_from=valid_from,
                valid_to=_parse_socrata_date(row.get("vigenciahasta")),
                unit=str(row.get("unidad") or "COP"),
            )
        )
    return sorted(points, key=lambda point: point.valid_from, reverse=True)


def _nearest_on_or_before(points: list[TrmPoint], target: datetime) -> TrmPoint | None:
    for point in points:
        if point.valid_from <= target:
            return point
    return None


def _change(
    latest: TrmPoint,
    baseline: TrmPoint | None,
) -> tuple[float | None, float | None]:
    if baseline is None or baseline.value == 0:
        return None, None
    delta = round(latest.value - baseline.value, 2)
    return delta, round((delta / baseline.value) * 100, 2)


def trm_observation_from_rows(
    rows: Iterable[dict[str, Any]],
) -> IndicatorObservation | None:
    points = _trm_points(rows)
    if not points:
        return None

    latest = points[0]
    previous = points[1] if len(points) > 1 else None
    seven_day = _nearest_on_or_before(points, latest.valid_from - timedelta(days=7))
    thirty_day = _nearest_on_or_before(points, latest.valid_from - timedelta(days=30))
    daily_delta, daily_pct = _change(latest, previous)
    seven_day_delta, seven_day_pct = _change(latest, seven_day)
    thirty_day_delta, thirty_day_pct = _change(latest, thirty_day)
    definition = _definition_map()["trm_usd_cop"]
    valid_from = latest.valid_from.strftime("%Y-%m-%d")
    values = {
        "trm_cop_per_usd": latest.value,
        "daily_change_cop": daily_delta,
        "daily_change_pct": daily_pct,
        "seven_day_change_cop": seven_day_delta,
        "seven_day_change_pct": seven_day_pct,
        "thirty_day_change_cop": thirty_day_delta,
        "thirty_day_change_pct": thirty_day_pct,
    }
    headline = f"TRM vigente desde {valid_from}: {latest.value:.2f} {latest.unit}/USD."
    if seven_day_delta is not None and seven_day_pct is not None:
        direction = "depreciation" if seven_day_delta > 0 else "appreciation"
        headline += (
            f" Seven-day move: {seven_day_delta:+.2f} COP "
            f"({seven_day_pct:+.2f}%), peso {direction}."
        )
    return IndicatorObservation(
        indicator_id=definition.indicator_id,
        name=definition.name,
        category=definition.category,
        status="observed",
        frequency=definition.frequency,
        source_name=definition.source_name,
        source_url=definition.source_url,
        period=valid_from,
        release_date=valid_from + "T00:00:00Z",
        headline=headline,
        values={k: v for k, v in values.items() if v is not None},
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


def _parse_dmy_date(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", value)
    if not match:
        return None
    try:
        return datetime(
            int(match.group(3)),
            int(match.group(2)),
            int(match.group(1)),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None


def policy_rate_ibr_observation_from_rows(
    policy_rows: Iterable[dict[str, Any]],
    ibr_rows: Iterable[dict[str, Any]],
) -> IndicatorObservation | None:
    policy = next(iter(policy_rows), None)
    ibr = next(iter(ibr_rows), None)
    if not isinstance(policy, dict) or not isinstance(ibr, dict):
        return None
    policy_rate = _to_float_unrounded(policy.get("valor"))
    ibr_rate = _to_float_unrounded(ibr.get("valor"))
    policy_date = _parse_dmy_date(policy.get("fecha"))
    ibr_date = _parse_dmy_date(ibr.get("fecha"))
    if (
        policy_rate is None
        or ibr_rate is None
        or policy_date is None
        or ibr_date is None
    ):
        return None
    latest_date = max(policy_date, ibr_date)
    period = latest_date.strftime("%Y-%m-%d")
    spread = round(ibr_rate - policy_rate, 3)
    definition = _definition_map()["policy_rate_ibr"]
    return _headline_definition(
        definition,
        status="observed",
        period=period,
        release_date=period + "T00:00:00Z",
        headline=(
            f"BanRep {period}: policy rate {policy_rate:.2f}%, "
            f"IBR overnight nominal {ibr_rate:.3f}%, spread {spread:+.3f} pp."
        ),
        values={
            "policy_rate_pct": policy_rate,
            "policy_rate_date": policy_date.strftime("%Y-%m-%d"),
            "ibr_overnight_nominal_pct": ibr_rate,
            "ibr_date": ibr_date.strftime("%Y-%m-%d"),
            "ibr_policy_spread_pp": spread,
        },
    )


def _iso_date_from_ymd(value: str) -> str:
    return value + "T00:00:00Z"


def _latest_xm_item(items: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    dated_items = [
        item for item in items if isinstance(item.get("Date"), str) and item.get("Date")
    ]
    if not dated_items:
        return None
    return max(dated_items, key=lambda item: str(item["Date"]))


def _hourly_entity_values(item: dict[str, Any]) -> list[float]:
    entities = item.get("HourlyEntities")
    if not isinstance(entities, list) or not entities:
        return []
    values = entities[0].get("Values")
    if not isinstance(values, dict):
        return []
    parsed: list[float] = []
    for hour in range(1, 25):
        value = _to_float(values.get(f"Hour{hour:02d}"))
        if value is not None:
            parsed.append(value)
    return parsed


def _daily_entity_value(item: dict[str, Any]) -> float | None:
    entities = item.get("DailyEntities")
    if not isinstance(entities, list) or not entities:
        return None
    return _to_float_unrounded(entities[0].get("Value"))


def electricity_demand_component_from_xm_response(
    payload: dict[str, Any],
) -> IndicatorComponent | None:
    candidates: list[tuple[str, list[float], float]] = []
    for item in payload.get("Items", []):
        if not isinstance(item, dict) or not isinstance(item.get("Date"), str):
            continue
        hourly_kwh = _hourly_entity_values(item)
        if not hourly_kwh:
            continue
        demand_gwh = round(sum(hourly_kwh) / 1_000_000, 2)
        candidates.append((str(item["Date"]), hourly_kwh, demand_gwh))
    if not candidates:
        return None
    complete_candidates = [
        candidate
        for candidate in candidates
        if candidate[2] >= XM_MIN_FULL_DAY_DEMAND_GWH
    ]
    period, hourly_kwh, demand_gwh = max(
        complete_candidates or candidates,
        key=lambda candidate: candidate[0],
    )
    peak_mw = round((max(hourly_kwh) / 1_000), 2)
    return _component(
        component_id="electricity_demand",
        name="Electricity demand",
        status="observed",
        source_name="XM",
        source_url=XM_API_SOURCE_URL,
        period=period,
        release_date=_iso_date_from_ymd(period),
        headline=(
            f"XM {period}: SIN electricity demand {demand_gwh:.2f} GWh; "
            f"hourly peak {peak_mw:.2f} MW."
        ),
        values={
            "demand_gwh": demand_gwh,
            "peak_hourly_mw": peak_mw,
            "hour_count": len(hourly_kwh),
        },
        next_step="Add regulated/non-regulated demand split when useful.",
    )


def reservoir_component_from_xm_response(
    payload: dict[str, Any],
) -> IndicatorComponent | None:
    item = _latest_xm_item(payload.get("Items", []))
    if item is None:
        return None
    value = _daily_entity_value(item)
    if value is None:
        return None
    reservoir_pct = round(value * 100, 2) if abs(value) <= 1 else value
    period = str(item["Date"])
    return _component(
        component_id="reservoir_useful_volume",
        name="Reservoir useful volume",
        status="observed",
        source_name="XM",
        source_url=XM_API_SOURCE_URL,
        period=period,
        release_date=_iso_date_from_ymd(period),
        headline=f"XM {period}: SIN useful reservoir volume {reservoir_pct:.2f}%.",
        values={"reservoir_useful_volume_pct": reservoir_pct},
        next_step="Add reservoir-level stress detail when useful.",
    )


def spot_price_component_from_xm_response(
    payload: dict[str, Any],
) -> IndicatorComponent | None:
    item = _latest_xm_item(payload.get("Items", []))
    if item is None:
        return None
    price = _daily_entity_value(item)
    if price is None:
        return None
    price = round(price, 2)
    period = str(item["Date"])
    return _component(
        component_id="spot_price",
        name="Spot price",
        status="observed",
        source_name="XM",
        source_url=XM_API_SOURCE_URL,
        period=period,
        release_date=_iso_date_from_ymd(period),
        headline=f"XM {period}: weighted national spot price {price:.2f} COP/kWh.",
        values={"spot_price_cop_per_kwh": price},
        next_step="Add scarcity-price spread when useful.",
    )


def _period_from_anh_count(row: dict[str, Any]) -> tuple[int, int, int] | None:
    year = _to_float(row.get("vigencia"))
    month = _to_float(row.get("mes"))
    count = _to_float(row.get("count"))
    if year is None or month is None or count is None:
        return None
    return int(year), int(month), int(count)


def latest_complete_anh_period(
    period_counts: Iterable[dict[str, Any]],
) -> tuple[int, int] | None:
    periods = [
        period for row in period_counts if (period := _period_from_anh_count(row))
    ]
    if not periods:
        return None
    max_count = max(count for _, _, count in periods)
    min_complete_count = max_count * ANH_COMPLETENESS_RATIO
    for year, month, count in sorted(periods, reverse=True):
        if count >= min_complete_count:
            return year, month
    year, month, _ = max(periods)
    return year, month


def _top_departments(
    rows: Iterable[dict[str, Any]],
    value_field: str,
) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    for row in rows:
        department = normalize_whitespace(str(row.get("departamento") or "Unknown"))
        value = _to_float(row.get(value_field))
        if value is None:
            continue
        totals[department] = totals.get(department, 0.0) + value
    return [
        {"name": name, "value": round(value, 2)}
        for name, value in sorted(totals.items(), key=lambda item: item[1], reverse=True)[
            :5
        ]
    ]


def _anh_period_label(rows: list[dict[str, Any]], year: int, month: int) -> str:
    for row in rows:
        month_name = row.get("nombre_mes") or row.get("nombremes")
        if isinstance(month_name, str) and month_name.strip():
            return f"{month_name.strip()} {year}"
    return f"{year}-{month:02d}"


def crude_oil_component_from_anh_rows(
    rows: Iterable[dict[str, Any]],
    *,
    year: int,
    month: int,
    release_date: str,
) -> IndicatorComponent | None:
    row_list = list(rows)
    total_barrels = sum(
        value
        for row in row_list
        if (value := _to_float(row.get("produccion_bls"))) is not None
    )
    if not total_barrels:
        return None
    days = monthrange(year, month)[1]
    average_bpd = round(total_barrels / days, 2)
    period = f"{year}-{month:02d}"
    return _component(
        component_id="oil_production",
        name="Crude oil production",
        status="observed",
        source_name="ANH / datos.gov.co",
        source_url=ANH_CRUDE_SOURCE_URL,
        period=period,
        release_date=release_date,
        headline=(
            f"ANH crude {period}: {average_bpd:,.0f} barrels/day average "
            f"from {_anh_period_label(row_list, year, month)} field rows."
        ),
        values={
            "total_barrels": round(total_barrels, 2),
            "average_barrels_per_day": average_bpd,
            "field_rows": len(row_list),
            "top_departments_by_barrels": _top_departments(
                row_list, "produccion_bls"
            ),
        },
        next_step="Add field/operator contribution changes and royalty linkage later.",
    )


def fiscalized_gas_component_from_anh_rows(
    rows: Iterable[dict[str, Any]],
    *,
    year: int,
    month: int,
    release_date: str,
) -> IndicatorComponent | None:
    row_list = list(rows)
    total_kpc = sum(
        value
        for row in row_list
        if (value := _to_float(row.get("produccionkpc"))) is not None
    )
    if not total_kpc:
        return None
    days = monthrange(year, month)[1]
    average_mmcfd = round((total_kpc / days) / 1_000, 2)
    period = f"{year}-{month:02d}"
    return _component(
        component_id="gas_production",
        name="Fiscalized gas production",
        status="observed",
        source_name="ANH / datos.gov.co",
        source_url=ANH_GAS_SOURCE_URL,
        period=period,
        release_date=release_date,
        headline=(
            f"ANH gas {period}: {average_mmcfd:,.2f} million cubic feet/day "
            f"average from {_anh_period_label(row_list, year, month)} field rows."
        ),
        values={
            "total_kpc": round(total_kpc, 2),
            "average_million_cubic_feet_per_day": average_mmcfd,
            "field_rows": len(row_list),
            "top_departments_by_kpc": _top_departments(row_list, "produccionkpc"),
        },
        next_step="Add commercialized gas, demand balance, and import-risk linkage later.",
    )


def _socrata_rows_updated_release_date(metadata: dict[str, Any]) -> str:
    timestamp = metadata.get("rowsUpdatedAt") or metadata.get("viewLastModified")
    if isinstance(timestamp, int):
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%dT00:00:00Z"
        )
    return ""


_XLSX_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _xlsx_column_index(cell_ref: str) -> int:
    letters = "".join(char for char in cell_ref if char.isalpha())
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - ord("A") + 1)
    return index - 1


def _xlsx_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    try:
        root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    shared: list[str] = []
    for item in root.findall("m:si", _XLSX_NS):
        shared.append(
            "".join(text.text or "" for text in item.findall(".//m:t", _XLSX_NS))
        )
    return shared


def _xlsx_first_sheet_rows(xlsx_bytes: bytes) -> list[list[str]]:
    workbook = zipfile.ZipFile(io.BytesIO(xlsx_bytes))
    shared = _xlsx_shared_strings(workbook)
    root = ElementTree.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
    rows: list[list[str]] = []
    for row in root.findall(".//m:row", _XLSX_NS):
        values_by_index: dict[int, str] = {}
        for cell in row.findall("m:c", _XLSX_NS):
            value_node = cell.find("m:v", _XLSX_NS)
            value = ""
            if value_node is not None and value_node.text is not None:
                value = value_node.text
                if cell.get("t") == "s":
                    value = shared[int(value)]
            elif cell.get("t") == "inlineStr":
                value = "".join(
                    text.text or "" for text in cell.findall(".//m:t", _XLSX_NS)
                )
            if value:
                values_by_index[_xlsx_column_index(str(cell.get("r") or ""))] = value
        if not values_by_index:
            rows.append([])
            continue
        max_index = max(values_by_index)
        rows.append([values_by_index.get(index, "") for index in range(max_index + 1)])
    return rows


def _month_number_from_text(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    folded = fold_accents(value.lower())
    return _MONTHS_ES.get(folded) or _MONTH_ABBR_ES.get(folded[:3])


def _row_value_by_header(
    row: list[str],
    headers: list[str],
    header_text: str,
) -> float | None:
    folded_header = fold_accents(header_text.lower())
    for index, header in enumerate(headers):
        if folded_header in fold_accents(str(header).lower()):
            if index < len(row):
                return _to_float(row[index])
            return None
    return None


def fiscal_tax_observation_from_dian_xlsx(xlsx_bytes: bytes) -> IndicatorObservation | None:
    rows = _xlsx_first_sheet_rows(xlsx_bytes)
    header_index = next(
        (
            index
            for index, row in enumerate(rows)
            if any(str(value).strip() == "Año" for value in row)
            and any(str(value).strip() == "Mes" for value in row)
        ),
        None,
    )
    if header_index is None:
        return None
    headers = rows[header_index]
    data: list[tuple[int, int, list[str]]] = []
    for row in rows[header_index + 1 :]:
        if len(row) < 2:
            continue
        year = _to_float(row[1])
        month = _month_number_from_text(row[2]) if len(row) > 2 else None
        if year is None or month is None:
            continue
        data.append((int(year), month, row))
    if not data:
        return None
    year, month, latest_row = max(data, key=lambda item: (item[0], item[1]))
    previous_row = next(
        (
            row
            for row_year, row_month, row in data
            if row_year == year - 1 and row_month == month
        ),
        None,
    )
    total = _row_value_by_header(latest_row, headers, "Total (A+B+C)")
    internal = _row_value_by_header(latest_row, headers, "A. Internos")
    external = _row_value_by_header(latest_row, headers, "B. Externos")
    income_tax = _row_value_by_header(latest_row, headers, "1. Renta")
    internal_vat = _row_value_by_header(latest_row, headers, "2. IVA interno")
    tariff = _row_value_by_header(latest_row, headers, "19.Arancel")
    external_vat = _row_value_by_header(latest_row, headers, "20. IVA Externo")
    previous_total = (
        _row_value_by_header(previous_row, headers, "Total (A+B+C)")
        if previous_row
        else None
    )
    if total is None:
        return None
    annual_change = (
        round(((total - previous_total) / previous_total) * 100, 2)
        if total is not None and previous_total not in {None, 0}
        else None
    )
    release_text = " ".join(
        str(value)
        for row in rows[: header_index + 1] + rows[-4:]
        for value in row
        if value
    )
    release_date = _release_date_from_text(release_text)
    period = f"{year}-{month:02d}"
    definition = _definition_map()["fiscal_tax_pulse"]
    return _headline_definition(
        definition,
        status="observed",
        period=period,
        release_date=release_date,
        headline=(
            f"DIAN tax collection {period}: COP {total:,.0f} million gross "
            f"revenue"
            + (
                f" ({annual_change:+.2f}% y/y)."
                if annual_change is not None
                else "."
            )
        ),
        values={
            "gross_tax_revenue_cop_millions": total,
            "gross_tax_revenue_annual_variation_pct": annual_change,
            "internal_tax_revenue_cop_millions": internal,
            "external_tax_revenue_cop_millions": external,
            "income_tax_cop_millions": income_tax,
            "internal_vat_cop_millions": internal_vat,
            "tariff_revenue_cop_millions": tariff,
            "external_vat_cop_millions": external_vat,
            "previous_year_same_month_cop_millions": previous_total,
        },
    )


def _failed_observation(
    definition: IndicatorDefinition,
    message: str,
) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id=definition.indicator_id,
        name=definition.name,
        category=definition.category,
        status="failed",
        frequency=definition.frequency,
        source_name=definition.source_name,
        source_url=definition.source_url,
        headline=message,
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


def _fetch_trm_observation(client: httpx.Client) -> IndicatorObservation:
    definition = _definition_map()["trm_usd_cop"]
    try:
        response = client.get(
            TRM_API_URL,
            params={
                "$select": "valor,unidad,vigenciadesde,vigenciahasta",
                "$order": "vigenciadesde DESC",
                "$limit": str(TRM_ROWS_LIMIT),
            },
        )
        response.raise_for_status()
        rows = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_observation(
            definition,
            f"Structured TRM fetch failed: {exc.__class__.__name__}: {exc}",
        )

    if not isinstance(rows, list):
        return _failed_observation(
            definition,
            "Structured TRM fetch returned non-list JSON.",
        )
    observation = trm_observation_from_rows(rows)
    if observation is None:
        return _failed_observation(
            definition,
            "Structured TRM fetch returned no parseable rows.",
        )
    return observation


def _fetch_banrep_series_rows(
    client: httpx.Client,
    series_id: int,
) -> list[dict[str, Any]] | None:
    response = client.get(
        f"{BANREP_SERIES_API_BASE}/consultaInformacionSerieXTipoDato",
        params={"idSerie": str(series_id), "tipoDato": "1", "cantDatos": "5"},
        headers={"Content-type": "application/json; charset=utf-8"},
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        return None
    return [row for row in rows if isinstance(row, dict)]


def _fetch_policy_rate_ibr_observation(client: httpx.Client) -> IndicatorObservation:
    definition = _definition_map()["policy_rate_ibr"]
    try:
        policy_rows = _fetch_banrep_series_rows(client, BANREP_POLICY_RATE_SERIES_ID)
        ibr_rows = _fetch_banrep_series_rows(client, BANREP_IBR_OVERNIGHT_SERIES_ID)
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_observation(
            definition,
            f"BanRep SUAMECA fetch failed: {exc.__class__.__name__}: {exc}",
        )
    if policy_rows is None or ibr_rows is None:
        return _failed_observation(
            definition,
            "BanRep SUAMECA fetch returned non-list JSON.",
        )
    observation = policy_rate_ibr_observation_from_rows(policy_rows, ibr_rows)
    if observation is None:
        return _failed_observation(
            definition,
            "BanRep SUAMECA fetch returned no parseable policy/IBR rows.",
        )
    return observation


_DANE_HTML_INDICATORS: tuple[
    tuple[
        str,
        str,
        Callable[[str], IndicatorObservation | None],
    ],
    ...,
] = (
    ("ipc_inflation", IPC_URL, ipc_observation_from_html),
    ("labor_market", LABOR_MARKET_URL, labor_market_observation_from_html),
    ("retail_sales", EMC_URL, retail_sales_observation_from_html),
    ("manufacturing", EMMET_URL, manufacturing_observation_from_html),
)

_DANE_CONSTRUCTION_COMPONENTS: tuple[
    tuple[
        str,
        str,
        Callable[[str], IndicatorComponent | None],
    ],
    ...,
] = (
    ("cement", CEMENT_URL, cement_component_from_html),
    ("licenses", CONSTRUCTION_LICENSES_URL, construction_licenses_component_from_html),
    ("housing_finance", HOUSING_FINANCE_URL, housing_finance_component_from_html),
)

_DANE_TRADE_COMPONENTS: tuple[
    tuple[
        str,
        str,
        Callable[[str], IndicatorComponent | None],
    ],
    ...,
] = (
    ("exports", EXPORTS_URL, exports_component_from_html),
    ("imports", IMPORTS_URL, imports_component_from_html),
)

_XM_ENERGY_COMPONENTS: tuple[
    tuple[
        str,
        str,
        str,
        Callable[[dict[str, Any]], IndicatorComponent | None],
    ],
    ...,
] = (
    (
        "electricity_demand",
        XM_HOURLY_API_URL,
        "DemaReal",
        electricity_demand_component_from_xm_response,
    ),
    (
        "reservoir_useful_volume",
        XM_DAILY_API_URL,
        "PorcVoluUtilDiar",
        reservoir_component_from_xm_response,
    ),
    (
        "spot_price",
        XM_DAILY_API_URL,
        "PPPrecBolsNaci",
        spot_price_component_from_xm_response,
    ),
)

_ANH_PRODUCTION_COMPONENTS: tuple[
    tuple[
        str,
        str,
        str,
        str,
        Callable[..., IndicatorComponent | None],
    ],
    ...,
] = (
    (
        "oil_production",
        ANH_CRUDE_RESOURCE_ID,
        ANH_CRUDE_API_URL,
        "produccion_bls",
        crude_oil_component_from_anh_rows,
    ),
    (
        "gas_production",
        ANH_GAS_RESOURCE_ID,
        ANH_GAS_API_URL,
        "produccionkpc",
        fiscalized_gas_component_from_anh_rows,
    ),
)


def _fetch_dane_html_observation(
    client: httpx.Client,
    indicator_id: str,
    url: str,
    parser: Callable[[str], IndicatorObservation | None],
) -> IndicatorObservation:
    definition = _definition_map()[indicator_id]
    try:
        response = client.get(url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return _failed_observation(
            definition,
            f"DANE headline fetch failed: {exc.__class__.__name__}: {exc}",
        )
    observation = parser(response.text)
    if observation is None:
        return _failed_observation(
            definition,
            "DANE headline fetch returned no parseable current-result text.",
        )
    return observation


def _component_default(indicator_id: str, component_id: str) -> dict[str, Any]:
    defaults = {
        str(component["component_id"]): component
        for component in _BUNDLE_COMPONENTS[indicator_id]
    }
    return defaults[component_id]


def _failed_component(
    indicator_id: str,
    component_id: str,
    headline: str,
) -> IndicatorComponent:
    default = _component_default(indicator_id, component_id)
    return _component(
        component_id=component_id,
        name=str(default["name"]),
        status="failed",
        source_name=str(default["source_name"]),
        source_url=str(default["source_url"]),
        headline=headline,
        freshness_status="failed",
        next_step=str(default["next_step"]),
    )


def _fetch_dane_component(
    client: httpx.Client,
    component_id: str,
    url: str,
    parser: Callable[[str], IndicatorComponent | None],
    indicator_id: str = "construction_bundle",
) -> IndicatorComponent:
    try:
        response = client.get(url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return _failed_component(
            indicator_id,
            component_id=component_id,
            headline=f"DANE component fetch failed: {exc.__class__.__name__}: {exc}",
        )
    component = parser(response.text)
    if component is None:
        return _failed_component(
            indicator_id,
            component_id=component_id,
            headline="DANE component fetch returned no parseable current-result text.",
        )
    return component


def _fetch_xm_component(
    client: httpx.Client,
    component_id: str,
    url: str,
    metric_id: str,
    parser: Callable[[dict[str, Any]], IndicatorComponent | None],
) -> IndicatorComponent:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=XM_LOOKBACK_DAYS)
    try:
        response = client.post(
            url,
            json={
                "MetricId": metric_id,
                "StartDate": start_date.isoformat(),
                "EndDate": end_date.isoformat(),
                "Entity": "Sistema",
            },
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_component(
            "energy_system",
            component_id=component_id,
            headline=f"XM component fetch failed: {exc.__class__.__name__}: {exc}",
        )
    if not isinstance(payload, dict):
        return _failed_component(
            "energy_system",
            component_id=component_id,
            headline="XM component fetch returned non-object JSON.",
        )
    component = parser(payload)
    if component is None:
        return _failed_component(
            "energy_system",
            component_id=component_id,
            headline="XM component fetch returned no parseable rows.",
        )
    return component


def _fetch_anh_component(
    client: httpx.Client,
    component_id: str,
    resource_id: str,
    api_url: str,
    value_field: str,
    parser: Callable[..., IndicatorComponent | None],
) -> IndicatorComponent:
    try:
        counts_response = client.get(
            api_url,
            params={
                "$select": "vigencia,mes,count(*)",
                "$group": "vigencia,mes",
                "$order": "vigencia DESC, mes DESC",
                "$limit": str(ANH_PERIOD_COUNT_LIMIT),
            },
        )
        counts_response.raise_for_status()
        period_counts = counts_response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_component(
            "oil_gas_production",
            component_id=component_id,
            headline=f"ANH period lookup failed: {exc.__class__.__name__}: {exc}",
        )
    if not isinstance(period_counts, list):
        return _failed_component(
            "oil_gas_production",
            component_id=component_id,
            headline="ANH period lookup returned non-list JSON.",
        )
    period = latest_complete_anh_period(period_counts)
    if period is None:
        return _failed_component(
            "oil_gas_production",
            component_id=component_id,
            headline="ANH period lookup returned no parseable periods.",
        )
    year, month = period
    try:
        rows_response = client.get(
            api_url,
            params={
                "$select": f"vigencia,mes,departamento,{value_field}",
                "$where": f"vigencia='{year}' AND mes='{month}'",
                "$limit": "5000",
            },
        )
        rows_response.raise_for_status()
        rows = rows_response.json()
        metadata_response = client.get(
            f"https://www.datos.gov.co/api/views/{resource_id}"
        )
        metadata_response.raise_for_status()
        metadata = metadata_response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_component(
            "oil_gas_production",
            component_id=component_id,
            headline=f"ANH rows fetch failed: {exc.__class__.__name__}: {exc}",
        )
    if not isinstance(rows, list) or not isinstance(metadata, dict):
        return _failed_component(
            "oil_gas_production",
            component_id=component_id,
            headline="ANH rows fetch returned an unexpected JSON shape.",
        )
    component = parser(
        rows,
        year=year,
        month=month,
        release_date=_socrata_rows_updated_release_date(metadata),
    )
    if component is None:
        return _failed_component(
            "oil_gas_production",
            component_id=component_id,
            headline="ANH rows fetch returned no parseable production values.",
        )
    return component


def _fetch_dian_tax_observation(client: httpx.Client) -> IndicatorObservation:
    definition = _definition_map()["fiscal_tax_pulse"]
    try:
        response = client.get(DIAN_MONTHLY_TAX_ZIP_URL, follow_redirects=True)
        response.raise_for_status()
        archive = zipfile.ZipFile(io.BytesIO(response.content))
        xlsx_name = next(
            name for name in archive.namelist() if name.lower().endswith(".xlsx")
        )
        observation = fiscal_tax_observation_from_dian_xlsx(archive.read(xlsx_name))
    except (httpx.HTTPError, ValueError, KeyError, StopIteration, zipfile.BadZipFile) as exc:
        return _failed_observation(
            definition,
            f"DIAN tax collection fetch failed: {exc.__class__.__name__}: {exc}",
        )
    if observation is None:
        return _failed_observation(
            definition,
            "DIAN tax collection XLSX returned no parseable latest monthly row.",
        )
    return observation


def fetch_structured_indicator_observations() -> list[IndicatorObservation]:
    with httpx.Client(timeout=STRUCTURED_INDICATOR_TIMEOUT) as client:
        observations = [
            _fetch_trm_observation(client),
            _fetch_policy_rate_ibr_observation(client),
        ]
        observations.extend(
            _fetch_dane_html_observation(client, indicator_id, url, parser)
            for indicator_id, url, parser in _DANE_HTML_INDICATORS
        )
        construction_components = [
            _fetch_dane_component(client, component_id, url, parser)
            for component_id, url, parser in _DANE_CONSTRUCTION_COMPONENTS
        ]
        construction = construction_bundle_observation_from_components(
            construction_components
        )
        if construction:
            observations.append(construction)
        trade_components = [
            _fetch_dane_component(client, component_id, url, parser, "external_trade")
            for component_id, url, parser in _DANE_TRADE_COMPONENTS
        ]
        external_trade = external_trade_observation_from_components(trade_components)
        if external_trade:
            observations.append(external_trade)
        energy_components = [
            _fetch_xm_component(client, component_id, url, metric_id, parser)
            for component_id, url, metric_id, parser in _XM_ENERGY_COMPONENTS
        ]
        energy = energy_system_observation_from_components(energy_components)
        if energy:
            observations.append(energy)
        oil_gas_components = [
            _fetch_anh_component(
                client, component_id, resource_id, url, value_field, parser
            )
            for component_id, resource_id, url, value_field, parser in (
                _ANH_PRODUCTION_COMPONENTS
            )
        ]
        oil_gas = oil_gas_observation_from_components(oil_gas_components)
        if oil_gas:
            observations.append(oil_gas)
        observations.append(_fetch_dian_tax_observation(client))
    return observations


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

    component = _component(
        component_id="icoced",
        name="ICOCED costs",
        status="observed",
        source_name=item.source_name,
        source_url=item.url,
        period=_format_period(metadata.get("period_year"), metadata.get("period_month")),
        release_date=item.published_at,
        headline=item.raw_text,
        values=values,
        next_step="Parsed from the latest XLSX annex.",
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
        components=_complete_bundle_components("construction_bundle", [component]),
        why_it_matters=definition.why_it_matters,
        correlations=list(definition.correlations),
        next_step=definition.next_step,
    )


_SECOP_PROCESS_TYPES = {
    "secop_ii_procesos": "secop_ii_processes",
    "secop_ii_contratos": "secop_ii_contracts",
    "secop_i_procesos": "secop_i_processes",
    "secop_ii_adiciones": "secop_ii_additions",
    "secop_multas_sanciones": "secop_sanctions",
}


def _top_counts(counts: dict[str, int], limit: int = 5) -> list[dict[str, int | str]]:
    return [
        {"name": key, "records": value}
        for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[
            :limit
        ]
    ]


def _entity_from_item(item: CleanedItem, raw: RawItem | None) -> str:
    if raw and isinstance(raw.metadata.get("entity"), str):
        return raw.metadata["entity"]
    parts = item.clean_text.split("|")
    if len(parts) >= 2:
        return parts[-1].strip()
    parts = item.title.split(" — ")
    if len(parts) >= 3:
        return parts[-1].strip()
    return ""


def _secop_observation(
    raw_items: Iterable[RawItem],
    cleaned_items: Iterable[CleanedItem],
) -> IndicatorObservation | None:
    raw_by_id = {item.id: item for item in raw_items}
    secop_items = [
        item
        for item in cleaned_items
        if item.source_id
        in _SECOP_PROCESS_TYPES
        and not item.quality_notes
        and item.published_at
    ]
    if not secop_items:
        return None

    by_source: dict[str, int] = {}
    by_process_type: dict[str, int] = {}
    by_day: dict[str, int] = {}
    by_entity: dict[str, int] = {}
    latest: str | None = None
    for item in secop_items:
        raw = raw_by_id.get(item.id)
        by_source[item.source_id] = by_source.get(item.source_id, 0) + 1
        process_type = _SECOP_PROCESS_TYPES[item.source_id]
        by_process_type[process_type] = by_process_type.get(process_type, 0) + 1
        day = item.published_at[:10]
        by_day[day] = by_day.get(day, 0) + 1
        entity = _entity_from_item(item, raw)
        if entity:
            by_entity[entity] = by_entity.get(entity, 0) + 1
        if latest is None or item.published_at > latest:
            latest = item.published_at

    definition = _definition_map()["secop_procurement"]
    process_parts = [
        f"{count} {process_type}"
        for process_type, count in sorted(by_process_type.items())
    ]
    top_entity = _top_counts(by_entity, limit=1)
    entity_text = (
        f" Top entity: {top_entity[0]['name']} ({top_entity[0]['records']} records)."
        if top_entity
        else ""
    )
    headline = (
        f"SECOP pulse captured {len(secop_items)} rankable procurement records"
        f" in the freshness window ({', '.join(process_parts)})."
        + entity_text
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
            "records_by_process_type": by_process_type,
            "records_by_day": dict(sorted(by_day.items())),
            "top_entities": _top_counts(by_entity),
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


def _latest_text(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return max(left, right)


def _merge_observations(
    left: IndicatorObservation,
    right: IndicatorObservation,
) -> IndicatorObservation:
    if left.indicator_id != right.indicator_id:
        return right
    components_by_id: dict[str, IndicatorComponent] = {}
    for component in [*left.components, *right.components]:
        existing = components_by_id.get(component.component_id)
        if existing is None or (
            existing.status != "observed" and component.status == "observed"
        ):
            components_by_id[component.component_id] = component
    components = _complete_bundle_components(
        left.indicator_id,
        list(components_by_id.values()),
    )
    latest_release = _latest_text(left.release_date, right.release_date)
    latest = right if right.release_date == latest_release else left
    if left.headline and right.headline and left.headline != right.headline:
        headline = f"{left.headline} {right.headline}"
    else:
        headline = latest.headline or left.headline or right.headline
    status = "observed" if "observed" in {left.status, right.status} else latest.status
    values = {**left.values, **right.values}
    if left.indicator_id in _BUNDLE_COMPONENTS:
        values.update(_bundle_values(left.indicator_id, components))
        observed_ids = [
            component.component_id
            for component in components
            if component.status == "observed"
        ]
        if observed_ids:
            headline = (
                "Construction bundle has observed components: "
                f"{', '.join(observed_ids)}."
            )
    return replace(
        latest,
        status=status,
        period=latest.period or left.period or right.period,
        release_date=latest_release,
        headline=headline,
        values=values,
        components=components,
    )


def _store_observation(
    observed: dict[str, IndicatorObservation],
    observation: IndicatorObservation,
) -> None:
    existing = observed.get(observation.indicator_id)
    observed[observation.indicator_id] = (
        _merge_observations(existing, observation) if existing else observation
    )


def build_indicator_watch(
    raw_items: list[RawItem],
    cleaned_items: list[CleanedItem],
    extra_observations: Iterable[IndicatorObservation] = (),
    now: datetime | None = None,
) -> list[IndicatorObservation]:
    observed: dict[str, IndicatorObservation] = {}
    for observation in extra_observations:
        _store_observation(observed, observation)

    for item in raw_items:
        if item.source_id == "dane_icoced":
            observation = _icoced_observation(item)
            if observation:
                _store_observation(observed, observation)

    secop = _secop_observation(raw_items, cleaned_items)
    if secop:
        _store_observation(observed, secop)

    watch: list[IndicatorObservation] = []
    current = now or datetime.now(timezone.utc)
    for definition in INDICATOR_DEFINITIONS:
        observation = observed.get(definition.indicator_id) or _pending_observation(
            definition
        )
        if not observation.components:
            observation = replace(
                observation,
                components=_pending_components(observation.indicator_id),
            )
        elif observation.indicator_id in _BUNDLE_COMPONENTS:
            observation = replace(
                observation,
                components=_complete_bundle_components(
                    observation.indicator_id, observation.components
                ),
            )
        watch.append(_apply_freshness(observation, current))
    return watch
