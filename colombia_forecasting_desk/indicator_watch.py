from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

import httpx
from bs4 import BeautifulSoup

from .cleaner import fold_accents, normalize_whitespace
from .models import CleanedItem, IndicatorObservation, RawItem


STRUCTURED_INDICATOR_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
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
        source_url="https://www.banrep.gov.co/es/glosario/ibr",
        why_it_matters="Shows monetary stance and short-term peso liquidity.",
        correlations=(
            "IBR-policy spread can flag liquidity stress or market repricing",
            "policy rate + inflation surprise frames likelihood of cuts or pauses",
        ),
        next_step=(
            "datos.gov.co exposes IBR as a link resource into BanRep's statistics "
            "portal, not as a table; find a stable API/export before wiring."
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


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(str(value).replace(",", ".")), 2)
    except ValueError:
        return None


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

_PERCENT = r"(-?\d+(?:[,.]\d+)?)\s*%"
_NUMBER = r"(-?\d+(?:[,.]\d+)?)"
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


def _headline_definition(
    definition: IndicatorDefinition,
    *,
    status: str,
    period: str,
    release_date: str,
    headline: str,
    values: dict[str, Any],
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


def fetch_structured_indicator_observations() -> list[IndicatorObservation]:
    with httpx.Client(timeout=STRUCTURED_INDICATOR_TIMEOUT) as client:
        observations = [_fetch_trm_observation(client)]
        observations.extend(
            _fetch_dane_html_observation(client, indicator_id, url, parser)
            for indicator_id, url, parser in _DANE_HTML_INDICATORS
        )
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


def build_indicator_watch(
    raw_items: list[RawItem],
    cleaned_items: list[CleanedItem],
    extra_observations: Iterable[IndicatorObservation] = (),
) -> list[IndicatorObservation]:
    observed: dict[str, IndicatorObservation] = {}
    for observation in extra_observations:
        observed[observation.indicator_id] = observation

    for item in raw_items:
        if item.source_id == "dane_icoced":
            observation = _icoced_observation(item)
            if observation:
                observed[observation.indicator_id] = observation

    secop = _secop_observation(raw_items, cleaned_items)
    if secop:
        observed[secop.indicator_id] = secop

    watch: list[IndicatorObservation] = []
    for definition in INDICATOR_DEFINITIONS:
        watch.append(
            observed.get(definition.indicator_id) or _pending_observation(definition)
        )
    return watch
