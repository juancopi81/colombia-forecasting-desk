from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import httpx

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


def fetch_structured_indicator_observations() -> list[IndicatorObservation]:
    definition = _definition_map()["trm_usd_cop"]
    try:
        with httpx.Client(timeout=STRUCTURED_INDICATOR_TIMEOUT) as client:
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
        return [
            _failed_observation(
                definition,
                f"Structured TRM fetch failed: {exc.__class__.__name__}: {exc}",
            )
        ]

    if not isinstance(rows, list):
        return [
            _failed_observation(
                definition,
                "Structured TRM fetch returned non-list JSON.",
            )
        ]
    observation = trm_observation_from_rows(rows)
    if observation is None:
        return [
            _failed_observation(
                definition,
                "Structured TRM fetch returned no parseable rows.",
            )
        ]
    return [observation]


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
