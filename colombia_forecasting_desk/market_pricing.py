from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from typing import Any

import httpx

from .models import MarketPricingObservation

SCHEMA_VERSION = "market_pricing_watch.v1"
MARKET_PRICING_TIMEOUT = httpx.Timeout(12.0, connect=5.0)
MARKET_LOOKBACK_DAYS = 30
MAX_FRESHNESS_DAYS = 7
USER_AGENT = "Mozilla/5.0"

FRED_BRENT_URL = "https://fred.stlouisfed.org/series/DCOILBRENTEU"
FRED_BRENT_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
NASDAQ_HISTORICAL_URL = "https://api.nasdaq.com/api/quote/{symbol}/historical"
YAHOO_CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"

MARKET_DEFINITIONS: tuple[dict[str, str], ...] = (
    {
        "market_id": "brent_spot_fred",
        "name": "Brent crude spot price",
        "category": "energy_market",
        "symbol": "DCOILBRENTEU",
        "instrument_type": "commodity_spot",
        "currency": "USD/barrel",
        "source_name": "FRED / EIA",
        "source_url": FRED_BRENT_URL,
        "primary": "fred",
    },
    {
        "market_id": "ec_adr_nasdaq",
        "name": "Ecopetrol ADR",
        "category": "colombia_adr",
        "symbol": "EC",
        "instrument_type": "adr",
        "currency": "USD",
        "source_name": "Nasdaq public historical endpoint",
        "source_url": "https://www.nasdaq.com/market-activity/stocks/ec/historical",
        "primary": "nasdaq",
        "assetclass": "stocks",
    },
    {
        "market_id": "cib_adr_nasdaq",
        "name": "Grupo Cibest ADR",
        "category": "colombia_adr",
        "symbol": "CIB",
        "instrument_type": "adr",
        "currency": "USD",
        "source_name": "Nasdaq public historical endpoint",
        "source_url": "https://www.nasdaq.com/market-activity/stocks/cib/historical",
        "primary": "nasdaq",
        "assetclass": "stocks",
    },
    {
        "market_id": "colo_etf_nasdaq",
        "name": "Global X MSCI Colombia ETF",
        "category": "colombia_etf",
        "symbol": "COLO",
        "instrument_type": "etf",
        "currency": "USD",
        "source_name": "Nasdaq public historical endpoint",
        "source_url": "https://www.nasdaq.com/market-activity/etf/colo/historical",
        "primary": "nasdaq",
        "assetclass": "etf",
    },
)


def fetch_market_pricing_observations(
    *,
    now: datetime | None = None,
    client: httpx.Client | None = None,
) -> list[MarketPricingObservation]:
    """Fetch experimental no-key market-pricing context.

    These observations are review context only. They must fail closed because the
    ADR/ETF endpoints are public web endpoints, not contracted market-data APIs.
    """
    current = now or datetime.now(timezone.utc)
    close_client = client is None
    active_client = client or httpx.Client(
        timeout=MARKET_PRICING_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    try:
        return [
            _fetch_market_definition(active_client, definition, now=current)
            for definition in MARKET_DEFINITIONS
        ]
    finally:
        if close_client:
            active_client.close()


def attach_market_pricing_watch(
    m2_review_packet: dict[str, Any],
    market_pricing_watch: list[MarketPricingObservation],
) -> dict[str, Any]:
    """Return an M2 packet copy with market-pricing context embedded."""
    packet = dict(m2_review_packet)
    summary = dict(packet.get("summary") or {})
    inputs = dict(packet.get("inputs") or {})
    policy = dict(packet.get("policy") or {})

    summary["market_pricing_observation_count"] = len(market_pricing_watch)
    summary["market_pricing_observed_count"] = sum(
        1 for item in market_pricing_watch if item.status == "observed"
    )
    summary["market_pricing_failed_count"] = sum(
        1 for item in market_pricing_watch if item.status == "failed"
    )
    summary["market_pricing_stale_count"] = sum(
        1 for item in market_pricing_watch if item.status == "stale"
    )
    inputs["market_pricing_watch_artifact"] = "market_pricing_watch.json"
    policy["market_pricing_watch_policy"] = (
        "Market pricing watch is experimental review context only. It may help "
        "M2 inspect ADR, ETF, and Brent/oil context, but it is not investment "
        "advice, a forecast question, a ranking signal, or a probability input. "
        "Endpoint failures or stale closes must be treated as source-health "
        "caveats, not as absence of market movement."
    )

    packet["summary"] = summary
    packet["inputs"] = inputs
    packet["policy"] = policy
    packet["market_pricing_watch"] = [
        _observation_to_review_context(item) for item in market_pricing_watch
    ]
    return packet


def render_market_pricing_watch(
    observations: list[MarketPricingObservation],
    *,
    run_date: str,
) -> str:
    lines = [
        f"# Market Pricing Watch - {run_date}",
        "",
        (
            "Experimental fail-closed market-pricing context for M2. These rows "
            "are not investment advice, forecast questions, ranking signals, or "
            "probability inputs."
        ),
        "",
        (
            "Use them to inspect whether market pricing is aligned with, "
            "contradicts, or is independent from official Colombia evidence."
        ),
        "",
    ]
    if not observations:
        lines.append("No market-pricing observations were fetched for this run.")
        return "\n".join(lines).rstrip() + "\n"

    observed = [item for item in observations if item.status == "observed"]
    other = [item for item in observations if item.status != "observed"]
    lines.extend(
        [
            "Summary:",
            "",
            f"- Observed: {len(observed)}",
            f"- Failed: {sum(1 for item in observations if item.status == 'failed')}",
            f"- Stale: {sum(1 for item in observations if item.status == 'stale')}",
            "",
        ]
    )

    if observed:
        lines.extend(["Observed:", ""])
        for item in observed:
            lines.extend(_render_observation(item))
    if other:
        lines.extend(["Unavailable or stale:", ""])
        for item in other:
            lines.extend(_render_observation(item))
    return "\n".join(lines).rstrip() + "\n"


def _fetch_market_definition(
    client: httpx.Client,
    definition: dict[str, str],
    *,
    now: datetime,
) -> MarketPricingObservation:
    try:
        if definition["primary"] == "fred":
            return _fetch_fred_brent(client, definition, now=now)
        return _fetch_nasdaq_with_yahoo_fallback(client, definition, now=now)
    except Exception as exc:  # pragma: no cover - final fail-closed guard.
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message=f"Market pricing fetch failed: {exc.__class__.__name__}: {exc}",
        )


def _fetch_fred_brent(
    client: httpx.Client,
    definition: dict[str, str],
    *,
    now: datetime,
) -> MarketPricingObservation:
    end = now.date()
    start = end - timedelta(days=MARKET_LOOKBACK_DAYS)
    try:
        response = client.get(
            FRED_BRENT_CSV_URL,
            params={
                "id": definition["symbol"],
                "cosd": start.isoformat(),
                "coed": end.isoformat(),
            },
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message=f"FRED Brent fetch failed: {exc.__class__.__name__}: {exc}",
        )

    rows = list(csv.DictReader(StringIO(response.text)))
    latest = _latest_fred_row(rows, value_key=definition["symbol"], as_of=end)
    if latest is None:
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message="FRED Brent CSV returned no parseable non-empty daily rows.",
        )
    observed_date, close = latest
    return _priced_observation(
        definition,
        fetched_at=_iso_now(now),
        observed_date=observed_date,
        latest_close=close,
        source_name=definition["source_name"],
        source_url=definition["source_url"],
        now=now,
        headline=(
            f"Brent spot latest daily close was {close:.2f} "
            f"{definition['currency']} on {observed_date.isoformat()}."
        ),
        values={
            "close": close,
            "unit": definition["currency"],
            "lookback_days": MARKET_LOOKBACK_DAYS,
        },
        caveats=[
            "FRED/EIA is a spot-price series, not a Brent futures settlement.",
            "Daily series can have blanks on holidays or publication-lag days.",
        ],
        next_step=(
            "Use as oil/export/energy context only; corroborate with official "
            "Colombia evidence before forming a thesis."
        ),
    )


def _fetch_nasdaq_with_yahoo_fallback(
    client: httpx.Client,
    definition: dict[str, str],
    *,
    now: datetime,
) -> MarketPricingObservation:
    primary = _fetch_nasdaq_history(client, definition, now=now)
    if primary.status == "observed":
        return primary
    fallback = _fetch_yahoo_chart(client, definition, now=now)
    if fallback.status == "observed":
        return fallback
    return primary


def _fetch_nasdaq_history(
    client: httpx.Client,
    definition: dict[str, str],
    *,
    now: datetime,
) -> MarketPricingObservation:
    end = now.date()
    start = end - timedelta(days=MARKET_LOOKBACK_DAYS)
    url = NASDAQ_HISTORICAL_URL.format(symbol=definition["symbol"])
    try:
        response = client.get(
            url,
            params={
                "assetclass": definition["assetclass"],
                "fromdate": start.isoformat(),
                "todate": end.isoformat(),
                "limit": "10",
            },
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message=f"Nasdaq historical fetch failed: {exc.__class__.__name__}: {exc}",
        )
    rows = (
        payload.get("data", {})
        .get("tradesTable", {})
        .get("rows", [])
        if isinstance(payload, dict)
        else []
    )
    if not isinstance(rows, list):
        rows = []
    latest = _latest_nasdaq_row(rows, as_of=end)
    if latest is None:
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message="Nasdaq historical endpoint returned no parseable daily rows.",
        )
    observed_date, close, volume = latest
    return _priced_observation(
        definition,
        fetched_at=_iso_now(now),
        observed_date=observed_date,
        latest_close=close,
        source_name=definition["source_name"],
        source_url=definition["source_url"],
        now=now,
        headline=(
            f"{definition['name']} latest daily close was {close:.2f} "
            f"{definition['currency']} on {observed_date.isoformat()}."
        ),
        values={
            "close": close,
            "volume": volume,
            "lookback_days": MARKET_LOOKBACK_DAYS,
        },
        caveats=[
            "Nasdaq is a public web endpoint, not a contracted market-data API.",
            "The quote is for a US-listed ADR/ETF in USD, not local Colombian shares.",
        ],
        next_step=(
            "Use as market-pricing context only; compare with official Colombia "
            "sources before promoting a thesis."
        ),
    )


def _fetch_yahoo_chart(
    client: httpx.Client,
    definition: dict[str, str],
    *,
    now: datetime,
) -> MarketPricingObservation:
    symbol = definition["symbol"]
    url = YAHOO_CHART_URL.format(symbol=symbol)
    try:
        response = client.get(
            url,
            params={"range": "1mo", "interval": "1d"},
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message=f"Yahoo chart fallback failed: {exc.__class__.__name__}: {exc}",
        )

    latest = _latest_yahoo_row(payload, as_of=now.date())
    if latest is None:
        return _failed_observation(
            definition,
            fetched_at=_iso_now(now),
            message="Yahoo chart fallback returned no parseable daily rows.",
        )
    observed_date, close, volume = latest
    return _priced_observation(
        definition,
        fetched_at=_iso_now(now),
        observed_date=observed_date,
        latest_close=close,
        source_name="Yahoo Finance chart endpoint",
        source_url=f"https://finance.yahoo.com/quote/{symbol}",
        now=now,
        headline=(
            f"{definition['name']} latest Yahoo daily close was {close:.2f} "
            f"{definition['currency']} on {observed_date.isoformat()}."
        ),
        values={
            "close": close,
            "volume": volume,
            "lookback_days": MARKET_LOOKBACK_DAYS,
        },
        caveats=[
            "Yahoo Finance is an unofficial fallback endpoint and may rate-limit.",
            "The quote is for a US-listed ADR/ETF in USD, not local Colombian shares.",
        ],
        next_step=(
            "Treat as fallback context only; prefer Nasdaq when available and "
            "corroborate with official Colombia sources."
        ),
    )


def _latest_fred_row(
    rows: list[dict[str, str]],
    *,
    value_key: str,
    as_of: date,
) -> tuple[date, float] | None:
    parsed: list[tuple[date, float]] = []
    for row in rows:
        observed_date = _parse_iso_date(row.get("observation_date", ""))
        value = _to_float(row.get(value_key))
        if observed_date is None or value is None or observed_date > as_of:
            continue
        parsed.append((observed_date, value))
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])


def _latest_nasdaq_row(
    rows: list[Any],
    *,
    as_of: date,
) -> tuple[date, float, int | None] | None:
    parsed: list[tuple[date, float, int | None]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        observed_date = _parse_us_date(str(row.get("date") or ""))
        close = _to_float(row.get("close"))
        volume = _to_int(row.get("volume"))
        if observed_date is None or close is None or observed_date > as_of:
            continue
        parsed.append((observed_date, close, volume))
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])


def _latest_yahoo_row(
    payload: dict[str, Any],
    *,
    as_of: date,
) -> tuple[date, float, int | None] | None:
    chart = payload.get("chart") if isinstance(payload, dict) else None
    results = chart.get("result") if isinstance(chart, dict) else None
    if not isinstance(results, list) or not results:
        return None
    result = results[0]
    if not isinstance(result, dict):
        return None
    timestamps = result.get("timestamp")
    quote = (
        (result.get("indicators") or {}).get("quote") or []
        if isinstance(result.get("indicators"), dict)
        else []
    )
    if not isinstance(timestamps, list) or not quote or not isinstance(quote[0], dict):
        return None
    closes = quote[0].get("close") or []
    volumes = quote[0].get("volume") or []
    parsed: list[tuple[date, float, int | None]] = []
    for index, timestamp in enumerate(timestamps):
        if not isinstance(timestamp, int):
            continue
        observed_date = datetime.fromtimestamp(timestamp, timezone.utc).date()
        close = _to_float(closes[index] if index < len(closes) else None)
        volume = _to_int(volumes[index] if index < len(volumes) else None)
        if close is None or observed_date > as_of:
            continue
        parsed.append((observed_date, close, volume))
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])


def _priced_observation(
    definition: dict[str, str],
    *,
    fetched_at: str,
    observed_date: date,
    latest_close: float,
    source_name: str,
    source_url: str,
    now: datetime,
    headline: str,
    values: dict[str, Any],
    caveats: list[str],
    next_step: str,
) -> MarketPricingObservation:
    freshness = _freshness(observed_date, now.date())
    return MarketPricingObservation(
        market_id=definition["market_id"],
        name=definition["name"],
        category=definition["category"],
        symbol=definition["symbol"],
        instrument_type=definition["instrument_type"],
        status="observed" if freshness == "current" else "stale",
        source_name=source_name,
        source_url=source_url,
        fetched_at=fetched_at,
        observed_date=observed_date.isoformat(),
        latest_close=latest_close,
        currency=definition["currency"],
        headline=headline,
        values=values,
        freshness_status=freshness,
        caveats=caveats,
        next_step=next_step,
    )


def _failed_observation(
    definition: dict[str, str],
    *,
    fetched_at: str,
    message: str,
) -> MarketPricingObservation:
    return MarketPricingObservation(
        market_id=definition["market_id"],
        name=definition["name"],
        category=definition["category"],
        symbol=definition["symbol"],
        instrument_type=definition["instrument_type"],
        status="failed",
        source_name=definition["source_name"],
        source_url=definition["source_url"],
        fetched_at=fetched_at,
        currency=definition["currency"],
        headline=message,
        freshness_status="failed",
        caveats=[
            "Market-pricing source failed closed; do not treat silence as no move.",
        ],
        next_step="Retry source and corroborate before using this market context.",
    )


def _observation_to_review_context(
    item: MarketPricingObservation,
) -> dict[str, Any]:
    return {
        "schema_version": item.schema_version,
        "market_id": item.market_id,
        "name": item.name,
        "category": item.category,
        "symbol": item.symbol,
        "instrument_type": item.instrument_type,
        "status": item.status,
        "source_name": item.source_name,
        "source_url": item.source_url,
        "observed_date": item.observed_date,
        "latest_close": item.latest_close,
        "currency": item.currency,
        "headline": item.headline,
        "freshness_status": item.freshness_status,
        "caveats": list(item.caveats),
        "next_step": item.next_step,
    }


def _render_observation(item: MarketPricingObservation) -> list[str]:
    close = (
        f"{item.latest_close:.2f} {item.currency}"
        if isinstance(item.latest_close, float)
        else "(no close)"
    )
    lines = [
        (
            f"- `{item.market_id}` ({item.status}, {item.freshness_status}): "
            f"{item.name} / {item.symbol} - {close}; "
            f"date={item.observed_date or 'n/a'}; source={item.source_name}."
        ),
        f"  - Headline: {item.headline}",
    ]
    if item.caveats:
        lines.append(f"  - Caveats: {'; '.join(item.caveats)}")
    if item.next_step:
        lines.append(f"  - Next check: {item.next_step}")
    return lines


def _freshness(observed_date: date, as_of: date) -> str:
    if observed_date > as_of:
        return "future"
    if (as_of - observed_date).days <= MAX_FRESHNESS_DAYS:
        return "current"
    return "stale"


def _parse_iso_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_us_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _iso_now(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
