from __future__ import annotations

from datetime import datetime, timezone

import httpx

from colombia_forecasting_desk.market_pricing import (
    MARKET_DEFINITIONS,
    attach_market_pricing_watch,
    fetch_market_pricing_observations,
    render_market_pricing_watch,
    _fetch_nasdaq_with_yahoo_fallback,
)


def test_fetch_market_pricing_observations_parses_fred_and_nasdaq() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "fred.stlouisfed.org":
            return httpx.Response(
                200,
                text=(
                    "observation_date,DCOILBRENTEU\n"
                    "2026-05-18,116.73\n"
                    "2026-05-19,\n"
                ),
            )
        if request.url.host == "api.nasdaq.com":
            symbol = request.url.path.split("/")[3]
            close = {"EC": "$13.85", "CIB": "$65.88", "COLO": "37.29"}[symbol]
            return httpx.Response(
                200,
                json={
                    "data": {
                        "tradesTable": {
                            "rows": [
                                {
                                    "date": "05/22/2026",
                                    "close": close,
                                    "volume": "1,234",
                                }
                            ]
                        }
                    }
                },
            )
        raise AssertionError(f"unexpected URL {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    observations = fetch_market_pricing_observations(
        now=datetime(2026, 5, 26, 23, 59, tzinfo=timezone.utc),
        client=client,
    )

    by_id = {item.market_id: item for item in observations}
    assert by_id["brent_spot_fred"].status == "stale"
    assert by_id["brent_spot_fred"].latest_close == 116.73
    assert by_id["ec_adr_nasdaq"].status == "observed"
    assert by_id["ec_adr_nasdaq"].latest_close == 13.85
    assert by_id["cib_adr_nasdaq"].latest_close == 65.88
    assert by_id["colo_etf_nasdaq"].instrument_type == "etf"


def test_nasdaq_failure_can_fall_back_to_yahoo_chart() -> None:
    definition = next(
        item for item in MARKET_DEFINITIONS if item["market_id"] == "ec_adr_nasdaq"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.nasdaq.com":
            return httpx.Response(503, json={"error": "temporarily unavailable"})
        if request.url.host == "query2.finance.yahoo.com":
            return httpx.Response(
                200,
                json={
                    "chart": {
                        "result": [
                            {
                                "timestamp": [1779456600],
                                "indicators": {
                                    "quote": [
                                        {
                                            "close": [14.86],
                                            "volume": [6169155],
                                        }
                                    ]
                                },
                            }
                        ],
                        "error": None,
                    }
                },
            )
        raise AssertionError(f"unexpected URL {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    observation = _fetch_nasdaq_with_yahoo_fallback(
        client,
        definition,
        now=datetime(2026, 5, 26, 23, 59, tzinfo=timezone.utc),
    )

    assert observation.status == "observed"
    assert observation.source_name == "Yahoo Finance chart endpoint"
    assert observation.latest_close == 14.86
    assert "fallback" in observation.next_step.lower()


def test_attach_and_render_market_pricing_watch_contract() -> None:
    observation = fetch_market_pricing_observations(
        now=datetime(2026, 5, 26, 23, 59, tzinfo=timezone.utc),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    text=(
                        "observation_date,DCOILBRENTEU\n"
                        "2026-05-26,113.96\n"
                    ),
                ),
            )
        ),
    )[0]

    packet = attach_market_pricing_watch({"summary": {}, "inputs": {}}, [observation])

    assert packet["summary"]["market_pricing_observation_count"] == 1
    assert packet["summary"]["market_pricing_observed_count"] == 1
    assert packet["inputs"]["market_pricing_watch_artifact"] == (
        "market_pricing_watch.json"
    )
    assert "not investment advice" in packet["policy"]["market_pricing_watch_policy"]

    rendered = render_market_pricing_watch([observation], run_date="2026-05-26")
    assert "Market Pricing Watch - 2026-05-26" in rendered
    assert "not investment advice" in rendered
    assert "brent_spot_fred" in rendered
