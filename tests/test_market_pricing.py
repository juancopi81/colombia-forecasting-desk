from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

import colombia_forecasting_desk.market_pricing as market_pricing
from colombia_forecasting_desk.market_pricing import (
    MARKET_DEFINITIONS,
    _fetch_fred_brent,
    _fetch_market_definition,
    _fetch_nasdaq_history,
    _fetch_nasdaq_with_yahoo_fallback,
    _fetch_yahoo_chart,
    attach_market_pricing_watch,
    fetch_market_pricing_observations,
    render_market_pricing_watch,
)


def test_fetch_market_pricing_observations_parses_fred_and_nasdaq(
    monkeypatch,
) -> None:
    receipts = [f"2026-10-01T15:35:0{index}Z" for index in range(4)]
    receipt_clock = iter(receipts)
    monkeypatch.setattr(market_pricing, "_now_iso", lambda: next(receipt_clock))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "fred.stlouisfed.org":
            assert request.url.params["cosd"] == "2026-04-26"
            assert request.url.params["coed"] == "2026-05-26"
            return httpx.Response(
                200,
                text=(
                    "observation_date,DCOILBRENTEU\n"
                    "2026-05-18,116.73\n"
                    "2026-05-19,\n"
                    "2026-05-27,999.00\n"
                ),
            )
        if request.url.host == "api.nasdaq.com":
            assert request.url.params["fromdate"] == "2026-04-26"
            assert request.url.params["todate"] == "2026-05-26"
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
                                },
                                {
                                    "date": "05/27/2026",
                                    "close": "$999.00",
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
    assert [item.fetched_at for item in observations] == receipts
    assert by_id["brent_spot_fred"].status == "stale"
    assert by_id["brent_spot_fred"].freshness_status == "stale"
    assert by_id["brent_spot_fred"].observed_date == "2026-05-18"
    assert by_id["brent_spot_fred"].latest_close == 116.73
    assert by_id["ec_adr_nasdaq"].status == "observed"
    assert by_id["ec_adr_nasdaq"].freshness_status == "current"
    assert by_id["ec_adr_nasdaq"].observed_date == "2026-05-22"
    assert by_id["ec_adr_nasdaq"].latest_close == 13.85
    assert by_id["cib_adr_nasdaq"].latest_close == 65.88
    assert by_id["colo_etf_nasdaq"].instrument_type == "etf"


def test_nasdaq_failure_can_fall_back_to_yahoo_chart(monkeypatch) -> None:
    definition = next(
        item for item in MARKET_DEFINITIONS if item["market_id"] == "ec_adr_nasdaq"
    )
    receipts = iter(["2026-10-01T15:35:00Z", "2026-10-01T15:35:01Z"])
    monkeypatch.setattr(market_pricing, "_now_iso", lambda: next(receipts))

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
                                "meta": {"exchangeTimezoneName": "America/New_York"},
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
    assert observation.fetched_at == "2026-10-01T15:35:01Z"
    assert observation.source_name == "Yahoo Finance chart endpoint"
    assert observation.latest_close == 14.86
    assert "fallback" in observation.next_step.lower()


def _epoch(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


def _yahoo_session_payload(*, prior_bar: bool = True) -> dict:
    return {
        "chart": {
            "result": [{
                "meta": {
                    "exchangeTimezoneName": "America/New_York",
                    "regularMarketTime": _epoch("2026-10-02T15:36:08Z"),
                    "currentTradingPeriod": {"regular": {
                        "start": _epoch("2026-10-02T13:30:00Z"),
                        "end": _epoch("2026-10-02T20:00:00Z"),
                    }},
                },
                "timestamp": (
                    [_epoch("2026-10-01T13:30:00Z")] if prior_bar else []
                ) + [_epoch("2026-10-02T13:30:00Z")],
                "indicators": {"quote": [{
                    "close": ([100.0] if prior_bar else []) + [91.07],
                    "volume": ([1] if prior_bar else []) + [64575],
                }]},
            }],
        },
    }


def _fetch_yahoo_fixture(
    monkeypatch, payload, *, received="2026-10-02T15:36:08Z",
    evaluation="2026-10-02T23:59:59Z",
):
    monkeypatch.setattr(market_pricing, "_now_iso", lambda: received)
    with httpx.Client(transport=httpx.MockTransport(
        lambda _: httpx.Response(200, json=payload)
    )) as client:
        return _fetch_yahoo_chart(
            client, MARKET_DEFINITIONS[2],
            now=datetime.fromisoformat(evaluation),
        )


@pytest.mark.parametrize("prior_bar", [False, True])
def test_yahoo_unfinished_session_never_uses_run_date_end_as_close(
    monkeypatch, prior_bar,
) -> None:
    observation = _fetch_yahoo_fixture(
        monkeypatch, _yahoo_session_payload(prior_bar=prior_bar)
    )
    assert observation.source_name == "Yahoo Finance chart endpoint"
    assert observation.fetched_at == "2026-10-02T15:36:08Z"
    assert observation.status == ("observed" if prior_bar else "failed")
    assert observation.observed_date == ("2026-10-01" if prior_bar else "")
    assert observation.latest_close == (100.0 if prior_bar else None)
    assert "unconfirmed completion" in (
        observation.headline + " ".join(observation.caveats)
    )


@pytest.mark.parametrize(
    ("start", "end", "received", "exchange_tz", "observed_date"),
    [
        ("2026-10-02T13:30:00Z", "2026-10-02T20:00:00Z",
         "2026-10-02T20:01:00Z", "America/New_York", "2026-10-02"),
        ("2026-11-27T14:30:00Z", "2026-11-27T18:00:00Z",
         "2026-11-27T18:01:00Z", "America/New_York", "2026-11-27"),
        ("2026-11-03T14:30:00Z", "2026-11-03T21:00:00Z",
         "2026-11-03T21:01:00Z", "America/New_York", "2026-11-03"),
        ("2026-10-01T20:00:00Z", "2026-10-02T03:00:00Z",
         "2026-10-02T04:00:00Z", "Pacific/Auckland", "2026-10-02"),
    ],
    ids=["regular-close", "provider-early-close", "winter-offset", "exchange-date"],
)
def test_yahoo_completed_session_uses_provider_hours_and_exchange_date(
    monkeypatch, start, end, received, exchange_tz, observed_date,
) -> None:
    payload = _yahoo_session_payload(prior_bar=False)
    result = payload["chart"]["result"][0]
    result["timestamp"] = [_epoch(start)]
    result["meta"] = {
        "exchangeTimezoneName": exchange_tz,
        "regularMarketTime": _epoch(end),
        "currentTradingPeriod": {"regular": {"start": _epoch(start), "end": _epoch(end)}},
    }
    observation = _fetch_yahoo_fixture(
        monkeypatch, payload, received=received,
        evaluation=observed_date + "T23:59:59Z",
    )
    assert observation.status == "observed"
    assert observation.observed_date == observed_date
    assert observation.latest_close == 91.07


@pytest.mark.parametrize("metadata_problem", [
    "missing-session", "invalid-session", "different-session-date",
    "missing-source-time", "source-time-before-close", "source-time-in-future",
])
def test_yahoo_unconfirmed_metadata_prefers_prior_bar(monkeypatch, metadata_problem) -> None:
    payload = _yahoo_session_payload()
    meta = payload["chart"]["result"][0]["meta"]
    if metadata_problem == "missing-session":
        meta.pop("currentTradingPeriod")
    elif metadata_problem == "invalid-session":
        meta["currentTradingPeriod"]["regular"]["end"] = "invalid"
    elif metadata_problem == "different-session-date":
        meta["currentTradingPeriod"]["regular"] = {
            "start": _epoch("2026-10-01T13:30:00Z"),
            "end": _epoch("2026-10-01T20:00:00Z"),
        }
    elif metadata_problem == "missing-source-time":
        meta.pop("regularMarketTime")
    elif metadata_problem == "source-time-in-future":
        meta["regularMarketTime"] = _epoch("2026-10-02T22:00:00Z")
    observation = _fetch_yahoo_fixture(
        monkeypatch, payload, received="2026-10-02T21:00:00Z"
    )
    assert observation.observed_date == "2026-10-01"
    assert observation.latest_close == 100.0
    assert any("unconfirmed completion" in item for item in observation.caveats)


@pytest.mark.parametrize("exchange_tz", [None, "", "invalid/timezone"])
def test_yahoo_unknown_exchange_timezone_fails_closed(monkeypatch, exchange_tz) -> None:
    payload = _yahoo_session_payload()
    payload["chart"]["result"][0]["meta"]["exchangeTimezoneName"] = exchange_tz
    observation = _fetch_yahoo_fixture(monkeypatch, payload)
    assert observation.status == "failed"
    assert observation.latest_close is None
    assert "exchange timezone unavailable" in observation.headline


@pytest.mark.parametrize("received", ["", "2026-10-02T21:00:00"])
def test_yahoo_unknown_receipt_time_fails_closed(monkeypatch, received) -> None:
    observation = _fetch_yahoo_fixture(
        monkeypatch, _yahoo_session_payload(), received=received,
    )
    assert observation.status == "failed"
    assert observation.latest_close is None
    assert "receipt" in observation.headline


def test_yahoo_cached_unfinished_bar_is_not_completed_by_later_receipt(monkeypatch) -> None:
    observation = _fetch_yahoo_fixture(
        monkeypatch, _yahoo_session_payload(), received="2026-10-03T15:36:08Z",
        evaluation="2026-10-03T23:59:59Z",
    )
    assert observation.observed_date == "2026-10-01"
    assert observation.latest_close == 100.0


def test_yahoo_historical_evaluation_still_filters_completed_future_dates(monkeypatch) -> None:
    payload = _yahoo_session_payload()
    payload["chart"]["result"][0]["meta"]["regularMarketTime"] = _epoch("2026-10-02T20:00:00Z")
    observation = _fetch_yahoo_fixture(
        monkeypatch, payload, received="2026-10-02T21:00:00Z",
        evaluation="2026-10-01T23:59:59Z",
    )
    assert observation.observed_date == "2026-10-01"
    assert observation.fetched_at == "2026-10-02T21:00:00Z"
    assert observation.latest_close == 100.0


def test_yahoo_unconfirmed_fallback_keeps_stale_primary_with_caveat(monkeypatch) -> None:
    monkeypatch.setattr(market_pricing, "_now_iso", lambda: "2026-10-02T15:36:08Z")
    def handler(request):
        if request.url.host == "api.nasdaq.com":
            return httpx.Response(200, json={"data": {"tradesTable": {"rows": [
                {"date": "09/01/2026", "close": "$100", "volume": "1"},
            ]}}})
        return httpx.Response(200, json=_yahoo_session_payload(prior_bar=False))
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        observation = _fetch_nasdaq_with_yahoo_fallback(
            client, MARKET_DEFINITIONS[2],
            now=datetime.fromisoformat("2026-10-02T23:59:59Z"),
        )
    assert observation.status == "stale"
    assert observation.observed_date == "2026-09-01"
    assert observation.latest_close == 100.0
    assert observation.source_name == "Nasdaq public historical endpoint"
    assert any("unconfirmed completion" in item for item in observation.caveats)


def test_market_response_receipt_defaults_to_wall_clock() -> None:
    started = datetime.now(timezone.utc).replace(microsecond=0)
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                text="observation_date,DCOILBRENTEU\n2026-05-26,113.96\n",
            )
        )
    ) as client:
        observation = _fetch_fred_brent(
            client,
            MARKET_DEFINITIONS[0],
            now=datetime(2026, 5, 26, 23, 59, 59, tzinfo=timezone.utc),
        )
    finished = datetime.now(timezone.utc)

    assert started <= datetime.fromisoformat(observation.fetched_at) <= finished
    assert observation.observed_date == "2026-05-26"
    assert observation.status == "observed"
    assert observation.freshness_status == "current"


@pytest.mark.parametrize(
    ("fetcher", "definition"),
    [
        (_fetch_fred_brent, MARKET_DEFINITIONS[0]),
        (_fetch_nasdaq_history, MARKET_DEFINITIONS[1]),
        (_fetch_yahoo_chart, MARKET_DEFINITIONS[1]),
    ],
    ids=["fred", "nasdaq", "yahoo"],
)
@pytest.mark.parametrize("failure", ["transport", "http", "unparseable", "empty"])
def test_failed_market_fetch_only_stamps_received_responses(
    monkeypatch, fetcher, definition, failure,
) -> None:
    clock_calls = []
    response_returned = False

    def receipt_clock() -> str:
        assert response_returned
        clock_calls.append(True)
        return "2026-10-01T15:35:00Z"

    monkeypatch.setattr(market_pricing, "_now_iso", receipt_clock)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal response_returned
        if failure == "transport":
            raise httpx.ConnectError("offline", request=request)
        response_returned = True
        if failure == "http":
            return httpx.Response(503, text="unavailable")
        if failure == "unparseable":
            return httpx.Response(200, text="invalid source data")
        return httpx.Response(200, json={})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        observation = fetcher(
            client,
            definition,
            now=datetime(2026, 5, 26, 23, 59, 59, tzinfo=timezone.utc),
        )

    assert observation.status == "failed"
    assert observation.latest_close is None
    assert observation.observed_date == ""
    if failure == "transport":
        assert observation.fetched_at == ""
        assert clock_calls == []
    else:
        assert observation.fetched_at == "2026-10-01T15:35:00Z"
        assert clock_calls == [True]


def test_market_final_guard_does_not_invent_receipt_time(monkeypatch) -> None:
    def unexpected_failure(*args, **kwargs):
        raise RuntimeError("failed before request")

    monkeypatch.setattr(market_pricing, "_fetch_fred_brent", unexpected_failure)
    monkeypatch.setattr(market_pricing, "_now_iso", unexpected_failure)
    with httpx.Client(transport=httpx.MockTransport(unexpected_failure)) as client:
        observation = _fetch_market_definition(
            client,
            MARKET_DEFINITIONS[0],
            now=datetime(2026, 5, 26, 23, 59, 59, tzinfo=timezone.utc),
        )

    assert observation.status == "failed"
    assert observation.fetched_at == ""
    assert "RuntimeError" in observation.headline


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
