from __future__ import annotations

from colombia_forecasting_desk.cooccurrence_bundles import (
    attach_cooccurrence_bundles,
    build_cooccurrence_bundles,
    render_cooccurrence_bundles,
)
from colombia_forecasting_desk.models import IndicatorObservation, MarketPricingObservation


def _indicator(indicator_id: str, headline: str) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id=indicator_id,
        name=indicator_id.replace("_", " ").title(),
        category="test",
        status="observed",
        frequency="daily",
        source_name="Official Source",
        source_url=f"https://example.com/{indicator_id}",
        period="2026-05",
        release_date="2026-05-25T00:00:00Z",
        headline=headline,
        values={},
        freshness_status="current",
        components=[],
        why_it_matters="Test indicator.",
        correlations=[],
        next_step="Review.",
    )


def _card(card_id: str, family: str, title: str) -> dict:
    return {
        "schema_version": "indicator_tension_cards.v1",
        "card_id": card_id,
        "family": family,
        "title": title,
        "severity": "review",
        "trigger": f"{title} triggered.",
        "evidence": [],
        "caveats": [],
        "source_refs": [],
    }


def _market(market_id: str, status: str = "observed") -> MarketPricingObservation:
    return MarketPricingObservation(
        market_id=market_id,
        name=market_id.replace("_", " ").title(),
        category="test",
        symbol=market_id.upper(),
        instrument_type="test",
        status=status,
        source_name="Market Source",
        source_url=f"https://example.com/{market_id}",
        fetched_at="2026-05-26T23:59:00Z",
        observed_date="2026-05-26",
        latest_close=10.0,
        currency="USD",
        headline=f"{market_id} latest daily close was 10.0.",
        freshness_status="current" if status == "observed" else status,
        caveats=[],
        next_step="Review only.",
    )


def test_builds_fiscal_sovereign_bundle_from_tensions_and_pgn_review_item() -> None:
    indicators = [
        _indicator("fiscal_tax_pulse", "Fiscal pulse has tax, TES auction, and curve data.")
    ]
    cards = [
        _card("real_tax_revenue_squeeze", "fiscal_capacity", "Real tax revenue squeeze"),
        _card("tes_policy_spread", "sovereign_funding", "TES-policy spread tension"),
    ]
    packet = {
        "review_items": [
            {
                "origin_id": "bill:pgn-2026",
                "title": "Proyecto de Ley PGN 2026 addition",
                "question_seed": "Will the PGN 2026 addition advance?",
                "source_excerpts": [
                    {
                        "url": "https://example.com/pgn",
                    }
                ],
            }
        ]
    }

    bundles = build_cooccurrence_bundles(indicators, cards, packet)

    fiscal = next(
        bundle
        for bundle in bundles
        if bundle["bundle_id"] == "fiscal_sovereign_funding"
    )
    assert fiscal["disposition"] == "review_context_only"
    assert fiscal["review_context"]["neutral_bundle"] is True
    assert fiscal["review_context"]["cross_bundle_review_required"] is True
    assert any(item["input_id"] == "bill:pgn-2026" for item in fiscal["inputs"])
    assert any("Co-occurrence is not causality" in item for item in fiscal["guardrails"])

    rendered = render_cooccurrence_bundles(bundles, run_date="2026-05-25")
    assert "Fiscal / sovereign funding bundle" in rendered
    assert "not conclusions, thesis labels" in rendered


def test_energy_bundle_requires_a_review_item_not_only_energy_observation() -> None:
    indicators = [
        _indicator("energy_system", "XM energy system observed."),
        _indicator("ipc_inflation", "DANE IPC observed."),
    ]

    assert build_cooccurrence_bundles(indicators, [], {"review_items": []}) == []

    bundles = build_cooccurrence_bundles(
        indicators,
        [],
        {
            "review_items": [
                {
                    "origin_id": "bill:glp-san-andres",
                    "title": "GLP transport subsidy for San Andres",
                    "question_seed": "Will the GLP subsidy bill advance?",
                }
            ]
        },
    )

    energy = next(
        bundle for bundle in bundles if bundle["bundle_id"] == "energy_tariff_subsidy"
    )
    assert any(item["kind"] == "review_item" for item in energy["inputs"])
    assert "review_context_only" == energy["disposition"]


def test_short_terms_do_not_match_inside_unrelated_words() -> None:
    indicators = [
        _indicator("fiscal_tax_pulse", "Fiscal pulse has tax, TES auction, and curve data.")
    ]
    cards = [
        _card("real_tax_revenue_squeeze", "fiscal_capacity", "Real tax revenue squeeze")
    ]
    packet = {
        "review_items": [
            {
                "origin_id": "ise_activity",
                "title": "Will the next DANE ISE release show latest activity?",
                "question_seed": "Will the latest ISE release explain contribution details?",
                "topics": ["activity"],
            }
        ]
    }

    bundles = build_cooccurrence_bundles(indicators, cards, packet)
    fiscal = next(
        bundle
        for bundle in bundles
        if bundle["bundle_id"] == "fiscal_sovereign_funding"
    )

    assert all(item["input_id"] != "ise_activity" for item in fiscal["inputs"])


def test_attach_cooccurrence_bundles_updates_m2_packet_contract() -> None:
    bundle = {
        "schema_version": "cooccurrence_bundles.v1",
        "bundle_id": "construction_housing_cost",
        "title": "Construction / housing cost bundle",
        "disposition": "review_context_only",
        "input_count": 2,
        "inputs": [],
    }

    packet = attach_cooccurrence_bundles({"summary": {}, "inputs": {}}, [bundle])

    assert packet["summary"]["cooccurrence_bundle_count"] == 1
    assert packet["inputs"]["cooccurrence_bundles_artifact"] == (
        "cooccurrence_bundles.json"
    )
    assert "not thesis labels" in packet["policy"]["cooccurrence_bundle_policy"]


def test_market_pricing_bundle_uses_observed_market_rows_only() -> None:
    bundles = build_cooccurrence_bundles(
        [],
        [],
        {"review_items": []},
        [
            _market("brent_spot_fred"),
            _market("ec_adr_nasdaq"),
            _market("cib_adr_nasdaq", status="failed"),
        ],
    )

    market = next(
        bundle for bundle in bundles if bundle["bundle_id"] == "colombia_market_pricing"
    )

    assert market["disposition"] == "review_context_only"
    assert any(item["kind"] == "market_pricing" for item in market["inputs"])
    assert any(
        item["input_id"] == "brent_spot_fred"
        for item in market["inputs"]
    )
    assert all(
        item["input_id"] != "cib_adr_nasdaq"
        for item in market["inputs"]
    )
    assert "brent_spot_fred" in market["review_context"]["market_ids"]
