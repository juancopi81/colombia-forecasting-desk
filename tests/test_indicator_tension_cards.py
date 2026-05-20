from __future__ import annotations

from colombia_forecasting_desk.indicator_tension_cards import (
    build_indicator_tension_cards,
    render_indicator_tension_cards,
)
from colombia_forecasting_desk.models import IndicatorComponent, IndicatorObservation


def _component(
    component_id: str,
    values: dict,
    *,
    name: str = "Component",
    source_name: str = "Official Source",
    source_url: str = "https://example.com/component",
    period: str = "2026-05",
    status: str = "observed",
) -> IndicatorComponent:
    return IndicatorComponent(
        component_id=component_id,
        name=name,
        status=status,
        source_name=source_name,
        source_url=source_url,
        period=period,
        release_date="2026-05-20T00:00:00Z",
        headline=f"{name} headline.",
        values=values,
        freshness_status="current",
    )


def _observation(
    indicator_id: str,
    values: dict,
    *,
    name: str | None = None,
    category: str = "macro",
    components: list[IndicatorComponent] | None = None,
    source_name: str = "Official Source",
    source_url: str = "https://example.com/indicator",
    period: str = "2026-05",
    status: str = "observed",
) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id=indicator_id,
        name=name or indicator_id,
        category=category,
        status=status,
        frequency="monthly",
        source_name=source_name,
        source_url=source_url,
        period=period,
        release_date="2026-05-20T00:00:00Z",
        headline=f"{indicator_id} headline.",
        values=values,
        freshness_status="current",
        components=components or [],
        why_it_matters="Test indicator.",
        correlations=[],
        next_step="Review.",
    )


def test_indicator_tension_cards_trigger_on_observed_macro_tensions() -> None:
    policy = _observation(
        "policy_rate_ibr",
        {"policy_rate_pct": 11.25},
        name="Policy rate + IBR",
        source_name="Banco de la Republica",
    )
    fiscal = _observation(
        "fiscal_tax_pulse",
        {},
        name="Fiscal / tax pulse",
        category="fiscal",
        components=[
            _component(
                "tax_collection",
                {
                    "gross_tax_revenue_annual_variation_pct": 1.44,
                    "gross_tax_revenue_cop_millions": 24500000,
                },
                name="DIAN tax collection",
                source_name="DIAN",
            ),
            _component(
                "tes_auction",
                {
                    "max_cutoff_rate_pct": 14.79,
                    "total_issued_cop_billions": 6.0,
                    "bid_to_cover": 4.1,
                    "maturity_years": [2030, 2035, 2040, 2058],
                },
                name="MinHacienda TES auction",
                source_name="MinHacienda",
            ),
            _component(
                "banrep_tes_curve",
                {
                    "banrep_tes_5y_zero_coupon_pct": 14.48,
                    "banrep_tes_10y_zero_coupon_pct": 14.39,
                },
                name="BanRep TES zero-coupon curve",
                source_name="Banco de la Republica",
            ),
        ],
    )
    construction = _observation(
        "construction_bundle",
        {},
        category="construction",
        components=[
            _component(
                "icoced",
                {"icoced_total_annual_variation_pct": 6.33},
                name="ICOCED costs",
                source_name="DANE",
            )
        ],
    )
    ipc = _observation(
        "ipc_inflation",
        {"annual_variation_pct": 5.68},
        name="IPC / inflation",
        category="prices",
        source_name="DANE",
    )

    cards = build_indicator_tension_cards([policy, fiscal, construction, ipc])

    assert [card["card_id"] for card in cards] == [
        "tes_policy_spread",
        "real_policy_rate",
        "real_tax_revenue_squeeze",
        "tes_auction_high_funding_cost",
        "construction_cost_vs_ipc",
    ]
    assert [card["family"] for card in cards] == [
        "sovereign_funding",
        "monetary_stance",
        "fiscal_capacity",
        "sovereign_funding",
        "construction_cost_pressure",
    ]
    assert cards[0]["calculations"]["spreads_pp"]["5y"] == 3.23
    assert cards[1]["calculations"]["ex_post_real_policy_rate_pp"] == 5.57
    assert cards[2]["calculations"]["approx_real_tax_revenue_growth_pp"] == -4.24
    assert cards[3]["calculations"]["max_cutoff_rate_pct"] == 14.79
    assert cards[4]["calculations"]["spread_pp"] == 0.65
    assert all("Advisory screen only" in card["review_policy"] for card in cards)

    rendered = render_indicator_tension_cards(cards, run_date="2026-05-20")
    assert "TES-policy spread tension" in rendered
    assert "High ex-post real policy rate" in rendered
    assert "Real tax revenue squeeze" in rendered
    assert "TES auction high funding cost" in rendered
    assert "Construction cost vs IPC squeeze" in rendered
    assert "Family: `monetary_stance`" in rendered
    assert "not conclusions" in rendered


def test_indicator_tension_cards_stay_silent_below_thresholds() -> None:
    policy = _observation("policy_rate_ibr", {"policy_rate_pct": 11.25})
    fiscal = _observation(
        "fiscal_tax_pulse",
        {},
        components=[
            _component(
                "tes_auction",
                {"max_cutoff_rate_pct": 13.5, "bid_to_cover": 3.0},
            ),
            _component(
                "banrep_tes_curve",
                {
                    "banrep_tes_5y_zero_coupon_pct": 13.0,
                    "banrep_tes_10y_zero_coupon_pct": 13.2,
                },
            ),
        ],
    )
    construction = _observation(
        "construction_bundle",
        {},
        components=[
            _component("icoced", {"icoced_total_annual_variation_pct": 5.8})
        ],
    )
    ipc = _observation("ipc_inflation", {"annual_variation_pct": 8.0})

    cards = build_indicator_tension_cards([policy, fiscal, construction, ipc])

    assert cards == []
    assert (
        render_indicator_tension_cards(cards, run_date="2026-05-20")
        == "# Indicator Tension Cards - 2026-05-20\n\n"
        "Advisory screens that surface official indicator contrasts for "
        "human/LLM review. These are prompts to inspect, not conclusions.\n\n"
        "No indicator tension cards triggered.\n"
    )


def test_real_tax_revenue_squeeze_fails_closed_without_observed_tax_data() -> None:
    fiscal = _observation(
        "fiscal_tax_pulse",
        {},
        components=[
            _component(
                "tax_collection",
                {},
                name="DIAN tax collection",
                source_name="DIAN",
                status="pending_source",
            )
        ],
    )
    ipc = _observation("ipc_inflation", {"annual_variation_pct": 5.68})

    assert build_indicator_tension_cards([fiscal, ipc]) == []


def test_indicator_tension_cards_require_observed_components() -> None:
    policy = _observation("policy_rate_ibr", {"policy_rate_pct": 11.25})
    fiscal = _observation(
        "fiscal_tax_pulse",
        {},
        components=[
            _component(
                "banrep_tes_curve",
                {"banrep_tes_5y_zero_coupon_pct": 14.8},
                status="failed",
            )
        ],
    )

    assert build_indicator_tension_cards([policy, fiscal]) == []
