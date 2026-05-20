from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable

from .models import IndicatorComponent, IndicatorObservation

SCHEMA_VERSION = "indicator_tension_cards.v1"
MAX_TENSION_CARDS = 5
TES_POLICY_SPREAD_THRESHOLD_PP = 3.0
TES_AUCTION_CUTOFF_THRESHOLD_PCT = 14.0
REAL_POLICY_RATE_THRESHOLD_PP = 4.0
REAL_TAX_REVENUE_SQUEEZE_THRESHOLD_PP = 0.0
ICOCED_IPC_SPREAD_THRESHOLD_PP = 0.5

TensionRule = Callable[[dict[str, IndicatorObservation]], dict[str, Any] | None]


def build_indicator_tension_cards(
    indicator_watch: list[IndicatorObservation],
) -> list[dict[str, Any]]:
    """Build advisory cross-indicator review cards from observed indicators."""
    by_id = {indicator.indicator_id: indicator for indicator in indicator_watch}
    cards: list[dict[str, Any]] = []
    for rule in TENSION_RULES:
        card = rule(by_id)
        if card is not None:
            cards.append(card)
        if len(cards) >= MAX_TENSION_CARDS:
            break
    return cards


def render_indicator_tension_cards(
    cards: list[dict[str, Any]],
    *,
    run_date: str,
) -> str:
    lines = [
        f"# Indicator Tension Cards - {run_date}",
        "",
        (
            "Advisory screens that surface official indicator contrasts for "
            "human/LLM review. These are prompts to inspect, not conclusions."
        ),
        "",
    ]
    if not cards:
        lines.append("No indicator tension cards triggered.")
        return "\n".join(lines).rstrip() + "\n"

    for index, card in enumerate(cards, 1):
        lines.extend(
            [
                f"## {index}. {card.get('title', card.get('card_id', 'Card'))}",
                "",
                f"- Card id: `{card.get('card_id', '')}`",
                f"- Family: `{card.get('family', '')}`",
                f"- Severity: `{card.get('severity', 'review')}`",
                f"- Trigger: {card.get('trigger', '')}",
                f"- Why it matters: {card.get('why_it_matters', '')}",
                f"- Agent prompt: {card.get('agent_prompt', '')}",
                "",
            ]
        )
        evidence = [
            item for item in card.get("evidence") or [] if isinstance(item, dict)
        ]
        if evidence:
            lines.extend(["Evidence:", ""])
            for item in evidence:
                lines.append(
                    f"- {item.get('label', '')}: {item.get('value', '')} "
                    f"({item.get('source', '')})"
                )
            lines.append("")

        caveats = list(card.get("caveats") or [])
        if caveats:
            lines.extend(["Caveats:", ""])
            lines.extend(f"- {caveat}" for caveat in caveats)
            lines.append("")

        questions = list(card.get("suggested_questions") or [])
        if questions:
            lines.extend(["Suggested questions:", ""])
            lines.extend(f"- {question}" for question in questions)
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def tes_policy_spread_rule(
    by_id: dict[str, IndicatorObservation],
) -> dict[str, Any] | None:
    policy = by_id.get("policy_rate_ibr")
    fiscal = by_id.get("fiscal_tax_pulse")
    if policy is None or fiscal is None:
        return None

    policy_rate = _number(policy.values.get("policy_rate_pct"))
    tes_curve = _component(fiscal, "banrep_tes_curve")
    if policy_rate is None or tes_curve is None:
        return None

    tes_5y = _number(tes_curve.values.get("banrep_tes_5y_zero_coupon_pct"))
    tes_10y = _number(tes_curve.values.get("banrep_tes_10y_zero_coupon_pct"))
    spreads = {
        "5y": round(tes_5y - policy_rate, 3) if tes_5y is not None else None,
        "10y": round(tes_10y - policy_rate, 3) if tes_10y is not None else None,
    }
    observed_spreads = {
        tenor: spread for tenor, spread in spreads.items() if spread is not None
    }
    if not observed_spreads:
        return None

    trigger_tenor, trigger_spread = max(
        observed_spreads.items(),
        key=lambda item: item[1],
    )
    if trigger_spread < TES_POLICY_SPREAD_THRESHOLD_PP:
        return None

    evidence = [
        _evidence("BanRep policy rate", _pct(policy_rate), policy),
    ]
    if tes_5y is not None:
        evidence.append(_evidence("TES 5y zero-coupon", _pct(tes_5y), tes_curve))
    if tes_10y is not None:
        evidence.append(_evidence("TES 10y zero-coupon", _pct(tes_10y), tes_curve))
    evidence.append(
        {
            "label": f"TES {trigger_tenor} minus policy spread",
            "value": _pp(trigger_spread),
            "source": "computed",
        }
    )

    return _card(
        card_id="tes_policy_spread",
        family="sovereign_funding",
        title="TES-policy spread tension",
        trigger=(
            f"TES {trigger_tenor} minus policy rate is {_pp(trigger_spread)}, "
            f"above the {_pp(TES_POLICY_SPREAD_THRESHOLD_PP)} review threshold."
        ),
        evidence=evidence,
        calculations={
            "threshold_pp": TES_POLICY_SPREAD_THRESHOLD_PP,
            "policy_rate_pct": policy_rate,
            "tes_5y_zero_coupon_pct": tes_5y,
            "tes_10y_zero_coupon_pct": tes_10y,
            "spreads_pp": observed_spreads,
        },
        agent_prompt=(
            "Could elevated TES yields relative to the policy rate reflect "
            "fiscal-risk pricing, term premium, inflation expectations, "
            "liquidity stress, or market technicals?"
        ),
        why_it_matters=(
            "Government funding rates far above the policy rate can flag "
            "sovereign-risk or term-premium pressure before it becomes an "
            "explicit policy event."
        ),
        caveats=[
            "This is not proof of fiscal stress.",
            "TES term premium, maturity, liquidity, and auction composition matter.",
        ],
        suggested_questions=[
            "Will the next COP TES auction clear with a maximum cutoff rate above 14.0%?",
            "Will BanRep hold the policy rate at the next decision despite elevated TES yields?",
        ],
        source_refs=[
            _source_ref(policy, "policy_rate_ibr"),
            _source_ref(fiscal, "fiscal_tax_pulse"),
            _source_ref(tes_curve, "banrep_tes_curve"),
        ],
    )


def real_policy_rate_rule(
    by_id: dict[str, IndicatorObservation],
) -> dict[str, Any] | None:
    policy = by_id.get("policy_rate_ibr")
    ipc = by_id.get("ipc_inflation")
    if policy is None or ipc is None:
        return None

    policy_rate = _number(policy.values.get("policy_rate_pct"))
    ipc_annual = _number(ipc.values.get("annual_variation_pct"))
    if policy_rate is None or ipc_annual is None:
        return None

    real_rate = round(policy_rate - ipc_annual, 3)
    if real_rate < REAL_POLICY_RATE_THRESHOLD_PP:
        return None

    return _card(
        card_id="real_policy_rate",
        family="monetary_stance",
        title="High ex-post real policy rate",
        trigger=(
            f"Policy rate minus annual IPC is {_pp(real_rate)}, above the "
            f"{_pp(REAL_POLICY_RATE_THRESHOLD_PP)} review threshold."
        ),
        evidence=[
            _evidence("BanRep policy rate", _pct(policy_rate), policy),
            _evidence("DANE annual IPC", _pct(ipc_annual), ipc),
            {
                "label": "Ex-post real policy rate",
                "value": _pp(real_rate),
                "source": "computed",
            },
        ],
        calculations={
            "threshold_pp": REAL_POLICY_RATE_THRESHOLD_PP,
            "policy_rate_pct": policy_rate,
            "ipc_annual_variation_pct": ipc_annual,
            "ex_post_real_policy_rate_pp": real_rate,
        },
        agent_prompt=(
            "Does a high ex-post real policy rate suggest restrictive monetary "
            "stance, room for future cuts, or a reason BanRep may remain cautious?"
        ),
        why_it_matters=(
            "The policy rate relative to inflation is a cheap proxy for monetary "
            "stance and can frame rate-decision or credit-condition questions."
        ),
        caveats=[
            "This is an ex-post proxy; inflation expectations are the better stance measure.",
            "BanRep reaction functions also depend on core inflation, expectations, FX, and activity.",
        ],
        suggested_questions=[
            "Will BanRep keep the policy rate unchanged at its next decision?",
            "Will the next DANE IPC release leave the ex-post real policy rate above 4.0 pp?",
        ],
        source_refs=[
            _source_ref(policy, "policy_rate_ibr"),
            _source_ref(ipc, "ipc_inflation"),
        ],
    )


def real_tax_revenue_squeeze_rule(
    by_id: dict[str, IndicatorObservation],
) -> dict[str, Any] | None:
    fiscal = by_id.get("fiscal_tax_pulse")
    ipc = by_id.get("ipc_inflation")
    if fiscal is None or ipc is None:
        return None

    tax_collection = _component(fiscal, "tax_collection")
    tax_values = tax_collection.values if tax_collection is not None else fiscal.values
    nominal_tax_growth = _number(
        tax_values.get("gross_tax_revenue_annual_variation_pct")
    )
    ipc_annual = _number(ipc.values.get("annual_variation_pct"))
    if nominal_tax_growth is None or ipc_annual is None:
        return None

    real_tax_growth = round(nominal_tax_growth - ipc_annual, 3)
    if real_tax_growth > REAL_TAX_REVENUE_SQUEEZE_THRESHOLD_PP:
        return None

    tax_source = tax_collection or fiscal
    return _card(
        card_id="real_tax_revenue_squeeze",
        family="fiscal_capacity",
        title="Real tax revenue squeeze",
        trigger=(
            "Nominal gross tax-revenue growth minus annual IPC is "
            f"{_pp(real_tax_growth)}, at or below the "
            f"{_pp(REAL_TAX_REVENUE_SQUEEZE_THRESHOLD_PP)} review threshold."
        ),
        evidence=[
            _evidence("DIAN nominal gross tax revenue growth", _pct(nominal_tax_growth), tax_source),
            _evidence("DANE annual IPC", _pct(ipc_annual), ipc),
            {
                "label": "Approximate real tax revenue growth",
                "value": _pp(real_tax_growth),
                "source": "computed",
            },
        ],
        calculations={
            "threshold_pp": REAL_TAX_REVENUE_SQUEEZE_THRESHOLD_PP,
            "gross_tax_revenue_annual_variation_pct": nominal_tax_growth,
            "ipc_annual_variation_pct": ipc_annual,
            "approx_real_tax_revenue_growth_pp": real_tax_growth,
        },
        agent_prompt=(
            "Could tax collection be weakening in real terms, and does that "
            "connect to budget, debt-service, or fiscal-rule questions?"
        ),
        why_it_matters=(
            "Tax collection below inflation is a simple fiscal-capacity warning: "
            "nominal revenue can rise while real purchasing power falls."
        ),
        caveats=[
            "Monthly tax collection is affected by payment calendars and one-off settlements.",
            "This proxy uses headline IPC, not a full deflator or nominal GDP comparison.",
        ],
        suggested_questions=[
            "Will the next DIAN tax-collection release again show nominal gross revenue growth below annual IPC?",
            "Will official fiscal updates cite weak tax collection or revenue shortfalls?",
        ],
        source_refs=[
            _source_ref(fiscal, "fiscal_tax_pulse"),
            _source_ref(tax_collection, "tax_collection")
            if tax_collection is not None
            else None,
            _source_ref(ipc, "ipc_inflation"),
        ],
    )


def tes_auction_high_cost_rule(
    by_id: dict[str, IndicatorObservation],
) -> dict[str, Any] | None:
    fiscal = by_id.get("fiscal_tax_pulse")
    if fiscal is None:
        return None
    auction = _component(fiscal, "tes_auction")
    if auction is None:
        return None

    max_cutoff = _number(auction.values.get("max_cutoff_rate_pct"))
    if max_cutoff is None or max_cutoff < TES_AUCTION_CUTOFF_THRESHOLD_PCT:
        return None

    total_issued = _number(auction.values.get("total_issued_cop_billions"))
    bid_to_cover = _number(auction.values.get("bid_to_cover"))
    maturities = auction.values.get("maturity_years") or []
    evidence = [
        _evidence("Maximum cutoff rate", _pct(max_cutoff), auction),
    ]
    if total_issued is not None:
        evidence.append(
            _evidence("Issued amount", f"COP {total_issued:g} billones", auction)
        )
    if bid_to_cover is not None:
        evidence.append(_evidence("Bid-to-cover", f"{bid_to_cover:g}x", auction))
    if maturities:
        evidence.append(
            {
                "label": "Maturities",
                "value": ", ".join(str(maturity) for maturity in maturities),
                "source": auction.source_name,
            }
        )

    caveats = [
        "High cutoff rate is a funding-cost signal, not automatically weak demand.",
        "Compare tenor mix before interpreting this as a deterioration.",
    ]
    if bid_to_cover is not None and bid_to_cover >= 2:
        caveats.append(
            "Bid-to-cover was not low, so demand weakness is not the main claim."
        )

    return _card(
        card_id="tes_auction_high_funding_cost",
        family="sovereign_funding",
        title="TES auction high funding cost",
        trigger=(
            f"Latest COP TES auction max cutoff is {_pct(max_cutoff)}, at or "
            f"above the {_pct(TES_AUCTION_CUTOFF_THRESHOLD_PCT)} review threshold."
        ),
        evidence=evidence,
        calculations={
            "threshold_pct": TES_AUCTION_CUTOFF_THRESHOLD_PCT,
            "max_cutoff_rate_pct": max_cutoff,
            "total_issued_cop_billions": total_issued,
            "bid_to_cover": bid_to_cover,
            "maturity_years": maturities,
        },
        agent_prompt=(
            "Is the high cutoff mostly a fiscal-risk, duration, liquidity, "
            "supply-size, or auction-specific tenor story?"
        ),
        why_it_matters=(
            "High official auction cutoff rates directly affect government "
            "funding costs and can sharpen public-finance questions."
        ),
        caveats=caveats,
        suggested_questions=[
            "Will the next official COP TES auction report a maximum cutoff rate of at least 14.0%?",
            "Will official fiscal updates mention TES funding or debt-service pressure?",
        ],
        source_refs=[
            _source_ref(fiscal, "fiscal_tax_pulse"),
            _source_ref(auction, "tes_auction"),
        ],
    )


def construction_cost_vs_ipc_rule(
    by_id: dict[str, IndicatorObservation],
) -> dict[str, Any] | None:
    construction = by_id.get("construction_bundle")
    ipc = by_id.get("ipc_inflation")
    if construction is None or ipc is None:
        return None

    icoced = _component(construction, "icoced")
    icoced_values = icoced.values if icoced is not None else construction.values
    icoced_annual = _number(icoced_values.get("icoced_total_annual_variation_pct"))
    ipc_annual = _number(ipc.values.get("annual_variation_pct"))
    if icoced_annual is None or ipc_annual is None:
        return None

    spread = round(icoced_annual - ipc_annual, 3)
    if spread < ICOCED_IPC_SPREAD_THRESHOLD_PP:
        return None

    evidence = [
        _evidence(
            "ICOCED annual variation",
            _pct(icoced_annual),
            icoced or construction,
        ),
        _evidence("IPC annual variation", _pct(ipc_annual), ipc),
        {
            "label": "ICOCED minus IPC spread",
            "value": _pp(spread),
            "source": "computed",
        },
    ]

    return _card(
        card_id="construction_cost_vs_ipc",
        family="construction_cost_pressure",
        title="Construction cost vs IPC squeeze",
        trigger=(
            f"ICOCED annual variation exceeds IPC by {_pp(spread)}, above the "
            f"{_pp(ICOCED_IPC_SPREAD_THRESHOLD_PP)} review threshold."
        ),
        evidence=evidence,
        calculations={
            "threshold_pp": ICOCED_IPC_SPREAD_THRESHOLD_PP,
            "icoced_total_annual_variation_pct": icoced_annual,
            "ipc_annual_variation_pct": ipc_annual,
            "spread_pp": spread,
        },
        agent_prompt=(
            "Could construction input costs be creating housing, public works, "
            "or margin pressure that deserves a forecastable follow-up?"
        ),
        why_it_matters=(
            "Construction costs above headline inflation can point to public "
            "works budget pressure and housing affordability stress."
        ),
        caveats=[
            "The spread can close quickly through base effects.",
            "Cost pressure is not the same as housing-price inflation.",
        ],
        suggested_questions=[
            "Will the next DANE ICOCED release remain above the latest annual IPC rate?",
            "Will construction-cost pressure show up in public works or housing-finance indicators?",
        ],
        source_refs=[
            _source_ref(construction, "construction_bundle"),
            _source_ref(icoced, "icoced") if icoced is not None else None,
            _source_ref(ipc, "ipc_inflation"),
        ],
    )


TENSION_RULES: tuple[TensionRule, ...] = (
    tes_policy_spread_rule,
    real_policy_rate_rule,
    real_tax_revenue_squeeze_rule,
    tes_auction_high_cost_rule,
    construction_cost_vs_ipc_rule,
)


def _card(
    *,
    card_id: str,
    family: str,
    title: str,
    trigger: str,
    evidence: list[dict[str, Any]],
    calculations: dict[str, Any],
    agent_prompt: str,
    why_it_matters: str,
    caveats: list[str],
    suggested_questions: list[str],
    source_refs: list[dict[str, Any] | None],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "card_id": card_id,
        "family": family,
        "title": title,
        "severity": "review",
        "trigger": trigger,
        "evidence": evidence,
        "calculations": calculations,
        "agent_prompt": agent_prompt,
        "why_it_matters": why_it_matters,
        "caveats": caveats,
        "suggested_questions": suggested_questions,
        "source_refs": [ref for ref in source_refs if ref is not None],
        "review_policy": (
            "Advisory screen only. Use this to inspect whether a forecastable "
            "question exists; do not treat it as a conclusion or probability input."
        ),
    }


def _component(
    indicator: IndicatorObservation,
    component_id: str,
) -> IndicatorComponent | None:
    for component in indicator.components:
        if component.component_id == component_id and component.status == "observed":
            return component
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _pct(value: float) -> str:
    return f"{value:.2f}%"


def _pp(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f} pp"


def _evidence(
    label: str,
    value: str,
    source: IndicatorObservation | IndicatorComponent,
) -> dict[str, str]:
    return {
        "label": label,
        "value": value,
        "source": source.source_name,
        "period": source.period,
        "url": source.source_url,
    }


def _source_ref(
    source: IndicatorObservation | IndicatorComponent | None,
    source_id: str,
) -> dict[str, Any] | None:
    if source is None:
        return None
    payload = asdict(source)
    return {
        "source_id": source_id,
        "source_name": source.source_name,
        "source_url": source.source_url,
        "period": source.period,
        "release_date": source.release_date,
        "headline": source.headline,
        "values": payload.get("values", {}),
    }
