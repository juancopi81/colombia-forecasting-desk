from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable

from .cleaner import normalize_whitespace
from .models import IndicatorObservation
from .tagger import fold_accents

SCHEMA_VERSION = "cooccurrence_bundles.v1"
MAX_BUNDLES = 4
MAX_INPUTS_PER_BUNDLE = 8
MAX_REVIEW_ITEM_INPUTS = 3
MIN_INPUTS_PER_BUNDLE = 2

GENERIC_REVIEW_QUESTIONS = [
    "Are these inputs related, contradictory, or independent today?",
    "What alternative explanation would make the apparent relationship unimportant?",
    "Is there a dated official event or resolution window that could make this forecastable?",
    "What evidence outside this bundle should M2 inspect before choosing a thesis path?",
]
GENERIC_GUARDRAILS = [
    "Co-occurrence is not causality and does not imply a thesis is true.",
    "Use this as context packaging for M2, not as a forecast question or probability input.",
    "Inspect cross-bundle links and unbundled review items before deciding what matters.",
    "M2 may reject, rename, merge, or ignore this bundle if the source evidence points elsewhere.",
]


BUNDLE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "bundle_id": "fiscal_sovereign_funding",
        "title": "Fiscal / sovereign funding bundle",
        "description": (
            "Fiscal-capacity, budget, TES curve, and government-funding inputs "
            "co-occurred and may deserve joint review."
        ),
        "tension_card_ids": {
            "tes_policy_spread",
            "real_tax_revenue_squeeze",
            "tes_auction_high_funding_cost",
        },
        "indicator_ids": {"fiscal_tax_pulse"},
        "review_terms": {
            "presupuesto",
            "pgn",
            "fiscal",
            "deuda",
            "recaudo",
            "tributaria",
            "tributario",
            "tax collection",
            "tes",
            "hacienda",
            "regla fiscal",
            "adicion",
            "adición",
        },
        "review_questions": [
            "Are TES moves better explained by fiscal stress, duration, liquidity, inflation expectations, or auction composition?",
            "Do budget or PGN items create a concrete official event to monitor?",
        ],
    },
    {
        "bundle_id": "monetary_credit_transmission",
        "title": "Monetary / credit transmission bundle",
        "description": (
            "Policy-rate, inflation, IBR, TES, and credit-condition inputs "
            "co-occurred and may deserve joint review."
        ),
        "tension_card_ids": {"real_policy_rate", "tes_policy_spread"},
        "indicator_ids": {"policy_rate_ibr", "ipc_inflation", "fiscal_tax_pulse"},
        "review_terms": {
            "banrep",
            "banco de la republica",
            "banco de la república",
            "tasa",
            "ibr",
            "credito",
            "crédito",
            "hipotec",
            "vivienda",
            "inflacion",
            "inflación",
            "expectativas",
            "tes",
        },
        "review_questions": [
            "Do credit or market rates appear to follow policy, TES, inflation expectations, or liquidity conditions?",
            "What would show that monetary transmission is normal rather than stressed?",
        ],
    },
    {
        "bundle_id": "construction_housing_cost",
        "title": "Construction / housing cost bundle",
        "description": (
            "Construction-cost, housing-finance, cement, licenses, and IPC inputs "
            "co-occurred and may deserve joint review."
        ),
        "tension_card_ids": {"construction_cost_vs_ipc"},
        "indicator_ids": {"construction_bundle", "ipc_inflation"},
        "review_terms": {
            "construccion",
            "construcción",
            "icoced",
            "vivienda",
            "edificaciones",
            "cemento",
            "licencias",
            "hipotec",
            "obras",
        },
        "review_questions": [
            "Is the construction-cost signal showing up in housing finance, public works, or input categories?",
            "Could base effects or composition explain the cost pressure without a broader housing thesis?",
        ],
    },
    {
        "bundle_id": "energy_tariff_subsidy",
        "title": "Energy / tariff / subsidy bundle",
        "description": (
            "Energy-system, tariff, fuel, subsidy, and fiscal/inflation inputs "
            "co-occurred and may deserve joint review."
        ),
        "tension_card_ids": set(),
        "indicator_ids": {"energy_system", "ipc_inflation", "fiscal_tax_pulse"},
        "review_terms": {
            "energia",
            "energía",
            "glp",
            "gas licuado",
            "combustible",
            "tarifa",
            "subsidio",
            "electricidad",
            "reservorio",
            "embalse",
            "minminas",
            "creg",
        },
        "require_review_item": True,
        "review_questions": [
            "Are energy, tariff, or subsidy signals connected to household prices, fiscal cost, or only sector-specific administration?",
            "Is there an official tariff, subsidy, regulation, or legislative date that could resolve the question?",
        ],
    },
)


def build_cooccurrence_bundles(
    indicator_watch: list[IndicatorObservation],
    indicator_tension_cards: list[dict[str, Any]],
    m2_review_packet: dict[str, Any],
    *,
    max_bundles: int = MAX_BUNDLES,
) -> list[dict[str, Any]]:
    """Package related M1/M2 ingredients without choosing a thesis for M2."""
    indicators_by_id = {
        indicator.indicator_id: indicator
        for indicator in indicator_watch
        if indicator.status == "observed"
    }
    cards_by_id = {
        str(card.get("card_id") or ""): card
        for card in indicator_tension_cards
        if isinstance(card, dict)
    }
    review_items = [
        item
        for item in m2_review_packet.get("review_items") or []
        if isinstance(item, dict)
    ]

    bundles: list[dict[str, Any]] = []
    for definition in BUNDLE_DEFINITIONS:
        bundle = _bundle_from_definition(
            definition,
            indicators_by_id,
            cards_by_id,
            review_items,
        )
        if bundle is None:
            continue
        bundles.append(bundle)
        if len(bundles) >= max_bundles:
            break
    return bundles


def attach_cooccurrence_bundles(
    m2_review_packet: dict[str, Any],
    cooccurrence_bundles: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return an M2 packet copy with bundle context embedded for the agent."""
    packet = dict(m2_review_packet)
    summary = dict(packet.get("summary") or {})
    inputs = dict(packet.get("inputs") or {})
    policy = dict(packet.get("policy") or {})
    summary["cooccurrence_bundle_count"] = len(cooccurrence_bundles)
    inputs["cooccurrence_bundles_artifact"] = "cooccurrence_bundles.json"
    policy["cooccurrence_bundle_policy"] = (
        "Co-occurrence bundles package related ingredients for M2 review. "
        "They are not thesis labels, conclusions, forecast questions, or "
        "probability inputs. The reviewer should inspect cross-bundle links and "
        "unbundled items before deciding what matters."
    )
    packet["summary"] = summary
    packet["inputs"] = inputs
    packet["policy"] = policy
    packet["cooccurrence_bundles"] = cooccurrence_bundles
    return packet


def render_cooccurrence_bundles(
    bundles: list[dict[str, Any]],
    *,
    run_date: str,
) -> str:
    lines = [
        f"# Co-Occurrence Bundles - {run_date}",
        "",
        (
            "Neutral context bundles for M2. They package related ingredients "
            "that co-occurred today, but they are not conclusions, thesis "
            "labels, forecast questions, or probability inputs."
        ),
        "",
        (
            "The reviewer should inspect cross-bundle links and unbundled M2 "
            "items before deciding what matters."
        ),
        "",
    ]
    if not bundles:
        lines.append("No co-occurrence bundles met the minimum evidence threshold.")
        return "\n".join(lines).rstrip() + "\n"

    for index, bundle in enumerate(bundles, 1):
        lines.extend(_render_bundle(index, bundle))
    return "\n".join(lines).rstrip() + "\n"


def _bundle_from_definition(
    definition: dict[str, Any],
    indicators_by_id: dict[str, IndicatorObservation],
    cards_by_id: dict[str, dict[str, Any]],
    review_items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    inputs: list[dict[str, Any]] = []
    card_ids = set(definition.get("tension_card_ids") or set())
    indicator_ids = set(definition.get("indicator_ids") or set())
    review_terms = set(definition.get("review_terms") or set())

    for card_id in sorted(card_ids):
        card = cards_by_id.get(card_id)
        if card:
            inputs.append(_input_from_tension_card(card))

    for indicator_id in sorted(indicator_ids):
        indicator = indicators_by_id.get(indicator_id)
        if indicator:
            inputs.append(_input_from_indicator(indicator))

    matched_review_items = [
        item for item in review_items if _review_item_matches(item, review_terms)
    ]
    for item in matched_review_items[:MAX_REVIEW_ITEM_INPUTS]:
        inputs.append(_input_from_review_item(item))

    inputs = _dedupe_inputs(inputs)[:MAX_INPUTS_PER_BUNDLE]
    if len(inputs) < MIN_INPUTS_PER_BUNDLE:
        return None
    if definition.get("require_review_item") and not any(
        item["kind"] == "review_item" for item in inputs
    ):
        return None

    bundle_id = str(definition["bundle_id"])
    return {
        "schema_version": SCHEMA_VERSION,
        "bundle_id": bundle_id,
        "title": str(definition["title"]),
        "disposition": "review_context_only",
        "description": str(definition["description"]),
        "input_count": len(inputs),
        "inputs": inputs,
        "review_questions": [
            *GENERIC_REVIEW_QUESTIONS,
            *[str(question) for question in definition.get("review_questions") or []],
        ],
        "guardrails": list(GENERIC_GUARDRAILS),
        "source_refs": _source_refs(bundle_id, inputs),
        "review_context": {
            "neutral_bundle": True,
            "not_a_thesis": True,
            "cross_bundle_review_required": True,
            "tension_card_ids": [
                item["input_id"] for item in inputs if item["kind"] == "tension_card"
            ],
            "indicator_ids": [
                item["input_id"] for item in inputs if item["kind"] == "indicator"
            ],
            "review_item_ids": [
                item["input_id"] for item in inputs if item["kind"] == "review_item"
            ],
        },
    }


def _input_from_tension_card(card: dict[str, Any]) -> dict[str, Any]:
    card_id = str(card.get("card_id") or "indicator_tension")
    return {
        "kind": "tension_card",
        "input_id": card_id,
        "title": str(card.get("title") or card_id),
        "summary": normalize_whitespace(str(card.get("trigger") or "")),
        "source": "indicator_tension_cards",
        "url": "",
        "artifact_refs": [
            {
                "artifact": "indicator_tension_cards.json",
                "key": "card_id",
                "value": card_id,
            }
        ],
    }


def _input_from_indicator(indicator: IndicatorObservation) -> dict[str, Any]:
    return {
        "kind": "indicator",
        "input_id": indicator.indicator_id,
        "title": indicator.name,
        "summary": normalize_whitespace(indicator.headline),
        "source": indicator.source_name,
        "url": indicator.source_url,
        "artifact_refs": [
            {
                "artifact": "indicator_watch.json",
                "key": "indicator_id",
                "value": indicator.indicator_id,
            }
        ],
    }


def _input_from_review_item(item: dict[str, Any]) -> dict[str, Any]:
    origin_id = str(item.get("origin_id") or _hash_id(str(item)))
    title = normalize_whitespace(
        str(item.get("title") or item.get("question_seed") or origin_id)
    )
    summary = normalize_whitespace(
        str(item.get("question_seed") or item.get("summary") or title)
    )
    return {
        "kind": "review_item",
        "input_id": origin_id,
        "title": title,
        "summary": summary,
        "source": "m2_review_packet",
        "url": _first_excerpt_url(item),
        "artifact_refs": [
            {
                "artifact": "m2_review_packet.json",
                "key": "origin_id",
                "value": origin_id,
            }
        ],
    }


def _review_item_matches(item: dict[str, Any], terms: set[str]) -> bool:
    if not terms:
        return False
    text = fold_accents(
        normalize_whitespace(
            " ".join(
                [
                    str(item.get("title") or ""),
                    str(item.get("question_seed") or ""),
                    str(item.get("bucket") or ""),
                    str(item.get("recommendation") or ""),
                    " ".join(str(value) for value in item.get("entities") or []),
                    " ".join(str(value) for value in item.get("topics") or []),
                ]
            )
        ).lower()
    )
    folded_terms = {fold_accents(term.lower()) for term in terms}
    return any(_contains_term(text, term) for term in folded_terms)


def _contains_term(text: str, term: str) -> bool:
    if not term:
        return False
    if " " in term:
        return term in text
    if len(term) <= 4:
        return bool(
            re.search(
                rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])",
                text,
            )
        )
    return term in text


def _first_excerpt_url(item: dict[str, Any]) -> str:
    for excerpt in item.get("source_excerpts") or []:
        if not isinstance(excerpt, dict):
            continue
        url = str(excerpt.get("url") or "")
        if url:
            return url
    traceability = item.get("traceability")
    if isinstance(traceability, dict):
        urls = traceability.get("source_urls")
        if isinstance(urls, list):
            for url in urls:
                if url:
                    return str(url)
    return ""


def _dedupe_inputs(inputs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in inputs:
        key = (str(item.get("kind") or ""), str(item.get("input_id") or ""))
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _source_refs(bundle_id: str, inputs: list[dict[str, Any]]) -> dict[str, Any]:
    artifact_refs: list[dict[str, str]] = [
        {
            "artifact": "cooccurrence_bundles.json",
            "key": "bundle_id",
            "value": bundle_id,
        }
    ]
    source_urls: list[str] = []
    for item in inputs:
        artifact_refs.extend(
            ref
            for ref in item.get("artifact_refs") or []
            if isinstance(ref, dict)
        )
        url = str(item.get("url") or "")
        if url:
            source_urls.append(url)
    return {
        "artifact_refs": _unique_refs(artifact_refs),
        "source_urls": _unique_strings(source_urls),
    }


def _render_bundle(index: int, bundle: dict[str, Any]) -> list[str]:
    lines = [
        f"## {index}. {bundle.get('title', bundle.get('bundle_id', 'Bundle'))}",
        "",
        f"- Bundle id: `{bundle.get('bundle_id', '')}`",
        f"- Disposition: `{bundle.get('disposition', 'review_context_only')}`",
        f"- Inputs: {bundle.get('input_count', 0)}",
        f"- Description: {bundle.get('description', '')}",
        "",
        "Inputs:",
        "",
    ]
    for item in bundle.get("inputs") or []:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"- `{item.get('kind', '')}` `{item.get('input_id', '')}`: "
            f"{item.get('title', '')} — {item.get('summary', '')}"
        )
    questions = [
        str(question) for question in bundle.get("review_questions") or [] if question
    ]
    if questions:
        lines.extend(["", "Review questions:", ""])
        lines.extend(f"- {question}" for question in questions)
    guardrails = [
        str(guardrail) for guardrail in bundle.get("guardrails") or [] if guardrail
    ]
    if guardrails:
        lines.extend(["", "Guardrails:", ""])
        lines.extend(f"- {guardrail}" for guardrail in guardrails)
    lines.append("")
    return lines


def _hash_id(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def _unique_refs(refs: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for ref in refs:
        artifact = str(ref.get("artifact") or "")
        key = str(ref.get("key") or "")
        value = str(ref.get("value") or "")
        identity = (artifact, key, value)
        if not artifact or identity in seen:
            continue
        seen.add(identity)
        output.append({"artifact": artifact, "key": key, "value": value})
    return output


def _unique_strings(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
