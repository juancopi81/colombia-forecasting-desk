from __future__ import annotations

from collections import Counter
import hashlib
from dataclasses import asdict
from typing import Any
from urllib.parse import urldefrag

from .cleaner import normalize_whitespace
from .models import (
    CleanedItem,
    IndicatorObservation,
    RawItem,
    RunSummary,
    SourceHealth,
)

SCHEMA_VERSION = "m2_review_packet.v1"
MAX_REVIEW_ITEMS = 24
MAX_LEGISLATIVE_REVIEW_ITEMS = 10
MAX_INDICATOR_REVIEW_ITEMS = 6
MAX_EVENT_REVIEW_ITEMS = 4
MAX_CROSS_IMPACT_ITEMS = 4
MAX_EXCERPTS_PER_ITEM = 4
OFFICIAL_EXCERPT_CHARS = 2600
MEDIA_EXCERPT_CHARS = 1000
STRUCTURED_EXCERPT_CHARS = 1800
NEWS_SOURCE_TYPES = {"news"}
CROSS_IMPACT_RULES = (
    {
        "keywords": (
            "presupuesto",
            "pgn",
            "fiscal",
            "deuda",
            "tribut",
            "hacienda",
        ),
        "indicator_ids": ("fiscal_tax_pulse", "policy_rate_ibr"),
        "reason": (
            "public-finance legislation can change or reveal fiscal funding "
            "pressure, TES context, or monetary-policy constraints"
        ),
    },
    {
        "keywords": (
            "subsidio",
            "glp",
            "gas licuado",
            "combustible",
            "energia",
            "energía",
            "transporte",
        ),
        "indicator_ids": ("fiscal_tax_pulse", "ipc_inflation", "trm_usd_cop"),
        "reason": (
            "subsidy or energy-cost legislation can interact with household "
            "prices, fiscal cost, and imported-cost pressure"
        ),
    },
    {
        "keywords": ("soat", "tarifa", "seguro", "motocicleta"),
        "indicator_ids": ("ipc_inflation", "fiscal_tax_pulse"),
        "reason": (
            "household transport-cost legislation can interact with inflation "
            "pressure or public-finance exposure"
        ),
    },
)


def build_m2_review_packet(
    run_summary: RunSummary,
    raw_items: list[RawItem],
    cleaned_items: list[CleanedItem],
    m1_candidates: dict[str, Any],
    m2_ranked_questions: dict[str, Any],
    legislative_reconciliations: list[dict[str, Any]],
    source_health: list[SourceHealth],
    indicator_watch: list[IndicatorObservation],
    indicator_tension_cards: list[dict[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build an evidence-rich packet for LLM/human M2 review.

    The packet packages source excerpts and structured evidence. It is not a
    scoring authority; it is meant to let the LLM challenge M1/M2 heuristics.
    """
    indexes = _EvidenceIndexes(raw_items, cleaned_items)
    legislative_by_id = {
        str(record.get("canonical_bill_id") or ""): record
        for record in legislative_reconciliations
        if isinstance(record, dict)
    }
    indicator_by_id = {
        indicator.indicator_id: indicator for indicator in indicator_watch
    }

    ranked_records = _ranked_review_items(m2_ranked_questions)
    ranked_items = [
        _review_item_from_ranked(
            ranked,
            legislative_by_id,
            indexes,
        )
        for ranked in ranked_records
    ]
    candidate_items: list[dict[str, Any]] = []
    for candidate in m1_candidates.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        candidate_items.append(
            _review_item_from_candidate(candidate, indicator_by_id, indexes)
        )

    cross_impact_items = _cross_impact_items(
        ranked_records,
        legislative_by_id,
        indicator_by_id,
        indexes,
    )

    tension_cards = list(indicator_tension_cards or [])
    review_items = _balanced_review_items(
        ranked_items,
        candidate_items,
        cross_impact_items,
    )
    item_type_counts = Counter(
        str(item.get("item_type") or "unknown") for item in review_items
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "generated_at": generated_at or run_summary.finished_at,
        "policy": {
            "purpose": (
                "Give the LLM and human reviewer enough source content to "
                "challenge deterministic M1/M2 heuristics."
            ),
            "not_allowed": [
                "Do not reject solely because a deterministic score is low.",
                "Do not estimate probability unless evidence is sufficient.",
                "Do not treat summaries as a substitute for the source excerpts.",
            ],
            "review_instruction": (
                "Read the excerpts first, then decide whether the heuristic "
                "over-ranked, under-ranked, or missed the public-interest angle."
            ),
            "composition": (
                "The packet reserves room for legislative records, indicator "
                "seeds, event leads, advisory cross-impact hypotheses, and "
                "Indicator Tension Cards so structured laws do not crowd out "
                "macro/fiscal signals."
            ),
        },
        "inputs": {
            "m1_candidates_artifact": "m1_candidates.json",
            "m2_ranked_questions_artifact": "m2_ranked_questions.json",
            "legislative_reconciler_artifact": "legislative_reconciler.json",
            "raw_items_artifact": "raw_items.json",
            "cleaned_items_artifact": "cleaned_items.json",
            "indicator_watch_artifact": "indicator_watch.json",
            "indicator_tension_cards_artifact": "indicator_tension_cards.json",
            "source_health_artifact": "source_health.json",
        },
        "summary": {
            "review_item_count": len(review_items),
            "items_with_source_excerpts": sum(
                1 for item in review_items if item.get("source_excerpts")
            ),
            "heuristic_challenge_count": sum(
                1 for item in review_items if item.get("heuristic_risk_flags")
            ),
            "source_caveat_count": len(_source_caveats(source_health)),
            "indicator_tension_card_count": len(tension_cards),
            "item_type_counts": dict(sorted(item_type_counts.items())),
            "quota_policy": {
                "max_total": MAX_REVIEW_ITEMS,
                "max_legislative_ranked_records": MAX_LEGISLATIVE_REVIEW_ITEMS,
                "max_indicator_seeds": MAX_INDICATOR_REVIEW_ITEMS,
                "max_event_leads": MAX_EVENT_REVIEW_ITEMS,
                "max_cross_impact_hypotheses": MAX_CROSS_IMPACT_ITEMS,
            },
        },
        "source_caveats": _source_caveats(source_health),
        "indicator_tension_cards": tension_cards,
        "review_items": review_items,
    }


def render_m2_review_packet(packet: dict[str, Any]) -> str:
    """Render the M2 review packet as a paste-friendly Markdown artifact."""
    summary = packet.get("summary") if isinstance(packet.get("summary"), dict) else {}
    lines = [
        f"# M2 Review Packet - {packet.get('run_date', '')}",
        "",
        "## Review Policy",
        "",
        str((packet.get("policy") or {}).get("review_instruction") or ""),
        "",
        "- This packet is content-first: read the source excerpts before "
        "trusting scores.",
        "- Heuristic labels are advisory and should be challenged when the "
        "evidence suggests a better question.",
        "- Do not estimate probability unless the packet has enough evidence "
        "for resolution criteria.",
        "",
        "## Summary",
        "",
        f"- Review items: {summary.get('review_item_count', 0)}",
        f"- Items with source excerpts: {summary.get('items_with_source_excerpts', 0)}",
        f"- Heuristic challenge items: {summary.get('heuristic_challenge_count', 0)}",
        f"- Source caveats: {summary.get('source_caveat_count', 0)}",
        f"- Indicator tension cards: {summary.get('indicator_tension_card_count', 0)}",
        "",
    ]
    item_type_counts = summary.get("item_type_counts")
    if isinstance(item_type_counts, dict) and item_type_counts:
        lines.extend(["Composition:", ""])
        for item_type, count in sorted(item_type_counts.items()):
            lines.append(f"- `{item_type}`: {count}")
        lines.append("")
    caveats = packet.get("source_caveats") or []
    if caveats:
        lines.extend(["## Source Caveats", ""])
        for caveat in caveats[:8]:
            if not isinstance(caveat, dict):
                continue
            lines.append(
                f"- `{caveat.get('source_id', '')}`: {caveat.get('reason', '')}"
            )
        lines.append("")

    tension_cards = [
        card
        for card in packet.get("indicator_tension_cards") or []
        if isinstance(card, dict)
    ]
    if tension_cards:
        lines.extend(["## Indicator Tension Cards", ""])
        lines.append(
            "Advisory screens that flag official indicator contrasts for review; "
            "they are not conclusions or probability inputs."
        )
        lines.append("")
        for index, card in enumerate(tension_cards[:5], 1):
            lines.extend(_render_tension_card(index, card))

    lines.extend(["## Review Items", ""])
    for index, item in enumerate(packet.get("review_items") or [], 1):
        if not isinstance(item, dict):
            continue
        lines.extend(_render_review_item(index, item))
    return "\n".join(lines).rstrip() + "\n"


class _EvidenceIndexes:
    def __init__(
        self,
        raw_items: list[RawItem],
        cleaned_items: list[CleanedItem],
    ) -> None:
        self.raw_by_id = {item.id: item for item in raw_items}
        self.cleaned_by_id = {item.id: item for item in cleaned_items}
        self.ids_by_url: dict[str, list[str]] = {}
        for item in raw_items:
            for key in _url_keys(item.url):
                self.ids_by_url.setdefault(key, []).append(item.id)
        for item in cleaned_items:
            for key in _url_keys(item.url):
                self.ids_by_url.setdefault(key, []).append(item.id)

    def by_item_ids(
        self,
        item_ids: list[str],
    ) -> list[tuple[RawItem | None, CleanedItem | None]]:
        pairs: list[tuple[RawItem | None, CleanedItem | None]] = []
        seen: set[str] = set()
        for item_id in item_ids:
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            pairs.append(
                (self.raw_by_id.get(item_id), self.cleaned_by_id.get(item_id))
            )
        return pairs

    def by_urls(
        self,
        urls: list[str],
    ) -> list[tuple[RawItem | None, CleanedItem | None]]:
        item_ids: list[str] = []
        for url in urls:
            for key in _url_keys(url):
                item_ids.extend(self.ids_by_url.get(key, []))
        return self.by_item_ids(item_ids)


def _ranked_review_items(
    m2_ranked_questions: dict[str, Any],
) -> list[dict[str, Any]]:
    ranked = [
        item
        for item in m2_ranked_questions.get("ranked_questions") or []
        if isinstance(item, dict)
    ]
    audit_refs = {
        str(item.get("rank_id") or "")
        for key in ("possible_false_negatives", "possible_false_positives")
        for item in (m2_ranked_questions.get("heuristic_audit") or {}).get(key, [])
        if isinstance(item, dict)
    }
    selected: list[dict[str, Any]] = []
    for item in ranked:
        if item.get("bucket") == "ready_for_m3":
            selected.append(item)
    for item in ranked:
        if str(item.get("rank_id") or "") in audit_refs:
            selected.append(item)
    for item in ranked:
        if item.get("bucket") in {"public_interest_but_unready", "watchlist"}:
            selected.append(item)
    return _unique_ranked(selected)


def _balanced_review_items(
    ranked_items: list[dict[str, Any]],
    candidate_items: list[dict[str, Any]],
    cross_impact_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build a compact but balanced queue for human/LLM review."""
    review_items: list[dict[str, Any]] = []
    seen: set[str] = set()

    indicator_items = [
        item for item in candidate_items if item.get("item_type") == "indicator_seed"
    ]
    event_items = [
        item
        for item in candidate_items
        if item.get("item_type") not in {"indicator_seed", "legislative_bill"}
    ]

    for group, limit in (
        (ranked_items, MAX_LEGISLATIVE_REVIEW_ITEMS),
        (indicator_items, MAX_INDICATOR_REVIEW_ITEMS),
        (event_items, MAX_EVENT_REVIEW_ITEMS),
        (cross_impact_items, MAX_CROSS_IMPACT_ITEMS),
    ):
        added = 0
        for item in group:
            if added >= limit:
                break
            before = len(review_items)
            _append_review_item(review_items, seen, item)
            if len(review_items) > before:
                added += 1

    for item in [*candidate_items, *cross_impact_items]:
        if len(review_items) >= MAX_REVIEW_ITEMS:
            break
        _append_review_item(review_items, seen, item)

    return review_items[:MAX_REVIEW_ITEMS]


def _review_item_from_ranked(
    ranked: dict[str, Any],
    legislative_by_id: dict[str, dict[str, Any]],
    indexes: _EvidenceIndexes,
) -> dict[str, Any]:
    canonical_id = str(ranked.get("canonical_bill_id") or "")
    record = legislative_by_id.get(canonical_id, {})
    source_evidence = [
        item for item in record.get("source_evidence") or [] if isinstance(item, dict)
    ]
    urls = [str(item.get("url") or "") for item in source_evidence]
    excerpts = _source_excerpts(indexes.by_urls(urls), preferred_urls=urls)
    if not excerpts and source_evidence:
        excerpts = [
            _summary_excerpt(item)
            for item in source_evidence[:MAX_EXCERPTS_PER_ITEM]
        ]
    return {
        "packet_item_id": _packet_item_id(
            "ranked",
            ranked.get("rank_id") or canonical_id,
        ),
        "item_type": "legislative_ranked_record",
        "origin_id": canonical_id,
        "question_seed": ranked.get("question_seed") or "",
        "recommendation": ranked.get("recommendation") or "",
        "bucket": ranked.get("bucket") or "",
        "heuristic_score": ranked.get("overall_score"),
        "heuristic_reasons": list(ranked.get("score_reasons") or []),
        "heuristic_penalties": list(ranked.get("penalties") or []),
        "heuristic_risk_flags": list(ranked.get("heuristic_risk_flags") or []),
        "llm_review_hint": ranked.get("llm_review_hint") or "",
        "missing_evidence": list(ranked.get("missing_evidence") or []),
        "source_ids": list(ranked.get("source_ids") or []),
        "source_urls": _unique_preserve_order(urls),
        "source_excerpts": excerpts,
        "traceability": _traceability(
            [
                {
                    "artifact": "m2_ranked_questions.json",
                    "key": "rank_id",
                    "value": str(ranked.get("rank_id") or ""),
                },
                {
                    "artifact": "legislative_reconciler.json",
                    "key": "canonical_bill_id",
                    "value": canonical_id,
                },
            ],
            excerpts,
            urls,
        ),
        "structured_context": {
            "ranked_record": _compact_dict(ranked),
            "legislative_reconciler_record": _compact_dict(record),
        },
    }


def _review_item_from_candidate(
    candidate: dict[str, Any],
    indicator_by_id: dict[str, IndicatorObservation],
    indexes: _EvidenceIndexes,
) -> dict[str, Any]:
    evidence = (
        candidate.get("evidence")
        if isinstance(candidate.get("evidence"), dict)
        else {}
    )
    item_ids = [str(item_id) for item_id in evidence.get("item_ids") or []]
    links = [link for link in evidence.get("links") or [] if isinstance(link, dict)]
    urls = [str(link.get("url") or "") for link in links]
    excerpts = _source_excerpts(
        indexes.by_item_ids(item_ids) + indexes.by_urls(urls),
        preferred_urls=urls,
    )
    structured_context: dict[str, Any] = {"candidate": _compact_dict(candidate)}
    indicator = indicator_by_id.get(str(candidate.get("origin_id") or ""))
    if indicator is not None:
        structured_context["indicator_observation"] = asdict(indicator)
        excerpts.append(_indicator_excerpt(indicator))
    artifact_refs = [
        {
            "artifact": "m1_candidates.json",
            "key": "candidate_id",
            "value": str(candidate.get("candidate_id") or ""),
        }
    ]
    if indicator is not None:
        artifact_refs.append(
            {
                "artifact": "indicator_watch.json",
                "key": "indicator_id",
                "value": indicator.indicator_id,
            }
        )
    if item_ids:
        artifact_refs.append(
            {
                "artifact": "raw_items.json / cleaned_items.json",
                "key": "item_ids",
                "value": item_ids,
            }
        )
    return {
        "packet_item_id": _packet_item_id(
            "candidate",
            candidate.get("candidate_id") or candidate.get("origin_id") or "",
        ),
        "item_type": str(candidate.get("candidate_type") or "candidate"),
        "origin_id": str(candidate.get("origin_id") or ""),
        "question_seed": str(candidate.get("question_seed") or ""),
        "recommendation": str(candidate.get("decision_hint") or "candidate"),
        "bucket": "m1_candidate",
        "heuristic_score": (candidate.get("m1_scores") or {}).get(
            "forecastability_score"
        ),
        "heuristic_reasons": list(candidate.get("reasons") or []),
        "heuristic_penalties": list(candidate.get("noise_reasons") or []),
        "heuristic_risk_flags": [],
        "llm_review_hint": (
            "Read the source excerpts and decide whether this M1 candidate is "
            "actually public-interest and forecastable."
        ),
        "missing_evidence": list(candidate.get("missing_evidence") or []),
        "source_ids": list(candidate.get("source_ids") or []),
        "source_urls": _unique_preserve_order(urls),
        "source_excerpts": excerpts[:MAX_EXCERPTS_PER_ITEM],
        "traceability": _traceability(
            artifact_refs,
            excerpts[:MAX_EXCERPTS_PER_ITEM],
            urls,
        ),
        "structured_context": structured_context,
    }


def _cross_impact_items(
    ranked_records: list[dict[str, Any]],
    legislative_by_id: dict[str, dict[str, Any]],
    indicator_by_id: dict[str, IndicatorObservation],
    indexes: _EvidenceIndexes,
) -> list[dict[str, Any]]:
    observed_indicators = {
        indicator_id: indicator
        for indicator_id, indicator in indicator_by_id.items()
        if indicator.status == "observed"
    }
    if not observed_indicators:
        return []

    items: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for ranked in ranked_records:
        canonical_id = str(ranked.get("canonical_bill_id") or "")
        if not canonical_id:
            continue
        record = legislative_by_id.get(canonical_id, {})
        text = _cross_impact_search_text(ranked, record)
        if not text:
            continue
        for rule in CROSS_IMPACT_RULES:
            if not any(keyword in text for keyword in rule["keywords"]):
                continue
            for indicator_id in rule["indicator_ids"]:
                indicator = observed_indicators.get(indicator_id)
                if indicator is None:
                    continue
                pair = (canonical_id, indicator_id)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                items.append(
                    _review_item_from_cross_impact(
                        ranked,
                        record,
                        indicator,
                        str(rule["reason"]),
                        indexes,
                    )
                )
                break
            break
        if len(items) >= MAX_CROSS_IMPACT_ITEMS:
            break
    return items


def _review_item_from_cross_impact(
    ranked: dict[str, Any],
    record: dict[str, Any],
    indicator: IndicatorObservation,
    reason: str,
    indexes: _EvidenceIndexes,
) -> dict[str, Any]:
    canonical_id = str(ranked.get("canonical_bill_id") or "")
    title = str(
        ranked.get("display_title")
        or record.get("display_title")
        or canonical_id
        or "legislative item"
    )
    source_evidence = [
        item for item in record.get("source_evidence") or [] if isinstance(item, dict)
    ]
    urls = [str(item.get("url") or "") for item in source_evidence]
    legislative_excerpts = _source_excerpts(indexes.by_urls(urls), preferred_urls=urls)
    if not legislative_excerpts and source_evidence:
        legislative_excerpts = [
            _summary_excerpt(item)
            for item in source_evidence[: MAX_EXCERPTS_PER_ITEM - 1]
        ]
    excerpts = [
        *legislative_excerpts[: MAX_EXCERPTS_PER_ITEM - 1],
        _indicator_excerpt(indicator),
    ][:MAX_EXCERPTS_PER_ITEM]
    question_seed = (
        f"Should {title} be reviewed alongside {indicator.name} because {reason}?"
    )
    indicator_correlations = list(indicator.correlations or [])[:2]
    reasons = [
        "Advisory cross-impact hypothesis generated from existing metadata.",
        f"Potential link: {reason}.",
    ]
    if indicator_correlations:
        reasons.append(
            "Indicator context: " + "; ".join(indicator_correlations)
        )
    return {
        "packet_item_id": _packet_item_id(
            "cross",
            f"{canonical_id}:{indicator.indicator_id}:{reason}",
        ),
        "item_type": "cross_impact_hypothesis",
        "origin_id": f"cross:{canonical_id}:{indicator.indicator_id}",
        "question_seed": question_seed,
        "recommendation": "review_hypothesis",
        "bucket": "cross_domain_hypothesis",
        "heuristic_score": None,
        "heuristic_reasons": reasons,
        "heuristic_penalties": [
            "This is not causal evidence and should not set a probability.",
            "Needs timing, mechanism, and resolution criteria before M3.",
        ],
        "heuristic_risk_flags": ["advisory_cross_impact"],
        "llm_review_hint": (
            "Use this only to decide whether the legal item and indicator "
            "should be researched together; do not treat it as a causal claim."
        ),
        "missing_evidence": [
            "causal mechanism",
            "timing alignment",
            "forecastable threshold or resolution source",
        ],
        "source_ids": _unique_preserve_order(
            [
                *[str(item.get("source_id") or "") for item in source_evidence],
                indicator.indicator_id,
            ]
        ),
        "source_urls": _unique_preserve_order([*urls, indicator.source_url]),
        "source_excerpts": excerpts,
        "traceability": _traceability(
            [
                {
                    "artifact": "m2_ranked_questions.json",
                    "key": "rank_id",
                    "value": str(ranked.get("rank_id") or ""),
                },
                {
                    "artifact": "legislative_reconciler.json",
                    "key": "canonical_bill_id",
                    "value": canonical_id,
                },
                {
                    "artifact": "indicator_watch.json",
                    "key": "indicator_id",
                    "value": indicator.indicator_id,
                },
            ],
            excerpts,
            [*urls, indicator.source_url],
        ),
        "structured_context": {
            "hypothesis": {
                "relationship": reason,
                "review_policy": "advisory_only_not_causal_evidence",
            },
            "ranked_record": _compact_dict(ranked),
            "legislative_reconciler_record": _compact_dict(record),
            "indicator_observation": asdict(indicator),
        },
    }


def _append_review_item(
    output: list[dict[str, Any]],
    seen: set[str],
    item: dict[str, Any],
) -> None:
    if len(output) >= MAX_REVIEW_ITEMS:
        return
    keys = _review_item_keys(item)
    if not keys or any(key in seen for key in keys):
        return
    seen.update(keys)
    output.append(item)


def _review_item_keys(item: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    packet_id = str(item.get("packet_item_id") or "")
    origin_id = str(item.get("origin_id") or "")
    question_seed = normalize_whitespace(str(item.get("question_seed") or "")).lower()
    if packet_id:
        keys.add(f"packet:{packet_id}")
    if origin_id:
        keys.add(f"origin:{origin_id}")
    if question_seed:
        keys.add(f"question:{question_seed}")
    return keys


def _cross_impact_search_text(ranked: dict[str, Any], record: dict[str, Any]) -> str:
    parts = [
        ranked.get("display_title"),
        ranked.get("question_seed"),
        " ".join(str(item) for item in ranked.get("public_interest_signals") or []),
        record.get("display_title"),
        record.get("title_normalized"),
    ]
    return normalize_whitespace(" ".join(str(part or "") for part in parts)).lower()


def _traceability(
    artifact_refs: list[dict[str, Any]],
    excerpts: list[dict[str, Any]],
    source_urls: list[str],
) -> dict[str, Any]:
    source_item_ids = _unique_preserve_order(
        [
            str(excerpt.get("item_id") or "")
            for excerpt in excerpts
            if isinstance(excerpt, dict) and excerpt.get("item_id")
        ]
    )
    return {
        "artifact_refs": [
            ref
            for ref in artifact_refs
            if isinstance(ref, dict) and str(ref.get("value") or "").strip()
        ],
        "source_item_ids": source_item_ids,
        "source_urls": _unique_preserve_order(source_urls),
    }


def _source_excerpts(
    pairs: list[tuple[RawItem | None, CleanedItem | None]],
    preferred_urls: list[str],
) -> list[dict[str, Any]]:
    excerpts: list[dict[str, Any]] = []
    seen: set[str] = set()
    preferred = {_defrag(url) for url in preferred_urls if url}
    sorted_pairs = sorted(
        pairs,
        key=lambda pair: (
            0 if _pair_url(pair) in preferred else 1,
            _pair_source_id(pair),
            _pair_title(pair),
        ),
    )
    for raw, cleaned in sorted_pairs:
        item_id = (
            raw.id
            if raw is not None
            else cleaned.id
            if cleaned is not None
            else ""
        )
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        excerpt = _item_excerpt(raw, cleaned)
        if excerpt:
            excerpts.append(excerpt)
        if len(excerpts) >= MAX_EXCERPTS_PER_ITEM:
            break
    return excerpts


def _item_excerpt(raw: RawItem | None, cleaned: CleanedItem | None) -> dict[str, Any]:
    item = raw or cleaned
    if item is None:
        return {}
    metadata = dict((raw.metadata if raw is not None else cleaned.metadata) or {})
    clean_text = cleaned.clean_text if cleaned is not None else ""
    raw_text = raw.raw_text if raw is not None else ""
    source_type = item.source_type
    text = clean_text or raw_text or item.title
    max_chars = _excerpt_limit(source_type, metadata)
    excerpt, truncated = _clip_text(text, max_chars)
    return {
        "item_id": item.id,
        "source_id": item.source_id,
        "source_name": item.source_name,
        "source_type": source_type,
        "title": item.title,
        "url": item.url,
        "published_at": item.published_at,
        "content_kind": _content_kind(item.url, metadata),
        "metadata_hints": _metadata_hints(metadata),
        "char_count": len(text),
        "excerpt_char_count": len(excerpt),
        "truncated": truncated,
        "excerpt": excerpt,
    }


def _summary_excerpt(evidence: dict[str, Any]) -> dict[str, Any]:
    summary = normalize_whitespace(str(evidence.get("summary") or ""))
    excerpt, truncated = _clip_text(summary, STRUCTURED_EXCERPT_CHARS)
    return {
        "item_id": "",
        "source_id": str(evidence.get("source_id") or ""),
        "source_name": str(evidence.get("source_id") or ""),
        "source_type": "structured_summary",
        "title": str(evidence.get("role") or "source evidence"),
        "url": str(evidence.get("url") or ""),
        "published_at": str(evidence.get("date") or ""),
        "content_kind": "structured_summary",
        "metadata_hints": {"role": str(evidence.get("role") or "")},
        "char_count": len(summary),
        "excerpt_char_count": len(excerpt),
        "truncated": truncated,
        "excerpt": excerpt,
    }


def _indicator_excerpt(indicator: IndicatorObservation) -> dict[str, Any]:
    text = "\n".join(
        [
            indicator.headline,
            f"Period: {indicator.period}",
            f"Values: {indicator.values}",
            f"Why it matters: {indicator.why_it_matters}",
            f"Next step: {indicator.next_step}",
        ]
    )
    excerpt, truncated = _clip_text(text, STRUCTURED_EXCERPT_CHARS)
    return {
        "item_id": indicator.indicator_id,
        "source_id": indicator.indicator_id,
        "source_name": indicator.source_name,
        "source_type": "structured_indicator",
        "title": indicator.name,
        "url": indicator.source_url,
        "published_at": indicator.release_date,
        "content_kind": "structured_indicator",
        "metadata_hints": {"period": indicator.period, "status": indicator.status},
        "char_count": len(text),
        "excerpt_char_count": len(excerpt),
        "truncated": truncated,
        "excerpt": excerpt,
    }


def _excerpt_limit(source_type: str, metadata: dict[str, Any]) -> int:
    if source_type in NEWS_SOURCE_TYPES:
        return MEDIA_EXCERPT_CHARS
    if metadata.get("content_extraction") or metadata.get("parsed_content"):
        return OFFICIAL_EXCERPT_CHARS
    return STRUCTURED_EXCERPT_CHARS


def _content_kind(url: str, metadata: dict[str, Any]) -> str:
    if metadata.get("content_extraction") or metadata.get("parsed_content"):
        return "parsed_content"
    if metadata.get("extraction"):
        return str(metadata.get("extraction"))
    lower = url.lower()
    if ".pdf" in lower:
        return "pdf_link"
    if any(ext in lower for ext in (".xlsx", ".xls", ".csv")):
        return "spreadsheet_link"
    return "html_or_api"


def _metadata_hints(metadata: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "content_extraction",
        "parsed_content",
        "extraction",
        "document_title",
        "agenda_action_type",
        "project_label",
        "status",
        "edition_number",
    ]
    return {key: metadata[key] for key in keys if key in metadata and metadata[key]}


def _source_caveats(source_health: list[SourceHealth]) -> list[dict[str, str]]:
    caveats: list[dict[str, str]] = []
    for health in source_health:
        if health.failure_count:
            caveats.append(
                {
                    "source_id": health.source_id,
                    "reason": "source failed; silence is not evidence of no activity",
                    "content_mode": health.content_mode,
                }
            )
        elif health.document_link_count and health.parsed_content_count == 0:
            caveats.append(
                {
                    "source_id": health.source_id,
                    "reason": "document links were found but content was not parsed",
                    "content_mode": health.content_mode,
                }
            )
    return caveats


def _render_tension_card(index: int, card: dict[str, Any]) -> list[str]:
    lines = [
        f"### {index}. {card.get('title', card.get('card_id', 'Indicator tension'))}",
        "",
        f"- Card id: `{card.get('card_id', '')}`",
        f"- Family: `{card.get('family', '')}`",
        f"- Severity: `{card.get('severity', 'review')}`",
        f"- Trigger: {card.get('trigger', '')}",
        f"- Why it matters: {card.get('why_it_matters', '')}",
        f"- Agent prompt: {card.get('agent_prompt', '')}",
        "",
    ]
    evidence = [item for item in card.get("evidence") or [] if isinstance(item, dict)]
    if evidence:
        lines.append("Evidence:")
        for item in evidence[:8]:
            label = str(item.get("label") or "")
            value = str(item.get("value") or "")
            source = str(item.get("source") or "")
            lines.append(f"- {label}: {value} ({source})")
        lines.append("")
    caveats = [str(caveat) for caveat in card.get("caveats") or []]
    if caveats:
        lines.append("Caveats:")
        lines.extend(f"- {caveat}" for caveat in caveats)
        lines.append("")
    questions = [str(question) for question in card.get("suggested_questions") or []]
    if questions:
        lines.append("Suggested questions:")
        lines.extend(f"- {question}" for question in questions)
        lines.append("")
    return lines


def _render_review_item(index: int, item: dict[str, Any]) -> list[str]:
    score = item.get("heuristic_score")
    score_text = "n/a" if score is None else str(score)
    lines = [
        f"### {index}. {item.get('question_seed', item.get('origin_id', ''))}",
        "",
        f"- Type: `{item.get('item_type', '')}`",
        f"- Bucket: `{item.get('bucket', '')}`",
        f"- Recommendation: `{item.get('recommendation', '')}`",
        f"- Heuristic score: `{score_text}`",
        f"- Risk flags: "
        f"{', '.join(item.get('heuristic_risk_flags') or []) or 'none'}",
        f"- LLM review hint: {item.get('llm_review_hint', '')}",
        "",
    ]
    if item.get("heuristic_reasons"):
        lines.append("Reasons surfaced:")
        lines.extend(f"- {reason}" for reason in item["heuristic_reasons"])
        lines.append("")
    if item.get("heuristic_penalties"):
        lines.append("Reasons to be skeptical:")
        lines.extend(f"- {reason}" for reason in item["heuristic_penalties"])
        lines.append("")
    if item.get("missing_evidence"):
        lines.append("Missing evidence:")
        lines.extend(f"- {reason}" for reason in item["missing_evidence"])
        lines.append("")
    traceability = item.get("traceability")
    if isinstance(traceability, dict):
        trace_lines = _render_traceability(traceability)
        if trace_lines:
            lines.extend(trace_lines)
    excerpts = [ex for ex in item.get("source_excerpts") or [] if isinstance(ex, dict)]
    if excerpts:
        lines.append("Source excerpts:")
        lines.append("")
        for excerpt in excerpts:
            lines.extend(_render_excerpt(excerpt))
    else:
        lines.extend(
            [
                "Source excerpts:",
                "",
                "- _No parsed source excerpt matched this item._",
                "",
            ]
        )
    return lines


def _render_traceability(traceability: dict[str, Any]) -> list[str]:
    lines = ["Original artifacts:"]
    artifact_refs = [
        ref for ref in traceability.get("artifact_refs") or [] if isinstance(ref, dict)
    ]
    for ref in artifact_refs[:6]:
        artifact = str(ref.get("artifact") or "")
        key = str(ref.get("key") or "")
        value = ref.get("value")
        if isinstance(value, list):
            value_text = ", ".join(str(item) for item in value[:8])
        else:
            value_text = str(value or "")
        if artifact and key and value_text:
            lines.append(f"- `{artifact}`: `{key}={value_text}`")
    item_ids = traceability.get("source_item_ids") or []
    if item_ids:
        lines.append(
            "- `raw_items.json` / `cleaned_items.json`: "
            f"`item_ids={', '.join(str(item_id) for item_id in item_ids[:8])}`"
        )
    source_urls = traceability.get("source_urls") or []
    if source_urls:
        lines.append(f"- source URLs: {len(source_urls)}")
    if len(lines) == 1:
        return []
    lines.append("")
    return lines


def _render_excerpt(excerpt: dict[str, Any]) -> list[str]:
    title = str(excerpt.get("title") or excerpt.get("source_id") or "source")
    url = str(excerpt.get("url") or "")
    suffix = " (truncated)" if excerpt.get("truncated") else ""
    lines = [
        f"#### {title}",
        "",
        f"- Source: `{excerpt.get('source_id', '')}`",
        f"- Published: `{excerpt.get('published_at', '')}`",
        f"- Content kind: `{excerpt.get('content_kind', '')}`{suffix}",
    ]
    if url:
        lines.append(f"- URL: {url}")
    lines.extend(["", "```text", str(excerpt.get("excerpt") or ""), "```", ""])
    return lines


def _clip_text(text: str, max_chars: int) -> tuple[str, bool]:
    clean = normalize_whitespace(text)
    if len(clean) <= max_chars:
        return clean, False
    return clean[: max_chars - 20].rstrip() + " ... [truncated]", True


def _url_keys(url: str) -> list[str]:
    clean = str(url or "").strip()
    if not clean:
        return []
    no_fragment = _defrag(clean)
    return _unique_preserve_order([clean, no_fragment])


def _defrag(url: str) -> str:
    return urldefrag(str(url or "").strip()).url


def _pair_url(pair: tuple[RawItem | None, CleanedItem | None]) -> str:
    raw, cleaned = pair
    item = raw or cleaned
    return _defrag(item.url if item is not None else "")


def _pair_source_id(pair: tuple[RawItem | None, CleanedItem | None]) -> str:
    raw, cleaned = pair
    item = raw or cleaned
    return item.source_id if item is not None else ""


def _pair_title(pair: tuple[RawItem | None, CleanedItem | None]) -> str:
    raw, cleaned = pair
    item = raw or cleaned
    return item.title if item is not None else ""


def _packet_item_id(kind: str, seed: object) -> str:
    digest = hashlib.sha1(f"{kind}\x1f{seed}".encode("utf-8")).hexdigest()[:12]
    return f"m2pkt_{digest}"


def _unique_ranked(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = str(item.get("rank_id") or item.get("canonical_bill_id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _unique_preserve_order(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, item in value.items():
        if key == "source_excerpts":
            continue
        if item is None or item == "" or item == [] or item == {}:
            continue
        output[key] = item
    return output
