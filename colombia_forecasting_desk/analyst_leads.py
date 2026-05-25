from __future__ import annotations

import hashlib
import re
from typing import Any

from .cleaner import normalize_whitespace
from .models import RunSummary

SCHEMA_VERSION = "analyst_leads.v1"
MAX_FORECAST_QUESTIONS = 4
MAX_ANALYST_INSIGHTS = 6
MAX_INVESTIGATION_LEADS = 5
MAX_EVIDENCE_ITEMS = 4
MAX_CAVEATS = 6

OUTPUT_CONTRACT: dict[str, dict[str, Any]] = {
    "forecast_question": {
        "definition": (
            "A source-backed question that appears ready for M3 evidence-pack "
            "work because it has a concrete future resolution path."
        ),
        "required_fields": [
            "claim_or_question",
            "evidence",
            "caveats",
            "next_check",
            "disposition",
        ],
        "promotion_rule": (
            "Only ready_for_m3 or select_for_evidence_pack M2 items with "
            "source evidence can become forecast_question leads."
        ),
    },
    "analyst_insight": {
        "definition": (
            "A descriptive, source-backed finding that matters but does not "
            "need to be forced into a yes/no forecast question."
        ),
        "required_fields": [
            "claim_or_question",
            "evidence",
            "caveats",
            "next_check",
            "disposition",
        ],
        "promotion_rule": (
            "May cite deterministic screens or source-backed patterns, but "
            "must not receive a probability or forecast-log treatment."
        ),
    },
    "investigation_lead": {
        "definition": (
            "A plausible lead that needs more research before it can become an "
            "insight or forecast question."
        ),
        "required_fields": [
            "claim_or_question",
            "evidence",
            "caveats",
            "next_check",
            "disposition",
        ],
        "promotion_rule": (
            "Use when timing, resolution criteria, source coverage, or causal "
            "mechanism is still underqualified."
        ),
    },
}


def build_analyst_leads(
    run_summary: RunSummary,
    m2_review_packet: dict[str, Any],
    indicator_tension_cards: list[dict[str, Any]] | None = None,
    procurement_concentration_leads: list[dict[str, Any]] | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the first human-facing output surface after M2 packet assembly."""
    review_items = [
        item
        for item in m2_review_packet.get("review_items") or []
        if isinstance(item, dict)
    ]
    tension_cards = [
        card for card in indicator_tension_cards or [] if isinstance(card, dict)
    ]
    procurement_leads = [
        lead
        for lead in procurement_concentration_leads or []
        if isinstance(lead, dict) and lead.get("lead_type") == "analyst_insight"
    ]

    forecast_questions = _forecast_question_leads(review_items)
    analyst_insights = _analyst_insight_leads(tension_cards, procurement_leads)
    investigation_leads = _investigation_leads(review_items)
    leads = [*forecast_questions, *analyst_insights, *investigation_leads]

    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "generated_at": generated_at or run_summary.finished_at,
        "policy": {
            "purpose": (
                "Separate forecastable questions from non-forecast insights "
                "and early investigation leads before M3 work."
            ),
            "not_allowed": [
                "Do not add analyst_insight or investigation_lead items to the forecast log.",
                "Do not assign probabilities outside an M3 Case File.",
                "Do not treat deterministic tension cards as conclusions.",
            ],
            "output_contract": OUTPUT_CONTRACT,
        },
        "inputs": {
            "m2_review_packet_artifact": "m2_review_packet.json",
            "indicator_tension_cards_artifact": "indicator_tension_cards.json",
            "m1_candidates_artifact": "m1_candidates.json",
            "m2_ranked_questions_artifact": "m2_ranked_questions.json",
            "indicator_watch_artifact": "indicator_watch.json",
            "raw_items_artifact": "raw_items.json",
            "cleaned_items_artifact": "cleaned_items.json",
        },
        "summary": {
            "lead_count": len(leads),
            "forecast_question_count": len(forecast_questions),
            "analyst_insight_count": len(analyst_insights),
            "investigation_lead_count": len(investigation_leads),
            "review_item_count": len(review_items),
            "indicator_tension_card_count": len(tension_cards),
            "procurement_concentration_lead_count": len(procurement_leads),
        },
        "leads": leads,
    }


def render_analyst_leads(payload: dict[str, Any]) -> str:
    """Render analyst leads as a compact human review artifact."""
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    leads = [lead for lead in payload.get("leads") or [] if isinstance(lead, dict)]
    by_type = {
        lead_type: [lead for lead in leads if lead.get("lead_type") == lead_type]
        for lead_type in OUTPUT_CONTRACT
    }

    lines = [
        f"# Analyst Leads - {payload.get('run_date', '')}",
        "",
        (
            "This artifact separates forecastable questions from source-backed "
            "insights and early investigation leads."
        ),
        "",
        "Insights and investigation leads are not forecast-log entries and "
        "should not receive probabilities until an M3 Case File says they are ready.",
        "",
        "## Output Contract",
        "",
    ]
    for lead_type, contract in OUTPUT_CONTRACT.items():
        lines.append(f"- `{lead_type}`: {contract['definition']}")
    lines.extend(
        [
            "",
            "Required fields for every lead: `claim_or_question`, `evidence`, "
            "`caveats`, `next_check`, and `disposition`.",
            "",
            "## Summary",
            "",
            f"- Total leads: {summary.get('lead_count', 0)}",
            f"- Forecast questions: {summary.get('forecast_question_count', 0)}",
            f"- Analyst insights: {summary.get('analyst_insight_count', 0)}",
            f"- Investigation leads: {summary.get('investigation_lead_count', 0)}",
            "",
        ]
    )

    sections = (
        ("Forecast Questions", "forecast_question"),
        ("Analyst Insights", "analyst_insight"),
        ("Investigation Leads", "investigation_lead"),
    )
    for title, lead_type in sections:
        lines.extend([f"## {title}", ""])
        section_leads = by_type.get(lead_type) or []
        if not section_leads:
            lines.append("No leads in this class.")
            lines.append("")
            continue
        for index, lead in enumerate(section_leads, 1):
            lines.extend(_render_lead(index, lead))
    return "\n".join(lines).rstrip() + "\n"


def _forecast_question_leads(review_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    leads: list[dict[str, Any]] = []
    for item in review_items:
        if len(leads) >= MAX_FORECAST_QUESTIONS:
            break
        if not _ready_for_forecast_question(item):
            continue
        leads.append(_lead_from_review_item(item, "forecast_question"))
    return leads


def _analyst_insight_leads(
    tension_cards: list[dict[str, Any]],
    procurement_leads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    leads: list[dict[str, Any]] = []
    for card in tension_cards:
        if len(leads) >= MAX_ANALYST_INSIGHTS:
            break
        leads.append(_lead_from_tension_card(card))
    for lead in procurement_leads:
        if len(leads) >= MAX_ANALYST_INSIGHTS:
            break
        leads.append(lead)
    return leads


def _investigation_leads(review_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    leads: list[dict[str, Any]] = []
    seen_origins = {
        _core_origin(str(item.get("origin_id") or ""))
        for item in review_items
        if _ready_for_forecast_question(item)
    }
    candidates = sorted(
        (
            (priority, index, item)
            for index, item in enumerate(review_items)
            if (priority := _investigation_priority(item)) is not None
        ),
        key=lambda entry: (entry[0], entry[1]),
    )
    for _, _, item in candidates:
        if len(leads) >= MAX_INVESTIGATION_LEADS:
            break
        origin_id = _core_origin(str(item.get("origin_id") or ""))
        if origin_id and origin_id in seen_origins:
            continue
        if _ready_for_forecast_question(item):
            continue
        leads.append(_lead_from_review_item(item, "investigation_lead"))
        if origin_id:
            seen_origins.add(origin_id)
    return leads


def _ready_for_forecast_question(item: dict[str, Any]) -> bool:
    recommendation = str(item.get("recommendation") or "")
    bucket = str(item.get("bucket") or "")
    has_evidence = bool(item.get("source_excerpts"))
    return has_evidence and (
        recommendation == "select_for_evidence_pack" or bucket == "ready_for_m3"
    )


def _investigation_priority(item: dict[str, Any]) -> int | None:
    item_type = str(item.get("item_type") or "")
    if item_type == "cross_impact_hypothesis":
        return 0
    risk_flags = {str(flag) for flag in item.get("heuristic_risk_flags") or []}
    if "possible_false_negative_public_interest" in risk_flags:
        return 1
    if str(item.get("bucket") or "") == "public_interest_but_unready":
        return 2
    return None


def _lead_from_review_item(
    item: dict[str, Any],
    lead_type: str,
) -> dict[str, Any]:
    question = _clean_text(str(item.get("question_seed") or ""))
    source_excerpts = [
        excerpt
        for excerpt in item.get("source_excerpts") or []
        if isinstance(excerpt, dict)
    ]
    traceability = (
        item.get("traceability") if isinstance(item.get("traceability"), dict) else {}
    )
    missing = [str(value) for value in item.get("missing_evidence") or [] if value]
    penalties = [str(value) for value in item.get("heuristic_penalties") or [] if value]
    risk_flags = [str(value) for value in item.get("heuristic_risk_flags") or [] if value]

    if lead_type == "forecast_question":
        disposition = "select_for_evidence_pack"
        next_check = (
            _clean_text(str(item.get("llm_review_hint") or ""))
            or "Build an M3 Case File with resolution criteria and deadline."
        )
    else:
        disposition = "research_more_before_m3"
        next_check = (
            _clean_text(str(item.get("llm_review_hint") or ""))
            or _first_nonempty(missing)
            or "Find stronger source evidence, timing, and resolution criteria."
        )

    return {
        "lead_id": _lead_id(lead_type, str(item.get("packet_item_id") or question)),
        "lead_type": lead_type,
        "title": _lead_title(item, question),
        "claim_or_question": question,
        "disposition": disposition,
        "evidence": _evidence_from_excerpts(source_excerpts),
        "caveats": _compact_strings([*missing, *penalties, *risk_flags], MAX_CAVEATS),
        "next_check": next_check,
        "source_refs": {
            "artifact_refs": list(traceability.get("artifact_refs") or []),
            "source_item_ids": list(traceability.get("source_item_ids") or []),
            "source_urls": list(traceability.get("source_urls") or []),
        },
        "review_context": {
            "item_type": str(item.get("item_type") or ""),
            "origin_id": str(item.get("origin_id") or ""),
            "bucket": str(item.get("bucket") or ""),
            "recommendation": str(item.get("recommendation") or ""),
        },
    }


def _lead_from_tension_card(card: dict[str, Any]) -> dict[str, Any]:
    card_id = str(card.get("card_id") or "indicator_tension")
    caveats = [
        *[str(value) for value in card.get("caveats") or [] if value],
        str(card.get("review_policy") or ""),
    ]
    return {
        "lead_id": _lead_id("analyst_insight", card_id),
        "lead_type": "analyst_insight",
        "title": str(card.get("title") or card_id),
        "claim_or_question": _clean_text(str(card.get("trigger") or "")),
        "disposition": "monitor_or_research",
        "evidence": _evidence_from_card(card),
        "caveats": _compact_strings(caveats, MAX_CAVEATS),
        "next_check": _clean_text(str(card.get("agent_prompt") or "")),
        "source_refs": {
            "artifact_refs": [
                {
                    "artifact": "indicator_tension_cards.json",
                    "key": "card_id",
                    "value": card_id,
                }
            ],
            "source_refs": list(card.get("source_refs") or []),
            "source_urls": _source_urls_from_card(card),
        },
        "review_context": {
            "family": str(card.get("family") or ""),
            "severity": str(card.get("severity") or "review"),
            "suggested_questions": list(card.get("suggested_questions") or []),
        },
    }


def _evidence_from_excerpts(
    excerpts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for excerpt in excerpts[:MAX_EVIDENCE_ITEMS]:
        title = str(excerpt.get("title") or excerpt.get("source_name") or "Source")
        content_kind = str(excerpt.get("content_kind") or "")
        value = _evidence_excerpt_value(str(excerpt.get("excerpt") or ""), content_kind)
        evidence.append(
            {
                "label": title,
                "value": _trim(value, 700),
                "source": str(excerpt.get("source_name") or ""),
                "url": str(excerpt.get("url") or ""),
                "item_id": str(excerpt.get("item_id") or ""),
                "content_kind": content_kind,
            }
        )
    return evidence


def _evidence_excerpt_value(excerpt: str, content_kind: str) -> str:
    text = _clean_text(excerpt)
    if content_kind == "structured_indicator" and " Values:" in text:
        text = text.split(" Values:", 1)[0]
    return text


def _evidence_from_card(card: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for item in card.get("evidence") or []:
        if not isinstance(item, dict):
            continue
        evidence.append(
            {
                "label": str(item.get("label") or ""),
                "value": str(item.get("value") or ""),
                "source": str(item.get("source") or ""),
                "period": str(item.get("period") or ""),
                "url": str(item.get("url") or ""),
            }
        )
        if len(evidence) >= MAX_EVIDENCE_ITEMS:
            break
    return evidence


def _source_urls_from_card(card: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for item in [*list(card.get("evidence") or []), *list(card.get("source_refs") or [])]:
        if isinstance(item, dict) and item.get("url"):
            urls.append(str(item["url"]))
        if isinstance(item, dict) and item.get("source_url"):
            urls.append(str(item["source_url"]))
    return _unique_preserve_order(urls)


def _render_lead(index: int, lead: dict[str, Any]) -> list[str]:
    label = "Question" if lead.get("lead_type") == "forecast_question" else "Claim"
    lines = [
        f"### {index}. {lead.get('title', lead.get('lead_id', 'Lead'))}",
        "",
        f"- Lead id: `{lead.get('lead_id', '')}`",
        f"- Disposition: `{lead.get('disposition', '')}`",
        f"- {label}: {lead.get('claim_or_question', '')}",
        f"- Next check: {lead.get('next_check', '')}",
        "",
    ]
    evidence = [item for item in lead.get("evidence") or [] if isinstance(item, dict)]
    if evidence:
        lines.extend(["Evidence:", ""])
        for item in evidence:
            source = str(item.get("source") or "").strip()
            value = str(item.get("value") or "").strip()
            label_text = str(item.get("label") or "").strip()
            if source:
                lines.append(f"- {label_text}: {value} ({source})")
            else:
                lines.append(f"- {label_text}: {value}")
        lines.append("")
    caveats = [str(item) for item in lead.get("caveats") or [] if item]
    if caveats:
        lines.extend(["Caveats:", ""])
        lines.extend(f"- {caveat}" for caveat in caveats)
        lines.append("")
    return lines


def _lead_title(item: dict[str, Any], fallback: str) -> str:
    structured = item.get("structured_context")
    if isinstance(structured, dict):
        ranked = structured.get("ranked_record")
        if isinstance(ranked, dict) and ranked.get("display_title"):
            return str(ranked["display_title"])
        candidate = structured.get("candidate")
        if isinstance(candidate, dict) and candidate.get("origin_id"):
            return str(candidate["origin_id"])
    return _trim(fallback, 90) or str(item.get("packet_item_id") or "Lead")


def _lead_id(lead_type: str, value: str) -> str:
    slug = _slugify(value)[:48] or "lead"
    digest = hashlib.sha1(f"{lead_type}:{value}".encode("utf-8")).hexdigest()[:8]
    return f"{lead_type}:{slug}:{digest}"


def _core_origin(origin_id: str) -> str:
    if not origin_id.startswith("cross:"):
        return origin_id
    parts = origin_id.split(":")
    if len(parts) <= 3:
        return origin_id
    return ":".join(parts[1:-1])


def _slugify(value: str) -> str:
    value = normalize_whitespace(value).lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def _clean_text(value: str) -> str:
    return normalize_whitespace(value)


def _trim(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


def _compact_strings(values: list[str], limit: int) -> list[str]:
    compact: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        compact.append(text)
        if len(compact) >= limit:
            break
    return compact


def _first_nonempty(values: list[str]) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _unique_preserve_order(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
