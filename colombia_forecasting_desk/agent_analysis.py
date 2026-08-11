from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "agent_analysis.v1"
ALLOWED_OVERALL_DISPOSITIONS = {
    "promote_to_m3",
    "shadow_track",
    "insight_only",
    "abstain",
}
ALLOWED_TENSION_REVIEW_DISPOSITIONS = {
    "promote_to_m3",
    "shadow_candidate",
    "insight_only",
    "no_action",
}
ALLOWED_RELATIONSHIP_TYPES = {"cross_bundle", "unbundled"}
ALLOWED_PUBLIC_CANDIDATE_DISPOSITIONS = {
    "promote_to_m3",
    "research_more",
    "insight_only",
    "reject",
    "none",
}
REQUIRED_FIELDS = (
    "run_date",
    "model",
    "reasoning_effort",
    "strongest_changed_signal",
    "tension_card_reviews",
    "relationships",
    "official_source_follow_up",
    "public_interest_candidate",
    "shadow_forecast",
    "overall_disposition",
    "overall_rationale",
)


@dataclass(frozen=True, slots=True)
class AgentAnalysisIssue:
    code: str
    message: str
    severity: str = "error"


def validate_agent_analysis(
    analysis: dict[str, Any],
    *,
    run_dir: str | Path | None = None,
) -> list[AgentAnalysisIssue]:
    """Validate one authored daily agent-analysis artifact."""
    issues: list[AgentAnalysisIssue] = []
    if str(analysis.get("schema_version") or "") != SCHEMA_VERSION:
        issues.append(
            AgentAnalysisIssue(
                "invalid_schema_version",
                f"`schema_version` must be `{SCHEMA_VERSION}`.",
            )
        )
    for field in REQUIRED_FIELDS:
        if field not in analysis:
            issues.append(
                AgentAnalysisIssue("missing_field", f"`{field}` is required.")
            )

    if "run_date" in analysis and not _is_iso_date(analysis.get("run_date")):
        issues.append(
            AgentAnalysisIssue(
                "invalid_run_date",
                "`run_date` must be an ISO date in YYYY-MM-DD format.",
            )
        )
    for field in ("model", "reasoning_effort", "overall_rationale"):
        if field in analysis and not _text(analysis.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"missing_{field}",
                    f"`{field}` must be non-empty.",
                )
            )

    disposition = _text(analysis.get("overall_disposition"))
    if (
        "overall_disposition" in analysis
        and disposition not in ALLOWED_OVERALL_DISPOSITIONS
    ):
        issues.append(
            AgentAnalysisIssue(
                "invalid_overall_disposition",
                "`overall_disposition` must be promote_to_m3, shadow_track, "
                "insight_only, or abstain.",
            )
        )

    if "strongest_changed_signal" in analysis:
        issues.extend(
            _validate_strongest_signal(analysis.get("strongest_changed_signal"))
        )
    if "tension_card_reviews" in analysis:
        issues.extend(
            _validate_tension_card_reviews(analysis.get("tension_card_reviews"))
        )
    if "relationships" in analysis:
        issues.extend(_validate_relationships(analysis.get("relationships")))
    if "official_source_follow_up" in analysis:
        issues.extend(
            _validate_official_source_follow_up(
                analysis.get("official_source_follow_up")
            )
        )
    if "public_interest_candidate" in analysis:
        issues.extend(
            _validate_public_interest_candidate(
                analysis.get("public_interest_candidate")
            )
        )
        public_candidate = analysis.get("public_interest_candidate")
        if (
            disposition == "promote_to_m3"
            and isinstance(public_candidate, dict)
            and public_candidate.get("disposition") != "promote_to_m3"
        ):
            issues.append(
                AgentAnalysisIssue(
                    "promote_to_m3_without_promoted_candidate",
                    "Overall promote_to_m3 requires the public candidate disposition "
                    "to be promote_to_m3.",
                )
            )
    if "overall_disposition" in analysis and "shadow_forecast" in analysis:
        shadow_forecast = analysis.get("shadow_forecast")
        if disposition == "shadow_track" and not isinstance(shadow_forecast, dict):
            issues.append(
                AgentAnalysisIssue(
                    "shadow_track_without_forecast",
                    "`shadow_track` requires a complete `shadow_forecast` object.",
                )
            )
        elif disposition == "shadow_track":
            issues.extend(
                _validate_shadow_forecast(
                    shadow_forecast,
                    run_date=analysis.get("run_date"),
                )
            )
        elif disposition != "shadow_track" and shadow_forecast is not None:
            issues.append(
                AgentAnalysisIssue(
                    "shadow_forecast_for_non_shadow_disposition",
                    "`shadow_forecast` must be null unless disposition is shadow_track.",
                )
            )
    if run_dir is not None:
        issues.extend(
            _validate_tension_card_coverage(
                analysis.get("tension_card_reviews"),
                Path(run_dir),
            )
        )
    return issues


def render_agent_analysis(analysis: dict[str, Any]) -> str:
    """Render a validated agent-analysis artifact as deterministic Markdown."""
    lines = [
        f"# Agent Analysis - {_text(analysis.get('run_date')) or 'unknown'}",
        "",
        f"- Schema: `{_text(analysis.get('schema_version')) or SCHEMA_VERSION}`",
        f"- Model: `{_text(analysis.get('model')) or 'unknown'}`",
        (
            "- Reasoning effort: "
            f"`{_text(analysis.get('reasoning_effort')) or 'unknown'}`"
        ),
        (
            "- Overall disposition: "
            f"`{_text(analysis.get('overall_disposition')) or 'unknown'}`"
        ),
        "",
        _text(analysis.get("overall_rationale")) or "No overall rationale recorded.",
        "",
    ]

    signal = _mapping(analysis.get("strongest_changed_signal"))
    lines.extend(
        [
            "## Strongest Changed Signal",
            "",
            _text(signal.get("signal")) or "Not recorded.",
            "",
            f"**Interpretation:** {_text(signal.get('interpretation')) or 'Not recorded.'}",
            "",
            "### Evidence",
            "",
        ]
    )
    lines.extend(_render_evidence_refs(signal.get("evidence_refs")))
    lines.extend(["", "### Alternative Explanations", ""])
    lines.extend(_render_text_list(signal.get("alternative_explanations")))
    lines.extend(["", "### Falsifiers", ""])
    lines.extend(_render_text_list(signal.get("falsifiers")))

    lines.extend(["", "## Tension Card Reviews", ""])
    reviews = _mapping_list(analysis.get("tension_card_reviews"))
    if not reviews:
        lines.append("No tension cards triggered for this run.")
    for review in reviews:
        lines.extend(
            [
                f"### `{_text(review.get('card_id')) or 'unknown'}`",
                "",
                (
                    "- Disposition: "
                    f"`{_text(review.get('disposition')) or 'unknown'}`"
                ),
                f"- Assessment: {_text(review.get('assessment')) or 'Not recorded.'}",
                (
                    "- Forecast implication: "
                    f"{_text(review.get('forecast_implication')) or 'Not recorded.'}"
                ),
                "",
            ]
        )

    lines.extend(["## Relationships", ""])
    for index, relationship in enumerate(
        _mapping_list(analysis.get("relationships")), 1
    ):
        relationship_type = _text(relationship.get("type")).replace("_", " ")
        lines.extend(
            [
                f"### {index}. {relationship_type.title() or 'Relationship'}",
                "",
                _text(relationship.get("relationship")) or "Not recorded.",
                "",
                (
                    "**Interpretation:** "
                    f"{_text(relationship.get('interpretation')) or 'Not recorded.'}"
                ),
                "",
                "Evidence:",
                "",
            ]
        )
        lines.extend(_render_evidence_refs(relationship.get("evidence_refs")))
        lines.extend(["", "Caveats:", ""])
        lines.extend(_render_text_list(relationship.get("caveats")))
        lines.append("")

    follow_up = _mapping(analysis.get("official_source_follow_up"))
    lines.extend(
        [
            "## Bounded Official-Source Follow-Up",
            "",
            f"- Candidate: {_text(follow_up.get('candidate')) or 'Not recorded.'}",
            f"- Scope: {_text(follow_up.get('bounded_scope')) or 'Not recorded.'}",
            "- Sources checked:",
        ]
    )
    for source in _mapping_list(follow_up.get("sources_checked")):
        source_id = _text(source.get("source_id")) or "unknown"
        url = _text(source.get("url"))
        label = f"[`{source_id}`](<{url}>)" if url else f"`{source_id}`"
        lines.append(f"  - {label}: {_text(source.get('result')) or 'Not recorded.'}")
    lines.extend(
        [
            f"- Result: {_text(follow_up.get('result')) or 'Not recorded.'}",
            "",
        ]
    )

    candidate = _mapping(analysis.get("public_interest_candidate"))
    lines.extend(
        [
            "## Public-Interest Candidate",
            "",
            f"- Candidate: {_text(candidate.get('candidate')) or 'Not recorded.'}",
            (
                "- Disposition: "
                f"`{_text(candidate.get('disposition')) or 'not_recorded'}`"
            ),
            f"- Rationale: {_text(candidate.get('rationale')) or 'Not recorded.'}",
            "- Evidence:",
        ]
    )
    lines.extend(
        f"  {line}" for line in _render_evidence_refs(candidate.get("evidence_refs"))
    )
    lines.extend(["- Missing evidence:"])
    lines.extend(
        f"  {line}" for line in _render_text_list(candidate.get("missing_evidence"))
    )
    lines.append("")

    lines.extend(["## Shadow Forecast", ""])
    shadow = analysis.get("shadow_forecast")
    if not isinstance(shadow, dict):
        lines.append("No internal shadow forecast recorded for this run.")
        return "\n".join(lines).rstrip() + "\n"

    resolver = _mapping(shadow.get("official_resolver"))
    criteria = _mapping(shadow.get("resolution_criteria"))
    baseline = _mapping(shadow.get("baseline"))
    resolver_url = _text(resolver.get("source_url"))
    resolver_name = _text(resolver.get("source_name")) or "Not recorded"
    resolver_text = (
        f"[{resolver_name}](<{resolver_url}>)" if resolver_url else resolver_name
    )
    lines.extend(
        [
            f"- ID: `{_text(shadow.get('id')) or 'not_recorded'}`",
            f"- Visibility: `{_text(shadow.get('visibility')) or 'not_recorded'}`",
            f"- Question: {_text(shadow.get('question')) or 'Not recorded.'}",
            f"- Probability: {_format_probability(shadow.get('probability'))}",
            f"- Prediction date: {_text(shadow.get('prediction_date')) or 'Not recorded.'}",
            f"- Resolution date: {_text(shadow.get('resolution_date')) or 'Not recorded.'}",
            f"- Official resolver: {resolver_text}",
            "",
            "### Resolution Criteria",
            "",
            f"- YES: {_text(criteria.get('yes')) or 'Not recorded.'}",
            f"- NO: {_text(criteria.get('no')) or 'Not recorded.'}",
            "",
            "### Baseline",
            "",
            f"- Name: {_text(baseline.get('name')) or 'Not recorded.'}",
            f"- Probability: {_format_probability(baseline.get('probability'))}",
            f"- Rationale: {_text(baseline.get('rationale')) or 'Not recorded.'}",
            "",
            "### Evidence",
            "",
        ]
    )
    lines.extend(_render_evidence_refs(shadow.get("evidence")))
    lines.extend(["", "### Rationale For", ""])
    lines.extend(_render_text_list(shadow.get("rationale_for")))
    lines.extend(["", "### Rationale Against", ""])
    lines.extend(_render_text_list(shadow.get("rationale_against")))
    lines.extend(
        [
            "",
            "### Falsifier",
            "",
            _text(shadow.get("falsifier")) or "Not recorded.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _validate_strongest_signal(value: Any) -> list[AgentAnalysisIssue]:
    if not isinstance(value, dict):
        return [
            AgentAnalysisIssue(
                "invalid_strongest_changed_signal",
                "`strongest_changed_signal` must be an object.",
            )
        ]

    issues: list[AgentAnalysisIssue] = []
    scalar_fields = {
        "signal": "summary",
        "interpretation": "interpretation",
    }
    for field, code_suffix in scalar_fields.items():
        if not _text(value.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"signal_missing_{code_suffix}",
                    f"`strongest_changed_signal.{field}` must be non-empty.",
                )
            )

    list_fields = (
        "evidence_refs",
        "alternative_explanations",
        "falsifiers",
    )
    for field in list_fields:
        if not _non_empty_list(value.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"signal_missing_{field}",
                    f"`strongest_changed_signal.{field}` must be a non-empty list.",
                )
            )
    if _non_empty_list(value.get("evidence_refs")):
        issues.extend(
            _validate_evidence_refs(
                value["evidence_refs"],
                "strongest_changed_signal.evidence_refs",
            )
        )
    return issues


def _validate_tension_card_coverage(
    reviews: Any,
    run_dir: Path,
) -> list[AgentAnalysisIssue]:
    artifact_path = run_dir / "indicator_tension_cards.json"
    if not artifact_path.exists():
        return [
            AgentAnalysisIssue(
                "missing_tension_cards_artifact",
                f"Cannot verify tension-card coverage: missing {artifact_path}.",
            )
        ]
    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [
            AgentAnalysisIssue(
                "invalid_tension_cards_artifact",
                f"Cannot verify tension-card coverage: {exc}.",
            )
        ]
    if not isinstance(artifact, list):
        return [
            AgentAnalysisIssue(
                "invalid_tension_cards_artifact",
                "`indicator_tension_cards.json` must contain a list.",
            )
        ]

    triggered = {
        _text(card.get("card_id"))
        for card in artifact
        if isinstance(card, dict) and _text(card.get("card_id"))
    }
    reviewed = {
        _text(review.get("card_id"))
        for review in reviews or []
        if isinstance(review, dict) and _text(review.get("card_id"))
    }
    issues: list[AgentAnalysisIssue] = []
    for card_id in sorted(triggered - reviewed):
        issues.append(
            AgentAnalysisIssue(
                "missing_tension_card_review",
                f"Triggered tension card `{card_id}` was not reviewed.",
            )
        )
    for card_id in sorted(reviewed - triggered):
        issues.append(
            AgentAnalysisIssue(
                "unknown_tension_card_review",
                f"Tension card review `{card_id}` was not triggered in this run.",
            )
        )
    return issues


def _validate_tension_card_reviews(value: Any) -> list[AgentAnalysisIssue]:
    if not isinstance(value, list):
        return [
            AgentAnalysisIssue(
                "invalid_tension_card_reviews",
                "`tension_card_reviews` must be a list.",
            )
        ]

    issues: list[AgentAnalysisIssue] = []
    seen: set[str] = set()
    for index, review in enumerate(value, 1):
        if not isinstance(review, dict):
            issues.append(
                AgentAnalysisIssue(
                    "invalid_tension_card_review",
                    f"`tension_card_reviews[{index}]` must be an object.",
                )
            )
            continue
        card_id = _text(review.get("card_id"))
        if not card_id:
            issues.append(
                AgentAnalysisIssue(
                    "tension_review_missing_card_id",
                    f"`tension_card_reviews[{index}].card_id` must be non-empty.",
                )
            )
        elif card_id in seen:
            issues.append(
                AgentAnalysisIssue(
                    "duplicate_tension_card_review",
                    f"Tension card `{card_id}` was reviewed more than once.",
                )
            )
        seen.add(card_id)

        for field in ("assessment", "forecast_implication"):
            if not _text(review.get(field)):
                issues.append(
                    AgentAnalysisIssue(
                        f"tension_review_missing_{field}",
                        f"`tension_card_reviews[{index}].{field}` must be non-empty.",
                    )
                )
        disposition = _text(review.get("disposition"))
        if disposition not in ALLOWED_TENSION_REVIEW_DISPOSITIONS:
            issues.append(
                AgentAnalysisIssue(
                    "invalid_tension_review_disposition",
                    f"`tension_card_reviews[{index}].disposition` is unsupported.",
                )
            )
    return issues


def _validate_relationships(value: Any) -> list[AgentAnalysisIssue]:
    if not _non_empty_list(value):
        return [
            AgentAnalysisIssue(
                "missing_relationship",
                "`relationships` must contain at least one cross-bundle or "
                "unbundled relationship.",
            )
        ]
    issues: list[AgentAnalysisIssue] = []
    for index, relationship in enumerate(value, 1):
        if not isinstance(relationship, dict):
            issues.append(
                AgentAnalysisIssue(
                    "invalid_relationship",
                    f"`relationships[{index}]` must be an object.",
                )
            )
            continue
        if _text(relationship.get("type")) not in ALLOWED_RELATIONSHIP_TYPES:
            issues.append(
                AgentAnalysisIssue(
                    "invalid_relationship_type",
                    f"`relationships[{index}].type` must be cross_bundle or unbundled.",
                )
            )
        scalar_fields = {
            "relationship": "summary",
            "interpretation": "interpretation",
        }
        for field, code_suffix in scalar_fields.items():
            if not _text(relationship.get(field)):
                issues.append(
                    AgentAnalysisIssue(
                        f"relationship_missing_{code_suffix}",
                        f"`relationships[{index}].{field}` must be non-empty.",
                    )
                )
        for field in ("evidence_refs", "caveats"):
            if not _non_empty_list(relationship.get(field)):
                issues.append(
                    AgentAnalysisIssue(
                        f"relationship_missing_{field}",
                        f"`relationships[{index}].{field}` must be a non-empty list.",
                    )
                )
        if _non_empty_list(relationship.get("evidence_refs")):
            issues.extend(
                _validate_evidence_refs(
                    relationship["evidence_refs"],
                    f"relationships[{index}].evidence_refs",
                )
            )
    return issues


def _validate_official_source_follow_up(value: Any) -> list[AgentAnalysisIssue]:
    if not isinstance(value, dict):
        return [
            AgentAnalysisIssue(
                "invalid_official_source_follow_up",
                "`official_source_follow_up` must be an object.",
            )
        ]
    issues: list[AgentAnalysisIssue] = []
    for field in ("candidate", "bounded_scope", "result"):
        if not _text(value.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"follow_up_missing_{field}",
                    f"`official_source_follow_up.{field}` must be non-empty.",
                )
            )
    if not _non_empty_list(value.get("sources_checked")):
        issues.append(
            AgentAnalysisIssue(
                "follow_up_missing_sources_checked",
                "`official_source_follow_up.sources_checked` must be a non-empty list.",
            )
        )
    else:
        for index, source in enumerate(value["sources_checked"], 1):
            if not isinstance(source, dict):
                issues.append(
                    AgentAnalysisIssue(
                        "invalid_checked_source",
                        "`official_source_follow_up.sources_checked"
                        f"[{index}]` must be an object.",
                    )
                )
                continue
            for field in ("source_id", "url", "result"):
                if not _text(source.get(field)):
                    issues.append(
                        AgentAnalysisIssue(
                            f"checked_source_missing_{field}",
                            "`official_source_follow_up.sources_checked"
                            f"[{index}].{field}` must be non-empty.",
                        )
                    )
    return issues


def _validate_public_interest_candidate(value: Any) -> list[AgentAnalysisIssue]:
    if not isinstance(value, dict):
        return [
            AgentAnalysisIssue(
                "invalid_public_interest_candidate",
                "`public_interest_candidate` must be an object.",
            )
        ]
    issues: list[AgentAnalysisIssue] = []
    for field in ("candidate", "rationale"):
        if not _text(value.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"public_candidate_missing_{field}",
                    f"`public_interest_candidate.{field}` must be non-empty.",
                )
            )
    disposition = _text(value.get("disposition"))
    if disposition not in ALLOWED_PUBLIC_CANDIDATE_DISPOSITIONS:
        issues.append(
            AgentAnalysisIssue(
                "invalid_public_candidate_disposition",
                "`public_interest_candidate.disposition` is unsupported.",
            )
        )
    if disposition != "none" and not _non_empty_list(value.get("evidence_refs")):
        issues.append(
            AgentAnalysisIssue(
                "public_candidate_missing_evidence_refs",
                "A public-interest candidate requires at least one evidence reference.",
            )
        )
    elif _non_empty_list(value.get("evidence_refs")):
        issues.extend(
            _validate_evidence_refs(
                value["evidence_refs"],
                "public_interest_candidate.evidence_refs",
            )
        )
    if not isinstance(value.get("missing_evidence"), list):
        issues.append(
            AgentAnalysisIssue(
                "invalid_public_candidate_missing_evidence",
                "`public_interest_candidate.missing_evidence` must be a list.",
            )
        )
    elif disposition == "research_more" and not value["missing_evidence"]:
        issues.append(
            AgentAnalysisIssue(
                "public_candidate_research_more_without_missing_evidence",
                "A research_more public candidate must name missing evidence.",
            )
        )
    return issues


def _validate_shadow_forecast(
    value: dict[str, Any],
    *,
    run_date: Any,
) -> list[AgentAnalysisIssue]:
    issues: list[AgentAnalysisIssue] = []
    for field in ("id", "question", "falsifier"):
        if not _text(value.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"shadow_missing_{field}",
                    f"`shadow_forecast.{field}` must be non-empty.",
                )
            )
    if value.get("visibility") != "internal":
        issues.append(
            AgentAnalysisIssue(
                "shadow_not_internal",
                "`shadow_forecast.visibility` must be `internal`.",
            )
        )
    if not _is_probability(value.get("probability")):
        issues.append(
            AgentAnalysisIssue(
                "shadow_invalid_probability",
                "`shadow_forecast.probability` must be a number from 0 to 1.",
            )
        )

    prediction_date = _parse_iso_date(value.get("prediction_date"))
    resolution_date = _parse_iso_date(value.get("resolution_date"))
    analysis_date = _parse_iso_date(run_date)
    if prediction_date is None:
        issues.append(
            AgentAnalysisIssue(
                "shadow_invalid_prediction_date",
                "`shadow_forecast.prediction_date` must be YYYY-MM-DD.",
            )
        )
    if resolution_date is None:
        issues.append(
            AgentAnalysisIssue(
                "shadow_invalid_resolution_date",
                "`shadow_forecast.resolution_date` must be YYYY-MM-DD.",
            )
        )
    if (
        prediction_date is not None
        and analysis_date is not None
        and prediction_date != analysis_date
    ):
        issues.append(
            AgentAnalysisIssue(
                "shadow_prediction_date_mismatch",
                "Shadow prediction date must match the agent-analysis run date.",
            )
        )
    if (
        prediction_date is not None
        and resolution_date is not None
        and resolution_date < prediction_date
    ):
        issues.append(
            AgentAnalysisIssue(
                "shadow_resolution_before_prediction",
                "Shadow resolution date cannot precede its prediction date.",
            )
        )

    resolver = value.get("official_resolver")
    if not isinstance(resolver, dict) or not all(
        _text(resolver.get(field)) for field in ("source_name", "source_url")
    ):
        issues.append(
            AgentAnalysisIssue(
                "shadow_incomplete_official_resolver",
                "Shadow forecast requires official_resolver source_name and source_url.",
            )
        )

    criteria = value.get("resolution_criteria")
    if not isinstance(criteria, dict) or not all(
        _text(criteria.get(field)) for field in ("yes", "no")
    ):
        issues.append(
            AgentAnalysisIssue(
                "shadow_incomplete_resolution_criteria",
                "Shadow forecast requires explicit YES and NO resolution criteria.",
            )
        )

    baseline = value.get("baseline")
    if (
        not isinstance(baseline, dict)
        or not _text(baseline.get("name"))
        or not _is_probability(baseline.get("probability"))
        or not _text(baseline.get("rationale"))
    ):
        issues.append(
            AgentAnalysisIssue(
                "shadow_invalid_baseline",
                "Shadow forecast baseline requires name, probability, and rationale.",
            )
        )

    for field in ("evidence", "rationale_for", "rationale_against"):
        if not _non_empty_list(value.get(field)):
            issues.append(
                AgentAnalysisIssue(
                    f"shadow_missing_{field}",
                    f"`shadow_forecast.{field}` must be a non-empty list.",
                )
            )
    if _non_empty_list(value.get("evidence")):
        issues.extend(
            _validate_evidence_refs(value["evidence"], "shadow_forecast.evidence")
        )
    return issues


def _validate_evidence_refs(
    refs: list[Any],
    location: str,
) -> list[AgentAnalysisIssue]:
    issues: list[AgentAnalysisIssue] = []
    for index, ref in enumerate(refs, 1):
        if not isinstance(ref, dict) or not all(
            _text(ref.get(field)) for field in ("artifact", "locator")
        ):
            issues.append(
                AgentAnalysisIssue(
                    "invalid_evidence_ref",
                    f"`{location}[{index}]` requires artifact and locator.",
                )
            )
    return issues


def _is_iso_date(value: Any) -> bool:
    return _parse_iso_date(value) is not None


def _parse_iso_date(value: Any) -> date | None:
    text = _text(value)
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == text else None


def _is_probability(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and 0.0 <= float(value) <= 1.0
    )


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _mapping_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _render_evidence_refs(value: Any) -> list[str]:
    refs = _mapping_list(value)
    if not refs:
        return ["- None recorded."]
    lines: list[str] = []
    for ref in refs:
        artifact = _text(ref.get("artifact")) or "unknown"
        locator = _text(ref.get("locator")) or "unknown"
        note = _text(ref.get("note"))
        line = f"- `{artifact}` (`{locator}`)"
        if note:
            line += f": {note}"
        lines.append(line)
    return lines


def _render_text_list(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        return ["- None recorded."]
    return [f"- {_text(item) or 'Not recorded.'}" for item in value]


def _format_probability(value: Any) -> str:
    if not _is_probability(value):
        return "Not recorded."
    return f"{float(value) * 100:.1f}%"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _non_empty_list(value: Any) -> bool:
    return isinstance(value, list) and bool(value)
