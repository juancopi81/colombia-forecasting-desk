from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from .models import (
    CleanedItem,
    IndicatorObservation,
    RunSummary,
    SourceFailure,
    SourceHealth,
)
from .source_quality import is_unparsed_link_only_source

ACCEPTANCE_SCHEMA_VERSION = "m1_acceptance.v2"
OPERATIONAL_GATE_MIN_SOURCES = 5
MIN_OPERATIONAL_RAW_ITEMS = 25
MIN_OPERATIONAL_CLEANED_ITEMS = 10
MIN_OPERATIONAL_RANKABLE_ITEMS = 5
MIN_OPERATIONAL_OBSERVED_INDICATORS = 8
MAX_SOURCE_FAILURE_SHARE = 0.4
MAX_HIGH_IMPACT_FAILURES = 3
TOP_SOURCE_SHARE_WARNING_THRESHOLD = 0.6
HIGH_IMPACT_SOURCE_TERMS = {
    "registraduria",
    "cne",
    "dian",
    "minhacienda",
    "banrep",
    "dane",
    "congreso",
    "gacetas",
    "diario_oficial",
}


def _issue(
    code: str,
    severity: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "message": message,
    }
    if details:
        payload["details"] = details
    return payload


def _candidate_evidence(candidate: dict[str, Any]) -> dict[str, Any]:
    evidence = candidate.get("evidence")
    return evidence if isinstance(evidence, dict) else {}


def _candidate_source_ids(candidate: dict[str, Any]) -> list[str]:
    evidence = _candidate_evidence(candidate)
    source_ids = evidence.get("source_ids")
    if isinstance(source_ids, list):
        return [str(source_id) for source_id in source_ids if source_id]
    return []


def _has_evidence_excerpt(candidate: dict[str, Any]) -> bool:
    evidence = _candidate_evidence(candidate)
    if str(evidence.get("starting_evidence") or "").strip():
        return True
    return bool(str(evidence.get("evidence_text") or "").strip())


def _source_family(source_id: str) -> str:
    if source_id.startswith("secop_"):
        return "secop"
    if source_id.startswith("dane_"):
        return "dane"
    if source_id.startswith("banrep_"):
        return "banrep"
    if source_id.startswith("eltiempo_"):
        return "eltiempo"
    return source_id


def _is_high_impact_failure(failure: SourceFailure) -> bool:
    haystack = f"{failure.source_id} {failure.source_name}".lower()
    return any(term in haystack for term in HIGH_IMPACT_SOURCE_TERMS)


def _is_full_operational_run(run_summary: RunSummary) -> bool:
    return run_summary.sources_checked >= OPERATIONAL_GATE_MIN_SOURCES


def _observed_indicator_count(
    indicator_watch: Iterable[IndicatorObservation] | None,
) -> int | None:
    if indicator_watch is None:
        return None
    return sum(1 for indicator in indicator_watch if indicator.status == "observed")


def build_acceptance_report(
    run_summary: RunSummary,
    m1_candidates: dict[str, Any],
    source_health: Iterable[SourceHealth],
    failures: Iterable[SourceFailure],
    cleaned_items: Iterable[CleanedItem],
    indicator_watch: Iterable[IndicatorObservation] | None = None,
) -> dict[str, Any]:
    candidates = [
        candidate
        for candidate in m1_candidates.get("candidates", [])
        if isinstance(candidate, dict)
    ]
    source_health_list = list(source_health)
    failure_list = list(failures)
    cleaned_list = list(cleaned_items)
    issues: list[dict[str, Any]] = []
    high_impact_failures = [
        failure for failure in failure_list if _is_high_impact_failure(failure)
    ]

    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "(missing)")
        if not str(candidate.get("resolution_source") or "").strip():
            issues.append(
                _issue(
                    "candidate_missing_resolution_source",
                    "error",
                    "Candidate has no deterministic resolution source.",
                    details={"candidate_id": candidate_id},
                )
            )
        if not _has_evidence_excerpt(candidate):
            issues.append(
                _issue(
                    "candidate_missing_evidence",
                    "error",
                    "Candidate has no evidence excerpt.",
                    details={"candidate_id": candidate_id},
                )
            )
        if candidate.get("candidate_type") == "event_signal":
            noise = candidate.get("noise_reasons")
            if isinstance(noise, list) and noise:
                issues.append(
                    _issue(
                        "candidate_has_noise_reasons",
                        "error",
                        "Forecastable candidate still carries noise reasons.",
                        details={"candidate_id": candidate_id, "noise_reasons": noise},
                    )
                )

    if run_summary.raw_items > 0 and not candidates:
        issues.append(
            _issue(
                "no_candidates_from_nonempty_run",
                "error",
                "Run collected raw items but produced no M1 candidates.",
                details={"raw_items": run_summary.raw_items},
            )
        )

    link_only_sources = {
        health.source_id
        for health in source_health_list
        if is_unparsed_link_only_source(health)
    }
    for candidate in candidates:
        overlapping = sorted(link_only_sources & set(_candidate_source_ids(candidate)))
        if overlapping:
            issues.append(
                _issue(
                    "candidate_from_link_only_source",
                    "error",
                    "Candidate depends on a document-link-only source.",
                    details={
                        "candidate_id": candidate.get("candidate_id"),
                        "source_ids": overlapping,
                    },
                )
            )

    if _is_full_operational_run(run_summary):
        if run_summary.raw_items < MIN_OPERATIONAL_RAW_ITEMS:
            issues.append(
                _issue(
                    "operational_raw_items_below_minimum",
                    "error",
                    "Full M1 run collected too few raw items to trust as operational.",
                    details={
                        "raw_items": run_summary.raw_items,
                        "minimum": MIN_OPERATIONAL_RAW_ITEMS,
                    },
                )
            )
        if run_summary.cleaned_items < MIN_OPERATIONAL_CLEANED_ITEMS:
            issues.append(
                _issue(
                    "operational_cleaned_items_below_minimum",
                    "error",
                    "Full M1 run retained too few cleaned items to trust as operational.",
                    details={
                        "cleaned_items": run_summary.cleaned_items,
                        "minimum": MIN_OPERATIONAL_CLEANED_ITEMS,
                    },
                )
            )

        source_failure_share = (
            run_summary.sources_failed / run_summary.sources_checked
            if run_summary.sources_checked
            else 0.0
        )
        if source_failure_share > MAX_SOURCE_FAILURE_SHARE:
            issues.append(
                _issue(
                    "operational_source_failure_share_too_high",
                    "error",
                    "Too many sources failed for a full M1 run to be operational.",
                    details={
                        "sources_failed": run_summary.sources_failed,
                        "sources_checked": run_summary.sources_checked,
                        "failure_share": round(source_failure_share, 3),
                        "maximum": MAX_SOURCE_FAILURE_SHARE,
                    },
                )
            )

        rankable_count = sum(1 for item in cleaned_list if not item.quality_notes)
        if rankable_count < MIN_OPERATIONAL_RANKABLE_ITEMS:
            issues.append(
                _issue(
                    "operational_rankable_items_below_minimum",
                    "error",
                    "Full M1 run produced too few rankable items to support M2.",
                    details={
                        "rankable_items": rankable_count,
                        "minimum": MIN_OPERATIONAL_RANKABLE_ITEMS,
                    },
                )
            )

        observed_indicators = _observed_indicator_count(indicator_watch)
        if (
            observed_indicators is not None
            and observed_indicators < MIN_OPERATIONAL_OBSERVED_INDICATORS
        ):
            issues.append(
                _issue(
                    "operational_observed_indicators_below_minimum",
                    "error",
                    "Full M1 run observed too few Indicator Watch cards.",
                    details={
                        "observed_indicators": observed_indicators,
                        "minimum": MIN_OPERATIONAL_OBSERVED_INDICATORS,
                    },
                )
            )

        if len(high_impact_failures) > MAX_HIGH_IMPACT_FAILURES:
            issues.append(
                _issue(
                    "operational_high_impact_failures_too_high",
                    "error",
                    "Too many high-impact sources failed for a full M1 run.",
                    details={
                        "high_impact_failures": [
                            failure.source_id for failure in high_impact_failures
                        ],
                        "maximum": MAX_HIGH_IMPACT_FAILURES,
                    },
                )
            )

    for failure in high_impact_failures:
        issues.append(
            _issue(
                "high_impact_source_failed",
                "warning",
                "High-impact source failed during this run.",
                details={
                    "source_id": failure.source_id,
                    "error_class": failure.error_class,
                },
            )
        )

    for health in source_health_list:
        if health.document_link_count > 0 and health.parsed_content_count == 0:
            issues.append(
                _issue(
                    "document_source_unparsed",
                    "warning",
                    "Source exposes document links but no parsed content.",
                    details={
                        "source_id": health.source_id,
                        "document_link_count": health.document_link_count,
                    },
                )
            )

    rankable = [item for item in cleaned_list if not item.quality_notes]
    untagged = [
        item
        for item in rankable
        if not item.detected_entities and not item.detected_topics
    ]
    if untagged:
        issues.append(
            _issue(
                "rankable_items_without_tags",
                "warning",
                "Some rankable items have neither entity nor topic tags.",
                details={
                    "untagged_count": len(untagged),
                    "sample_item_ids": [item.id for item in untagged[:5]],
                },
            )
        )

    source_counter: Counter[str] = Counter()
    for candidate in candidates:
        for source_id in _candidate_source_ids(candidate):
            source_counter[_source_family(source_id)] += 1
    if candidates and source_counter:
        family, count = source_counter.most_common(1)[0]
        share = count / len(candidates)
        if share > TOP_SOURCE_SHARE_WARNING_THRESHOLD:
            issues.append(
                _issue(
                    "candidate_source_concentration",
                    "warning",
                    "Too many candidates come from one source family.",
                    details={
                        "source_family": family,
                        "candidate_count": count,
                        "candidate_share": round(share, 3),
                    },
                )
            )

    error_count = sum(1 for issue in issues if issue["severity"] == "error")
    warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
    return {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "status": "fail" if error_count else "pass",
        "strict_pass": error_count == 0,
        "error_count": error_count,
        "warning_count": warning_count,
        "issues": issues,
    }
