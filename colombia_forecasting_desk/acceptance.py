from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from .models import CleanedItem, RunSummary, SourceFailure, SourceHealth
from .source_quality import is_unparsed_link_only_source

ACCEPTANCE_SCHEMA_VERSION = "m1_acceptance.v1"
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


def build_acceptance_report(
    run_summary: RunSummary,
    m1_candidates: dict[str, Any],
    source_health: Iterable[SourceHealth],
    failures: Iterable[SourceFailure],
    cleaned_items: Iterable[CleanedItem],
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

    for failure in failure_list:
        if _is_high_impact_failure(failure):
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
