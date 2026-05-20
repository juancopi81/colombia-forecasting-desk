from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .cleaner import fold_accents, normalize_whitespace
from .models import RunSummary
from .ranker import parse_iso

SCHEMA_VERSION = "m2_legislative_ranking.v1"
MAX_REVIEW_QUEUE = 15
MAX_RANKED_QUESTIONS = 200

STRUCTURAL_MOVEMENTS = {
    "agenda_debate",
    "conciliacion_publicada",
    "ponencia_publicada",
    "texto_aprobado_publicado",
}
LOW_SIGNAL_MOVEMENTS = {"registry_publication", "registry_status"}
NON_ACTIONABLE_STATES = {"resolved", "blocked"}
ACTIVE_FORECAST_STATUSES = {
    "draft_for_human_review",
    "open",
    "posted",
    "pending_review",
}

PUBLIC_INTEREST_TERMS: dict[str, tuple[str, ...]] = {
    "household_costs": (
        "alimento",
        "combustible",
        "energia",
        "gas",
        "glp",
        "servicio publico",
        "subsidio",
        "tarifa",
        "transporte",
    ),
    "public_finance": (
        "arancel",
        "fiscal",
        "impuesto",
        "presupuesto",
        "regalia",
        "tribut",
    ),
    "health_social": (
        "educacion",
        "laboral",
        "pension",
        "salud",
        "soat",
        "vivienda",
    ),
    "institutional": (
        "contratacion",
        "corrupcion",
        "eleccion",
        "justicia",
        "seguridad",
        "transparencia",
    ),
    "business_regulatory": (
        "aduana",
        "comercio",
        "empresa",
        "industria",
        "mineria",
        "regulator",
        "zona franca",
    ),
    "regional_impact": (
        "amazonas",
        "antioquia",
        "arauca",
        "atlantico",
        "bogota",
        "bolivar",
        "boyaca",
        "caldas",
        "caqueta",
        "casanare",
        "cauca",
        "cesar",
        "choco",
        "cordoba",
        "cundinamarca",
        "guainia",
        "guaviare",
        "huila",
        "la guajira",
        "magdalena",
        "meta",
        "narino",
        "norte de santander",
        "putumayo",
        "quindio",
        "risaralda",
        "san andres",
        "santander",
        "sucre",
        "tolima",
        "valle del cauca",
        "vaupes",
        "vichada",
    ),
}


def build_legislative_m2_ranking(
    reconciliations: list[dict[str, Any]],
    run_summary: RunSummary,
    forecast_log_path: str | Path = "forecasts/forecast_log.jsonl",
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a transparent M2 triage artifact from legislative reconciliations.

    The ranking is advisory: it should reduce the review surface, not hide
    records from a human or LLM reviewer.
    """
    generated = generated_at or run_summary.finished_at
    active_forecasts = _load_active_forecasts(Path(forecast_log_path))
    items = [
        _rank_record(record, run_summary, active_forecasts)
        for record in reconciliations
        if isinstance(record, dict)
    ]
    items = sorted(
        items,
        key=lambda item: (
            _bucket_rank(item["bucket"]),
            -float(item["overall_score"]),
            str(item.get("canonical_bill_id") or ""),
        ),
    )
    audit = _heuristic_audit(items, len(reconciliations))
    review_queue = _review_queue(items, audit)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "generated_at": generated,
        "inputs": {
            "legislative_reconciler_artifact": "legislative_reconciler.json",
            "forecast_log_artifact": str(forecast_log_path),
            "record_count": len(reconciliations),
            "ranked_question_limit": MAX_RANKED_QUESTIONS,
            "truncated_record_count": max(0, len(items) - MAX_RANKED_QUESTIONS),
            "policy": (
                "Advisory deterministic triage. Do not discard a legislative "
                "lead solely because of its rank; inspect reasons, penalties, "
                "and audit flags before M3."
            ),
        },
        "bucket_counts": _bucket_counts(items),
        "ranked_questions": items[:MAX_RANKED_QUESTIONS],
        "buckets": _bucket_ids(items),
        "review_queue": review_queue,
        "heuristic_audit": audit,
    }


def _rank_record(
    record: dict[str, Any],
    run_summary: RunSummary,
    active_forecasts: list[dict[str, Any]],
) -> dict[str, Any]:
    readiness = record.get("m2_readiness") if isinstance(record, dict) else {}
    readiness_state = (
        str(readiness.get("state") or "unknown")
        if isinstance(readiness, dict)
        else "unknown"
    )
    canonical_id = str(record.get("canonical_bill_id") or "")
    display_title = str(record.get("display_title") or canonical_id)
    status = record.get("status") if isinstance(record.get("status"), dict) else {}
    latest = (
        record.get("latest_movement")
        if isinstance(record.get("latest_movement"), dict)
        else {}
    )
    contradiction = (
        record.get("contradiction")
        if isinstance(record.get("contradiction"), dict)
        else {}
    )
    evidence = [
        item
        for item in record.get("source_evidence") or []
        if isinstance(item, dict)
    ]
    active_duplicate = _active_forecast_match(record, active_forecasts)

    dimensions = {
        "forecastability": _forecastability_score(
            record, readiness_state, status, latest
        ),
        "public_interest": _public_interest_score(record),
        "freshness": _freshness_score(run_summary.run_date, latest, status),
        "source_quality": _source_quality_score(evidence),
        "decision_window": _decision_window_score(readiness_state, status, latest),
        "novelty": 0.25 if active_duplicate else 0.85,
    }
    overall = _overall_score(dimensions)
    if readiness_state in NON_ACTIONABLE_STATES:
        overall = min(overall, 0.2)
    if contradiction.get("has_contradiction"):
        overall = min(overall, 0.35)
    if active_duplicate:
        overall = min(overall, 0.45)

    reasons, keyword_signals = _score_reasons(record, dimensions, active_duplicate)
    penalties = _penalties(record, dimensions, active_duplicate)
    missing = _missing_evidence(record, readiness)
    bucket = _bucket(readiness_state, dimensions, contradiction, active_duplicate)
    question = _question(record, bucket)
    risk_flags = _item_risk_flags(bucket, dimensions, missing, contradiction)

    return {
        "rank_id": _rank_id(run_summary.run_date, canonical_id, question),
        "canonical_bill_id": canonical_id,
        "display_title": display_title,
        "question_seed": question,
        "bucket": bucket,
        "recommendation": _recommendation(bucket, risk_flags),
        "overall_score": round(overall, 3),
        "dimension_scores": {key: round(value, 3) for key, value in dimensions.items()},
        "score_reasons": reasons,
        "penalties": penalties,
        "missing_evidence": missing,
        "heuristic_risk_flags": risk_flags,
        "llm_review_hint": _llm_review_hint(bucket, risk_flags, keyword_signals),
        "resolution_source_hint": (
            "Official Congreso registry, Gacetas del Congreso, and Diario Oficial."
        ),
        "latest_movement": _compact_mapping(latest),
        "status": _compact_mapping(status),
        "decision_state": str(record.get("decision_state") or "unknown"),
        "readiness_state": readiness_state,
        "source_ids": _source_ids(evidence),
        "public_interest_signals": keyword_signals,
    }


def _forecastability_score(
    record: dict[str, Any],
    readiness_state: str,
    status: dict[str, Any],
    latest: dict[str, Any],
) -> float:
    if readiness_state in NON_ACTIONABLE_STATES:
        return 0.0
    score = 0.15
    if record.get("origin_project"):
        score += 0.25
    if record.get("display_title") and not str(
        record.get("canonical_bill_id", "")
    ).startswith("bill:research:"):
        score += 0.15
    if status.get("stage") == "active":
        score += 0.2
    if latest:
        score += 0.15
    if latest.get("source_id") in {"gacetas_congreso", "senado_agenda_legislativa"}:
        score += 0.1
    if readiness_state == "ready":
        score += 0.1
    return _clamp(score)


def _public_interest_score(record: dict[str, Any]) -> float:
    text = _record_text(record)
    matched_groups = 0
    matched_terms = 0
    for terms in PUBLIC_INTEREST_TERMS.values():
        group_matches = _matched_public_interest_terms(text, terms)
        if group_matches:
            matched_groups += 1
            matched_terms += min(len(group_matches), 3)
    score = 0.1 + matched_groups * 0.15 + matched_terms * 0.035
    if _term_matches(text, "subsidio") or _term_matches(text, "tarifa"):
        score += 0.1
    if _term_matches(text, "san andres"):
        score += 0.08
    return _clamp(score)


def _freshness_score(
    run_date: str,
    latest: dict[str, Any],
    status: dict[str, Any],
) -> float:
    date_value = str(latest.get("date") or status.get("as_of") or "")
    if not date_value:
        return 0.0
    run_dt = parse_iso(f"{run_date}T23:59:59Z")
    event_dt = parse_iso(date_value)
    if run_dt is None or event_dt is None:
        return 0.25
    days = abs((run_dt - event_dt).days)
    if days <= 3:
        return 1.0
    if days <= 7:
        return 0.8
    if days <= 30:
        return 0.55
    if days <= 90:
        return 0.3
    return 0.1


def _source_quality_score(evidence: list[dict[str, Any]]) -> float:
    roles = {str(item.get("role") or "") for item in evidence}
    source_ids = {str(item.get("source_id") or "") for item in evidence}
    score = 0.0
    if "identity_status" in roles:
        score += 0.35
    if "movement" in roles:
        score += 0.25
    if "final_act" in roles:
        score += 0.2
    if {"senado_leyes_registry", "camara_proyectos_ley_registry"} & source_ids:
        score += 0.15
    if "gacetas_congreso" in source_ids:
        score += 0.15
    if len(source_ids) >= 2:
        score += 0.1
    return _clamp(score)


def _decision_window_score(
    readiness_state: str,
    status: dict[str, Any],
    latest: dict[str, Any],
) -> float:
    if readiness_state != "ready" or status.get("stage") != "active":
        return 0.0
    action_type = str(latest.get("action_type") or "")
    if action_type in STRUCTURAL_MOVEMENTS:
        return 0.85
    if action_type in LOW_SIGNAL_MOVEMENTS:
        return 0.35
    if action_type:
        return 0.55
    return 0.2


def _overall_score(dimensions: dict[str, float]) -> float:
    return _clamp(
        dimensions["forecastability"] * 0.28
        + dimensions["public_interest"] * 0.2
        + dimensions["freshness"] * 0.16
        + dimensions["source_quality"] * 0.18
        + dimensions["decision_window"] * 0.12
        + dimensions["novelty"] * 0.06
    )


def _bucket(
    readiness_state: str,
    dimensions: dict[str, float],
    contradiction: dict[str, Any],
    active_duplicate: bool,
) -> str:
    if readiness_state in {"resolved", "blocked"} or contradiction.get(
        "has_contradiction"
    ):
        return "blocked_or_resolved"
    if readiness_state != "ready":
        if dimensions["public_interest"] >= 0.55:
            return "public_interest_but_unready"
        return "research_more"
    if active_duplicate:
        return "watchlist"
    if (
        dimensions["forecastability"] >= 0.65
        and dimensions["source_quality"] >= 0.55
        and dimensions["decision_window"] >= 0.5
        and dimensions["public_interest"] >= 0.25
    ):
        return "ready_for_m3"
    return "watchlist"


def _score_reasons(
    record: dict[str, Any],
    dimensions: dict[str, float],
    active_duplicate: bool,
) -> tuple[list[str], list[str]]:
    reasons: list[str] = []
    if record.get("origin_project"):
        reasons.append("Clean bill number, chamber, and year are present.")
    status = record.get("status") if isinstance(record.get("status"), dict) else {}
    if status.get("stage") == "active":
        reasons.append("Official registry status is active/unresolved.")
    latest = (
        record.get("latest_movement")
        if isinstance(record.get("latest_movement"), dict)
        else {}
    )
    action_type = str(latest.get("action_type") or "")
    if action_type in STRUCTURAL_MOVEMENTS:
        reasons.append(f"Latest movement is substantive: {action_type}.")
    elif action_type:
        reasons.append(f"Latest movement is available: {action_type}.")
    if dimensions["source_quality"] >= 0.65:
        reasons.append(
            "Evidence combines official identity/status and follow-up sources."
        )
    keyword_signals = _public_interest_signals(record)
    if keyword_signals:
        reasons.append(
            "Public-interest terms detected: "
            + ", ".join(keyword_signals[:5])
            + "."
        )
    if active_duplicate:
        reasons.append("A likely related active forecast already exists in forecast_log.jsonl.")
    return reasons or ["Record retained for transparent M2 review."], keyword_signals


def _penalties(
    record: dict[str, Any],
    dimensions: dict[str, float],
    active_duplicate: bool,
) -> list[str]:
    penalties: list[str] = []
    readiness = (
        record.get("m2_readiness")
        if isinstance(record.get("m2_readiness"), dict)
        else {}
    )
    state = str(readiness.get("state") or "unknown")
    if state != "ready":
        penalties.append(f"M2 readiness is `{state}`, not `ready`.")
    if dimensions["public_interest"] < 0.3:
        penalties.append("Public-interest hook is weak or not captured by deterministic signals.")
    if dimensions["decision_window"] < 0.5:
        penalties.append("Decision window is unclear or latest movement is registry-only.")
    if dimensions["freshness"] < 0.4:
        penalties.append("Latest official movement is stale or undated.")
    contradiction = (
        record.get("contradiction")
        if isinstance(record.get("contradiction"), dict)
        else {}
    )
    if contradiction.get("has_contradiction"):
        penalties.append("Official evidence has a material contradiction.")
    if active_duplicate:
        penalties.append("Likely duplicate of an active forecast-log item.")
    return penalties


def _missing_evidence(record: dict[str, Any], readiness: object) -> list[str]:
    missing: list[str] = []
    if isinstance(readiness, dict):
        missing.extend(str(item) for item in readiness.get("missing") or [] if item)
    missing.extend(["human public-interest framing", "exact forecast deadline/window"])
    return _unique_preserve_order(missing)


def _item_risk_flags(
    bucket: str,
    dimensions: dict[str, float],
    missing: list[str],
    contradiction: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    material_missing = [
        item
        for item in missing
        if item
        not in {"human public-interest framing", "exact forecast deadline/window"}
    ]
    if bucket == "ready_for_m3" and material_missing:
        flags.append("possible_false_positive_missing_evidence")
    if (
        bucket in {"research_more", "watchlist"}
        and dimensions["forecastability"] >= 0.75
    ):
        flags.append("possible_false_negative_structurally_strong")
    if bucket != "ready_for_m3" and dimensions["public_interest"] >= 0.65:
        flags.append("possible_false_negative_public_interest")
    if dimensions["public_interest"] < 0.3 and dimensions["forecastability"] >= 0.75:
        flags.append("keyword_blind_spot_possible")
    if contradiction.get("has_contradiction"):
        flags.append("needs_human_reconciliation")
    return flags


def _llm_review_hint(
    bucket: str,
    risk_flags: list[str],
    keyword_signals: list[str],
) -> str:
    if risk_flags:
        return (
            "Treat the deterministic rank as suspect here; inspect the source "
            "record and decide whether the heuristic under- or over-ranked it."
        )
    if bucket == "ready_for_m3":
        return (
            "Check whether the suggested question has a concrete resolution date "
            "and whether the public-interest hook is strong enough for an evidence pack."
        )
    if bucket == "public_interest_but_unready":
        return (
            "Public-interest terms are visible, but the record lacks enough clean "
            "identity/status/resolution evidence for probability work."
        )
    if keyword_signals:
        return (
            "Review as a watchlist item; public-interest signals exist but "
            "readiness is incomplete."
        )
    return (
        "Review only if lower-ranked legislative items are being sampled for "
        "hidden salience."
    )


def _question(record: dict[str, Any], bucket: str) -> str:
    title = str(
        record.get("display_title") or record.get("canonical_bill_id") or "this bill"
    )
    if bucket == "ready_for_m3":
        return f"Will {title} receive a substantive next official legislative movement?"
    return f"Could {title} become a forecastable unresolved legislative decision?"


def _recommendation(bucket: str, risk_flags: list[str]) -> str:
    if "possible_false_positive_missing_evidence" in risk_flags:
        return "research_more_before_m3"
    if bucket == "ready_for_m3":
        return "select_for_evidence_pack"
    if bucket in {"public_interest_but_unready", "research_more"}:
        return "research_more_before_m3"
    if bucket == "watchlist":
        return "monitor"
    return "do_not_rank_for_m3"


def _heuristic_audit(
    items: list[dict[str, Any]],
    input_records: int,
) -> dict[str, Any]:
    possible_false_negatives = [
        _audit_ref(item)
        for item in items
        if any(
            flag.startswith("possible_false_negative")
            or flag == "keyword_blind_spot_possible"
            for flag in item.get("heuristic_risk_flags", [])
        )
    ]
    possible_false_positives = [
        _audit_ref(item)
        for item in items
        if "possible_false_positive_missing_evidence"
        in item.get("heuristic_risk_flags", [])
    ]
    top_scores = [
        float(item.get("overall_score") or 0.0)
        for item in items
        if item.get("bucket") in {"ready_for_m3", "watchlist"}
    ][:10]
    risk_flags: list[dict[str, Any]] = []
    if len(top_scores) >= 5 and max(top_scores) - min(top_scores) <= 0.08:
        risk_flags.append(
            {
                "type": "rank_compression",
                "severity": "medium",
                "message": (
                    "Top scores are tightly clustered; human/LLM review should "
                    "not over-trust rank order."
                ),
            }
        )
    if len(possible_false_negatives) >= 5:
        risk_flags.append(
            {
                "type": "many_possible_false_negatives",
                "severity": "medium",
                "message": (
                    "Several lower-bucket records have strong individual signals; "
                    "sample them during review."
                ),
            }
        )
    if not any(item.get("bucket") == "ready_for_m3" for item in items) and any(
        item.get("readiness_state") == "ready" for item in items
    ):
        risk_flags.append(
            {
                "type": "all_ready_records_suppressed",
                "severity": "high",
                "message": (
                    "The heuristic suppressed every ready record; inspect watchlist "
                    "items before concluding there is no M2 candidate."
                ),
            }
        )
    return {
        "review_policy": (
            "Ranking is advisory. The LLM and human reviewer should challenge "
            "low-ranked records when risk flags or public-interest signals exist."
        ),
        "input_record_count": input_records,
        "ranked_record_count": len(items),
        "possible_false_negative_count": len(possible_false_negatives),
        "possible_false_positive_count": len(possible_false_positives),
        "possible_false_negatives": possible_false_negatives[:10],
        "possible_false_positives": possible_false_positives[:10],
        "risk_flags": risk_flags,
    }


def _review_queue(
    items: list[dict[str, Any]], audit: dict[str, Any]
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(item: dict[str, Any]) -> None:
        key = str(item.get("rank_id") or item.get("canonical_bill_id") or "")
        if not key or key in seen or len(selected) >= MAX_REVIEW_QUEUE:
            return
        seen.add(key)
        selected.append(_audit_ref(item))

    for item in items:
        if item.get("bucket") == "ready_for_m3":
            add(item)
    risky_ids = {
        ref["rank_id"]
        for key in ("possible_false_negatives", "possible_false_positives")
        for ref in audit.get(key, [])
        if isinstance(ref, dict) and ref.get("rank_id")
    }
    for item in items:
        if item.get("rank_id") in risky_ids:
            add(item)
    for item in items:
        if item.get("bucket") in {"public_interest_but_unready", "watchlist"}:
            add(item)
    return selected


def _bucket_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        bucket = str(item.get("bucket") or "unknown")
        counts[bucket] = counts.get(bucket, 0) + 1
    return counts


def _bucket_ids(items: list[dict[str, Any]]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {}
    for item in items:
        bucket = str(item.get("bucket") or "unknown")
        buckets.setdefault(bucket, []).append(str(item.get("rank_id") or ""))
    return buckets


def _bucket_rank(bucket: str) -> int:
    return {
        "ready_for_m3": 0,
        "public_interest_but_unready": 1,
        "watchlist": 2,
        "research_more": 3,
        "blocked_or_resolved": 4,
    }.get(bucket, 9)


def _audit_ref(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank_id": str(item.get("rank_id") or ""),
        "canonical_bill_id": str(item.get("canonical_bill_id") or ""),
        "bucket": str(item.get("bucket") or ""),
        "overall_score": item.get("overall_score"),
        "question_seed": str(item.get("question_seed") or ""),
        "risk_flags": list(item.get("heuristic_risk_flags") or []),
    }


def _public_interest_signals(record: dict[str, Any]) -> list[str]:
    text = _record_text(record)
    signals: list[str] = []
    for group, terms in PUBLIC_INTEREST_TERMS.items():
        matches = _matched_public_interest_terms(text, terms)
        if matches:
            signals.append(f"{group}:{'/'.join(matches[:3])}")
    return signals


def _matched_public_interest_terms(
    text: str,
    terms: tuple[str, ...],
) -> list[str]:
    return [term for term in terms if _term_matches(text, term)]


def _term_matches(text: str, term: str) -> bool:
    normalized_term = fold_accents(normalize_whitespace(term).lower())
    if not normalized_term:
        return False

    parts = normalized_term.split()
    if len(parts) > 1:
        pattern = r"\b" + r"\s+".join(_word_pattern(part) for part in parts) + r"\b"
    else:
        pattern = r"\b" + _word_pattern(normalized_term) + r"\b"
    return re.search(pattern, text) is not None


def _word_pattern(word: str) -> str:
    escaped = re.escape(word)
    if len(word) <= 5:
        return escaped
    return escaped + r"\w*"


def _record_text(record: dict[str, Any]) -> str:
    parts = [
        str(record.get("display_title") or ""),
        str(record.get("title_normalized") or ""),
    ]
    for key in ("status", "latest_movement"):
        value = record.get(key)
        if isinstance(value, dict):
            parts.extend(str(v) for v in value.values() if isinstance(v, str))
    for evidence in record.get("source_evidence") or []:
        if isinstance(evidence, dict):
            parts.append(str(evidence.get("summary") or ""))
    return fold_accents(normalize_whitespace(" ".join(parts)).lower())


def _source_ids(evidence: list[dict[str, Any]]) -> list[str]:
    return _unique_preserve_order(str(item.get("source_id") or "") for item in evidence)


def _compact_mapping(value: dict[str, Any]) -> dict[str, Any]:
    return {str(k): v for k, v in value.items() if v not in ("", None, [], {})}


def _active_forecast_match(
    record: dict[str, Any],
    active_forecasts: list[dict[str, Any]],
) -> bool:
    canonical_id = str(record.get("canonical_bill_id") or "")
    title = fold_accents(str(record.get("display_title") or "").lower())
    project = (
        record.get("origin_project")
        if isinstance(record.get("origin_project"), dict)
        else {}
    )
    project_tokens = [
        str(project.get("number") or ""),
        str(project.get("year") or ""),
        str(project.get("chamber") or ""),
    ]
    for forecast in active_forecasts:
        text = fold_accents(
            " ".join(
                str(forecast.get(key) or "")
                for key in (
                    "forecast_id",
                    "question",
                    "evidence_pack",
                    "forecast_draft",
                )
            ).lower()
        )
        if canonical_id and canonical_id in text:
            return True
        if all(token and token in text for token in project_tokens):
            return True
        title_terms = [term for term in re.findall(r"[a-z0-9]{4,}", title) if term]
        if title_terms and sum(1 for term in title_terms[:8] if term in text) >= 4:
            return True
    return False


def _load_active_forecasts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    active: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        if str(record.get("status") or "") in ACTIVE_FORECAST_STATUSES:
            active.append(record)
    return active


def _rank_id(run_date: str, canonical_id: str, question: str) -> str:
    digest = hashlib.sha1(
        "\x1f".join([run_date, canonical_id, question]).encode("utf-8")
    ).hexdigest()[:12]
    return f"m2leg_{digest}"


def _unique_preserve_order(values: Any) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))
