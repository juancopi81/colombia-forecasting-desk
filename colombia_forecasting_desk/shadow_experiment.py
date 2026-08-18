from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from colombia_forecasting_desk.agent_analysis import (
    ALLOWED_OVERALL_DISPOSITIONS as ALLOWED_DISPOSITIONS,
)
from colombia_forecasting_desk.agent_analysis import (
    SCHEMA_VERSION as AGENT_ANALYSIS_SCHEMA_VERSION,
)
from colombia_forecasting_desk.shadow_forecasts import (
    SCHEMA_VERSION as SHADOW_FORECAST_SCHEMA_VERSION,
)

SUMMARY_SCHEMA_VERSION = "shadow_experiment_summary.v2"
RUN_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
INTERPRETATION_GUARDRAIL = (
    "Descriptive only. This small sample does not establish model quality."
)


@dataclass(frozen=True, slots=True)
class ShadowExperimentConfig:
    start_date: date
    target_decision_grade_runs: int


def load_shadow_experiment_config(path: Path) -> ShadowExperimentConfig:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Shadow experiment config must be a mapping.")

    try:
        start_date = date.fromisoformat(str(payload["start_date"]))
        target_runs = int(payload["target_decision_grade_runs"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            "Shadow experiment config requires a valid start_date and "
            "target_decision_grade_runs."
        ) from exc
    if target_runs <= 0:
        raise ValueError("target_decision_grade_runs must be positive.")
    return ShadowExperimentConfig(start_date, target_runs)


def _load_valid_analysis(path: Path, expected_run_date: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("schema_version") != AGENT_ANALYSIS_SCHEMA_VERSION:
        return None
    if payload.get("run_date") != expected_run_date:
        return None
    if _analysis_disposition(payload) not in ALLOWED_DISPOSITIONS:
        return None
    return payload


def _analysis_disposition(analysis: dict[str, Any]) -> str:
    value = analysis.get("overall_disposition")
    return value.strip() if isinstance(value, str) else ""


def _acceptance_failure_reason(path: Path, expected_run_date: str) -> str | None:
    if not path.exists():
        return "missing_acceptance_report"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "invalid_acceptance_report"
    if not isinstance(payload, dict) or payload.get("run_date") != expected_run_date:
        return "invalid_acceptance_report"
    if payload.get("status") != "pass":
        return "status_not_pass"
    if "strict_pass" in payload and payload.get("strict_pass") is not True:
        return "strict_pass_false"
    error_count = payload.get("error_count", 0)
    if isinstance(error_count, bool) or not isinstance(error_count, (int, float)):
        return "invalid_acceptance_report"
    if error_count > 0:
        return "error_level_failure"
    issues = payload.get("issues", [])
    if not isinstance(issues, list):
        return "invalid_acceptance_report"
    if any(
        isinstance(issue, dict)
        and str(issue.get("severity", "")).lower() in {"error", "fatal"}
        for issue in issues
    ):
        return "error_level_failure"
    return None


def _collect_analyses(
    runs_dir: Path, start_date: date
) -> tuple[list[tuple[str, dict[str, Any]]], dict[str, Any]]:
    analyses: list[tuple[str, dict[str, Any]]] = []
    missing_analysis_dates: list[str] = []
    invalid_analysis_dates: list[str] = []
    non_decision_grade_dates: list[str] = []
    non_decision_grade_reasons: Counter[str] = Counter()
    if not runs_dir.exists():
        return analyses, _skipped_runs_summary(
            missing_analysis_dates,
            invalid_analysis_dates,
            non_decision_grade_dates,
            non_decision_grade_reasons,
        )

    for run_dir in sorted(path for path in runs_dir.iterdir() if path.is_dir()):
        if not RUN_DIR_RE.fullmatch(run_dir.name):
            continue
        try:
            run_date = date.fromisoformat(run_dir.name)
        except ValueError:
            continue
        if run_date < start_date:
            continue
        analysis_path = run_dir / "agent_analysis.json"
        if not analysis_path.exists():
            missing_analysis_dates.append(run_dir.name)
            continue
        analysis = _load_valid_analysis(analysis_path, run_dir.name)
        if analysis is None:
            invalid_analysis_dates.append(run_dir.name)
            continue
        acceptance_reason = _acceptance_failure_reason(
            run_dir / "acceptance_report.json", run_dir.name
        )
        if acceptance_reason is not None:
            non_decision_grade_dates.append(run_dir.name)
            non_decision_grade_reasons[acceptance_reason] += 1
            continue
        analyses.append((run_dir.name, analysis))
    return analyses, _skipped_runs_summary(
        missing_analysis_dates,
        invalid_analysis_dates,
        non_decision_grade_dates,
        non_decision_grade_reasons,
    )


def _skipped_runs_summary(
    missing_analysis_dates: list[str],
    invalid_analysis_dates: list[str],
    non_decision_grade_dates: list[str],
    non_decision_grade_reasons: Counter[str],
) -> dict[str, Any]:
    return {
        "missing_agent_analysis": {
            "count": len(missing_analysis_dates),
            "run_dates": missing_analysis_dates,
        },
        "invalid_agent_analysis": {
            "count": len(invalid_analysis_dates),
            "run_dates": invalid_analysis_dates,
        },
        "non_decision_grade": {
            "count": len(non_decision_grade_dates),
            "run_dates": non_decision_grade_dates,
            "reasons": dict(sorted(non_decision_grade_reasons.items())),
        },
    }


def _followup_completed(analysis: dict[str, Any]) -> bool:
    followup = analysis.get("official_source_follow_up")
    if isinstance(followup, dict):
        if not all(
            isinstance(followup.get(field), str) and followup[field].strip()
            for field in ("candidate", "bounded_scope", "result")
        ):
            return False
        sources = followup.get("sources_checked")
        return (
            isinstance(sources, list)
            and bool(sources)
            and all(
                isinstance(source, dict)
                and all(
                    isinstance(source.get(field), str) and source[field].strip()
                    for field in ("source_id", "url", "result")
                )
                for source in sources
            )
        )

    return False


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 4)


def _tension_review_counts(
    analysis: dict[str, Any], run_dir: Path
) -> tuple[int, int]:
    triggered: Any = []
    artifact_path = run_dir / "indicator_tension_cards.json"
    if artifact_path.exists():
        try:
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            artifact = None
        if isinstance(artifact, list):
            triggered = [
                card.get("card_id") for card in artifact if isinstance(card, dict)
            ]
    if not isinstance(triggered, list):
        triggered = []
    triggered_ids = {item for item in triggered if isinstance(item, str) and item}

    reviews = analysis.get("tension_card_reviews")
    if not isinstance(reviews, list):
        reviews = []
    reviewed_ids: set[str] = set()
    for review in reviews:
        if not isinstance(review, dict):
            continue
        card_id = review.get("card_id")
        if card_id not in triggered_ids:
            continue
        reviewed_ids.add(card_id)
    return len(triggered_ids), len(reviewed_ids)


def _valid_shadow_forecast(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("schema_version") != SHADOW_FORECAST_SCHEMA_VERSION:
        return False
    if not isinstance(payload.get("forecast_id"), str) or not payload["forecast_id"]:
        return False
    try:
        date.fromisoformat(str(payload.get("run_date")))
    except ValueError:
        return False
    return payload.get("status") in {"open", "resolved"}


def _load_shadow_forecasts(
    path: Path, counted_run_dates: set[str]
) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], 0
    forecasts_by_id: dict[str, dict[str, Any]] = {}
    invalid_rows = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            invalid_rows += 1
            continue
        if not _valid_shadow_forecast(payload):
            invalid_rows += 1
            continue
        if payload["run_date"] not in counted_run_dates:
            continue
        forecasts_by_id[payload["forecast_id"]] = payload
    return [forecasts_by_id[key] for key in sorted(forecasts_by_id)], invalid_rows


def _has_complete_baseline(forecast: dict[str, Any]) -> bool:
    baseline = forecast.get("baseline")
    if not isinstance(baseline, dict):
        return False
    label = baseline.get("label")
    probability = baseline.get("probability")
    return (
        isinstance(label, str)
        and bool(label.strip())
        and not isinstance(probability, bool)
        and isinstance(probability, (int, float))
        and 0 <= probability <= 1
    )


def _iso_date(value: Any) -> date | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _is_overdue(forecast: dict[str, Any], as_of: date | None) -> bool:
    if forecast.get("status") != "open" or as_of is None:
        return False
    deadline = _iso_date(
        forecast.get("resolution_check_window_end")
        or forecast.get("resolution_deadline")
    )
    return deadline is not None and deadline < as_of


def _shadow_forecast_summary(
    forecasts: list[dict[str, Any]], invalid_rows: int, as_of: date | None
) -> dict[str, Any]:
    open_forecasts = [row for row in forecasts if row["status"] == "open"]
    resolved_forecasts = [row for row in forecasts if row["status"] == "resolved"]
    complete_baselines = sum(_has_complete_baseline(row) for row in forecasts)
    brier_scores = [
        float(row["brier_score"])
        for row in resolved_forecasts
        if not isinstance(row.get("brier_score"), bool)
        and isinstance(row.get("brier_score"), (int, float))
        and 0 <= row["brier_score"] <= 1
    ]
    comparable_scores: list[tuple[float, float, float]] = []
    for row in resolved_forecasts:
        model_brier = row.get("brier_score")
        baseline_brier = row.get("baseline_brier_score")
        improvement = row.get("brier_improvement_vs_baseline")
        if not (
            _valid_brier_score(model_brier)
            and _valid_brier_score(baseline_brier)
            and _valid_brier_improvement(improvement)
        ):
            continue
        expected_improvement = round(float(baseline_brier) - float(model_brier), 4)
        if improvement != expected_improvement:
            continue
        comparable_scores.append(
            (float(model_brier), float(baseline_brier), float(improvement))
        )
    return {
        "created": len(forecasts),
        "open": len(open_forecasts),
        "resolved": len(resolved_forecasts),
        "overdue": sum(_is_overdue(row, as_of) for row in open_forecasts),
        "baseline_fields_present": complete_baselines,
        "baseline_fields_missing": len(forecasts) - complete_baselines,
        "resolved_with_brier": len(brier_scores),
        "brier_mean_resolved": (
            round(sum(brier_scores) / len(brier_scores), 4)
            if brier_scores
            else None
        ),
        "resolved_with_comparable_brier": len(comparable_scores),
        "model_brier_mean_comparable": _mean_or_none(
            [model for model, _, _ in comparable_scores]
        ),
        "baseline_brier_mean_resolved": _mean_or_none(
            [baseline for _, baseline, _ in comparable_scores]
        ),
        "brier_improvement_mean_resolved": _mean_or_none(
            [improvement for _, _, improvement in comparable_scores]
        ),
        "invalid_log_rows_ignored": invalid_rows,
    }


def _valid_brier_score(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and 0 <= value <= 1
    )


def _valid_brier_improvement(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and -1 <= value <= 1
    )


def _mean_or_none(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def build_shadow_experiment_summary(
    *,
    config_path: Path,
    runs_dir: Path,
    shadow_log_path: Path,
) -> dict[str, Any]:
    config = load_shadow_experiment_config(config_path)
    analyses, skipped_runs = _collect_analyses(runs_dir, config.start_date)
    run_dates = [run_date for run_date, _ in analyses]
    counted_runs = len(analyses)
    remaining_runs = max(config.target_decision_grade_runs - counted_runs, 0)
    dispositions = Counter(
        _analysis_disposition(analysis) for _, analysis in analyses
    )
    completed_followups = sum(
        _followup_completed(analysis) for _, analysis in analyses
    )
    tension_counts = [
        _tension_review_counts(analysis, runs_dir / run_date)
        for run_date, analysis in analyses
    ]
    triggered_cards = sum(triggered for triggered, _ in tension_counts)
    reviewed_cards = sum(reviewed for _, reviewed in tension_counts)
    forecasts, invalid_log_rows = _load_shadow_forecasts(
        shadow_log_path, set(run_dates)
    )
    latest_run_date = date.fromisoformat(run_dates[-1]) if run_dates else None

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "status": "ready_for_review" if remaining_runs == 0 else "collecting",
        "interpretation_guardrail": INTERPRETATION_GUARDRAIL,
        "runs": {
            "start_date": config.start_date.isoformat(),
            "target_decision_grade_runs": config.target_decision_grade_runs,
            "counted_decision_grade_runs": counted_runs,
            "remaining_decision_grade_runs": remaining_runs,
            "run_dates": run_dates,
            "latest_run_date": run_dates[-1] if run_dates else None,
            "skipped_runs": skipped_runs,
        },
        "disposition_counts": dict(sorted(dispositions.items())),
        "official_source_follow_up": {
            "completed_runs": completed_followups,
            "incomplete_runs": counted_runs - completed_followups,
            "completion_rate": _rate(completed_followups, counted_runs),
        },
        "tension_card_review": {
            "triggered_cards": triggered_cards,
            "reviewed_cards": reviewed_cards,
            "unreviewed_cards": triggered_cards - reviewed_cards,
            "coverage_rate": _rate(reviewed_cards, triggered_cards),
        },
        "shadow_forecasts": _shadow_forecast_summary(
            forecasts, invalid_log_rows, latest_run_date
        ),
    }


def _format_rate(value: float | None) -> str:
    return "not available" if value is None else f"{value:.2%}"


def render_shadow_experiment_markdown(summary: dict[str, Any]) -> str:
    runs = summary["runs"]
    skipped = runs["skipped_runs"]
    followup = summary["official_source_follow_up"]
    tension = summary["tension_card_review"]
    forecasts = summary["shadow_forecasts"]
    dispositions = summary["disposition_counts"]
    disposition_rows = (
        "\n".join(
            f"| `{name}` | {count} |" for name, count in sorted(dispositions.items())
        )
        or "| _none_ | 0 |"
    )
    brier = forecasts["brier_mean_resolved"]
    brier_text = "not available" if brier is None else f"{brier:.4f}"
    baseline_brier = forecasts["baseline_brier_mean_resolved"]
    baseline_brier_text = (
        "not available" if baseline_brier is None else f"{baseline_brier:.4f}"
    )
    improvement = forecasts["brier_improvement_mean_resolved"]
    improvement_text = (
        "not available" if improvement is None else f"{improvement:+.4f}"
    )
    comparable_model_brier = forecasts["model_brier_mean_comparable"]
    comparable_model_brier_text = (
        "not available"
        if comparable_model_brier is None
        else f"{comparable_model_brier:.4f}"
    )

    return "\n".join(
        (
            "# Shadow Forecast Experiment",
            "",
            f"**Status:** `{summary['status']}`",
            "**Decision-grade runs:** "
            f"{runs['counted_decision_grade_runs']} / "
            f"{runs['target_decision_grade_runs']}",
            f"**Start date:** `{runs['start_date']}`",
            "",
            f"> {summary['interpretation_guardrail']}",
            "",
            "## Run Collection",
            "",
            f"- Remaining decision-grade runs: {runs['remaining_decision_grade_runs']}",
            f"- Latest counted run: {runs['latest_run_date'] or 'none'}",
            "- Missing agent analysis: "
            f"{skipped['missing_agent_analysis']['count']}",
            "- Invalid agent analysis: "
            f"{skipped['invalid_agent_analysis']['count']}",
            "- Non-decision-grade runs skipped: "
            f"{skipped['non_decision_grade']['count']}",
            "",
            "## Dispositions",
            "",
            "| Disposition | Runs |",
            "| --- | ---: |",
            disposition_rows,
            "",
            "## Review Compliance",
            "",
            "- Bounded follow-ups completed: "
            f"{followup['completed_runs']} / "
            f"{runs['counted_decision_grade_runs']} "
            f"({_format_rate(followup['completion_rate'])})",
            "- Triggered tension cards reviewed: "
            f"{tension['reviewed_cards']} / {tension['triggered_cards']} "
            f"({_format_rate(tension['coverage_rate'])})",
            "",
            "## Shadow Forecasts",
            "",
            f"- Created: {forecasts['created']}",
            f"- Open: {forecasts['open']}",
            f"- Resolved: {forecasts['resolved']}",
            f"- Overdue: {forecasts['overdue']}",
            "- Complete baseline fields: "
            f"{forecasts['baseline_fields_present']} / {forecasts['created']}",
            "- Resolved forecasts with Brier score: "
            f"{forecasts['resolved_with_brier']}",
            "- Resolved forecasts with comparable model/baseline scores: "
            f"{forecasts['resolved_with_comparable_brier']}",
            f"- Mean model Brier score (all scored resolved rows): {brier_text}",
            "- Mean model Brier score (paired rows only): "
            f"{comparable_model_brier_text}",
            f"- Mean baseline Brier score (paired rows only): {baseline_brier_text}",
            "- Mean Brier improvement vs baseline (positive is better): "
            f"{improvement_text}",
            "",
        )
    )


def write_shadow_experiment_summary(
    *,
    config_path: Path,
    runs_dir: Path,
    shadow_log_path: Path,
    output_dir: Path,
) -> tuple[dict[str, Any], Path, Path]:
    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=shadow_log_path,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "shadow_experiment_summary.json"
    markdown_path = output_dir / "shadow_experiment_summary.md"
    json_path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_shadow_experiment_markdown(summary), encoding="utf-8"
    )
    return summary, json_path, markdown_path
