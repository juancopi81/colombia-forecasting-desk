from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from colombia_forecasting_desk.shadow_experiment import (
    build_shadow_experiment_summary,
    load_shadow_experiment_config,
    write_shadow_experiment_summary,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_config(path: Path, *, start_date: str, target_runs: int) -> None:
    path.write_text(
        "\n".join(
            (
                "schema_version: shadow_experiment.v1",
                f"start_date: {start_date}",
                f"target_decision_grade_runs: {target_runs}",
                "",
            )
        ),
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(f"{json.dumps(row)}\n" for row in rows),
        encoding="utf-8",
    )


def _agent_analysis(run_date: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "agent_analysis.v1",
        "run_date": run_date,
        "overall_disposition": "shadow_track",
        "official_source_follow_up": _official_followup(),
        "tension_card_reviews": [
            {
                "card_id": "real_policy_rate",
                "assessment": "The stance remains restrictive.",
                "forecast_implication": "Review the next BanRep decision.",
                "disposition": "shadow_candidate",
            }
        ],
    }
    payload.update(overrides)
    return payload


def _official_followup(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "candidate": "Next BanRep policy-rate decision",
        "bounded_scope": "Checked one official calendar.",
        "sources_checked": [
            {
                "source_id": "banrep_junta_calendar",
                "url": "https://www.banrep.gov.co/es/calendario-junta-directiva",
                "result": "A dated meeting exists.",
            }
        ],
        "result": "The candidate has an official resolver.",
    }
    payload.update(overrides)
    return payload


def _acceptance_report(run_date: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "m1_acceptance.v2",
        "run_date": run_date,
        "status": "pass",
        "strict_pass": True,
        "error_count": 0,
        "warning_count": 0,
        "issues": [],
    }
    payload.update(overrides)
    return payload


def _write_decision_grade_run(
    runs_dir: Path,
    run_date: str,
    analysis: dict[str, object] | None = None,
) -> None:
    _write_json(
        runs_dir / run_date / "agent_analysis.json",
        analysis or _agent_analysis(run_date),
    )
    _write_json(
        runs_dir / run_date / "acceptance_report.json",
        _acceptance_report(run_date),
    )
    reviews = (analysis or _agent_analysis(run_date)).get(
        "tension_card_reviews", []
    )
    _write_json(
        runs_dir / run_date / "indicator_tension_cards.json",
        [
            {"card_id": review["card_id"]}
            for review in reviews
            if isinstance(review, dict) and isinstance(review.get("card_id"), str)
        ],
    )


def test_counts_only_valid_looking_decision_grade_analyses_after_start_date(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    log_path = tmp_path / "shadow_forecast_log.jsonl"
    _write_config(config_path, start_date="2026-08-12", target_runs=2)

    _write_decision_grade_run(runs_dir, "2026-08-11")
    _write_decision_grade_run(runs_dir, "2026-08-12")
    _write_json(
        runs_dir / "2026-08-13" / "agent_analysis.json",
        {**_agent_analysis("2026-08-13"), "overall_disposition": ""},
    )
    _write_json(
        runs_dir / "2026-08-13" / "acceptance_report.json",
        _acceptance_report("2026-08-13"),
    )
    _write_json(
        runs_dir / "2026-08-14" / "agent_analysis.json",
        {**_agent_analysis("2026-08-15")},
    )
    _write_json(
        runs_dir / "2026-08-14" / "acceptance_report.json",
        _acceptance_report("2026-08-14"),
    )
    _write_json(
        runs_dir / "2026-08-15" / "agent_analysis.json",
        _agent_analysis("2026-08-15"),
    )
    _write_json(
        runs_dir / "2026-08-16" / "agent_analysis.json",
        _agent_analysis("2026-08-16"),
    )
    _write_json(
        runs_dir / "2026-08-16" / "acceptance_report.json",
        _acceptance_report("2026-08-16", strict_pass=False),
    )
    _write_json(
        runs_dir / "2026-08-17" / "acceptance_report.json",
        _acceptance_report("2026-08-17"),
    )

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=log_path,
    )

    assert summary["status"] == "collecting"
    assert summary["runs"] == {
        "start_date": "2026-08-12",
        "target_decision_grade_runs": 2,
        "counted_decision_grade_runs": 1,
        "remaining_decision_grade_runs": 1,
        "run_dates": ["2026-08-12"],
        "latest_run_date": "2026-08-12",
        "skipped_runs": {
            "missing_agent_analysis": {
                "count": 1,
                "run_dates": ["2026-08-17"],
            },
            "invalid_agent_analysis": {
                "count": 2,
                "run_dates": ["2026-08-13", "2026-08-14"],
            },
            "non_decision_grade": {
                "count": 2,
                "run_dates": ["2026-08-15", "2026-08-16"],
                "reasons": {
                    "missing_acceptance_report": 1,
                    "strict_pass_false": 1,
                },
            },
        },
    }


def test_acceptance_requires_pass_without_error_level_failures(tmp_path: Path) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    _write_config(config_path, start_date="2026-08-12", target_runs=2)

    _write_json(
        runs_dir / "2026-08-12" / "agent_analysis.json",
        _agent_analysis("2026-08-12"),
    )
    acceptance_without_strict = _acceptance_report("2026-08-12")
    acceptance_without_strict.pop("strict_pass")
    _write_json(
        runs_dir / "2026-08-12" / "acceptance_report.json",
        acceptance_without_strict,
    )
    for run_date, acceptance in (
        (
            "2026-08-13",
            _acceptance_report("2026-08-13", error_count=1),
        ),
        (
            "2026-08-14",
            _acceptance_report(
                "2026-08-14",
                issues=[{"severity": "error", "code": "strict_failure"}],
            ),
        ),
        (
            "2026-08-15",
            _acceptance_report("2026-08-15", status="fail"),
        ),
    ):
        _write_json(
            runs_dir / run_date / "agent_analysis.json",
            _agent_analysis(run_date),
        )
        _write_json(
            runs_dir / run_date / "acceptance_report.json",
            acceptance,
        )

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=tmp_path / "missing.jsonl",
    )

    assert summary["runs"]["run_dates"] == ["2026-08-12"]
    assert summary["runs"]["skipped_runs"]["non_decision_grade"] == {
        "count": 3,
        "run_dates": ["2026-08-13", "2026-08-14", "2026-08-15"],
        "reasons": {
            "error_level_failure": 2,
            "status_not_pass": 1,
        },
    }


def test_summarizes_dispositions_across_counted_runs(tmp_path: Path) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    _write_config(config_path, start_date="2026-08-12", target_runs=3)
    for run_date, disposition in (
        ("2026-08-12", "shadow_track"),
        ("2026-08-13", "insight_only"),
        ("2026-08-14", "shadow_track"),
    ):
        _write_decision_grade_run(
            runs_dir,
            run_date,
            _agent_analysis(run_date, overall_disposition=disposition),
        )

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=tmp_path / "missing.jsonl",
    )

    assert summary["status"] == "ready_for_review"
    assert summary["disposition_counts"] == {
        "insight_only": 1,
        "shadow_track": 2,
    }


def test_stops_counting_after_configured_decision_grade_target(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    _write_config(config_path, start_date="2026-08-12", target_runs=2)
    for run_date, disposition in (
        ("2026-08-12", "shadow_track"),
        ("2026-08-13", "insight_only"),
        ("2026-08-14", "insight_only"),
    ):
        _write_decision_grade_run(
            runs_dir,
            run_date,
            _agent_analysis(run_date, overall_disposition=disposition),
        )
    _write_json(
        runs_dir / "2026-08-15" / "acceptance_report.json",
        _acceptance_report("2026-08-15"),
    )
    _write_json(
        runs_dir / "2026-08-16" / "agent_analysis.json",
        {**_agent_analysis("2026-08-16"), "overall_disposition": ""},
    )
    _write_json(
        runs_dir / "2026-08-16" / "acceptance_report.json",
        _acceptance_report("2026-08-16"),
    )
    _write_json(
        runs_dir / "2026-08-17" / "agent_analysis.json",
        _agent_analysis("2026-08-17"),
    )
    _write_json(
        runs_dir / "2026-08-17" / "acceptance_report.json",
        _acceptance_report("2026-08-17", strict_pass=False),
    )

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=tmp_path / "missing.jsonl",
    )

    assert summary["runs"]["counted_decision_grade_runs"] == 2
    assert summary["runs"]["run_dates"] == ["2026-08-12", "2026-08-13"]
    assert summary["runs"]["latest_run_date"] == "2026-08-13"
    assert summary["disposition_counts"] == {
        "insight_only": 1,
        "shadow_track": 1,
    }
    assert summary["runs"]["skipped_runs"] == {
        "missing_agent_analysis": {"count": 0, "run_dates": []},
        "invalid_agent_analysis": {"count": 0, "run_dates": []},
        "non_decision_grade": {"count": 0, "run_dates": [], "reasons": {}},
    }


def test_summarizes_bounded_followup_completion(tmp_path: Path) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    _write_config(config_path, start_date="2026-08-12", target_runs=5)
    followups: tuple[object, ...] = (
        _official_followup(),
        _official_followup(result="No promotion, but the check was completed."),
        _official_followup(sources_checked=[]),
        _official_followup(candidate=""),
        _official_followup(
            sources_checked=[
                {
                    "source_id": "banrep_junta_calendar",
                    "url": "https://www.banrep.gov.co/es/calendario-junta-directiva",
                    "result": "",
                }
            ]
        ),
    )
    for offset, followup in enumerate(followups, start=12):
        run_date = f"2026-08-{offset:02d}"
        _write_decision_grade_run(
            runs_dir,
            run_date,
            _agent_analysis(run_date, official_source_follow_up=followup),
        )

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=tmp_path / "missing.jsonl",
    )

    assert summary["official_source_follow_up"] == {
        "completed_runs": 2,
        "incomplete_runs": 3,
        "completion_rate": 0.4,
    }


def test_summarizes_tension_card_review_coverage(tmp_path: Path) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    _write_config(config_path, start_date="2026-08-12", target_runs=2)
    _write_decision_grade_run(
        runs_dir,
        "2026-08-12",
        _agent_analysis(
            "2026-08-12",
            tension_card_reviews=[
                {
                    "card_id": "real_policy_rate",
                    "assessment": "Reviewed.",
                    "forecast_implication": "Possible shadow case.",
                    "disposition": "shadow_candidate",
                },
            ],
        ),
    )
    _write_json(
        runs_dir / "2026-08-12" / "indicator_tension_cards.json",
        [
            {"card_id": "real_policy_rate"},
            {"card_id": "construction_cost"},
        ],
    )
    _write_decision_grade_run(
        runs_dir,
        "2026-08-13",
        _agent_analysis(
            "2026-08-13",
            tension_card_reviews=[
                {
                    "card_id": "tes_policy_spread",
                    "assessment": "Reviewed.",
                    "forecast_implication": "No clean case today.",
                    "disposition": "no_action",
                }
            ],
        ),
    )

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=tmp_path / "missing.jsonl",
    )

    assert summary["tension_card_review"] == {
        "triggered_cards": 3,
        "reviewed_cards": 2,
        "unreviewed_cards": 1,
        "coverage_rate": 0.6667,
    }


def test_summarizes_shadow_forecasts_for_counted_runs(tmp_path: Path) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    log_path = tmp_path / "shadow_forecast_log.jsonl"
    _write_config(config_path, start_date="2026-08-12", target_runs=2)
    for run_date in ("2026-08-12", "2026-08-20"):
        _write_decision_grade_run(runs_dir, run_date)

    complete_baseline = {"label": "no_change_50_50", "probability": 0.5}
    rows = [
        {
            "schema_version": "shadow_forecast.v1",
            "forecast_id": "open_overdue",
            "run_date": "2026-08-12",
            "status": "open",
            "resolution_deadline": "2026-08-18",
            "baseline": complete_baseline,
        },
        {
            "schema_version": "shadow_forecast.v1",
            "forecast_id": "open_current",
            "run_date": "2026-08-20",
            "status": "open",
            "resolution_check_window_end": "2026-08-22",
            "brier_score": 0.99,
        },
        {
            "schema_version": "shadow_forecast.v1",
            "forecast_id": "resolved_scored",
            "run_date": "2026-08-12",
            "status": "resolved",
            "baseline": complete_baseline,
            "brier_score": 0.16,
            "baseline_brier_score": 0.25,
            "brier_improvement_vs_baseline": 0.09,
        },
        {
            "schema_version": "shadow_forecast.v1",
            "forecast_id": "resolved_unscored",
            "run_date": "2026-08-20",
            "status": "resolved",
            "baseline": complete_baseline,
        },
        {
            "schema_version": "shadow_forecast.v1",
            "forecast_id": "outside_experiment",
            "run_date": "2026-08-13",
            "status": "open",
            "baseline": complete_baseline,
        },
    ]
    _write_jsonl(log_path, rows)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("{malformed\n")

    summary = build_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=log_path,
    )

    assert summary["shadow_forecasts"] == {
        "created": 4,
        "open": 2,
        "resolved": 2,
        "overdue": 1,
        "baseline_fields_present": 3,
        "baseline_fields_missing": 1,
        "resolved_with_brier": 1,
        "brier_mean_resolved": 0.16,
        "resolved_with_comparable_brier": 1,
        "model_brier_mean_comparable": 0.16,
        "baseline_brier_mean_resolved": 0.25,
        "brier_improvement_mean_resolved": 0.09,
        "invalid_log_rows_ignored": 1,
    }


def test_writes_deterministic_json_and_descriptive_markdown_without_mutating_log(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "shadow_experiment.yaml"
    runs_dir = tmp_path / "runs"
    log_path = tmp_path / "forecasts" / "shadow_forecast_log.jsonl"
    output_dir = tmp_path / "summary"
    _write_config(config_path, start_date="2026-08-12", target_runs=10)
    _write_decision_grade_run(runs_dir, "2026-08-12")
    _write_jsonl(
        log_path,
        [
            {
                "schema_version": "shadow_forecast.v1",
                "forecast_id": "shadow_one",
                "run_date": "2026-08-12",
                "status": "open",
                "resolution_deadline": "2026-08-20",
                "baseline": {
                    "label": "no_change_50_50",
                    "probability": 0.5,
                },
            }
        ],
    )
    original_log = log_path.read_bytes()

    summary, json_path, markdown_path = write_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=log_path,
        output_dir=output_dir,
    )

    assert json.loads(json_path.read_text(encoding="utf-8")) == summary
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "# Shadow Forecast Experiment" in markdown
    assert "**Status:** `collecting`" in markdown
    assert "**Decision-grade runs:** 1 / 10" in markdown
    assert "does not establish model quality" in markdown
    assert "Mean baseline Brier score" in markdown
    assert log_path.read_bytes() == original_log

    first_json = json_path.read_bytes()
    first_markdown = markdown_path.read_bytes()
    write_shadow_experiment_summary(
        config_path=config_path,
        runs_dir=runs_dir,
        shadow_log_path=log_path,
        output_dir=output_dir,
    )
    assert json_path.read_bytes() == first_json
    assert markdown_path.read_bytes() == first_markdown


def test_cli_supports_config_runs_log_and_output_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "custom.yaml"
    runs_dir = tmp_path / "custom-runs"
    log_path = tmp_path / "custom-shadow.jsonl"
    output_dir = tmp_path / "custom-output"
    _write_config(config_path, start_date="2026-08-12", target_runs=1)
    _write_decision_grade_run(runs_dir, "2026-08-12")
    _write_jsonl(log_path, [])
    repo_root = Path(__file__).resolve().parent.parent

    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "summarize_shadow_experiment.py"),
            "--config",
            str(config_path),
            "--runs-dir",
            str(runs_dir),
            "--shadow-log",
            str(log_path),
            "--output-dir",
            str(output_dir),
        ],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (output_dir / "shadow_experiment_summary.json").exists()
    assert (output_dir / "shadow_experiment_summary.md").exists()
    assert "ready_for_review" in result.stdout


def test_repository_config_defines_the_ten_run_experiment() -> None:
    repo_root = Path(__file__).resolve().parent.parent

    config = load_shadow_experiment_config(
        repo_root / "config" / "shadow_experiment.yaml"
    )

    assert config.start_date.isoformat() == "2026-08-12"
    assert config.target_decision_grade_runs == 10
