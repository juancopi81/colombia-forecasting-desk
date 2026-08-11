from __future__ import annotations

import json
from pathlib import Path

from scripts.finalize_agent_analysis import finalize
from colombia_forecasting_desk.shadow_forecasts import resolve_shadow_forecast


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _analysis(run_date: str, *, shadow: bool) -> dict:
    shadow_forecast = (
        {
            "id": f"shadow_{run_date.replace('-', '')}_trm",
            "visibility": "internal",
            "question": "Will official TRM exceed 3,200 COP/USD on 2026-08-18?",
            "probability": 0.58,
            "prediction_date": run_date,
            "resolution_date": "2026-08-18",
            "official_resolver": {
                "source_name": "Superfinanciera",
                "source_url": "https://www.superfinanciera.gov.co/",
            },
            "resolution_criteria": {
                "yes": "Official TRM exceeds 3,200 COP/USD.",
                "no": "Official TRM is at or below 3,200 COP/USD.",
            },
            "baseline": {
                "name": "coin flip",
                "probability": 0.5,
                "rationale": "Direction-only baseline.",
            },
            "evidence": [
                {
                    "artifact": "market_pricing_watch.json",
                    "locator": "series_id=trm",
                    "note": "Official TRM context.",
                }
            ],
            "rationale_for": ["Recent appreciation may mean-revert."],
            "rationale_against": ["Short-horizon FX is noisy."],
            "falsifier": "A broad risk-off dollar move.",
        }
        if shadow
        else None
    )
    return {
        "schema_version": "agent_analysis.v1",
        "run_date": run_date,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "strongest_changed_signal": {
            "signal": "TRM moved sharply.",
            "evidence_refs": [
                {
                    "artifact": "market_pricing_watch.json",
                    "locator": "series_id=trm",
                    "note": "Official observation.",
                }
            ],
            "interpretation": "Worth an internal test.",
            "alternative_explanations": ["Broad dollar move."],
            "falsifiers": ["Immediate reversal."],
        },
        "tension_card_reviews": [],
        "relationships": [
            {
                "type": "unbundled",
                "relationship": "TRM versus policy stance.",
                "evidence_refs": [
                    {
                        "artifact": "market_pricing_watch.json",
                        "locator": "series_id=trm",
                        "note": "Cross-series review.",
                    }
                ],
                "interpretation": "Potential interaction.",
                "caveats": ["Not causal."],
            }
        ],
        "official_source_follow_up": {
            "candidate": "TRM direction",
            "bounded_scope": "Checked one official resolver.",
            "sources_checked": [
                {
                    "source_id": "trm",
                    "url": "https://www.superfinanciera.gov.co/",
                    "result": "Resolver exists.",
                }
            ],
            "result": "Internal-only candidate.",
        },
        "public_interest_candidate": {
            "candidate": "No public candidate",
            "disposition": "none",
            "rationale": "No public decision hook.",
            "evidence_refs": [
                {
                    "artifact": "market_pricing_watch.json",
                    "locator": "series_id=trm",
                    "note": "Context only.",
                }
            ],
            "missing_evidence": [],
        },
        "shadow_forecast": shadow_forecast,
        "overall_disposition": "shadow_track" if shadow else "insight_only",
        "overall_rationale": "Keep public gates unchanged.",
    }


def _prepare(tmp_path: Path, *, shadow: bool = True) -> dict[str, Path]:
    run_date = "2026-08-12"
    runs_dir = tmp_path / "runs"
    run_dir = runs_dir / run_date
    _write_json(run_dir / "agent_analysis.json", _analysis(run_date, shadow=shadow))
    _write_json(run_dir / "indicator_tension_cards.json", [])
    _write_json(
        run_dir / "acceptance_report.json",
        {
            "run_date": run_date,
            "status": "pass",
            "strict_pass": True,
            "error_count": 0,
            "issues": [],
        },
    )
    _write_json(run_dir / "run_summary.json", {"run_date": run_date})
    _write_json(run_dir / "analyst_leads.json", {"summary": {}, "leads": []})
    _write_json(run_dir / "m2_ranked_questions.json", {"bucket_counts": {}})
    config = tmp_path / "shadow_experiment.yaml"
    config.write_text(
        "start_date: 2026-08-12\ntarget_decision_grade_runs: 10\n",
        encoding="utf-8",
    )
    return {
        "run_date": run_date,
        "runs_dir": runs_dir,
        "ledger": tmp_path / "forecasts" / "shadow_forecast_log.jsonl",
        "config": config,
        "output": tmp_path / "forecasts",
    }


def test_finalize_appends_shadow_once_and_is_idempotent(tmp_path: Path) -> None:
    paths = _prepare(tmp_path)

    first = finalize(
        run_date=str(paths["run_date"]),
        runs_dir=paths["runs_dir"],
        ledger_path=paths["ledger"],
        experiment_config=paths["config"],
        experiment_output_dir=paths["output"],
    )
    original = paths["ledger"].read_bytes()
    second = finalize(
        run_date=str(paths["run_date"]),
        runs_dir=paths["runs_dir"],
        ledger_path=paths["ledger"],
        experiment_config=paths["config"],
        experiment_output_dir=paths["output"],
    )

    assert first["shadow_action"] == "appended"
    assert second["shadow_action"] == "already_recorded"
    assert paths["ledger"].read_bytes() == original
    assert (paths["runs_dir"] / str(paths["run_date"]) / "agent_analysis.md").exists()
    assert (paths["runs_dir"] / str(paths["run_date"]) / "review.html").exists()
    assert (paths["output"] / "shadow_experiment_summary.json").exists()


def test_finalize_records_abstention_without_creating_shadow_ledger(tmp_path: Path) -> None:
    paths = _prepare(tmp_path, shadow=False)

    result = finalize(
        run_date=str(paths["run_date"]),
        runs_dir=paths["runs_dir"],
        ledger_path=paths["ledger"],
        experiment_config=paths["config"],
        experiment_output_dir=paths["output"],
    )

    assert result["shadow_action"] == "not_selected"
    assert not paths["ledger"].exists()
    assert result["summary_status"] == "collecting"


def test_finalize_is_idempotent_after_shadow_forecast_resolution(tmp_path: Path) -> None:
    paths = _prepare(tmp_path)
    finalize(
        run_date=str(paths["run_date"]),
        runs_dir=paths["runs_dir"],
        ledger_path=paths["ledger"],
        experiment_config=paths["config"],
        experiment_output_dir=paths["output"],
    )
    resolve_shadow_forecast(
        paths["ledger"],
        forecast_id="shadow_20260812_trm",
        outcome="YES",
        resolved_at="2026-08-18T17:00:00Z",
        resolution_value="Official TRM exceeded 3,200 COP/USD.",
        resolution_url="https://www.superfinanciera.gov.co/resolution",
    )

    result = finalize(
        run_date=str(paths["run_date"]),
        runs_dir=paths["runs_dir"],
        ledger_path=paths["ledger"],
        experiment_config=paths["config"],
        experiment_output_dir=paths["output"],
    )

    assert result["shadow_action"] == "already_recorded"
