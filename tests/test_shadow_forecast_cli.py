from __future__ import annotations

import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APPEND_SCRIPT = REPO_ROOT / "scripts" / "append_shadow_forecast.py"
VALIDATE_SCRIPT = REPO_ROOT / "scripts" / "validate_shadow_forecasts.py"


def _load_script(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


append_cli = _load_script(APPEND_SCRIPT, "append_shadow_forecast")
validate_cli = _load_script(VALIDATE_SCRIPT, "validate_shadow_forecasts")


def _analysis(**overrides: object) -> dict[str, object]:
    evidence = {
        "artifact": "indicator_tension_cards.json",
        "locator": "card_id=real_policy_rate",
        "note": "Triggered official-indicator tension.",
    }
    analysis: dict[str, object] = {
        "schema_version": "agent_analysis.v1",
        "run_date": "2026-08-11",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "strongest_changed_signal": {
            "signal": "The ex-post real policy rate is elevated.",
            "evidence_refs": [evidence],
            "interpretation": "Policy is restrictive.",
            "alternative_explanations": ["Expectations may remain elevated."],
            "falsifiers": ["Expectations rise materially."],
        },
        "tension_card_reviews": [
            {
                "card_id": "real_policy_rate",
                "assessment": "Useful lead.",
                "forecast_implication": "Inspect the next decision.",
                "disposition": "shadow_candidate",
            }
        ],
        "relationships": [
            {
                "type": "cross_bundle",
                "relationship": "Policy restraint and peso strength coincide.",
                "evidence_refs": [evidence],
                "interpretation": "FX strength may affect policy space.",
                "caveats": ["Global dollar weakness may dominate."],
            }
        ],
        "official_source_follow_up": {
            "candidate": "Next BanRep decision",
            "bounded_scope": "Checked the official calendar.",
            "sources_checked": [
                {
                    "source_id": "banrep_junta_calendar",
                    "url": "https://www.banrep.gov.co/es/calendario-junta-directiva",
                    "result": "Decision scheduled for 2026-09-30.",
                }
            ],
            "result": "A dated official resolver exists.",
        },
        "public_interest_candidate": {
            "candidate": "Will BanRep change the policy rate?",
            "disposition": "research_more",
            "rationale": "Expectations evidence is incomplete.",
            "evidence_refs": [evidence],
            "missing_evidence": ["Expectations survey."],
        },
        "overall_disposition": "shadow_track",
        "shadow_forecast": {
            "id": "shadow_20260811_banrep_sep_rate_change",
            "visibility": "internal",
            "question": "Will BanRep change the 12% policy rate on 2026-09-30?",
            "probability": 0.62,
            "prediction_date": "2026-08-11",
            "resolution_date": "2026-09-30",
            "official_resolver": {
                "source_name": "Banco de la Republica",
                "source_url": "https://www.banrep.gov.co/es/estadisticas/tasas-interes-politica-monetaria",
            },
            "resolution_criteria": {
                "yes": "The official decision changes the 12% rate.",
                "no": "The official decision keeps the rate at 12%.",
            },
            "baseline": {
                "name": "Persistence / unchanged policy rate",
                "probability": 0.35,
                "rationale": "Most decisions leave the rate unchanged.",
            },
            "evidence": [evidence],
            "rationale_for": ["The real policy rate is elevated."],
            "rationale_against": ["Inflation remains above target."],
            "falsifier": "Expectations rise materially before the meeting.",
        },
        "overall_rationale": "Track internally without publication.",
    }
    analysis.update(overrides)
    return analysis


def _write_analysis(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
    (path.parent / "indicator_tension_cards.json").write_text(
        json.dumps([{"card_id": "real_policy_rate"}]), encoding="utf-8"
    )


def test_append_cli_writes_one_open_internal_row(tmp_path: Path) -> None:
    analysis_path = tmp_path / "agent_analysis.json"
    ledger = tmp_path / "forecasts" / "shadow_forecast_log.jsonl"
    _write_analysis(analysis_path, _analysis())

    code = append_cli.main(
        ["--analysis", str(analysis_path), "--ledger", str(ledger)]
    )

    assert code == 0
    row = json.loads(ledger.read_text(encoding="utf-8"))
    assert row["forecast_id"] == "shadow_20260811_banrep_sep_rate_change"
    assert row["status"] == "open"
    assert row["visibility"] == "internal"
    assert row["source_analysis"] == str(analysis_path)


def test_append_cli_refuses_non_shadow_disposition_without_writing(
    tmp_path: Path,
) -> None:
    analysis_path = tmp_path / "agent_analysis.json"
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    _write_analysis(
        analysis_path,
        _analysis(overall_disposition="insight_only", shadow_forecast=None),
    )

    code = append_cli.main(
        ["--analysis", str(analysis_path), "--ledger", str(ledger)]
    )

    assert code == 1
    assert not ledger.exists()


def test_validate_cli_reports_valid_row_count(tmp_path: Path, capsys) -> None:
    analysis_path = tmp_path / "agent_analysis.json"
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    _write_analysis(analysis_path, _analysis())
    assert append_cli.main(
        ["--analysis", str(analysis_path), "--ledger", str(ledger)]
    ) == 0
    capsys.readouterr()

    code = validate_cli.main([str(ledger)])

    output = capsys.readouterr().out
    assert code == 0
    assert "rows: 1" in output
    assert "issues: 0" in output


def test_append_cli_rejects_missing_shadow_forecast_without_writing(
    tmp_path: Path,
) -> None:
    analysis_path = tmp_path / "agent_analysis.json"
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    payload = _analysis()
    del payload["shadow_forecast"]
    _write_analysis(analysis_path, payload)

    assert append_cli.main(
        ["--analysis", str(analysis_path), "--ledger", str(ledger)]
    ) == 1
    assert not ledger.exists()


def test_append_cli_requires_run_aware_tension_card_validation(
    tmp_path: Path,
) -> None:
    analysis_path = tmp_path / "agent_analysis.json"
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    _write_analysis(analysis_path, _analysis())
    (tmp_path / "indicator_tension_cards.json").unlink()

    assert append_cli.main(
        ["--analysis", str(analysis_path), "--ledger", str(ledger)]
    ) == 1
    assert not ledger.exists()


def test_validate_cli_fails_on_invalid_jsonl(tmp_path: Path, capsys) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    ledger.write_text("not-json\n", encoding="utf-8")

    code = validate_cli.main([str(ledger)])

    output = capsys.readouterr().out
    assert code == 1
    assert "invalid_json" in output
