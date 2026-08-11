from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATE_SCRIPT = REPO_ROOT / "scripts" / "validate_agent_analysis.py"
RENDER_SCRIPT = REPO_ROOT / "scripts" / "render_agent_analysis.py"


def _valid_insight_analysis(run_date: str) -> dict:
    evidence_ref = {
        "artifact": "indicator_watch.json",
        "locator": "indicator_id=ipc_inflation",
        "note": "Official indicator input.",
    }
    return {
        "schema_version": "agent_analysis.v1",
        "run_date": run_date,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "strongest_changed_signal": {
            "signal": "Annual IPC changed.",
            "evidence_refs": [evidence_ref],
            "interpretation": "Inflation pressure deserves review.",
            "alternative_explanations": ["Base effects may dominate."],
            "falsifiers": ["A broad reversal across core categories."],
        },
        "tension_card_reviews": [],
        "relationships": [
            {
                "type": "unbundled",
                "relationship": "Inflation changed while the peso strengthened.",
                "evidence_refs": [evidence_ref],
                "interpretation": "Imported-price pressure may be easing.",
                "caveats": ["Pass-through is delayed and incomplete."],
            }
        ],
        "official_source_follow_up": {
            "candidate": "Next DANE IPC release",
            "bounded_scope": "Checked the DANE IPC technical page only.",
            "sources_checked": [
                {
                    "source_id": "dane_ipc",
                    "url": "https://www.dane.gov.co/ipc",
                    "result": "The latest release is available; next date was not confirmed.",
                }
            ],
            "result": "Useful insight, but no clean forecast window was verified.",
        },
        "public_interest_candidate": {
            "candidate": "Inflation pressure and household purchasing power",
            "disposition": "insight_only",
            "rationale": "The observed change matters without forcing a forecast.",
            "evidence_refs": [evidence_ref],
            "missing_evidence": [],
        },
        "shadow_forecast": None,
        "overall_disposition": "insight_only",
        "overall_rationale": "Record the interpretation without a probability.",
    }


def _write_run(run_dir: Path) -> Path:
    run_dir.mkdir(parents=True)
    analysis_path = run_dir / "agent_analysis.json"
    analysis_path.write_text(
        json.dumps(_valid_insight_analysis(run_dir.name)),
        encoding="utf-8",
    )
    (run_dir / "indicator_tension_cards.json").write_text("[]\n", encoding="utf-8")
    return analysis_path


def test_validate_agent_analysis_cli_accepts_valid_run_artifact(
    tmp_path: Path,
) -> None:
    analysis_path = _write_run(tmp_path / "runs" / "2026-08-11")

    result = subprocess.run(
        [sys.executable, str(VALIDATE_SCRIPT), str(analysis_path)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert str(analysis_path) in result.stdout
    assert "disposition: insight_only" in result.stdout
    assert "issues: 0" in result.stdout
    assert result.stderr == ""


def test_validate_agent_analysis_cli_rejects_unreviewed_triggered_card(
    tmp_path: Path,
) -> None:
    analysis_path = _write_run(tmp_path / "runs" / "2026-08-11")
    (analysis_path.parent / "indicator_tension_cards.json").write_text(
        '[{"card_id": "real_policy_rate"}]\n',
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(VALIDATE_SCRIPT), str(analysis_path)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "missing_tension_card_review" in result.stdout
    assert "real_policy_rate" in result.stdout
    assert result.stderr == ""


def test_render_agent_analysis_cli_validates_and_writes_markdown(
    tmp_path: Path,
) -> None:
    runs_dir = tmp_path / "runs"
    _write_run(runs_dir / "2026-08-11")

    result = subprocess.run(
        [
            sys.executable,
            str(RENDER_SCRIPT),
            "--date",
            "2026-08-11",
            "--runs-dir",
            str(runs_dir),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    output_path = runs_dir / "2026-08-11" / "agent_analysis.md"
    assert result.returncode == 0
    assert f"Wrote {output_path}" in result.stdout
    assert result.stderr == ""
    assert output_path.read_text(encoding="utf-8").startswith(
        "# Agent Analysis - 2026-08-11\n"
    )


def test_render_agent_analysis_cli_refuses_invalid_input_without_writing(
    tmp_path: Path,
) -> None:
    runs_dir = tmp_path / "runs"
    analysis_path = _write_run(runs_dir / "2026-08-11")
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    analysis["relationships"] = []
    analysis_path.write_text(json.dumps(analysis), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(RENDER_SCRIPT),
            "--date",
            "2026-08-11",
            "--runs-dir",
            str(runs_dir),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "missing_relationship" in result.stderr
    assert not (analysis_path.parent / "agent_analysis.md").exists()
