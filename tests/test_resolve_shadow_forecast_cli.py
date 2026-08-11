from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from colombia_forecasting_desk.shadow_forecasts import append_shadow_forecast

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "resolve_shadow_forecast.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("resolve_shadow_forecast", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


resolve_cli = _load_script()


def _open_row() -> dict[str, object]:
    return {
        "schema_version": "shadow_forecast.v1",
        "forecast_id": "shadow_20260811_ipc_next_release",
        "created_at": "2026-08-11T16:00:00Z",
        "run_date": "2026-08-11",
        "question": "Will the next annual IPC reading exceed 6.03%?",
        "probability": 0.6,
        "status": "open",
        "resolution_deadline": "2026-09-07",
        "resolution_check_window_end": "2026-09-09",
        "resolution_source": "DANE IPC official release",
        "resolution_criteria": "YES if annual IPC exceeds 6.03%; otherwise NO.",
        "baseline": {"label": "unchanged_from_current", "probability": 0.5},
        "evidence_refs": [
            {
                "artifact": "indicator_watch.json",
                "locator": "indicator_id=ipc_inflation",
                "note": "Latest official IPC reading.",
            }
        ],
        "rationale_for": ["Recent inflation remained elevated."],
        "rationale_against": ["Base effects may lower annual inflation."],
        "falsifier": "A broad monthly disinflation in the next release.",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "visibility": "internal",
        "source_analysis": "runs/2026-08-11/agent_analysis.json",
    }


def _resolution_args(ledger: Path) -> list[str]:
    return [
        "--ledger",
        str(ledger),
        "--forecast-id",
        "shadow_20260811_ipc_next_release",
        "--outcome",
        "NO",
        "--resolved-at",
        "2026-09-07T10:00:00-05:00",
        "--resolution-value",
        "Annual IPC was 5.95%.",
        "--resolution-url",
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/ipc",
    ]


def test_resolve_cli_requires_explicit_evidence_and_updates_open_row(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    append_shadow_forecast(ledger, _open_row())

    code = resolve_cli.main(_resolution_args(ledger))

    assert code == 0
    row = json.loads(ledger.read_text(encoding="utf-8"))
    assert row["status"] == "resolved"
    assert row["outcome"] == "NO"
    assert row["resolution_value"] == "Annual IPC was 5.95%."
    assert row["resolution_url"].startswith("https://www.dane.gov.co/")
    assert row["brier_score"] == 0.36


def test_resolve_cli_refuses_to_change_resolved_row(tmp_path: Path) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    append_shadow_forecast(ledger, _open_row())
    args = _resolution_args(ledger)
    assert resolve_cli.main(args) == 0
    resolved_bytes = ledger.read_bytes()

    assert resolve_cli.main(args) == 1
    assert ledger.read_bytes() == resolved_bytes
