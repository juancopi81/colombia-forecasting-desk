"""Finalize one decision-grade agent analysis and refresh derived surfaces.

This command performs no network or LLM work. It validates an already-authored
``agent_analysis.json``, renders its Markdown companion, optionally appends one
protected internal shadow forecast, refreshes the experiment summary, and
rerenders the review HTML. It never writes ``forecasts/forecast_log.jsonl``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.agent_analysis import (  # noqa: E402
    render_agent_analysis,
    validate_agent_analysis,
)
from colombia_forecasting_desk.review_html import (  # noqa: E402
    DEFAULT_WINDOW,
    find_run_dirs,
    load_run_artifacts,
    render_daily_review_html,
    render_runs_index_html,
)
from colombia_forecasting_desk.shadow_experiment import (  # noqa: E402
    write_shadow_experiment_summary,
)
from colombia_forecasting_desk.shadow_forecasts import (  # noqa: E402
    ShadowForecastError,
    append_shadow_forecast,
    read_shadow_forecast_ledger,
    shadow_forecast_from_analysis,
)


def _load_analysis(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


def _append_shadow_idempotently(
    *,
    analysis: dict[str, Any],
    run_dir: Path,
    ledger_path: Path,
    source_analysis: str,
) -> str:
    existing_rows: list[dict[str, Any]] = []
    if ledger_path.exists():
        existing_rows, issues = read_shadow_forecast_ledger(ledger_path)
        if issues:
            messages = "; ".join(issue.message for issue in issues)
            raise ShadowForecastError(f"Existing shadow ledger is invalid: {messages}")

    forecast = analysis.get("shadow_forecast")
    forecast_id = str(forecast.get("id") or "") if isinstance(forecast, dict) else ""
    existing = next(
        (row for row in existing_rows if row.get("forecast_id") == forecast_id),
        None,
    )
    row = shadow_forecast_from_analysis(
        analysis,
        source_analysis=source_analysis,
        created_at=str(existing.get("created_at")) if existing else None,
        run_dir=run_dir,
    )
    if existing is not None:
        comparable_keys = set(row) - {"status"}
        differs = any(existing.get(key) != row.get(key) for key in comparable_keys)
        if differs or existing.get("status") not in {"open", "resolved"}:
            raise ShadowForecastError(
                f"Existing forecast_id {forecast_id} differs from this analysis."
            )
        return "already_recorded"
    append_shadow_forecast(ledger_path, row)
    return "appended"


def finalize(
    *,
    run_date: str,
    runs_dir: Path,
    ledger_path: Path,
    experiment_config: Path,
    experiment_output_dir: Path,
    index_window: int = DEFAULT_WINDOW,
) -> dict[str, Any]:
    run_dir = runs_dir / run_date
    analysis_path = run_dir / "agent_analysis.json"
    analysis = _load_analysis(analysis_path)
    issues = validate_agent_analysis(analysis, run_dir=run_dir)
    if issues:
        details = "; ".join(f"{issue.code}: {issue.message}" for issue in issues)
        raise ValueError(f"Invalid {analysis_path}: {details}")

    (run_dir / "agent_analysis.md").write_text(
        render_agent_analysis(analysis), encoding="utf-8"
    )

    shadow_action = "not_selected"
    if analysis.get("overall_disposition") == "shadow_track":
        try:
            relative_analysis = analysis_path.relative_to(REPO_ROOT)
        except ValueError:
            relative_analysis = analysis_path
        shadow_action = _append_shadow_idempotently(
            analysis=analysis,
            run_dir=run_dir,
            ledger_path=ledger_path,
            source_analysis=str(relative_analysis),
        )

    summary, summary_json, summary_markdown = write_shadow_experiment_summary(
        config_path=experiment_config,
        runs_dir=runs_dir,
        shadow_log_path=ledger_path,
        output_dir=experiment_output_dir,
    )

    daily_path = run_dir / "review.html"
    daily_path.write_text(
        render_daily_review_html(load_run_artifacts(run_dir)), encoding="utf-8"
    )
    index_path = runs_dir / "review_index.html"
    index_path.write_text(
        render_runs_index_html(find_run_dirs(runs_dir, window=index_window)),
        encoding="utf-8",
    )
    return {
        "analysis_markdown": run_dir / "agent_analysis.md",
        "shadow_action": shadow_action,
        "summary_status": summary["status"],
        "summary_json": summary_json,
        "summary_markdown": summary_markdown,
        "daily_review": daily_path,
        "review_index": index_path,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Finalize one authored daily agent-analysis artifact."
    )
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD).")
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument(
        "--ledger",
        type=Path,
        default=REPO_ROOT / "forecasts" / "shadow_forecast_log.jsonl",
    )
    parser.add_argument(
        "--experiment-config",
        type=Path,
        default=REPO_ROOT / "config" / "shadow_experiment.yaml",
    )
    parser.add_argument(
        "--experiment-output-dir",
        type=Path,
        default=REPO_ROOT / "forecasts",
    )
    parser.add_argument("--index-window", type=int, default=DEFAULT_WINDOW)
    args = parser.parse_args(argv)

    try:
        result = finalize(
            run_date=args.date,
            runs_dir=args.runs_dir,
            ledger_path=args.ledger,
            experiment_config=args.experiment_config,
            experiment_output_dir=args.experiment_output_dir,
            index_window=args.index_window,
        )
    except (OSError, ValueError, ShadowForecastError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
