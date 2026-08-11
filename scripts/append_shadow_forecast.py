"""Append one internal shadow forecast from a validated agent analysis."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.shadow_forecasts import (  # noqa: E402
    ShadowForecastError,
    append_shadow_forecast,
    shadow_forecast_from_analysis,
)

DEFAULT_LEDGER = REPO_ROOT / "forecasts" / "shadow_forecast_log.jsonl"


def load_analysis(path: Path) -> dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ShadowForecastError(f"Could not read analysis {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ShadowForecastError("Analysis artifact must be a JSON object.")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Append one open internal forecast from agent_analysis.json."
    )
    parser.add_argument("--analysis", required=True, type=Path)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    args = parser.parse_args(argv)

    try:
        analysis = load_analysis(args.analysis)
        row = shadow_forecast_from_analysis(
            analysis,
            source_analysis=str(args.analysis),
            run_dir=args.analysis.parent,
        )
        append_shadow_forecast(args.ledger, row)
    except ShadowForecastError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"appended: {row['forecast_id']}")
    print(f"ledger: {args.ledger}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
