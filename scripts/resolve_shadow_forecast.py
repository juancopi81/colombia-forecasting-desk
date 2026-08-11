"""Explicitly resolve one open internal shadow forecast.

This command never fetches or infers an outcome. The operator must provide the
outcome and official resolution evidence as command-line arguments.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.shadow_forecasts import (  # noqa: E402
    ShadowForecastError,
    resolve_shadow_forecast,
)

DEFAULT_LEDGER = REPO_ROOT / "forecasts" / "shadow_forecast_log.jsonl"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Resolve one open shadow forecast from explicit official evidence."
    )
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--forecast-id", required=True)
    parser.add_argument("--outcome", required=True, choices=("YES", "NO"))
    parser.add_argument("--resolved-at", required=True)
    parser.add_argument("--resolution-value", required=True)
    parser.add_argument("--resolution-url", required=True)
    args = parser.parse_args(argv)

    try:
        row = resolve_shadow_forecast(
            args.ledger,
            forecast_id=args.forecast_id,
            outcome=args.outcome,
            resolved_at=args.resolved_at,
            resolution_value=args.resolution_value,
            resolution_url=args.resolution_url,
        )
    except ShadowForecastError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"resolved: {row['forecast_id']}")
    print(f"outcome: {row['outcome']}")
    print(f"brier_score: {row['brier_score']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
