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
    void_shadow_forecast,
)

DEFAULT_LEDGER = REPO_ROOT / "forecasts" / "shadow_forecast_log.jsonl"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Resolve one open shadow forecast from explicit official evidence."
    )
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--forecast-id", required=True)
    parser.add_argument("--outcome", choices=("YES", "NO"))
    parser.add_argument("--resolved-at")
    parser.add_argument("--resolution-value")
    parser.add_argument("--resolution-url")
    parser.add_argument("--resolution-artifact")
    parser.add_argument("--void", action="store_true")
    parser.add_argument("--reason")
    parser.add_argument("--human-approved", action="store_true", help="Assert Juan explicitly approved voiding this particular forecast.")
    args = parser.parse_args(argv)

    try:
        if args.void:
            if args.outcome or not args.resolved_at:
                raise ShadowForecastError("--void needs --resolved-at and cannot have an outcome.")
            row = void_shadow_forecast(args.ledger, forecast_id=args.forecast_id, reason=args.reason or "", voided_at=args.resolved_at, human_approved=args.human_approved)
            print(f"void and unscored: {row['forecast_id']}")
            return 0
        if not all((args.outcome, args.resolved_at, args.resolution_value, args.resolution_url)):
            raise ShadowForecastError("Resolution requires --outcome, --resolved-at, --resolution-value and --resolution-url.")
        row = resolve_shadow_forecast(
            args.ledger,
            forecast_id=args.forecast_id,
            outcome=args.outcome,
            resolved_at=args.resolved_at,
            resolution_value=args.resolution_value,
            resolution_url=args.resolution_url,
            resolution_artifact=args.resolution_artifact,
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
