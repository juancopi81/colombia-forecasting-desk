"""Validate and record prospective internal family forecasts or written skips."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.forecast_families import append_family_forecasts  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--ledger", type=Path, default=REPO_ROOT / "forecasts/shadow_forecast_log.jsonl")
    parser.add_argument("--skips", type=Path, default=REPO_ROOT / "forecasts/family_skips.jsonl")
    parser.add_argument("--public-ledger", type=Path, default=REPO_ROOT / "forecasts/forecast_log.jsonl")
    parser.add_argument("--model", required=True)
    parser.add_argument("--reasoning-effort", required=True)
    args = parser.parse_args(argv)
    try:
        result = append_family_forecasts(args.runs_dir / args.date, args.ledger, args.skips, model=args.model, reasoning_effort=args.reasoning_effort, public_ledger_path=args.public_ledger)
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
