"""Resolve exact official first-release observations and flag missing records."""
from __future__ import annotations
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.forecast_families import resolve_family_forecasts  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=datetime.now(ZoneInfo("America/Bogota")).date().isoformat())
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--ledger", type=Path, default=REPO_ROOT / "forecasts/shadow_forecast_log.jsonl")
    args = parser.parse_args(argv)
    try:
        result = resolve_family_forecasts(args.runs_dir, args.ledger, as_of=args.date)
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
