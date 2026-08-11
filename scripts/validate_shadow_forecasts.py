"""Validate the internal shadow-forecast JSONL ledger."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.shadow_forecasts import (  # noqa: E402
    read_shadow_forecast_ledger,
)

DEFAULT_LEDGER = REPO_ROOT / "forecasts" / "shadow_forecast_log.jsonl"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the internal shadow-forecast JSONL ledger."
    )
    parser.add_argument("ledger", nargs="?", type=Path, default=DEFAULT_LEDGER)
    args = parser.parse_args(argv)

    rows, issues = read_shadow_forecast_ledger(args.ledger)
    print(args.ledger)
    print(f"rows: {len(rows)}")
    print(f"issues: {len(issues)}")
    for issue in issues:
        location = f"row {issue.row_number}: " if issue.row_number else ""
        print(f"- {location}{issue.code}: {issue.message}")
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
