"""Freeze today's validated story files for historical weekly reporting."""
from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from colombia_forecasting_desk.cases import snapshot_cases


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=datetime.now(ZoneInfo("America/Bogota")).date().isoformat())
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--cases-dir", type=Path, default=REPO_ROOT / "cases")
    args = parser.parse_args(argv)
    try:
        print(snapshot_cases(args.cases_dir, args.runs_dir / args.date))
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
