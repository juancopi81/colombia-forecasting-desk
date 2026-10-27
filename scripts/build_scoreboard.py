"""Refresh public and internal scores with an explicit historical cutoff."""
from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.forecast_families import write_scoreboard  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=datetime.now(ZoneInfo("America/Bogota")).date().isoformat())
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)
    try:
        artifact = write_scoreboard(args.repo_root, as_of=args.date)
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(args.repo_root / "forecasts/scoreboard.json")
    print(f"resolved: public {len(artifact['public']['resolved'])}, internal {len(artifact['internal']['resolved'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
