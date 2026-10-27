"""Match official daily artifacts to the desk's running stories."""
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
from colombia_forecasting_desk.cases import build_case_updates, load_cases


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=datetime.now(ZoneInfo("America/Bogota")).date().isoformat())
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--cases-dir", type=Path, default=REPO_ROOT / "cases")
    args = parser.parse_args(argv)
    try:
        run_dir = args.runs_dir / args.date
        if not run_dir.is_dir():
            raise ValueError(f"missing run directory: {run_dir}")
        payload = build_case_updates(run_dir, load_cases(args.cases_dir))
        target = run_dir / "case_updates.json"
        target.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
        print(target)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
