"""Prepare one chart from captured official JSON/XLSX, without network access."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.brief_charts import BOGOTA, DATA_FILE, compile_chart_data


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    args = parser.parse_args(argv)
    try:
        run = args.runs_dir / args.date
        now = datetime.now(BOGOTA)
        if now.date().isoformat() != args.date:
            raise ValueError("prepare charts in today's dated run; never add later evidence to historical runs")
        request = json.loads(args.request.read_text())
        data = compile_chart_data(request, run, generated_at=now.isoformat(), cutoff=now)
        target = run / DATA_FILE
        if target.exists():
            old = json.loads(target.read_text())
            data["generated_at"] = old.get("generated_at")
            if old != data:
                raise ValueError("prepared chart data is immutable; do not replace an existing source vintage")
        else:
            target.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    except (OSError, ValueError, TypeError, KeyError, IndexError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
