"""Match current official artifacts against the watchlist, using cached history."""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.entities import build_watchlist_hits, load_watchlist, render_watchlist_hits


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--watchlist", type=Path, default=REPO_ROOT / "config/watchlist.yaml")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "data/entity_history")
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--metasources", type=Path, default=REPO_ROOT / "config/metasources.yaml")
    args = parser.parse_args(argv)
    try:
        run_dir = args.runs_dir / args.date.isoformat()
        if not run_dir.is_dir():
            raise ValueError("run directory does not exist")
        payload = build_watchlist_hits(run_dir, load_watchlist(args.watchlist), cache_dir=args.cache_dir, metasources_path=args.metasources)
        rendered = render_watchlist_hits(payload)
        (run_dir / "watchlist_hits.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (run_dir / "watchlist_hits.md").write_text(rendered, encoding="utf-8")
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Watchlist: {len(payload['hits'])} matches, {len(payload['patterns'])} review prompts; wrote {run_dir / 'watchlist_hits.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
