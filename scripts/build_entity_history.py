"""Fetch separate, bounded official history and write uncommitted review reports."""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.entities import load_watchlist
from colombia_forecasting_desk.entity_history import build_entity_history, fetch_divipola, render_entity_history


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--watchlist", type=Path, default=REPO_ROOT / "config/watchlist.yaml")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "data/entity_history")
    parser.add_argument("--reviews-dir", type=Path, default=REPO_ROOT / "reviews")
    parser.add_argument("--watch-id", action="append", default=[])
    parser.add_argument("--offline", action="store_true", help="Render existing caches without any network request")
    args = parser.parse_args(argv)
    try:
        config = load_watchlist(args.watchlist)
        entries = [entry for entry in config["entries"] if entry.get("active", True) and entry["kind"] in {"company", "public_entity"} and (not args.watch_id or entry["watch_id"] in args.watch_id)]
        if args.watch_id and set(args.watch_id) - {entry["watch_id"] for entry in entries}:
            raise ValueError("unknown, inactive or non-organization watch ID")
        args.cache_dir.mkdir(parents=True, exist_ok=True)
        args.reviews_dir.mkdir(parents=True, exist_ok=True)
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            if not args.offline:
                try:
                    divipola = fetch_divipola(client)
                except (httpx.HTTPError, ValueError) as exc:
                    print(f"DIVIPOLA unavailable ({type(exc).__name__}); existing catalog retained.", file=sys.stderr)
                else:
                    (args.cache_dir / "divipola.json").write_text(json.dumps(divipola, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            for entry in entries:
                cache = args.cache_dir / f"{entry['watch_id']}.json"
                if args.offline:
                    history = json.loads(cache.read_text(encoding="utf-8"))
                else:
                    history = build_entity_history(entry, config["settings"], as_of=args.date, client=client)
                    cache.write_text(json.dumps(history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                report = args.reviews_dir / f"entity-{entry['watch_id']}-{history['as_of']}.md"
                report.write_text(render_entity_history(history), encoding="utf-8")
                print(f"{entry['watch_id']}: wrote {report}; coverage gaps={len(history['coverage_gaps'])}")
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
