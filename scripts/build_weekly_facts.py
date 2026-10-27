"""Collect dated evidence for the Friday brief without fetching or forecasting."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from colombia_forecasting_desk.weekly_brief import build_weekly_facts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week", required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--weekly-dir", type=Path, default=REPO_ROOT / "weekly")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--as-of")
    parser.add_argument("--cutoff-at")
    args = parser.parse_args(argv)
    try:
        facts = build_weekly_facts(args.runs_dir, args.week, repo_root=args.repo_root, as_of=args.as_of, cutoff_at=args.cutoff_at)
        folder = args.weekly_dir / args.week
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "weekly_facts.json"
        path.write_text(json.dumps(facts, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
