"""Render the deterministic HTML review surface for the forecasting desk.

This reads the structured artifacts a run already produced and writes:

* ``runs/YYYY-MM-DD/review.html`` — the daily TLDR for one run, built to make a
  monitor/no-post day feel informative.
* ``runs/review_index.html`` — a recent-runs index that surfaces patterns
  (forecast-question droughts, recurring themes, source reliability) across the
  last ``--window`` runs.

It is a pure renderer: no LLM, no network, no new dependency, byte-stable for a
given set of artifacts. Run it any time after ``scan_metasources.py``; it never
re-runs the pipeline and never promotes or reinterprets anything.

Usage:
    uv run python scripts/render_review.py
    uv run python scripts/render_review.py --date 2026-05-29
    uv run python scripts/render_review.py --window 21
    uv run python scripts/render_review.py --daily-only
    uv run python scripts/render_review.py --index-only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.review_html import (  # noqa: E402
    DEFAULT_WINDOW,
    find_run_dirs,
    load_run_artifacts,
    render_daily_review_html,
    render_runs_index_html,
)

RUNS_DIR = REPO_ROOT / "runs"
DAILY_FILENAME = "review.html"
INDEX_FILENAME = "review_index.html"


def _resolve_daily_dir(runs_root: Path, date: str | None) -> Path | None:
    if date:
        return runs_root / date
    run_dirs = find_run_dirs(runs_root, window=None)
    return run_dirs[-1] if run_dirs else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render the daily and recent-runs HTML review surface."
    )
    parser.add_argument(
        "--date",
        help="Run date (YYYY-MM-DD) for the daily view. Defaults to the latest run.",
        default=None,
    )
    parser.add_argument(
        "--runs-dir",
        help="Root directory for run artifacts.",
        default=str(RUNS_DIR),
    )
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW,
        help=f"Number of recent runs for the index view (default {DEFAULT_WINDOW}).",
    )
    parser.add_argument(
        "--daily-only",
        action="store_true",
        help="Render only the daily view.",
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Render only the recent-runs index.",
    )
    args = parser.parse_args(argv)

    if args.daily_only and args.index_only:
        parser.error("--daily-only and --index-only are mutually exclusive.")

    runs_root = Path(args.runs_dir)
    written: list[Path] = []

    if not args.index_only:
        daily_dir = _resolve_daily_dir(runs_root, args.date)
        if daily_dir is None or not daily_dir.exists():
            print(f"No run directory found for daily view (date={args.date or 'latest'}).")
            return 1
        artifacts = load_run_artifacts(daily_dir)
        out_path = daily_dir / DAILY_FILENAME
        out_path.write_text(render_daily_review_html(artifacts), encoding="utf-8")
        written.append(out_path)

    if not args.daily_only:
        run_dirs = find_run_dirs(runs_root, window=args.window)
        if not run_dirs:
            print("No dated run directories found for the index view.")
            return 1
        index_path = runs_root / INDEX_FILENAME
        index_path.write_text(render_runs_index_html(run_dirs), encoding="utf-8")
        written.append(index_path)

    for path in written:
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
