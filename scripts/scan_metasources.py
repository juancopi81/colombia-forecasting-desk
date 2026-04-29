"""Run the M1 metasource pipeline end-to-end.

Usage:
    uv run python scripts/scan_metasources.py [--date YYYY-MM-DD] [--config PATH]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.pipeline import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    RUNS_DIR,
    run,
)


def _print_source_report(result) -> None:
    print("")
    print("Source health:")
    print("source_id | raw | dated | rankable | failures")
    print("--- | ---: | ---: | ---: | ---:")
    for health in result.source_health:
        print(
            f"{health.source_id} | {health.raw_count} | {health.dated_count} | "
            f"{health.rankable_count} | {health.failure_count}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the M1 metasource pipeline.")
    parser.add_argument(
        "--date",
        help="Override run date (YYYY-MM-DD). Defaults to today (UTC).",
        default=None,
    )
    parser.add_argument(
        "--config",
        help="Path to metasources YAML config.",
        default=str(DEFAULT_CONFIG_PATH),
    )
    parser.add_argument(
        "--runs-dir",
        help="Root directory for run artifacts.",
        default=str(RUNS_DIR),
    )
    parser.add_argument(
        "--source-report",
        action="store_true",
        help="Print per-source raw, dated, rankable, and failure counts.",
    )
    args = parser.parse_args()

    try:
        result = run(date=args.date, config_path=args.config, runs_root=args.runs_dir)
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.error("Pipeline failed: %s: %s", exc.__class__.__name__, exc)
        return 1

    print(f"Wrote {result.run_dir}")
    print(
        f"  raw_items={len(result.raw_items)} "
        f"cleaned_items={len(result.cleaned_items)} "
        f"clusters={len(result.clusters)} "
        f"failures={len(result.failures)}"
    )
    if args.source_report:
        _print_source_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
