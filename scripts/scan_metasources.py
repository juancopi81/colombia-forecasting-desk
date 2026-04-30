"""Run the M1 metasource pipeline end-to-end.

Usage:
    uv run python scripts/scan_metasources.py [--date YYYY-MM-DD] [--config PATH]
    uv run python scripts/scan_metasources.py --source <source_id>
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
    run_single_source,
)

SANDBOX_TITLE_PREVIEW = 5


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


def _print_sandbox_report(result) -> None:
    health = result.source_health[0] if result.source_health else None
    if health is None:
        print("(no source health record)")
        return
    print("")
    print(f"Sandbox: {health.source_id}")
    print(
        f"  raw={health.raw_count} dated={health.dated_count} "
        f"cleaned={health.cleaned_count} rankable={health.rankable_count} "
        f"failures={health.failure_count}"
    )
    if health.failures:
        print("  failure messages:")
        for msg in health.failures:
            print(f"    - {msg}")
    preview = result.cleaned_items[:SANDBOX_TITLE_PREVIEW]
    if preview:
        print(f"  first {len(preview)} cleaned items:")
        for item in preview:
            published = item.published_at or "(no date)"
            title = item.title or "(no title)"
            print(f"    - [{published}] {title}")


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
    parser.add_argument(
        "--source",
        help=(
            "Run only one source (sandbox mode). Writes artifacts to "
            "runs/sandbox/<source_id>/ and prints a per-source report."
        ),
        default=None,
    )
    args = parser.parse_args()

    try:
        if args.source:
            result = run_single_source(
                source_id=args.source,
                config_path=args.config,
                runs_root=args.runs_dir,
                date=args.date,
            )
        else:
            result = run(
                date=args.date,
                config_path=args.config,
                runs_root=args.runs_dir,
            )
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
    if args.source:
        _print_sandbox_report(result)
    elif args.source_report:
        _print_source_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
