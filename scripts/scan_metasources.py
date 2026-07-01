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
    print(
        "source_id | acceptance | content | raw | dated | rankable | tagged | "
        "untagged | doc_links | parsed | failures"
    )
    print("--- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:")
    for health in result.source_health:
        print(
            f"{health.source_id} | {health.acceptance_status} | "
            f"{health.content_mode} | {health.raw_count} | "
            f"{health.dated_count} | {health.rankable_count} | "
            f"{health.tagged_count} | {health.untagged_rankable_count} | "
            f"{health.document_link_count} | {health.parsed_content_count} | "
            f"{health.failure_count}"
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
    print(
        f"  content={health.content_mode} "
        f"acceptance={health.acceptance_status} "
        f"doc_links={health.document_link_count} "
        f"parsed={health.parsed_content_count} "
        f"tagged={health.tagged_count} "
        f"untagged={health.untagged_rankable_count}"
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
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit nonzero if M1 acceptance_report.json has error-level issues.",
    )
    args = parser.parse_args()

    try:
        if args.source:
            result = run_single_source(
                source_id=args.source,
                config_path=args.config,
                runs_root=args.runs_dir,
                date=args.date,
                strict_requested=args.strict,
            )
        else:
            result = run(
                date=args.date,
                config_path=args.config,
                runs_root=args.runs_dir,
                strict_requested=args.strict,
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
    observed_indicators = [
        item for item in result.indicator_watch if item.status == "observed"
    ]
    print(
        f"  indicators={len(result.indicator_watch)} "
        f"observed={len(observed_indicators)}"
    )
    observed_markets = [
        item for item in result.market_pricing_watch if item.status == "observed"
    ]
    print(
        f"  market_pricing={len(result.market_pricing_watch)} "
        f"observed={len(observed_markets)}"
    )
    print(
        "  m3_preflight_opportunities="
        f"{len(result.m3_preflight_opportunities.get('opportunities', []))}"
    )
    print(
        f"  candidates={len(result.m1_candidates.get('candidates', []))} "
        f"acceptance={result.acceptance_report.get('status', 'unknown')} "
        f"errors={result.acceptance_report.get('error_count', 'n/a')} "
        f"warnings={result.acceptance_report.get('warning_count', 'n/a')}"
    )
    if args.source:
        _print_sandbox_report(result)
    elif args.source_report:
        _print_source_report(result)
    if args.strict and not result.acceptance_report.get("strict_pass", False):
        print("Strict acceptance failed. See acceptance_report.json.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
