"""Write deterministic M3 preflight opportunity artifacts for one run.

Usage:
    uv run python scripts/write_m3_preflight_opportunities.py --date 2026-06-29
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.m3_preflight_opportunities import (  # noqa: E402
    DEFAULT_WINDOW_DAYS,
    PreflightInputError,
    write_m3_preflight_opportunities,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write m3_preflight_opportunities.json/.md for one run."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Run date to inspect (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--runs-dir",
        default="runs",
        help="Root directory for run artifacts.",
    )
    parser.add_argument(
        "--config",
        default="config/metasources.yaml",
        help="Path to metasources YAML config.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help=(
            "Maximum days from the run date to a scheduled event. "
            f"Defaults to {DEFAULT_WINDOW_DAYS}."
        ),
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.runs_dir) / args.date
    try:
        artifact, json_path, markdown_path = write_m3_preflight_opportunities(
            run_dir,
            config_path=args.config,
            window_days=args.window_days,
        )
    except (PreflightInputError, ValueError) as exc:
        print(f"Failed to write M3 preflight opportunities: {exc}", file=sys.stderr)
        return 1

    print(
        f"Wrote {json_path} and {markdown_path} "
        f"({artifact.get('summary', {}).get('opportunity_count', 0)} opportunities)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
