"""Write the deterministic ten-run shadow-forecast experiment summary."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.shadow_experiment import (  # noqa: E402
    write_shadow_experiment_summary,
)

DEFAULT_CONFIG = REPO_ROOT / "config" / "shadow_experiment.yaml"
DEFAULT_RUNS_DIR = REPO_ROOT / "runs"
DEFAULT_SHADOW_LOG = REPO_ROOT / "forecasts" / "shadow_forecast_log.jsonl"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "forecasts"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize the configured shadow-forecast experiment without network "
            "access, LLM calls, or forecast-log mutation."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Shadow experiment YAML config.",
    )
    parser.add_argument(
        "--runs-dir",
        type=Path,
        default=DEFAULT_RUNS_DIR,
        help="Root directory containing dated run directories.",
    )
    parser.add_argument(
        "--shadow-log",
        type=Path,
        default=DEFAULT_SHADOW_LOG,
        help="Shadow forecast JSONL ledger to read.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for shadow_experiment_summary.json and .md.",
    )
    args = parser.parse_args(argv)

    try:
        summary, json_path, markdown_path = write_shadow_experiment_summary(
            config_path=args.config,
            runs_dir=args.runs_dir,
            shadow_log_path=args.shadow_log,
            output_dir=args.output_dir,
        )
    except (OSError, ValueError) as exc:
        print(f"Failed to summarize shadow experiment: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote {json_path}")
    print(f"Wrote {markdown_path}")
    print(f"Status: {summary['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
