"""Write post-editorial M2 sampling-decision artifacts for one run.

Run after ``candidate_questions.md`` exists:

    uv run python scripts/write_m2_sampling_decisions.py --date 2026-06-02
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.m2_sampling_decisions import (  # noqa: E402
    MissingCandidateQuestionsError,
    write_m2_sampling_decisions,
)

RUNS_DIR = REPO_ROOT / "runs"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Write m2_sampling_decisions.json/.md from candidate_questions.md "
            "and m2_ranked_questions.json."
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Run date (YYYY-MM-DD). The run directory must already exist.",
    )
    parser.add_argument(
        "--runs-dir",
        default=str(RUNS_DIR),
        help="Root directory for run artifacts.",
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.runs_dir) / args.date
    try:
        _, json_path, markdown_path = write_m2_sampling_decisions(run_dir)
    except MissingCandidateQuestionsError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Failed to write M2 sampling decisions: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote {json_path}")
    print(f"Wrote {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
