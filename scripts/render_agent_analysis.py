"""Validate and render one authored daily agent-analysis artifact."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.agent_analysis import (  # noqa: E402
    render_agent_analysis,
    validate_agent_analysis,
)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("top-level JSON value must be an object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate agent_analysis.json and render agent_analysis.md."
    )
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD).")
    parser.add_argument(
        "--runs-dir",
        default="runs",
        help="Root directory for run artifacts.",
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.runs_dir) / args.date
    input_path = run_dir / "agent_analysis.json"
    output_path = run_dir / "agent_analysis.md"
    try:
        analysis = _load(input_path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Cannot render {input_path}: {exc}", file=sys.stderr)
        return 1

    issues = validate_agent_analysis(analysis, run_dir=run_dir)
    if issues:
        print(f"Cannot render invalid {input_path}:", file=sys.stderr)
        for issue in issues:
            print(f"- {issue.code}: {issue.message}", file=sys.stderr)
        return 1

    output_path.write_text(render_agent_analysis(analysis), encoding="utf-8")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
