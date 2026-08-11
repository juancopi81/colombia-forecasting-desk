"""Validate authored daily agent-analysis JSON artifacts."""
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
    validate_agent_analysis,
)


def load_analysis(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        return None, [f"read_error: {exc}"]
    except json.JSONDecodeError as exc:
        return None, [f"invalid_json: {exc}"]
    if not isinstance(value, dict):
        return None, ["invalid_artifact: top-level JSON value must be an object"]
    return value, []


def validate_path(path: Path) -> tuple[str, list[str]]:
    analysis, errors = load_analysis(path)
    if analysis is None:
        return "unreadable", errors
    issues = validate_agent_analysis(analysis, run_dir=path.parent)
    return str(analysis.get("overall_disposition") or "missing"), [
        f"{issue.code}: {issue.message}" for issue in issues
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate daily agent_analysis.json artifacts."
    )
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Agent-analysis JSON path(s) to validate.",
    )
    args = parser.parse_args(argv)

    failed = False
    for index, path in enumerate(args.paths):
        if index:
            print()
        disposition, issues = validate_path(path)
        print(path)
        print(f"disposition: {disposition}")
        if not issues:
            print("issues: 0")
            continue
        failed = True
        print(f"issues: {len(issues)}")
        for issue in issues:
            print(f"- {issue}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
