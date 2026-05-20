"""Validate the M3 case-file contract in evidence-pack Markdown files."""
from __future__ import annotations

import argparse
from pathlib import Path

from colombia_forecasting_desk.m3_case_file import (
    extract_m3_case_file,
    validate_evidence_pack_markdown,
)


def validate_path(path: Path) -> list[str]:
    try:
        markdown_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"read_error: {exc}"]

    issues = validate_evidence_pack_markdown(markdown_text)
    return [f"{issue.code}: {issue.message}" for issue in issues]


def case_gate(path: Path) -> str:
    try:
        markdown_text = path.read_text(encoding="utf-8")
    except OSError:
        return "unreadable"

    case_file = extract_m3_case_file(markdown_text)
    if case_file is None:
        return "missing"
    return str(case_file.get("m3_gate") or "missing")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate M3 Case File YAML blocks in evidence packs."
    )
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Evidence-pack Markdown path(s) to validate.",
    )
    args = parser.parse_args(argv)

    failed = False
    for index, path in enumerate(args.paths):
        if index:
            print()
        print(path)
        print(f"gate: {case_gate(path)}")

        issues = validate_path(path)
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
