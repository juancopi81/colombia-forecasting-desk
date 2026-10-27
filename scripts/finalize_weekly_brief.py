"""Validate an authored weekly brief against its frozen facts and render it."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from colombia_forecasting_desk.weekly_brief import (
    append_usefulness_feedback, render_weekly_brief, render_weekly_html, validate_weekly_brief, week_bounds,
)


def finalize(*, week: str, weekly_dir: Path) -> dict[str, str]:
    week_bounds(week)
    folder = weekly_dir / week
    facts = json.loads((folder / "weekly_facts.json").read_text(encoding="utf-8"))
    brief = json.loads((folder / "weekly_brief.json").read_text(encoding="utf-8"))
    issues = validate_weekly_brief(brief, facts=facts)
    if issues:
        raise ValueError("; ".join(issues))
    if brief["week"] != week:
        raise ValueError("brief week does not match its output folder")
    markdown, page = render_weekly_brief(brief), render_weekly_html(brief)
    (folder / "weekly_brief.md").write_text(markdown, encoding="utf-8")
    (folder / "weekly.html").write_text(page, encoding="utf-8")
    return {"week": week, "markdown": str(folder / "weekly_brief.md"), "html": str(folder / "weekly.html")}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week", required=True)
    parser.add_argument("--weekly-dir", type=Path, default=REPO_ROOT / "weekly")
    parser.add_argument("--feedback-file", type=Path)
    parser.add_argument("--usefulness-log", type=Path, default=REPO_ROOT / "reviews" / "usefulness_log.md")
    args = parser.parse_args(argv)
    try:
        result = finalize(week=args.week, weekly_dir=args.weekly_dir)
        if args.feedback_file:
            feedback = json.loads(args.feedback_file.read_text(encoding="utf-8"))
            append_usefulness_feedback(args.usefulness_log, feedback)
    except (OSError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
