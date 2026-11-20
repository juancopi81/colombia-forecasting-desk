"""Validate an authored weekly brief against its frozen facts and render it."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from colombia_forecasting_desk.weekly_brief import (
    append_usefulness_feedback, render_weekly_brief, render_weekly_html, validate_weekly_brief, week_bounds, weekly_chart_data,
)
from colombia_forecasting_desk.brief_charts import write_chart_images


def finalize(*, week: str, weekly_dir: Path, require_chart_decision: bool = False) -> dict[str, str]:
    week_bounds(week)
    folder = weekly_dir / week
    facts = json.loads((folder / "weekly_facts.json").read_text(encoding="utf-8"))
    brief = json.loads((folder / "weekly_brief.json").read_text(encoding="utf-8"))
    if require_chart_decision and isinstance(brief, dict) and brief.get("schema_version") != "weekly_brief.v2":
        raise ValueError("new briefs require weekly_brief.v2 and chart_decision; use --allow-legacy only for historical v1 briefs")
    issues = validate_weekly_brief(brief, facts=facts)
    if issues:
        raise ValueError("; ".join(issues))
    if brief["week"] != week:
        raise ValueError("brief week does not match its output folder")
    data = weekly_chart_data(brief, facts)
    images = write_chart_images(data, folder) if data else []
    markdown = render_weekly_brief(brief, chart_data=data, chart_image_path=str((folder / "brief_chart.png").resolve()))
    page = render_weekly_html(brief, chart_data=data)
    (folder / "weekly_brief.md").write_text(markdown, encoding="utf-8")
    (folder / "weekly.html").write_text(page, encoding="utf-8")
    if brief.get("schema_version") == "weekly_brief.v2":
        receipt = {"schema_version": "weekly_brief_receipt.v1", "week": week,
                   "context_validated": True, "cutoff_at": facts["cutoff_at"],
                   "sha256": {name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
                              for name in ("weekly_facts.json", "weekly_brief.json", "weekly_brief.md", "weekly.html", *images)}}
        (folder / "weekly_brief_receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return {"week": week, "markdown": str(folder / "weekly_brief.md"), "html": str(folder / "weekly.html")}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week", required=True)
    parser.add_argument("--weekly-dir", type=Path, default=REPO_ROOT / "weekly")
    parser.add_argument("--feedback-file", type=Path)
    parser.add_argument("--usefulness-log", type=Path, default=REPO_ROOT / "reviews" / "usefulness_log.md")
    parser.add_argument("--allow-legacy", action="store_true", help="Rebuild an existing historical v1 brief, including feedback-only maintenance.")
    args = parser.parse_args(argv)
    try:
        result = finalize(week=args.week, weekly_dir=args.weekly_dir, require_chart_decision=not args.allow_legacy)
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
