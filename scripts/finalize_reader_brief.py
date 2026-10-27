"""Validate an authored reader brief, then write Markdown and the daily HTML."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.reader_brief import render_reader_brief, validate_reader_brief
from colombia_forecasting_desk.review_html import load_run_artifacts, render_daily_review_html


def _urls(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set().union(*(_urls(v) for v in value.values())) if value else set()
    if isinstance(value, list):
        return set().union(*(_urls(v) for v in value)) if value else set()
    if isinstance(value, str) and value.startswith("https://"):
        return {value}
    return set()


def finalize(*, run_date: str, runs_dir: Path) -> dict[str, Any]:
    from datetime import date
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError("date must use YYYY-MM-DD")
    run_dir = runs_dir / run_date
    brief = json.loads((run_dir / "reader_brief.json").read_text(encoding="utf-8"))
    art = load_run_artifacts(run_dir)
    source_urls: set[str] = set()
    # The brief's own URLs must never establish its provenance.
    for path in sorted(run_dir.glob("*.json")):
        if path.name == "reader_brief.json":
            continue
        source_urls.update(_urls(json.loads(path.read_text(encoding="utf-8"))))
    rows = [*(art.get("_forecast_log") or []), *(art.get("_shadow_forecast_log") or [])]
    if art.get("_forecast_log_parse_errors") or art.get("_shadow_forecast_log_parse_errors"):
        raise ValueError("Cannot validate forecast references against a malformed ledger")
    issues = validate_reader_brief(brief, run_date=run_date, forecast_rows=rows, source_urls=source_urls)
    if issues:
        raise ValueError("; ".join(issues))
    acceptance = art.get("acceptance_report.json")
    if isinstance(acceptance, dict) and acceptance.get("status") != "pass" and not brief.get("data_problems"):
        issues.append("an incomplete run must disclose its material data problem")
    waiting = [row for row in rows if row.get("status") == "open" and row.get("needs_review")]
    shown = {item["forecast_id"] for item in brief.get("open_forecasts", [])}
    if any(row.get("forecast_id") not in shown for row in waiting):
        issues.append("forecasts flagged for review must appear under open_forecasts")
    if issues:
        raise ValueError("; ".join(issues))
    markdown = render_reader_brief(brief)
    rendered = render_daily_review_html(art)
    (run_dir / "reader_brief.md").write_text(markdown, encoding="utf-8")
    (run_dir / "review.html").write_text(rendered, encoding="utf-8")
    receipt = {
        "schema_version": "reader_brief_receipt.v1", "run_date": run_date,
        "context_validated": True,
        "sha256": {name: hashlib.sha256((run_dir / name).read_bytes()).hexdigest()
                   for name in ("reader_brief.json", "reader_brief.md", "review.html")},
    }
    (run_dir / "reader_brief_receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {"run_date": run_date, "markdown": str(run_dir / "reader_brief.md"), "html": str(run_dir / "review.html")}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    args = parser.parse_args(argv)
    try:
        result = finalize(run_date=args.date, runs_dir=args.runs_dir)
    except (OSError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
