"""Count validated decision-grade reader runs and flag the ten-run review."""
from __future__ import annotations
import argparse
import hashlib
from datetime import date
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from colombia_forecasting_desk.reader_brief import validate_reader_brief


def check_usefulness(runs_dir: Path, as_of: str) -> dict:
    date.fromisoformat(as_of)
    dates = []
    for folder in sorted(runs_dir.iterdir()) if runs_dir.exists() else []:
        try:
            day = date.fromisoformat(folder.name)
            if not folder.is_dir() or folder.name > as_of or day.weekday() >= 5:
                continue
            brief = json.loads((folder / "reader_brief.json").read_text(encoding="utf-8"))
            acceptance = json.loads((folder / "acceptance_report.json").read_text(encoding="utf-8"))
            receipt = json.loads((folder / "reader_brief_receipt.json").read_text(encoding="utf-8"))
            finalized = (
                receipt.get("schema_version") == "reader_brief_receipt.v1"
                and receipt.get("run_date") == folder.name
                and receipt.get("context_validated") is True
                and all(receipt.get("sha256", {}).get(name) == hashlib.sha256((folder / name).read_bytes()).hexdigest()
                        for name in ("reader_brief.json", "reader_brief.md"))
            )
        except (OSError, ValueError):
            continue
        if finalized and acceptance.get("status") == "pass" and acceptance.get("strict_pass") is True and not validate_reader_brief(brief, run_date=folder.name):
            dates.append(folder.name)
    return {"schema_version": "usefulness_check.v1", "as_of": as_of,
            "target_runs": 10, "decision_grade_reader_runs": len(dates), "run_dates": dates,
            "status": "review_due" if len(dates) >= 10 else "collecting",
            "questions": ["Did any brief change what Juan believes, plans or checks?",
                          "Did every observed scheduled event receive a call or written skip?",
                          "Which stories or watchlist entries should be narrowed or dropped?"],
            "feedback_log": "reviews/usefulness_log.md",
            "limit": "Usefulness needs Juan's explicit feedback; run counts do not establish calibration or usefulness."}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    args = parser.parse_args(argv)
    try:
        result = check_usefulness(args.runs_dir, args.date)
        target = args.runs_dir / args.date / "usefulness_check.json"
        if not target.parent.is_dir():
            raise ValueError("the current run directory must already exist")
        target.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps({key: value for key, value in result.items() if key != "questions"}))
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
