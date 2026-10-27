"""Capture official GEIH history and exact dated DANE first-release bulletins.

This is an explicit network command. Run after the strict scan and before the
family planner/resolver; never invoke it from a renderer or validator.
"""
from __future__ import annotations
import argparse
from datetime import date, timedelta
import json
from pathlib import Path
import sys

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.family_evidence import capture_first_release, capture_geih_history  # noqa: E402
from colombia_forecasting_desk.forecast_families import _events, _read, _rows, _write  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--ledger", type=Path, default=REPO_ROOT / "forecasts/shadow_forecast_log.jsonl")
    parser.add_argument("--history", action="store_true", help="Force national GEIH historical comparator intake.")
    parser.add_argument("--history-url", help="Official current GEIH annex URL; otherwise discover it from the captured labor card.")
    parser.add_argument("--slot-id", help="Restrict bulletin capture to one declared slot or open family row.")
    parser.add_argument("--release-url", help="A specific official dated bulletin for the selected --slot-id.")
    args = parser.parse_args(argv)
    if args.release_url and not args.slot_id:
        parser.error("--release-url requires --slot-id")
    notices, captures = [], []
    history_count = 0
    try:
        run_dir = args.runs_dir / args.date
        if not run_dir.is_dir():
            raise ValueError(f"Missing run directory: {run_dir}")
        day = date.fromisoformat(args.date)
        raw = _read(run_dir / "raw_items.json", [])
        preflight = _read(run_dir / "m3_preflight_opportunities.json", {"opportunities": []})
        labor_due = any(event["family_id"] == "unemployment_down_yoy" and day < date.fromisoformat(event["event_date"]) <= day + timedelta(days=7) for event in _events(preflight, raw))
        rows = [row for row in _rows(args.ledger) if row.get("status") == "open" and row.get("forecast_family") in ("ipc_annual_up", "ise_annual_up", "unemployment_down_yoy") and row.get("family_event", {}).get("event_date", "9999") <= args.date]
        if args.slot_id:
            rows = [row for row in rows if row.get("slot_id") == args.slot_id]
            if not rows:
                slots = _read(run_dir / "family_forecast_slots.json", {}).get("slots", [])
                rows = [slot for slot in slots if slot.get("slot_id") == args.slot_id and slot.get("event_date", "9999") <= args.date]
            if not rows:
                raise ValueError("The selected occurred event has no declared slot or open family forecast")
        with httpx.Client(follow_redirects=True, timeout=30, max_redirects=3) as client:
            if args.history or args.history_url or labor_due and not args.slot_id:
                try:
                    history = capture_geih_history(run_dir, client=client, source_url=args.history_url)
                    history_count = len(history["rows"])
                except (ValueError, OSError, httpx.HTTPError) as exc:
                    notices.append(f"GEIH comparator history unavailable: {exc}")
            for row in rows:
                try:
                    capture = capture_first_release(run_dir, row, client=client, source_url=args.release_url)
                    captures.append(capture["slot_id"])
                except (ValueError, OSError, httpx.HTTPError) as exc:
                    notices.append(f"First-release evidence unavailable for {row.get('slot_id')}: {exc}")
        report = {"schema_version": "family_evidence_capture.v1", "run_date": args.date, "history_rows": history_count, "release_slots": captures, "notices": notices}
        _write(run_dir / "family_evidence_capture.json", report)
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 2 if notices else 0


if __name__ == "__main__":
    raise SystemExit(main())
