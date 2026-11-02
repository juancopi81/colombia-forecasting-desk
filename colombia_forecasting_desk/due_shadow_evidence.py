"""Bounded minutes intake for the still-open PL012 discretionary forecast.

An open ledger row means no definitive registry result has been accepted. This
collector preserves another declared resolver source; it never decides a vote.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from .camara_minutes import INDEX_URL, capture_minutes


FORECAST_ID = "shadow_20261026_camara_pl012_first_debate_approval"
BOGOTA = ZoneInfo("America/Bogota")
CAVEAT = (
    "The index window is a bounded record search, not the event window or proof "
    "of complete session coverage. Later-posted records may describe an earlier "
    "event; the original resolution criteria and exclusive cutoff stay unchanged. "
    "Empty, missing, partial or unread records leave the forecast open."
)


def _reusable_capture(directory: Path, date_from: str, date_to: str) -> dict | None:
    """Reuse preserved complete queries, including empty queries, on this day."""
    try:
        report = json.loads((directory / "capture.json").read_text(encoding="utf-8"))
        receipts = json.loads((directory / "receipts.json").read_text(encoding="utf-8"))
        if (report.get("schema_version") != "camara_minutes_capture.v1"
                or report.get("index_url") != INDEX_URL
                or report.get("query", {}).get("fecha_desde") != date_from
                or report.get("query", {}).get("fecha_hasta") != date_to
                or report.get("limits") != {"max_pages": 2, "max_documents": 1}
                or report.get("index_status") != "complete"
                or not report.get("finished_at")
                or receipts.get("finished_at") != report["finished_at"]
                or any(document.get("status") != "downloaded_unread" for document in report["documents"])
                or not receipts.get("sources")):
            return None
        for source in receipts["sources"]:
            if source.get("artifact"):
                artifact = Path(source["artifact"])
                if (not artifact.resolve().is_relative_to(directory.resolve())
                        or hashlib.sha256(artifact.read_bytes()).hexdigest() != source.get("sha256")):
                    return None
        return report
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None


def capture_due_shadow_evidence(
    run_dir: Path, rows: list[dict], *, now: datetime | None = None, capture=capture_minutes,
) -> dict:
    """Capture once per invocation, retrying failed/partial attempts without overwrites.

    ``now`` and ``capture`` are injectable for offline tests. Production captures
    may only enter today's Bogotá run, never a historical run directory.
    """
    run_dir = Path(run_dir)
    day = date.fromisoformat(run_dir.name)
    report = {"schema_version": "due_shadow_evidence_capture.v1", "run_date": day.isoformat(),
              "checks": [], "notices": [], "caveat": CAVEAT}
    for row in rows:
        if row.get("forecast_id") != FORECAST_ID or row.get("status") != "open":
            continue
        deadline = date.fromisoformat(row["resolution_deadline"])
        if day <= deadline:
            continue
        current = now or datetime.now(BOGOTA)
        if current.tzinfo is None or current.astimezone(BOGOTA).date() != day:
            raise ValueError("Due-shadow network intake requires today's Bogotá run date")
        date_from = (deadline - timedelta(days=7)).isoformat()
        date_to = day.isoformat()
        check = {
            "forecast_id": FORECAST_ID, "status": "unavailable", "outcome": None,
            "trigger": "Open due ledger row: no definitive registry outcome has been accepted",
            "resolution_deadline": row["resolution_deadline"],
            "resolution_check_window_end": row.get("resolution_check_window_end"),
            "resolution_source": row["resolution_source"],
            "resolution_criteria": row["resolution_criteria"],
            "query_date_from": date_from, "query_date_to": date_to,
            "capture_report": None, "receipts": None, "notices": [],
        }
        report["checks"].append(check)
        root = run_dir / "sources" / f"camara-minutes-due-{FORECAST_ID}"
        previous = sorted(root.glob("attempt-*"), reverse=True)
        result = None
        for directory in previous:
            result = _reusable_capture(directory, date_from, date_to)
            if result is not None:
                check["status"] = "reused"
                break
        if result is None:
            number = 1
            while (root / f"attempt-{number:04d}").exists():
                number += 1
            directory = root / f"attempt-{number:04d}"
            try:
                result = capture(directory, date_from=date_from, date_to=date_to,
                                 max_pages=2, max_documents=1)
                check["status"] = "captured"
            except (ValueError, OSError) as exc:
                check["notices"].append(f"Minutes capture unavailable: {exc}; any partial files are retained")
        check["capture_report"] = str((directory / "capture.json").relative_to(run_dir))
        check["receipts"] = str((directory / "receipts.json").relative_to(run_dir))
        if result is not None:
            check.update(index_status=result["index_status"], matching_records=len(result["records"]),
                         documents=len(result["documents"]))
            check["notices"].extend(result["notices"])
        check["notices"].append(
            "Official record review required: verify bill identity, session/vote and the original "
            "exclusive event cutoff; this capture leaves the forecast open."
        )
        report["notices"].extend(f"{FORECAST_ID}: {notice}" for notice in check["notices"])
        break
    (run_dir / "due_shadow_evidence_capture.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report
