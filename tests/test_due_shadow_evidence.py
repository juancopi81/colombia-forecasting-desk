from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from functools import partial
import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from colombia_forecasting_desk.camara_minutes import INDEX_URL, capture_minutes
from colombia_forecasting_desk.due_shadow_evidence import FORECAST_ID, capture_due_shadow_evidence
from colombia_forecasting_desk.weekly_brief import _source_receipts


FIXTURES = Path(__file__).parent / "fixtures/camara_minutes"
HTML = (FIXTURES / "index.html").read_bytes()
SAMPLE = json.loads((FIXTURES / "page.json").read_text())
NOW = datetime(2026, 11, 2, 15, tzinfo=timezone.utc)
ROW = {
    "forecast_id": FORECAST_ID, "status": "open", "run_date": "2026-10-26",
    "resolution_deadline": "2026-10-26", "resolution_check_window_end": "2026-10-30",
    "resolution_source": "PL012 registry https://www.camara.gov.co/ultraprocesados/ or certified Comisión Tercera minutes",
    "resolution_criteria": "YES only for approval before2026-10-27T00:00:00-05:00. Missing or partial records are not NO.",
    "probability": 0.55, "baseline": {"probability": 0.5, "label": "uninformative_50_50"},
}


def setup(tmp_path, *, day="2026-11-02", rows=None):
    run = tmp_path / "runs" / day
    run.mkdir(parents=True)
    ledger = tmp_path / "shadow.jsonl"
    ledger.write_text("".join(json.dumps(row) + "\n" for row in (rows if rows is not None else [ROW])))
    return run, ledger


def collector(mode, requests):
    def handler(request):
        requests.append(request)
        if str(request.url) == INDEX_URL:
            return httpx.Response(200, content=HTML)
        if mode == "unavailable":
            raise httpx.ConnectError("transient failure", request=request)
        if request.method == "GET":
            return httpx.Response(200, content=b"%PDF-1.7\nunread fixture\n%%EOF")
        query = parse_qs(request.content.decode(), keep_blank_values=True)
        assert query["fecha_desde"][0] in ("2026-10-19", "")
        assert query["fecha_hasta"][0] in ("2026-11-02", "")
        count = 10 if mode == "partial" else 1 if mode == "pdf" else 0
        page = int(query["page"][0])
        items = []
        for index in range(count):
            item = deepcopy(SAMPLE["data"]["items"][0])
            item.update(id=str((page - 1) * 10 + index + 1), fecha_iso="2026-10-31")
            items.append(item)
        total = 21 if mode == "partial" else count
        return httpx.Response(200, json={"success": True, "data": {
            "items": items, "total": total, "total_pages": (total + 9) // 10}})
    return partial(capture_minutes, transport=httpx.MockTransport(handler))


def load_cli():
    path = Path(__file__).parents[1] / "scripts/capture_family_evidence.py"
    spec = importlib.util.spec_from_file_location("capture_due_shadow_integration_cli", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("mode", ["empty", "pdf"])
def test_daily_intake_captures_due_minutes_preserves_ledger_and_reuses_complete_query(tmp_path, monkeypatch, mode):
    run, ledger = setup(tmp_path)
    before = ledger.read_bytes()
    requests = []
    cli = load_cli()
    monkeypatch.setattr(cli, "capture_due_shadow_evidence", partial(
        capture_due_shadow_evidence, now=NOW, capture=collector(mode, requests)))
    args = ["--date", run.name, "--runs-dir", str(run.parent), "--ledger", str(ledger)]
    assert cli.main(args) == 2
    report = json.loads((run / "due_shadow_evidence_capture.json").read_text())
    check = report["checks"][0]
    assert check["status"] == "captured" and check["outcome"] is None
    assert check["query_date_from"] == "2026-10-19"
    assert check["query_date_to"] == "2026-11-02"
    assert check["resolution_check_window_end"] == "2026-10-30"
    assert check["resolution_criteria"] == ROW["resolution_criteria"]
    assert check["resolution_source"] == ROW["resolution_source"]
    detailed = json.loads((run / check["capture_report"]).read_text())
    if mode == "pdf":
        # A later index/upload date may hold the required earlier session record;
        # retrieval does not verify that date, bill identity, or outcome.
        assert detailed["records"][0]["index_date"] == "2026-10-31"
        assert detailed["documents"][0]["status"] == "downloaded_unread"
    else:
        assert any("never NO" in notice for notice in report["notices"])
    summary = json.loads((run / "family_evidence_capture.json").read_text())
    assert summary["due_shadow_checks"] == [FORECAST_ID]
    assert summary["notices"] == report["notices"]
    first_bytes = {path: path.read_bytes() for path in (run / "sources").rglob("*") if path.is_file()}
    count = len(requests)
    assert cli.main(args) == 2
    assert len(requests) == count
    assert json.loads((run / "due_shadow_evidence_capture.json").read_text())["checks"][0]["status"] == "reused"
    assert all(path.read_bytes() == content for path, content in first_bytes.items())
    assert ledger.read_bytes() == before
    gaps = []
    receipts = _source_receipts(run, datetime.fromisoformat(detailed["finished_at"]), gaps)
    assert not gaps and len(receipts) == count
    assert all(receipt["hash_verified"] for receipt in receipts)


@pytest.mark.parametrize("mode", ["unavailable", "partial", "interrupted"])
def test_failed_partial_or_interrupted_attempt_can_retry_without_overwriting(tmp_path, mode):
    run, _ = setup(tmp_path)
    requests = []
    if mode == "interrupted":
        directory = run / "sources" / f"camara-minutes-due-{FORECAST_ID}" / "attempt-0001"
        directory.mkdir(parents=True)
        (directory / "index.html").write_bytes(b"interrupted preserved bytes")
    else:
        first = capture_due_shadow_evidence(run, [deepcopy(ROW)], now=NOW, capture=collector(mode, requests))
        assert first["checks"][0]["index_status"] in ("unavailable", "truncated")
        assert first["notices"]
    previous = {path: path.read_bytes() for path in (run / "sources").rglob("*") if path.is_file()}
    second = capture_due_shadow_evidence(run, [deepcopy(ROW)], now=NOW, capture=collector("empty", requests))
    assert "attempt-0002" in second["checks"][0]["capture_report"]
    assert second["checks"][0]["status"] == "captured"
    assert all(path.read_bytes() == content for path, content in previous.items())
    assert second["checks"][0]["outcome"] is None


@pytest.mark.parametrize("day,status", [
    ("2026-10-25", "open"), ("2026-10-26", "open"),
    ("2026-11-02", "resolved"), ("2026-11-02", "void"),
])
def test_daily_intake_does_not_fetch_not_due_or_closed_row(tmp_path, monkeypatch, day, status):
    row = {**ROW, "status": status}
    run, ledger = setup(tmp_path, day=day, rows=[row])
    cli = load_cli()
    def unexpected(*args, **kwargs):
        pytest.fail("No minutes network capture should occur")
    monkeypatch.setattr(cli, "capture_due_shadow_evidence", partial(
        capture_due_shadow_evidence, now=NOW, capture=unexpected))
    assert cli.main(["--date", day, "--runs-dir", str(run.parent), "--ledger", str(ledger)]) == 0
    assert not (run / "sources").exists()


@pytest.mark.parametrize("option", ["--history", "--history-url", "--slot-id"])
def test_targeted_intake_does_not_fetch_unrelated_minutes(tmp_path, monkeypatch, option):
    family = {"forecast_id": "family", "status": "open", "forecast_family": "ipc_annual_up",
              "slot_id": "ipc-slot", "family_event": {"event_date": "2026-11-01"}}
    run, ledger = setup(tmp_path, rows=[ROW, family])
    cli = load_cli()
    def unexpected(*args, **kwargs):
        pytest.fail("Targeted family/history intake must not fetch minutes")
    monkeypatch.setattr(cli, "capture_due_shadow_evidence", unexpected)
    monkeypatch.setattr(cli, "capture_geih_history", lambda *args, **kwargs: {"rows": []})
    monkeypatch.setattr(cli, "capture_first_release", lambda *args, **kwargs: {"slot_id": "ipc-slot"})
    args = ["--date", run.name, "--runs-dir", str(run.parent), "--ledger", str(ledger), option]
    if option == "--history-url":
        args.append("https://www.dane.gov.co/history.xlsx")
    elif option == "--slot-id":
        args.append("ipc-slot")
    assert cli.main(args) == 0
    assert not (run / "due_shadow_evidence_capture.json").exists()


@pytest.mark.parametrize("now", [datetime(2026, 11, 3, 12, tzinfo=timezone.utc), datetime(2026, 11, 2, 15)])
def test_network_intake_rejects_historical_folder_or_naive_clock(tmp_path, now):
    run, _ = setup(tmp_path)
    def unexpected(*args, **kwargs):
        pytest.fail("Historical runs cannot receive live intake")
    with pytest.raises(ValueError, match="today's Bogotá"):
        capture_due_shadow_evidence(run, [ROW], now=now, capture=unexpected)
    assert not (run / "sources").exists()


def test_current_day_guard_uses_bogota_not_utc_date(tmp_path):
    run, _ = setup(tmp_path)
    requests = []
    report = capture_due_shadow_evidence(run, [ROW], now=datetime(2026, 11, 3, 2, tzinfo=timezone.utc),
                                        capture=collector("empty", requests))
    assert report["checks"] and requests
