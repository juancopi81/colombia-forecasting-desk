from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from colombia_forecasting_desk.camara_minutes import INDEX_URL, capture_minutes, official_url, parse_config

FIXTURES = Path(__file__).parent / "fixtures/camara_minutes"
HTML = (FIXTURES / "index.html").read_bytes()
SAMPLE = json.loads((FIXTURES / "page.json").read_text())
PDF = b"%PDF-1.7\nfixture bytes deliberately unread\n%%EOF"


def page(items, *, total=None):
    total = len(items) if total is None else total
    return {"success": True, "data": {"items": items, "total": total, "total_pages": (total + 9) // 10}}


def rows(count, *, start=1):
    result = []
    for identifier in range(start, start + count):
        item = deepcopy(SAMPLE["data"]["items"][0])
        item["id"] = str(identifier)
        result.append(item)
    return result


def run(tmp_path, respond, **kwargs):
    requests = []

    def handler(request):
        requests.append(request)
        if str(request.url) == INDEX_URL:
            return httpx.Response(200, content=HTML)
        return respond(request)

    report = capture_minutes(tmp_path / "capture", date_from="2026-06-01", date_to="2026-06-30",
                             transport=httpx.MockTransport(handler), **kwargs)
    return report, requests


def test_empty_shell_uses_current_nonce_and_preserves_source_bytes(tmp_path):
    def respond(request):
        if request.method == "POST":
            query = parse_qs(request.content.decode(), keep_blank_values=True)
            assert query["_ajax_nonce"] == ["fixture-current-nonce"]
            assert query["fecha_desde"] == ["2026-06-01"]
            assert query["fecha_hasta"] == ["2026-06-30"]
            assert query["per_page"] == ["10"]
            assert query["action"] == ["get_actas_y_otros_page"]
            return httpx.Response(200, json=SAMPLE)
        return httpx.Response(200, content=PDF, headers={"content-type": "application/pdf"})

    report, requests = run(tmp_path, respond)
    assert report["index_status"] == "complete"
    assert len(requests) == 3
    assert report["records"][0]["index_date"] == "2026-06-16"
    assert report["records"][0]["document_status"] == "downloaded_unread"
    assert report["documents"][0]["text_extraction"] == "not_attempted"
    assert "_ajax_nonce" not in json.dumps(report)
    for receipt in report["receipts"]:
        body = (tmp_path / "capture" / receipt["artifact"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == receipt["sha256"]
        assert receipt["retrieved_at"].endswith("+00:00")
        assert receipt["request_started_at"] <= receipt["retrieved_at"]
        assert receipt["tls_certificate_verification_enabled"] is True
        assert receipt["body_complete"]
    assert json.loads((tmp_path / "capture/capture.json").read_text()) == report


def test_each_capture_refreshes_config_nonce(tmp_path):
    post_nonces = []
    for nonce in ("first", "fresh-second"):
        def handler(request):
            if request.method == "GET":
                return httpx.Response(200, content=HTML.replace(b"fixture-current-nonce", nonce.encode()))
            post_nonces.append(parse_qs(request.content.decode())["_ajax_nonce"][0])
            return httpx.Response(200, json=SAMPLE)
        capture_minutes(tmp_path / nonce, date_from="2026-06-16", date_to="2026-06-16",
                        max_documents=0, transport=httpx.MockTransport(handler))
    assert post_nonces == ["first", "fresh-second"]


def test_empty_filter_has_separate_bounded_unfiltered_diagnostic(tmp_path):
    def respond(request):
        query = parse_qs(request.content.decode(), keep_blank_values=True)
        return httpx.Response(200, json=page([]) if query["fecha_desde"][0] else page(rows(10), total=344))

    report, requests = run(tmp_path, respond)
    assert report["index_status"] == "complete"
    assert report["total"] == 0 and report["records"] == []
    assert report["documents"] == [] and len(requests) == 3
    assert report["diagnostic"]["status"] == "truncated"
    assert report["diagnostic"]["date_max"] == "2026-06-16"
    assert report["diagnostic"]["total"] == 344
    assert "never NO" in report["notices"][0]


@pytest.mark.parametrize("response", [
    httpx.Response(403, content=b"-1"),
    httpx.Response(200, content=b"-1"),
    httpx.Response(200, json={"success": False}),
    httpx.Response(200, content=b"<html>Access challenge</html>"),
    httpx.Response(200, json={"success": True, "data": {"items": [], "total": "0", "total_pages": 0}}),
    httpx.Response(200, json={"success": True, "data": {"items": [], "total": 1, "total_pages": 1}}),
])
def test_failed_or_malformed_api_is_unavailable_not_empty(tmp_path, response):
    report, requests = run(tmp_path, lambda request: response)
    assert report["index_status"] == "unavailable"
    assert report["total"] is None and report["notices"]
    assert report["diagnostic"] is None and len(requests) == 2
    assert (tmp_path / "capture/filtered-page-1-hop-0.json").exists()


@pytest.mark.parametrize("second", ["failure", "duplicate", "totals_changed", "valid"])
def test_pagination_detects_partial_failure_duplicates_and_change(tmp_path, second):
    def respond(request):
        current = parse_qs(request.content.decode())["page"][0]
        if current == "1":
            return httpx.Response(200, json=page(rows(10), total=11))
        if second == "failure":
            return httpx.Response(503, content=b"Unavailable")
        if second == "duplicate":
            return httpx.Response(200, json=page(rows(1), total=11))
        if second == "totals_changed":
            return httpx.Response(200, json=page(rows(2, start=11), total=12))
        return httpx.Response(200, json=page(rows(1, start=11), total=11))

    report, requests = run(tmp_path, respond, max_documents=0)
    assert report["index_status"] == ("complete" if second == "valid" else "truncated")
    assert len(report["records"]) == (11 if second == "valid" else 10)
    assert len(requests) == 3


def test_page_cap_reports_truncation(tmp_path):
    report, requests = run(tmp_path, lambda request: httpx.Response(200, json=page(rows(10), total=344)),
                           max_pages=1, max_documents=0)
    assert report["index_status"] == "truncated"
    assert report["total_pages"] == 35 and len(requests) == 2


@pytest.mark.parametrize("change", ["date_missing", "date_invalid", "outside_filter", "id_missing", "not_object"])
def test_malformed_or_outside_filter_rows_never_become_matching_evidence(tmp_path, change):
    item = rows(1)[0]
    if change == "date_missing":
        del item["fecha_iso"]
    elif change == "date_invalid":
        item["fecha_iso"] = "2026-06-99"
    elif change == "outside_filter":
        item["fecha_iso"] = "2026-05-31"
    elif change == "id_missing":
        item.pop("id")
    else:
        item = "malformed"
    report, requests = run(tmp_path, lambda request: httpx.Response(200, json=page([item])))
    assert report["index_status"] == "truncated"
    assert report["records"] == [] and report["documents"] == [] and len(requests) == 2


@pytest.mark.parametrize("url", [
    "https://evil.example/file.pdf", "https://www.camara.gov.co.evil.example/file.pdf",
    "http://www.camara.gov.co/file.pdf", "https://user@www.camara.gov.co/file.pdf",
    "https://www.camara.gov.co:444/file.pdf", "//127.0.0.1/file.pdf", "file:///tmp/file.pdf",
])
def test_foreign_or_unsafe_links_are_rejected_before_any_request(tmp_path, url):
    with pytest.raises(ValueError):
        official_url(url)
    item = rows(1)[0]
    item["enlace"] = url
    report, requests = run(tmp_path, lambda request: httpx.Response(200, json=page([item])))
    assert len(requests) == 2
    assert report["records"][0]["document_status"] == "rejected_link"
    assert report["notices"]


def test_foreign_endpoint_and_missing_config_are_rejected():
    for html in (b"<table><tbody></tbody></table>",
                 HTML.replace("Comisión Tercera".encode(), "Comisión Cuarta".encode()),
                 HTML.replace(b"https://www.camara.gov.co/wp-admin/admin-ajax.php", b"https://evil.example/ajax")):
        with pytest.raises(ValueError):
            parse_config(html)


@pytest.mark.parametrize("phase", ["index", "api", "document"])
def test_redirect_destination_checked_before_request(tmp_path, phase):
    requests = []

    def handler(request):
        requests.append(request)
        is_index = str(request.url) == INDEX_URL
        should_redirect = (phase == "index" and is_index or phase == "api" and request.method == "POST"
                           or phase == "document" and not is_index and request.method == "GET")
        if should_redirect:
            return httpx.Response(307, headers={"location": "https://evil.example/target.pdf"})
        if is_index:
            return httpx.Response(200, content=HTML)
        return httpx.Response(200, json=SAMPLE)

    report = capture_minutes(tmp_path / "capture", date_from="2026-06-01", date_to="2026-06-30",
                             transport=httpx.MockTransport(handler))
    assert all(request.url.host == "www.camara.gov.co" for request in requests)
    assert report["notices"]
    assert report["index_status"] == ("complete" if phase == "document" else "unavailable")


def test_valid_official_redirect_is_preserved_and_hashed(tmp_path):
    def respond(request):
        if request.method == "POST":
            return httpx.Response(200, json=SAMPLE)
        if request.url.path != "/wp-content/final.pdf":
            return httpx.Response(302, headers={"location": "/wp-content/final.pdf"}, content=b"redirect")
        return httpx.Response(200, content=PDF)
    report, requests = run(tmp_path, respond)
    assert len(requests) == 4
    assert report["documents"][0]["status"] == "downloaded_unread"
    assert report["receipts"][-2]["redirect_url"].endswith("/wp-content/final.pdf")


def test_document_download_cap_deduplicates_urls_and_never_parses_html_as_pdf(tmp_path):
    items = rows(3)
    items[-1]["enlace"] = "/wp-content/another.pdf"
    def respond(request):
        return httpx.Response(200, json=page(items)) if request.method == "POST" else httpx.Response(200, content=b"<html>Blocked</html>")
    report, requests = run(tmp_path, respond)
    assert len(requests) == 3 and len(report["documents"]) == 1
    assert report["records"][0]["document_status"] == "unavailable"
    assert report["records"][1]["document_status"] == "unavailable"
    assert report["records"][2]["document_status"] == "not_downloaded_limit"


def test_network_failure_is_recorded_as_unavailable(tmp_path):
    def respond(request):
        raise httpx.ConnectError("network unavailable", request=request)
    report, _ = run(tmp_path, respond)
    assert report["index_status"] == "unavailable"
    assert report["receipts"][-1]["status_code"] is None
    assert report["receipts"][-1]["retrieved_at"] is None
    assert report["receipts"][-1]["error"] == "network unavailable"


def test_retrieval_timestamp_records_body_receipt_not_request_start(tmp_path, monkeypatch):
    import colombia_forecasting_desk.camara_minutes as module
    clock = iter(f"2026-10-30T12:00:{i:02}+00:00" for i in range(10))
    monkeypatch.setattr(module, "_utc_now", lambda: next(clock))
    report, _ = run(tmp_path, lambda request: httpx.Response(200, json=SAMPLE), max_documents=0)
    assert report["receipts"][0]["request_started_at"] == "2026-10-30T12:00:01+00:00"
    assert report["receipts"][0]["retrieved_at"] == "2026-10-30T12:00:02+00:00"
    assert report["receipts"][1]["request_started_at"] == "2026-10-30T12:00:03+00:00"
    assert report["receipts"][1]["retrieved_at"] == "2026-10-30T12:00:04+00:00"


def test_weekly_provenance_discovers_capture_hashes_tls_and_respects_cutoff(tmp_path, monkeypatch):
    import colombia_forecasting_desk.camara_minutes as module
    from colombia_forecasting_desk.weekly_brief import _source_receipts

    clock = iter(f"2026-10-30T12:00:{i:02}+00:00" for i in range(10))
    monkeypatch.setattr(module, "_utc_now", lambda: next(clock))
    current_run = tmp_path / "runs/2026-10-30"
    report, _ = run(current_run / "sources", lambda request: (
        httpx.Response(200, json=SAMPLE) if request.method == "POST" else httpx.Response(200, content=PDF)))
    manifest = json.loads((current_run / "sources/capture/receipts.json").read_text())
    assert manifest["finished_at"] == report["finished_at"]
    for source, receipt in zip(manifest["sources"], report["receipts"], strict=True):
        assert source["retrieved_at"] == receipt["retrieved_at"]
        assert source["body_complete"] == receipt["body_complete"]
        assert source["sha256"] == receipt["sha256"]
        assert Path(source["artifact"]).is_absolute()
    cutoff = datetime.fromisoformat(report["finished_at"])
    gaps = []
    receipts = _source_receipts(current_run, cutoff, gaps)
    assert not gaps and len(receipts) == 3
    assert all(receipt["hash_verified"] and receipt["tls_verification"] is True for receipt in receipts)
    assert all(Path(receipt["source_ref"]["artifact"]).is_relative_to(current_run) for receipt in receipts)
    assert all(receipt["reviewed_pages"] == [] for receipt in receipts)
    assert _source_receipts(current_run, cutoff - timedelta(seconds=1), []) == []

    # The reader also checks each receipt's received time, independently of
    # the container timestamp, so a future source cannot enter an earlier cut.
    manifest["sources"][-1]["retrieved_at"] = (cutoff + timedelta(seconds=1)).isoformat()
    (current_run / "sources/capture/receipts.json").write_text(json.dumps(manifest))
    assert len(_source_receipts(current_run, cutoff, [])) == 2


def test_existing_output_is_never_overwritten(tmp_path):
    output = tmp_path / "capture"
    output.mkdir()
    (output / "capture.json").write_text("existing")
    with pytest.raises(FileExistsError):
        capture_minutes(output, date_from="2026-06-01", date_to="2026-06-30")
    assert (output / "capture.json").read_text() == "existing"


def test_cli_exits_two_for_incomplete_capture(tmp_path, monkeypatch, capsys):
    path = Path(__file__).parents[1] / "scripts/capture_camara_minutes.py"
    spec = importlib.util.spec_from_file_location("capture_camara_minutes_cli", path)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    monkeypatch.setattr(cli, "capture_minutes", lambda *a, **kw: {
        "index_status": "truncated", "records": [], "total": 3, "documents": [], "notices": ["incomplete"]})
    assert cli.main(["--date-from", "2026-06-01", "--date-to", "2026-06-30", "--output-dir", str(tmp_path)]) == 2
    assert json.loads(capsys.readouterr().out)["index_status"] == "truncated"
