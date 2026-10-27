from __future__ import annotations

from datetime import datetime, timezone
import io
import json
from pathlib import Path
import zipfile

import httpx
import pytest

import colombia_forecasting_desk.family_evidence as evidence
from colombia_forecasting_desk.forecast_families import plan_family_forecasts, resolve_family_forecasts
from tests.test_forecast_families import append, calendar, card, ledger, load_cli, prepare, setup_run, write

GEIH_URL = "https://www.dane.gov.co/files/operaciones/GEIH/anex-GEIH-ago2026.xlsx"
NOW = datetime(2026, 10, 27, 16, tzinfo=timezone.utc)
NEXT_DAY = datetime(2026, 10, 31, 16, tzinfo=timezone.utc)


def workbook(*, national=True, first_td=True):
    def row(index, values):
        cells = []
        for column, value in values.items():
            if isinstance(value, str):
                cells.append(f'<c r="{column}{index}" t="inlineStr"><is><t>{value}</t></is></c>')
            else:
                cells.append(f'<c r="{column}{index}"><v>{value}</v></c>')
        return f'<row r="{index}">' + "".join(cells) + '</row>'
    rows = [row(5, {"A": "Total Nacional"}), row(6, {"A": "Serie mensual 2025 - 2026"}), row(12, {"A": "Concepto", "B": 2025, "D": 2026}), row(13, {"B": "Ago", "C": "Sep", "D": "Ago"}), row(14, {"A": "Tasa Global de Participación (TGP)", "B": 70, "C": 70, "D": 70})]
    if first_td:
        rows.append(row(17, {"A": "Tasa de Desocupación (TD)", "B": 8.6, "C": 8.249999, "D": 9.4}))
    rows.extend([row(28, {"A": "Total 13 ciudades"}), row(30, {"A": "Tasa de Desocupación (TD)", "C": 1.1}), row(51, {"A": "Actualizado el 30 de septiembre de 2026"})])
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("xl/workbook.xml", '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="' + ("Total nacional" if national else "Total 13 ciudades") + '" sheetId="99" r:id="rId37"/></sheets></workbook>')
        archive.writestr("xl/_rels/workbook.xml.rels", '<Relationships><Relationship Id="rId37" Target="/xl/worksheets/custom.xml"/></Relationships>')
        archive.writestr("xl/worksheets/custom.xml", '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>' + "".join(rows) + '</sheetData></worksheet>')
    return buffer.getvalue()


def test_geih_history_uses_exact_sheet_section_and_published_precision():
    parsed = evidence.parse_geih_history(workbook())
    september = next(row for row in parsed["rows"] if row["period"] == "2025-09")
    assert september["raw_value"] == 8.249999
    assert september["national_unemployment_rate_pct"] == 8.2
    assert september["locator"] == "Total nacional!C17"
    assert september["precision_decimal_places"] == 1
    assert parsed["published_at"] == "2026-09-30"
    assert len(parsed["rows"]) == 3
    assert "not an archived first-release resolver" in parsed["caveat"]


@pytest.mark.parametrize("kwargs", [{"national": False}, {"first_td": False}])
def test_geih_history_never_substitutes_another_geography(kwargs):
    with pytest.raises(ValueError):
        evidence.parse_geih_history(workbook(**kwargs))


def test_history_capture_discovers_annex_and_enables_exact_next_month_comparator(tmp_path):
    labor = card("labor_market", period="2026-08", key="national_unemployment_rate_pct", value=9.4, national_unemployment_previous_year_pct=8.6, national_unemployment_previous_year_period="2025-08", official_documents=[{"url": GEIH_URL}])
    run = setup_run(tmp_path, cards=[labor], events=[calendar("labor")])
    assert not plan_family_forecasts(run)["slots"][0]["enabled"]
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=workbook(), request=request))) as client:
        first = evidence.capture_geih_history(run, client=client, now=NOW)
        second = evidence.capture_geih_history(run, client=client, now=NOW)
    assert first == second
    assert (run / first["source_artifact"]).read_bytes() == workbook()
    planned = plan_family_forecasts(run)
    assert planned["slots"][0]["enabled"]
    assert planned["slots"][0]["threshold"] == 8.2
    assert planned["slots"][0]["comparator_provenance"]["source_sha256"] == first["source_sha256"]
    # Tampering with a derived value cannot bypass the original annex/hash check.
    tampered = json.loads((run / "family_reference_history.json").read_text())
    tampered["rows"][1]["national_unemployment_rate_pct"] = 0.1
    write(run / "family_reference_history.json", tampered)
    assert not plan_family_forecasts(run)["slots"][0]["enabled"]


def test_history_capture_uses_bogota_civil_date_not_utc_date(tmp_path):
    labor = card("labor_market", period="2026-08", key="national_unemployment_rate_pct", value=9.4, official_documents=[{"url": GEIH_URL}])
    run = setup_run(tmp_path, cards=[labor], events=[calendar("labor")])
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=workbook(), request=request))) as client:
        evidence.capture_geih_history(run, client=client, now=datetime(2026, 10, 28, 1, tzinfo=timezone.utc))
    assert evidence.reference_history_for_run(run) is not None
    assert plan_family_forecasts(run)["slots"][0]["enabled"]


SPECS = {
    "ipc_annual_up": ("IPC", "headline_annual", 6.29, "Índice de Precios al Consumidor (IPC). Total nacional. En el mes de septiembre de 2026, el IPC registró una variación de 6,29% en comparación con septiembre de 2025."),
    "ise_annual_up": ("ISE", "original_total_annual", -0.04, "Indicador de Seguimiento a la Economía. Para el mes de septiembre de 2026pr, el ISE en su serie original se ubicó en 127,27, lo que representó un decrecimiento de 0,04% respecto al mes de septiembre de 2025pr (127,31)."),
    "unemployment_down_yoy": ("GEIH", "national_original_unemployment", 9.4, "Gran Encuesta Integrada de Hogares. Para el mes de septiembre de 2026, la tasa de desocupación del total nacional fue 9,4%, mientras que en el mismo mes de 2025 fue 8,2%."),
}


def text_for(family):
    return "Boletín técnico Bogotá D.C.\n30 de octubre de 2026\nSeptiembre 2026. " + SPECS[family][3] + "\nRevisión de cifras históricas de meses anteriores. Datos preliminares y provisionales."


def event_for(family):
    return {"reference_period": "2026-09", "event_date": "2026-10-30", "series_variant": SPECS[family][1]}


@pytest.mark.parametrize("family", SPECS)
def test_first_release_supports_all_dane_families_and_routine_revision_notes(monkeypatch, family):
    monkeypatch.setattr(evidence, "pdf_text_and_metadata", lambda content: (text_for(family), {"ModDate": "D:20261029120000-05'00'"}))
    result = evidence.parse_first_release(b"%PDF-fixture", source_url=f"https://www.dane.gov.co/files/bol-{SPECS[family][0]}-sep2026.pdf", event=event_for(family), family=family, last_modified="Fri, 30 Oct 2026 18:00:00 GMT")
    assert result["value"] == SPECS[family][2]
    assert result["first_release_verified"]
    assert result["event_date"] == "2026-10-30"


@pytest.mark.parametrize("family", SPECS)
@pytest.mark.parametrize("mistake", ["period", "series", "publication_date", "http_modified", "pdf_modified", "correction", "geography_or_variant"])
def test_first_release_rejects_wrong_target_or_later_document(monkeypatch, family, mistake):
    text = text_for(family)
    event = event_for(family)
    metadata = {"ModDate": "D:20261029120000-05'00'"}
    modified = "Fri, 30 Oct 2026 18:00:00 GMT"
    if mistake == "period":
        event["reference_period"] = "2026-08"
    elif mistake == "series":
        event["series_variant"] = "another_series"
    elif mistake == "publication_date":
        text = text.replace("30 de octubre", "29 de octubre")
    elif mistake == "http_modified":
        modified = "Sun, 01 Nov 2026 18:00:00 GMT"
    elif mistake == "pdf_modified":
        metadata["ModDate"] = "D:20261101120000-05'00'"
    elif mistake == "correction":
        text = "Boletín corregido. Fe de erratas. " + text
    elif family == "ipc_annual_up":
        text = text.replace("Índice de Precios al Consumidor (IPC)", "IPC sin alimentos ni regulados")
    elif family == "ise_annual_up":
        text = text.replace("serie original", "serie ajustada por efecto estacional y calendario")
    else:
        text = text.replace("del total nacional", "de las 13 ciudades")
    monkeypatch.setattr(evidence, "pdf_text_and_metadata", lambda content: (text, metadata))
    with pytest.raises(ValueError):
        evidence.parse_first_release(b"%PDF-fixture", source_url=f"https://www.dane.gov.co/files/bol-{SPECS[family][0]}-sep2026.pdf", event=event, family=family, last_modified=modified)


def make_open_row(tmp_path, family):
    if family == "ipc_annual_up":
        observation = card()
        release = "ipc"
    elif family == "ise_annual_up":
        observation = card("ise_activity", period="2026-08", key="annual_growth_pct", value=3.0)
        release = "ise"
    else:
        observation = card("labor_market", period="2026-08", key="national_unemployment_rate_pct", value=9.4, history=[{"period": "2025-09", "published_at": "2025-10-31", "national_unemployment_rate_pct": 8.2}])
        release = "labor"
    run = setup_run(tmp_path, cards=[observation], events=[calendar(release)])
    prepare(run)
    append(run)
    return json.loads(ledger(tmp_path).read_text())


@pytest.mark.parametrize("family", SPECS)
def test_next_day_bulletin_capture_auto_resolves_even_when_current_card_has_revised(monkeypatch, tmp_path, family):
    row = make_open_row(tmp_path, family)
    capture_dir = tmp_path / "runs/2026-10-31"
    url = f"https://www.dane.gov.co/files/bol-{SPECS[family][0]}-sep2026.pdf"
    indicator = row["family_event"]["indicator_id"]
    write(capture_dir / "indicator_watch.json", [card(indicator, period="2026-09", key=row["family_event"]["value_key"], value=99.0, official_documents=[{"url": url}], is_revision=True)])
    monkeypatch.setattr(evidence, "pdf_text_and_metadata", lambda content: (text_for(family), {"ModDate": "D:20261029120000-05'00'"}))
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"%PDF-first-release", headers={"last-modified": "Fri, 30 Oct 2026 18:00:00 GMT"}, request=request))) as client:
        captured = evidence.capture_first_release(capture_dir, row, client=client, now=NEXT_DAY)
    assert captured["source_sha256"]
    assert (capture_dir / captured["source_artifact"]).exists()
    before = json.loads(ledger(tmp_path).read_text())
    result = resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of="2026-10-31", now=NEXT_DAY)
    assert result["resolved"] == [row["forecast_id"]]
    resolved = json.loads(ledger(tmp_path).read_text())
    assert resolved["resolution_evidence"]["value"] == SPECS[family][2]
    assert resolved["resolution_evidence"]["source_url"] == url
    assert resolved["probability"] == before["probability"]


def test_tampered_or_missing_release_bytes_cannot_resolve(monkeypatch, tmp_path):
    family = "ipc_annual_up"
    row = make_open_row(tmp_path, family)
    capture_dir = tmp_path / "runs/2026-10-31"
    capture_dir.mkdir()
    monkeypatch.setattr(evidence, "pdf_text_and_metadata", lambda content: (text_for(family), {}))
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"%PDF-first-release", request=request))) as client:
        capture = evidence.capture_first_release(capture_dir, row, client=client, source_url="https://www.dane.gov.co/bol-IPC-sep2026.pdf", now=NEXT_DAY)
    path = capture_dir / "family_release_evidence.json"
    artifact = json.loads(path.read_text())
    artifact["observations"][0]["value"] = 9.9
    write(path, artifact)
    assert evidence.verified_release_for_row(row, capture_dir) is None
    artifact["observations"][0]["value"] = 6.29
    write(path, artifact)
    (capture_dir / capture["source_artifact"]).write_bytes(b"tampered")
    assert evidence.verified_release_for_row(row, capture_dir) is None
    assert not resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of="2026-10-31", now=NEXT_DAY)["resolved"]


def test_ipc_discovery_follows_captured_official_operation_page(monkeypatch, tmp_path):
    family = "ipc_annual_up"
    row = make_open_row(tmp_path, family)
    run = tmp_path / "runs/2026-10-31"
    write(run / "indicator_watch.json", [card(period="2026-09")])
    requests = []
    def handler(request):
        requests.append(str(request.url))
        body = b'<a href="/files/operaciones/IPC/sep2026/bol-IPC-sep2026.pdf">Download</a>' if request.url.path == "/estadisticas" else b"%PDF-first-release"
        return httpx.Response(200, content=body, request=request)
    monkeypatch.setattr(evidence, "pdf_text_and_metadata", lambda content: (text_for(family), {}))
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = evidence.capture_first_release(run, row, client=client, now=NEXT_DAY)
    assert len(requests) == 2
    assert result["source_url"].endswith("/IPC/sep2026/bol-IPC-sep2026.pdf")


def test_network_collector_missing_documents_emits_actionable_notice(monkeypatch, tmp_path):
    run = setup_run(tmp_path, events=[calendar("labor")], cards=[card("labor_market", key="national_unemployment_rate_pct", value=9.4)])
    result = load_cli("capture_family_evidence").main(["--date", run.name, "--runs-dir", str(run.parent), "--ledger", str(ledger(tmp_path))])
    assert result == 2
    report = json.loads((run / "family_evidence_capture.json").read_text())
    assert "--history-url" in report["notices"][0]
