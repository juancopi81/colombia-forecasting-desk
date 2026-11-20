"""Chart provenance, historical cutoffs and real brief delivery regressions."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
from zipfile import ZipFile

from bs4 import BeautifulSoup
import pytest

from colombia_forecasting_desk.brief_charts import (
    DATA_FILE, chart_html, compile_chart_data, day_cutoff, load_chart_data,
    render_chart_svg, validate_chart_decision,
)
from colombia_forecasting_desk.reader_brief import render_reader_brief, validate_reader_brief
from colombia_forecasting_desk.schemas import validate_artifact
from colombia_forecasting_desk.weekly_brief import build_weekly_facts, validate_weekly_brief

ROOT = Path(__file__).resolve().parents[1]
SHARED = ROOT / "tests/fixtures/shared_interfaces"
STAMP = "2026-11-20T14:00:00-05:00"
SOURCE = "https://www.dane.gov.co/chart-fixture.json"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


@pytest.fixture
def evidence(tmp_path):
    run = tmp_path / "runs/2026-11-20"
    run.mkdir(parents=True)
    # Synthetic serialization of the verified GDP figures, not an official JSON capture.
    original = run / "sources/gdp.json"
    write_json(original, {"series": "Real GDP annual growth, original series", "unit": "%",
                          "observations": [{"period": "2026-Q1", "value": 2.1},
                                           {"period": "2026-Q2", "value": 3.6},
                                           {"period": "2026-Q3", "value": 0.5}]})
    receipt = {"source_url": SOURCE, "artifact": "sources/gdp.json", "sha256": hashlib.sha256(original.read_bytes()).hexdigest(),
               "captured_at": "2026-11-17T15:30:00-05:00", "status_code": 200, "tls_verified": True, "content_type": "application/json"}
    write_json(run / "sources/gdp_receipt.json", receipt)
    request = {"schema_version": "brief_chart_request.v1", "id": "gdp", "title": "GDP growth slowed sharply",
               "source_receipt": "sources/gdp_receipt.json", "frequency": "quarterly", "series_locator": "/series", "unit_locator": "/unit",
               "points": [{"period_locators": [f"/observations/{i}/period"], "value_locator": f"/observations/{i}/value"} for i in range(3)]}
    data = compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))
    write_json(run / DATA_FILE, data)
    return run, request, data


def reader(data=None):
    brief = json.loads((SHARED / "reader_brief.valid.json").read_text())
    brief.update(schema_version="reader_brief.v2", run_date="2026-11-20", coming_up=[], open_forecasts=[], data_problems=[])
    brief["changes"][0].update(title="Growth slows", source_url=SOURCE,
                               what_happened="Annual GDP growth slowed from 3.6% to 0.5%.",
                               why_it_matters="Demand and employment warrant attention.")
    brief["chart_decision"] = {"reason": "The previous-quarter comparison makes the slowdown visible.", "chart": None}
    if data:
        brief["chart_decision"]["chart"] = {"data_id": data["data_id"], "story_index": 0,
                                                "why_it_matters": "Annual growth slowed by 3.1 percentage points; these are growth rates."}
    return brief


def weekly(data=None):
    brief = json.loads((SHARED / "weekly_brief.valid.json").read_text())
    brief.update(schema_version="weekly_brief.v2", week="2026-W47", as_of="2026-11-20", next_week=[],
                 patterns=[], data_problems=["Source and calendar coverage remains incomplete."])
    brief["top_stories"][0]["source_url"] = SOURCE
    brief["chart_decision"] = reader(data)["chart_decision"]
    return brief


def test_gdp_comparison_uses_source_values_and_preserves_annual_series(evidence):
    run, _, data = evidence
    assert load_chart_data(run, cutoff=day_cutoff(run.name)) == data
    assert data["points"][-2:] == [{"period": "2026-Q2", "value": 3.6}, {"period": "2026-Q3", "value": .5}]
    svg = BeautifulSoup(render_chart_svg(data), "html.parser")
    assert "original series" in svg.desc.text
    assert "3.6" in svg.get_text() and "0.5" in svg.get_text()
    assert "-0.8" not in svg.get_text()  # Separate adjusted quarter-on-quarter reading.
    assert "<script" not in str(svg)


@pytest.mark.parametrize("field,value", [("unit", "COP"), ("series", "Adjusted quarter-on-quarter GDP"),
                                          ("points", [{"period": "2026-Q3", "value": 99}])])
def test_authored_value_or_metadata_cannot_override_original(evidence, field, value):
    run, _, data = evidence
    data[field] = value
    write_json(run / DATA_FILE, data)
    with pytest.raises(ValueError, match="differs"):
        load_chart_data(run, cutoff=day_cutoff(run.name))


def test_modified_original_is_rejected_before_any_output(evidence):
    run, _, data = evidence
    (run / "sources/gdp.json").write_text('{"value": 99}')
    write_json(run / "reader_brief.json", reader(data))
    with pytest.raises(ValueError, match="hash mismatch"):
        module("finalize_reader_brief").finalize(run_date=run.name, runs_dir=run.parent)
    assert not (run / "reader_brief.md").exists()
    assert not (run / "brief_chart.png").exists()


@pytest.mark.parametrize("bad", [None, True, "0.5", float("inf"), float("nan")])
def test_missing_or_invalid_source_value_is_never_zero(evidence, bad):
    run, request, _ = evidence
    original = run / "sources/gdp.json"
    raw = json.loads(original.read_text()); raw["observations"][-1]["value"] = bad
    write_json(original, raw)
    rec = json.loads((run / "sources/gdp_receipt.json").read_text())
    rec["sha256"] = hashlib.sha256(original.read_bytes()).hexdigest()
    write_json(run / "sources/gdp_receipt.json", rec)
    with pytest.raises(ValueError, match="finite numbers"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))


def test_period_and_value_cannot_come_from_different_records(evidence):
    run, request, _ = evidence
    request["points"][-1]["value_locator"] = "/observations/0/value"
    with pytest.raises(ValueError, match="same series records"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))


def test_gaps_duplicates_and_path_escapes_are_rejected(evidence):
    run, request, _ = evidence
    request["points"].pop(1)
    with pytest.raises(ValueError, match="consecutive"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))
    request["points"][-1] = deepcopy(request["points"][0])
    with pytest.raises(ValueError, match="unique"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))
    request["source_receipt"] = "../outside.json"
    with pytest.raises(ValueError, match="outside"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))


def test_pdf_receipt_cannot_validate_a_manual_transcription(evidence):
    run, request, _ = evidence
    rec = json.loads((run / "sources/gdp_receipt.json").read_text())
    rec.update(source_url="https://www.dane.gov.co/bulletin.pdf", content_type="application/pdf")
    write_json(run / "sources/gdp_receipt.json", rec)
    with pytest.raises(ValueError, match="transcription"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))


@pytest.mark.parametrize("percentage_format", [False, True])
def test_xlsx_extracts_aligned_series_and_merged_year_headers(evidence, percentage_format):
    run, request, _ = evidence
    workbook = run / "sources/gdp.xlsx"
    with ZipFile(workbook, "w") as archive:
        archive.writestr("xl/workbook.xml", '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="GDP" sheetId="1" r:id="rId1"/></sheets></workbook>')
        archive.writestr("xl/_rels/workbook.xml.rels", '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>')
        sheet = '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>%</t></is></c><c r="B1"><v>2026</v></c></row><row r="2"><c r="B2" t="inlineStr"><is><t>II</t></is></c><c r="C2" t="inlineStr"><is><t>III</t></is></c></row><row r="3"><c r="A3" t="inlineStr"><is><t>Original annual GDP growth</t></is></c><c r="B3"><v>3.6</v></c><c r="C3"><v>0.5</v></c></row></sheetData></worksheet>'
        if percentage_format:
            archive.writestr("xl/styles.xml", '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="10"/></cellXfs></styleSheet>')
            sheet = sheet.replace('<c r="B3"><v>3.6</v>', '<c r="B3" s="1"><v>0.036</v>').replace('<c r="C3"><v>0.5</v>', '<c r="C3" s="1"><v>0.005</v>')
        archive.writestr("xl/worksheets/sheet1.xml", sheet)
    rec = json.loads((run / "sources/gdp_receipt.json").read_text())
    rec.update(artifact="sources/gdp.xlsx", source_url="https://www.dane.gov.co/gdp.xlsx", sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    write_json(run / "sources/gdp_receipt.json", rec)
    request.update(series_locator="GDP!A3", unit_locator="GDP!A1", points=[
        {"period_locators": ["GDP!B1", "GDP!B2"], "value_locator": "GDP!B3"},
        {"period_locators": ["GDP!B1", "GDP!C2"], "value_locator": "GDP!C3"}])
    data = compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))
    assert [point["value"] for point in data["points"]] == [3.6, .5]
    request["series_locator"] = "GDP!A1"
    with pytest.raises(ValueError, match="align"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))


def test_v2_requires_decision_and_accepts_quiet_written_skip():
    brief = reader()
    assert validate_artifact(brief) == []
    brief.pop("chart_decision")
    assert validate_artifact(brief)
    brief["chart_decision"] = {"reason": "No changed signal benefits from historical context.", "chart": None}
    brief["changes"] = []
    assert render_reader_brief(brief) == "Nothing needs you today.\n"
    assert not validate_weekly_brief(weekly())


def test_selecting_multiple_charts_or_unrelated_story_is_rejected(evidence):
    _, _, data = evidence
    brief = reader(data)
    brief["chart_decision"]["chart"] = [brief["chart_decision"]["chart"]] * 2
    assert validate_chart_decision(brief)
    brief = reader(data)
    brief["chart_decision"]["chart"]["story_index"] = 7
    assert validate_reader_brief(brief, chart_data=data)


def test_caption_words_count_toward_existing_limit(evidence):
    _, _, data = evidence
    brief = reader(data)
    brief["changes"][0]["why_it_matters"] = "costs " * 230
    assert any("maximum is 250" in issue for issue in validate_reader_brief(brief, chart_data=data))


def test_weekly_chart_uses_frozen_data_after_live_source_changes(evidence):
    run, _, data = evidence
    facts = build_weekly_facts(run.parent, "2026-W47", cutoff_at="2026-11-20T15:00:00-05:00")
    assert facts["chart_data"] == [data]
    assert not validate_weekly_brief(weekly(data), facts=facts)
    (run / "sources/gdp.json").write_text("later revision")
    assert not validate_weekly_brief(weekly(data), facts=facts)
    before = build_weekly_facts(run.parent, "2026-W47", cutoff_at="2026-11-20T13:00:00-05:00")
    assert before["chart_data"] == []
    assert any("chart evidence excluded" in gap for gap in before["coverage_gaps"])
    assert validate_weekly_brief(weekly(data), facts=before)


def test_weekly_finalizer_delivers_chart_without_reopening_later_sources(evidence):
    run, _, data = evidence
    facts = build_weekly_facts(run.parent, "2026-W47", cutoff_at="2026-11-20T15:00:00-05:00")
    folder = run.parent.parent / "weekly/2026-W47"
    write_json(folder / "weekly_facts.json", facts)
    write_json(folder / "weekly_brief.json", weekly(data))
    (run / "sources/gdp.json").write_text("later revision must not enter the frozen week")
    module("finalize_weekly_brief").finalize(week="2026-W47", weekly_dir=folder.parent, require_chart_decision=True)
    assert str((folder / "brief_chart.png").resolve()) in (folder / "weekly_brief.md").read_text()
    assert "3.6" in (folder / "brief_chart.svg").read_text()
    receipt = json.loads((folder / "weekly_brief_receipt.json").read_text())
    assert receipt["cutoff_at"] == facts["cutoff_at"]
    assert receipt["sha256"]["weekly_facts.json"] == hashlib.sha256((folder / "weekly_facts.json").read_bytes()).hexdigest()
    assert receipt["sha256"]["brief_chart.png"] == hashlib.sha256((folder / "brief_chart.png").read_bytes()).hexdigest()


def test_chart_receipt_or_extraction_after_cutoff_is_not_eligible(evidence):
    run, request, data = evidence
    with pytest.raises(ValueError, match="postdates"):
        compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff("2026-11-16"))
    facts = build_weekly_facts(run.parent, "2026-W47", cutoff_at="2026-11-20T15:00:00-05:00")
    facts["chart_data"][0]["source"]["captured_at"] = "2026-11-20T16:00:00-05:00"
    assert validate_weekly_brief(weekly(data), facts=facts)


def test_official_source_receipt_cannot_use_nonofficial_or_unsafe_url(evidence):
    run, request, _ = evidence
    rec_path = run / "sources/gdp_receipt.json"
    rec = json.loads(rec_path.read_text())
    for url in ["https://example.com/gdp.json", "https://user@www.dane.gov.co/gdp.json", "https://www.dane.gov.co/a\nfile.json"]:
        rec["source_url"] = url; write_json(rec_path, rec)
        with pytest.raises(ValueError, match="official"):
            compile_chart_data(request, run, generated_at=STAMP, cutoff=day_cutoff(run.name))


def test_flat_and_negative_series_keep_readable_zero_inclusive_geometry(evidence):
    _, _, data = evidence
    for values in [[0, 0, 0], [-1, -.5, -.5], [.00001, .00002, .00003]]:
        for point, value in zip(data["points"], values):
            point["value"] = value
        svg = render_chart_svg(data)
        assert "nan" not in svg.lower() and "inf" not in svg.lower()
        assert "fill=\"none\"" in svg and "<path" in svg


def test_v2_finalization_embeds_real_png_and_receipt_covers_it(evidence):
    run, _, data = evidence
    write_json(run / "reader_brief.json", reader(data))
    write_json(run / "raw_items.json", [{"url": SOURCE}])
    write_json(run / "acceptance_report.json", {"status": "pass", "strict_pass": True})
    module("finalize_reader_brief").finalize(run_date=run.name, runs_dir=run.parent, require_chart_decision=True)
    assert (run / "brief_chart.png").read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    assert str((run / "brief_chart.png").resolve()) in (run / "reader_brief.md").read_text()
    page = BeautifulSoup((run / "review.html").read_text(), "html.parser")
    assert page.select_one(".reader-brief .brief-chart svg")
    receipt = json.loads((run / "reader_brief_receipt.json").read_text())
    assert receipt["sha256"]["brief_chart.png"] == hashlib.sha256((run / "brief_chart.png").read_bytes()).hexdigest()
    assert module("check_usefulness").check_usefulness(run.parent, run.name)["decision_grade_reader_runs"] == 1
    (run / "brief_chart.png").write_bytes(b"altered")
    assert module("check_usefulness").check_usefulness(run.parent, run.name)["decision_grade_reader_runs"] == 0


def test_cli_rejects_v1_unless_explicit_historical_mode(evidence):
    run, _, _ = evidence
    brief = reader(); brief["schema_version"] = "reader_brief.v1"; brief.pop("chart_decision")
    write_json(run / "reader_brief.json", brief)
    write_json(run / "raw_items.json", [{"url": SOURCE}])
    cli = module("finalize_reader_brief")
    args = ["--date", run.name, "--runs-dir", str(run.parent)]
    assert cli.main(args) == 1
    assert cli.main([*args, "--allow-legacy"]) == 0


def test_html_labels_are_escaped(evidence):
    _, _, data = evidence
    data["title"] = "Growth & output < 4 percent"
    assert "Growth &amp; output &lt; 4 percent" in chart_html(reader(data), data)
