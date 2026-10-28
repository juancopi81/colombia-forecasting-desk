from copy import deepcopy
import importlib.util
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from colombia_forecasting_desk.plain_language import word_count
from colombia_forecasting_desk.reader_brief import (
    render_reader_brief, render_reader_brief_html, validate_reader_brief,
)

FIXTURES = Path(__file__).parent / "fixtures" / "reader_brief"


@pytest.fixture
def brief():
    return json.loads((FIXTURES / "oct27.json").read_text())


def test_october_27_golden_example(brief):
    rendered = render_reader_brief(brief)
    assert rendered == (FIXTURES / "oct27.md").read_text()
    assert word_count(rendered) <= 250
    html = render_reader_brief_html(brief)
    assert 'aria-label="Daily brief"' in html
    assert "55%" in html and "2026-10-30" in html


def test_truly_quiet_day_is_one_line(brief):
    for key in ("changes", "coming_up", "open_forecasts", "data_problems"):
        brief[key] = []
    assert render_reader_brief(brief) == "Nothing needs you today.\n"
    brief["data_problems"] = ["The official rate is unavailable."]
    assert "Data problems" in render_reader_brief(brief)


def test_upcoming_call_is_shown_once_in_markdown_and_html(brief):
    call = brief["open_forecasts"][0]
    brief["coming_up"][0].update(our_call={"forecast_id": call["forecast_id"], "probability": call["probability"], "outcome": "The committee approves the food-tax repeal."}, no_call_reason=None)
    for rendered in (render_reader_brief(brief), render_reader_brief_html(brief)):
        assert rendered.count("55%") == 1
        assert "Open forecasts" not in rendered
    brief["coming_up"][0].update(our_call=None, no_call_reason="This is a different upcoming event.")
    assert "Open forecasts" in render_reader_brief(brief)


def test_duplicate_hidden_call_still_has_to_match_the_ledger(brief):
    call = brief["open_forecasts"][0]
    brief["coming_up"][0].update(our_call={"forecast_id": call["forecast_id"], "probability": .8, "outcome": "The committee approves the food-tax repeal."}, no_call_reason=None)
    row = {**call, "status": "open", "run_date": "2026-10-26", "resolution_check_window_end": call["resolves_by"]}
    assert any("probability differs" in issue for issue in validate_reader_brief(brief, forecast_rows=[row]))


def test_total_visible_words_include_headings_and_labels(brief):
    brief["changes"][0]["why_it_matters"] = " ".join(["costs"] * 250)
    issues = validate_reader_brief(brief)
    assert any("maximum is 250" in issue for issue in issues)
    with pytest.raises(ValueError):
        render_reader_brief(brief)


@pytest.mark.parametrize("change", [
    lambda b: b["changes"][0].update(title="M3 decision"),
    lambda b: b["changes"][0].update(source_url="http://example.gov.co"),
    lambda b: b["coming_up"][0].update(no_call_reason="No source. Need more evidence."),
    lambda b: b["coming_up"][0].update(no_call_reason=None),
    lambda b: b["changes"].extend([deepcopy(b["changes"][0])] * 3),
    lambda b: b["open_forecasts"][0].update(plain_question="PL 012"),
    lambda b: b.update(headline="x" * 30),
])
def test_invalid_reader_fields(brief, change):
    change(brief)
    assert validate_reader_brief(brief)


def test_html_is_escaped_and_authored_html_is_rejected(brief):
    brief["headline"] = "Growth & rates < 3 percent"
    rendered = render_reader_brief_html(brief)
    assert "Growth &amp; rates &lt; 3 percent" in rendered
    brief["headline"] = '<script>alert("bad")</script>'
    assert validate_reader_brief(brief)


def test_unsafe_url_cannot_be_interpreted_as_markup(brief):
    brief["changes"][0]["source_url"] = 'https://example.gov.co/" onmouseover="alert(1)'
    assert validate_reader_brief(brief)


def test_provenance_checks_recorded_probabilities_and_sources(brief):
    call = brief["open_forecasts"][0]
    row = {"forecast_id": call["forecast_id"], "run_date": "2026-10-26", "status": "open",
           "probability": 0.55, "resolution_check_window_end": "2026-10-30"}
    sources = {brief["changes"][0]["source_url"]}
    assert not validate_reader_brief(brief, run_date="2026-10-27", forecast_rows=[row], source_urls=sources)
    assert validate_reader_brief(brief, forecast_rows=[], source_urls=set())
    row["probability"] = 0.65
    assert any("probability differs" in issue for issue in validate_reader_brief(brief, forecast_rows=[row]))
    row["probability"] = 0.55
    row["status"] = "resolved"
    assert any("closed forecast" in issue for issue in validate_reader_brief(brief, forecast_rows=[row]))


def _finalizer():
    path = Path(__file__).parent.parent / "scripts" / "finalize_reader_brief.py"
    spec = importlib.util.spec_from_file_location("finalize_reader_brief", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_finalizer_validates_before_writing_and_never_mutates_ledgers(tmp_path, brief):
    run = tmp_path / "runs" / "2026-10-27"
    run.mkdir(parents=True)
    forecasts = tmp_path / "forecasts"
    forecasts.mkdir()
    call = brief["open_forecasts"][0]
    row = {"forecast_id": call["forecast_id"], "run_date": "2026-10-26", "status": "open",
           "probability": 0.55, "resolution_check_window_end": "2026-10-30"}
    ledger = forecasts / "shadow_forecast_log.jsonl"
    ledger.write_text(json.dumps(row) + "\n")
    original = ledger.read_bytes()
    (run / "reader_brief.json").write_text(json.dumps(brief))
    with pytest.raises(ValueError, match="not captured"):
        _finalizer().finalize(run_date="2026-10-27", runs_dir=run.parent)
    assert not (run / "reader_brief.md").exists()
    (run / "raw_items.json").write_text(json.dumps([{"url": brief["changes"][0]["source_url"]}]))
    _finalizer().finalize(run_date="2026-10-27", runs_dir=run.parent)
    assert ledger.read_bytes() == original
    assert (run / "reader_brief.md").read_text() == render_reader_brief(brief)
    page = BeautifulSoup((run / "review.html").read_text(), "html.parser")
    assert page.select_one(".wrap").find().get("class") == ["reader-brief"]


def test_non_object_brief_is_rejected_without_writing(tmp_path):
    run = tmp_path / "2026-10-27"
    run.mkdir()
    (run / "reader_brief.json").write_text("[]")
    with pytest.raises(ValueError):
        _finalizer().finalize(run_date="2026-10-27", runs_dir=tmp_path)
    assert not (run / "review.html").exists()
