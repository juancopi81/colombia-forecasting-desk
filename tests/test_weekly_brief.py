from copy import deepcopy
from datetime import date
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
import yaml

from colombia_forecasting_desk.plain_language import word_count
from colombia_forecasting_desk.weekly_brief import (
    append_usefulness_feedback, build_weekly_facts, render_weekly_brief,
    render_weekly_html, validate_weekly_brief, week_bounds,
)

SHARED = Path(__file__).parent / "fixtures" / "shared_interfaces"
FIXTURES = Path(__file__).parent / "fixtures" / "weekly_brief"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def make_run(root, day="2026-10-23", *, value=-0.04):
    run = root / "runs" / day
    run.mkdir(parents=True)
    write_json(run / "indicator_watch.json", [{"indicator_id": "ise_activity", "period": "2026-08", "status": "observed", "headline": "Activity changed.", "values": {"annual_growth_pct": value}, "source_url": "https://www.dane.gov.co/activity"}])
    write_json(run / "agent_analysis.json", {"run_date": day, "strongest_changed_signal": {"signal": "A fiscal instrument was published.", "interpretation": "The budget exists.", "evidence_refs": []}})
    write_json(run / "raw_items.json", [{"id": "rate-clock", "source_id": "banrep_junta_calendar", "title": "BanRep interest-rate decision", "url": "https://www.banrep.gov.co/calendar", "fetched_at": day + "T15:00:00Z", "metadata": {"event_type": "banrep_policy_rate_decision", "scheduled_date": "2026-10-30"}}])
    return run


def forecast(identity="old", *, created="2026-09-01T15:00:00Z", resolved="2026-10-26T15:00:00Z"):
    return {"forecast_id": identity, "created_at": created, "run_date": created[:10], "question": "Will the official rate rise?", "status": "resolved", "resolved_at": resolved, "resolution_deadline": "2026-10-20", "resolution_check_window_end": "2026-10-23", "probability": 0.6, "outcome": "YES", "brier_score": 0.16, "baseline": {"probability": 0.5}}


def test_weekly_facts_deduplicate_and_exclude_later_runs(tmp_path):
    make_run(tmp_path, "2026-10-19", value=0.04)
    make_run(tmp_path, "2026-10-20")
    make_run(tmp_path, "2026-10-23")
    future = make_run(tmp_path, "2026-10-26", value=99)
    (tmp_path / "cases").mkdir()
    (tmp_path / "cases" / "future.yaml").write_text("future knowledge")
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    assert facts["run_dates"] == ["2026-10-19", "2026-10-20", "2026-10-23"]
    assert len(facts["indicator_changes"]) == 2
    assert facts["indicator_changes"][1]["last_seen"] == "2026-10-23"
    assert facts["indicator_changes"][1]["change_kind"] == "same_period_correction_or_revision"
    assert len(facts["agent_signals"]) == 1
    assert len(facts["events"]) == 1
    assert facts["events"][0]["family_id"] == "banrep_rate_decision"
    assert facts["cases"] == []
    assert any("live cases were not used" in gap for gap in facts["coverage_gaps"])
    assert all(item["values"]["annual_growth_pct"] != 99 for item in facts["indicator_changes"])


def test_full_followup_and_verified_source_survive_scanner_cap(tmp_path):
    run = make_run(tmp_path)
    text = "The next budget is in the complete source. " + ("supporting evidence " * 4000)
    (run / "official_followup.md").write_text(text)
    source = run / "sources" / "followup" / "budget.txt"
    source.parent.mkdir(parents=True)
    source.write_text("Budget amount: 634,952,040,871,099")
    write_json(source.parent / "metadata.json", {"fetched_at": "2026-10-23T15:30:00Z", "sources": [{"canonical_url": "https://official.gov.co/budget", "file": "sources/followup/budget.txt", "status_code": 200, "tls_verification": True, "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}], "fully_reviewed_pages_text_and_visual": [36, 37, 38]})
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    followup = facts["official_followups"][0]
    assert followup["text"] == text and len(text) > 60000
    receipt = followup["source_receipts"][0]
    assert receipt["hash_verified"] and receipt["reviewed_pages"] == [36, 37, 38]
    assert receipt["source_ref"]["artifact"] == str(source)
    early = build_weekly_facts(tmp_path / "runs", "2026-W43", cutoff_at="2026-10-23T15:00:00Z")
    assert not early["official_followups"] and not early["agent_signals"]


def test_later_predictions_resolutions_and_skips_cannot_change_history(tmp_path):
    make_run(tmp_path)
    directory = tmp_path / "forecasts"
    directory.mkdir()
    old = forecast()
    earlier = forecast("resolved", resolved="2026-10-22T15:00:00Z")
    future = forecast("future", created="2026-10-26T15:00:00Z")
    ledger = directory / "shadow_forecast_log.jsonl"
    ledger.write_text("\n".join(json.dumps(row) for row in [old, earlier, future]) + "\n")
    original = ledger.read_bytes()
    (directory / "family_skips.jsonl").write_text("\n".join(json.dumps(row) for row in [
        {"slot_id": "a", "run_date": "2026-10-23", "skip_reason": "No current rate"},
        {"slot_id": "b", "run_date": "2026-10-26", "skip_reason": "Future reason"},
    ]) + "\n")
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    internal = facts["forecasts"]["internal"]
    assert [row["forecast_id"] for row in internal["open"]] == ["old"]
    assert "outcome" not in internal["open"][0] and "brier_score" not in internal["open"][0]
    assert len(internal["resolved"]) == 1 and internal["right_side_n"] == 1
    assert internal["baseline_n"] == 1
    assert len(facts["forecasts"]["skips"]) == 1
    assert not facts["forecasts"]["made_this_week"]
    assert ledger.read_bytes() == original


def test_family_review_flags_use_the_canonical_date_without_future_knowledge(tmp_path):
    make_run(tmp_path)
    directory = tmp_path / "forecasts"
    directory.mkdir()
    rows = []
    for identity, since in [("eligible", "2026-10-01"), ("future-flag", "2026-10-26")]:
        row = forecast(identity)
        row.update(status="open", needs_review=True, needs_review_since=since)
        row.pop("resolved_at")
        rows.append(row)
    (directory / "shadow_forecast_log.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    projected = {row["forecast_id"]: row for row in facts["forecasts"]["internal"]["open"]}
    assert projected["eligible"]["needs_review"] is True
    assert projected["eligible"]["needs_review_since"] == "2026-10-01"
    assert "needs_review" not in projected["future-flag"]
    assert "needs_review_since" not in projected["future-flag"]


def test_frozen_scoreboard_is_preferred_over_live_ledger(tmp_path):
    run = make_run(tmp_path)
    snapshot = json.loads((SHARED / "scoreboard.valid.json").read_text())
    snapshot["as_of"] = "2026-10-23"
    snapshot["public"]["resolved"] = [forecast(resolved="2026-10-22T15:00:00Z")]
    write_json(run / "scoreboard.json", snapshot)
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    assert facts["forecasts"]["public"]["n_scored"] == 1
    assert facts["forecasts"]["scoreboard_source"].endswith("scoreboard.json")


def test_friday_intraday_flags_use_recording_time_and_survive_later_resolution(tmp_path):
    make_run(tmp_path)
    directory = tmp_path / "forecasts"
    directory.mkdir()
    known = forecast("known", resolved="2026-10-26T15:00:00Z")
    known.update(needs_review=True, needs_review_since="2026-10-23", needs_review_at="2026-10-23T14:00:00Z")
    later = forecast("later")
    later.update(status="open", needs_review=True, needs_review_since="2026-10-01", needs_review_at="2026-10-23T16:00:00Z")
    (directory / "shadow_forecast_log.jsonl").write_text("\n".join(json.dumps(row) for row in [known, later]) + "\n")
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43", cutoff_at="2026-10-23T15:00:00Z")
    rows = {row["forecast_id"]: row for row in facts["forecasts"]["internal"]["open"]}
    assert rows["known"]["needs_review"]
    assert "outcome" not in rows["known"]
    assert "needs_review" not in rows["later"]


def test_snapshot_hashes_and_future_views_are_checked(tmp_path):
    run = make_run(tmp_path)
    case = json.loads((SHARED / "case_file.valid.json").read_text())
    case["current_view"]["date"] = "2026-10-23"
    case["updates"][0]["date"] = "2026-10-23"
    folder = run / "cases_snapshot"
    folder.mkdir()
    path = folder / "case.yaml"
    path.write_text(yaml.safe_dump(case))
    manifest = {"run_date": "2026-10-23", "files": {"case.yaml": hashlib.sha256(path.read_bytes()).hexdigest()}}
    write_json(folder / "manifest.json", manifest)
    assert len(build_weekly_facts(tmp_path / "runs", "2026-W43")["cases"]) == 1
    case["current_view"]["date"] = "2026-10-27"
    path.write_text(yaml.safe_dump(case))
    manifest["files"]["case.yaml"] = hashlib.sha256(path.read_bytes()).hexdigest()
    write_json(folder / "manifest.json", manifest)
    assert not build_weekly_facts(tmp_path / "runs", "2026-W43")["cases"]


@pytest.mark.parametrize("receipt_list", [False, True])
def test_historical_receipt_paths_are_resolved_from_repo_root(tmp_path, receipt_list):
    run = make_run(tmp_path)
    (run / "official_followup.md").write_text("A verified original supports the finding.")
    folder = run / "sources" / "followup"
    folder.mkdir(parents=True)
    original = folder / "original.txt"
    original.write_text("Official record")
    source = {"url": "https://official.gov.co/record", "artifact": "runs/2026-10-23/sources/followup/original.txt", "status": 200,
              "tls_verified": True, "retrieved_at": "2026-10-23T15:00:00Z", "sha256": hashlib.sha256(original.read_bytes()).hexdigest()}
    write_json(folder / "receipts.json", [source] if receipt_list else {"sources": [source]})
    receipt = build_weekly_facts(tmp_path / "runs", "2026-W43")["official_followups"][0]["source_receipts"][0]
    assert receipt["hash_verified"] and receipt["tls_verification"] and receipt["status_code"] == 200
    assert receipt["source_ref"]["artifact"] == str(original)


@pytest.fixture
def brief():
    return json.loads((SHARED / "weekly_brief.valid.json").read_text())


def test_weekly_schema_render_caps_and_escaping(brief):
    assert not validate_weekly_brief(brief)
    assert word_count(render_weekly_brief(brief)) <= 450
    brief["big_picture"] = "Growth & inflation < 3 percent."
    assert "Growth &amp; inflation &lt; 3 percent." in render_weekly_html(brief)
    brief["big_picture"] = "First sentence. Second sentence. Third sentence."
    assert validate_weekly_brief(brief)
    brief["big_picture"] = '<script>alert("bad")</script>'
    assert validate_weekly_brief(brief)


def test_rendered_word_cap_is_not_a_json_character_count(brief):
    brief["top_stories"][0]["why_it_matters"] = " ".join(["growth"] * 450)
    assert any("450 words" in issue for issue in validate_weekly_brief(brief))


def test_event_identity_and_open_records_are_checked(tmp_path, brief):
    make_run(tmp_path)
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    brief["top_stories"][0]["source_url"] = "https://www.dane.gov.co/activity"
    brief["data_problems"] = ["Historical story and watchlist snapshots are unavailable."]
    assert not validate_weekly_brief(brief, facts=facts)
    brief["next_week"][0]["event"] = "Unemployment release"
    assert any("event is not established" in issue for issue in validate_weekly_brief(brief, facts=facts))
    brief["next_week"][0]["event"] = "Interest-rate decision"
    row = forecast()
    row.pop("outcome")
    row["status"] = "open"
    facts["forecasts"]["internal"]["open"] = [row]
    assert any("must be listed" in issue for issue in validate_weekly_brief(brief, facts=facts))
    brief["open_forecasts"] = [{"forecast_id": "old", "plain_question": "Will the official rate rise?", "probability": 0.6, "resolves_by": "2026-10-23"}]
    assert not validate_weekly_brief(brief, facts=facts)
    assert "Open forecasts" in render_weekly_brief(brief)


def test_feedback_requires_an_explicit_user_answer_and_is_idempotent(tmp_path):
    path = tmp_path / "reviews" / "usefulness_log.md"
    feedback = {"week": "2026-W43", "date": "2026-10-27", "answer": "It changed what I plan to check.\nKeep the budget story.", "source": "user_message", "source_ref": "supplied message 17"}
    assert append_usefulness_feedback(path, feedback)
    assert not append_usefulness_feedback(path, feedback)
    assert "> Keep the budget story." in path.read_text()
    with pytest.raises(ValueError):
        append_usefulness_feedback(path, {**feedback, "source": "inferred_from_silence"})


@pytest.mark.parametrize("week", ["2026-W00", "2026-W54", "../2026-W43", "2026-43"])
def test_invalid_week_cannot_select_paths(week):
    with pytest.raises(ValueError):
        week_bounds(week)


def test_historical_demo_golden():
    brief = json.loads((FIXTURES / "w43.json").read_text())
    assert render_weekly_brief(brief) == (FIXTURES / "w43.md").read_text()


def test_finalizer_writes_only_after_provenance_validation(tmp_path, brief):
    run = make_run(tmp_path)
    facts = build_weekly_facts(tmp_path / "runs", "2026-W43")
    brief["top_stories"][0]["source_url"] = "https://www.dane.gov.co/activity"
    brief["data_problems"] = ["Historical story and watchlist snapshots are unavailable."]
    folder = tmp_path / "weekly" / "2026-W43"
    write_json(folder / "weekly_facts.json", facts)
    write_json(folder / "weekly_brief.json", brief)
    path = Path(__file__).parent.parent / "scripts" / "finalize_weekly_brief.py"
    spec = importlib.util.spec_from_file_location("finalize_weekly_brief", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.finalize(week="2026-W43", weekly_dir=folder.parent)
    original = (folder / "weekly.html").read_bytes()
    assert (folder / "weekly_brief.md").read_text() == render_weekly_brief(brief)
    brief["top_stories"][0]["source_url"] = "https://unknown.gov.co/new-claim"
    write_json(folder / "weekly_brief.json", brief)
    with pytest.raises(ValueError, match="not in the dated facts"):
        module.finalize(week="2026-W43", weekly_dir=folder.parent)
    assert (folder / "weekly.html").read_bytes() == original


def test_dane_calendar_gaps_survive_weekly_facts_only_before_cutoff(tmp_path):
    run = make_run(tmp_path)
    write_json(run / 'm3_preflight_opportunities.json', {'generated_at': '2026-10-23T15:30:00Z', 'opportunities': [], 'caveats': [
        {'detector': 'dane_economic_release', 'reason': 'Annual calendar truncated; future coverage incomplete.'},
        {'detector': 'dane_economic_release', 'reason': 'Later notice', 'recorded_at': '2026-10-23T17:30:00Z'},
    ]})
    facts = build_weekly_facts(tmp_path / 'runs', '2026-W43', cutoff_at='2026-10-23T16:00:00Z')
    assert any('Annual calendar truncated' in gap for gap in facts['coverage_gaps'])
    assert not any('Later notice' in gap for gap in facts['coverage_gaps'])
    early = build_weekly_facts(tmp_path / 'runs', '2026-W43', cutoff_at='2026-10-23T15:00:00Z')
    assert not any('Annual calendar truncated' in gap for gap in early['coverage_gaps'])


def test_weekly_calendar_identity_deduplicates_copies_but_keeps_distinct_events(tmp_path):
    run = make_run(tmp_path)
    urls = [f'https://www.dane.gov.co/index.php/calendario/icalrepeat.detail/2026/10/30/{i}/-/geih' for i in (1, 2)]
    write_json(run / 'raw_items.json', [{'id': f'event-{i}', 'source_id': 'dane_publication_calendar',
        'title': f'GEIH thematic release {i}', 'url': url, 'fetched_at': '2026-10-23T15:00:00Z',
        'metadata': {'scheduled_date': '2026-10-30', 'release_family': 'labor', 'calendar_event_url': url}}
        for i, url in enumerate(urls)])
    write_json(run / 'm3_preflight_opportunities.json', {'generated_at': '2026-10-23T15:00:00Z',
        'opportunities': [{'opportunity_id': f'dane_labor_release_2026-10-30_{i}',
        'event_date': '2026-10-30', 'title': f'DANE thematic release {i}',
        'source_evidence': [{'source_id': 'dane_publication_calendar', 'url': url + '?utm_source=calendar'}]}
        for i, url in enumerate(urls)]})
    facts = build_weekly_facts(tmp_path / 'runs', '2026-W43', cutoff_at='2026-10-23T16:00:00Z')
    assert len(facts['events']) == 2
    assert {x['source_url'] for x in facts['events']} == set(urls)
