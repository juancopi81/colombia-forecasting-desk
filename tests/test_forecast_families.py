from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from colombia_forecasting_desk.forecast_families import (
    append_family_forecasts, baseline_for_slot, build_scoreboard,
    plan_family_forecasts, render_scoreboard, resolve_family_forecasts,
    write_scoreboard,
)
from colombia_forecasting_desk.schemas import validate_artifact
from colombia_forecasting_desk.shadow_forecasts import (
    ShadowForecastError, append_shadow_forecast, flag_shadow_forecasts_for_review,
    read_shadow_forecast_ledger, resolve_shadow_forecast, validate_shadow_forecast,
    void_shadow_forecast,
)

NOW = datetime(2026, 10, 27, 16, tzinfo=timezone.utc)
EVENT = "2026-10-30"
BANREP = "https://www.banrep.gov.co/es/comunicados-junta"
DANE = "https://www.dane.gov.co/estadisticas"


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False))


def card(identity="ipc_inflation", *, period="2026-08", value=6.0, key="annual_variation_pct", **values):
    return {"indicator_id": identity, "status": "observed", "freshness_status": "current", "period": period, "release_date": "2026-09-08T00:00:00Z", "source_url": BANREP if identity == "policy_rate_ibr" else DANE, "values": {key: value, **values}}


def calendar(family="ipc", day=EVENT, reference="2026-09"):
    return {"id": f"calendar_{family}", "source_id": "banrep_junta_calendar" if family == "banrep" else "dane_publication_calendar", "title": "Official scheduled release", "url": BANREP if family == "banrep" else DANE, "metadata": {"event_type": "banrep_policy_rate_decision" if family == "banrep" else "dane_economic_release", "release_family": family, "scheduled_date": day, "scheduled_at_local": day + "T10:00:00-05:00", "reference_period": day if family == "banrep" else reference}}


def setup_run(tmp_path, *, cards=None, events=None):
    run = tmp_path / "runs" / "2026-10-27"
    run.mkdir(parents=True)
    write(run / "indicator_watch.json", cards if cards is not None else [card()])
    write(run / "raw_items.json", events if events is not None else [calendar()])
    write(run / "m3_preflight_opportunities.json", {"opportunities": []})
    write(run / "acceptance_report.json", {"status": "pass"})
    return run


def prepare(run, *, probability=0.6):
    planned = plan_family_forecasts(run)
    write(run / "family_forecast_slots.json", planned)
    entries = [{"slot_id": slot["slot_id"], "decision": "forecast", "probability": probability, "rationale_for": ["The latest official observation supports an increase."], "rationale_against": ["Recent activity weakness could reduce inflation."], "falsifier": "The exact official first release is at or below the threshold.", "skip_reason": None, "evidence_refs": [{"artifact": "indicator_watch.json", "locator": f"indicator_id={slot['indicator_id']}"}]} for slot in planned["slots"] if slot["enabled"]]
    write(run / "family_forecasts.json", {"schema_version": "family_forecasts.v1", "run_date": run.name, "entries": entries})
    return planned


def append(run, **kwargs):
    root = run.parent.parent
    return append_family_forecasts(run, root / "forecasts/shadow_forecast_log.jsonl", root / "forecasts/family_skips.jsonl", model="test-model", reasoning_effort="high", **{"now": NOW, **kwargs})


def ledger(tmp_path):
    return tmp_path / "forecasts/shadow_forecast_log.jsonl"


def first_release(tmp_path, *, period="2026-09", value=6.1, variant="headline_annual", run_date=EVENT, published=EVENT, **extra):
    run = tmp_path / "runs" / run_date
    run.mkdir(parents=True, exist_ok=True)
    observation = card(period=period, value=value, series_variant=variant, **extra)
    observation["release_date"] = published + "T00:00:00Z"
    write(run / "indicator_watch.json", [observation])
    return observation


def test_planner_fully_specifies_exact_slot_and_fallback_baseline(tmp_path):
    run = setup_run(tmp_path)
    result = plan_family_forecasts(run)
    assert not validate_artifact(result)
    slot = result["slots"][0]
    assert slot["slot_id"] == "ipc_annual_up:2026-10-30"
    assert slot["reference_period"] == "2026-09"
    assert slot["threshold"] == 6
    assert slot["series_variant"] == "headline_annual"
    assert slot["cutoff_at"] == "2026-10-30T10:00:00-05:00"
    assert slot["baseline"]["label"] == "uninformative_50_50"
    assert slot["enabled"]


def test_failed_rate_is_explicit_and_labor_is_disabled(tmp_path):
    run = setup_run(tmp_path, cards=[], events=[calendar("banrep"), calendar("labor")])
    result = plan_family_forecasts(run)
    assert result["notices"] == ["BanRep slot unavailable: no current policy rate"]
    assert len(result["slots"]) == 1
    assert not result["slots"][0]["enabled"]
    assert "2025-09" in " ".join(result["slots"][0]["missing_evidence"])


def test_verified_official_fallback_unblocks_banrep_without_feed(tmp_path):
    run = setup_run(tmp_path, cards=[], events=[calendar("banrep")])
    fallback = {"policy_rate_pct": 12.25, "decision_date": "2026-09-30", "verified_at": "2026-10-27T16:00:00Z", "verified": True, "source_url": "https://banrep.gov.co/es/noticias/junta-directiva/junta-directiva-septiembre-2026"}
    result = plan_family_forecasts(run, policy_fallback=fallback)
    assert result["slots"][0]["threshold"] == 12.25
    assert result["slots"][0]["policy_rate_provenance"] == fallback
    assert result["slots"][0]["enabled"]
    for changed in ({"verified": False}, {"source_url": "https://news.example/result"}, {"verified_at": "2026-11-01T10:00:00Z"}, {"decision_date": "2026-05-01"}):
        assert not plan_family_forecasts(run, policy_fallback={**fallback, **changed})["slots"]


def test_only_one_to_seven_days_are_planned_and_midnight_is_conservative(tmp_path):
    run = setup_run(tmp_path, events=[calendar(day="2026-10-27"), calendar(day="2026-11-04"), calendar(day="2026-11-03")])
    raw = json.loads((run / "raw_items.json").read_text())
    raw[-1]["metadata"].pop("scheduled_at_local")
    write(run / "raw_items.json", raw)
    result = plan_family_forecasts(run)
    assert len(result["slots"]) == 1
    assert result["slots"][0]["cutoff_at"] == "2026-11-03T00:00:00-05:00"
    assert result["slots"][0]["cutoff_basis"] == "conservative_start_of_official_event_date"


def test_stale_comparator_and_wrong_labor_month_do_not_become_calls(tmp_path):
    labor = card("labor_market", period="2026-08", key="national_unemployment_rate_pct", value=9.4, national_unemployment_previous_year_pct=8.6, national_unemployment_previous_year_period="2025-08")
    run = setup_run(tmp_path, cards=[labor], events=[calendar("labor")])
    assert not plan_family_forecasts(run)["slots"][0]["enabled"]
    labor["values"].update(history=[{"period": "2025-09", "published_at": "2025-10-31", "national_unemployment_rate_pct": 9.1}], history_source_url=DANE)
    write(run / "indicator_watch.json", [labor])
    result = plan_family_forecasts(run)
    assert result["slots"][0]["enabled"]
    assert result["slots"][0]["threshold"] == 9.1


def test_survey_share_is_a_distribution_not_a_median():
    survey = card("banrep_eme_expectations", period="2026-10", key="median_pct", value=12.5)
    survey["source_url"] = BANREP
    assert baseline_for_slot("banrep_rate_decision", {}, threshold=12.25, as_of=NOW.date(), survey=survey)["probability"] == 0.5
    survey["values"].update(policy_rate_distribution=[{"rate_pct": 12.25, "count": 8}, {"rate_pct": 12.5, "count": 12}], policy_rate_distribution_event_date=EVENT)
    result = baseline_for_slot("banrep_rate_decision", {}, threshold=12.25, as_of=NOW.date(), survey=survey)
    assert result["probability"] == 0.6
    assert result["rule"] == "survey_share"


def test_empirical_baseline_uses_only_comparable_pre_forecast_official_history():
    observation = card(history=[{"period": "2026-05", "published_at": "2026-06-06", "annual_variation_pct": 5.9}, {"period": "2026-06", "published_at": "2026-07-07", "annual_variation_pct": 6.0}, {"period": "2026-07", "published_at": "2026-08-08", "annual_variation_pct": 5.8}, {"period": "2026-09", "published_at": "2026-10-01", "annual_variation_pct": 7.0}, {"period": "2026-10", "published_at": "2026-11-01", "annual_variation_pct": 8.0}])
    result = baseline_for_slot("ipc_annual_up", observation, threshold=6, as_of=NOW.date())
    assert result["probability"] == 0.5
    assert result["window"]["n"] == 2  # May→June, June→July; August is missing.
    assert result["window"]["end"] == "2026-09"


def test_append_is_idempotent_across_days_and_preserves_predictions(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    assert len(append(run)["appended"]) == 1
    before = ledger(tmp_path).read_bytes()
    assert len(append(run)["already_recorded"]) == 1
    assert ledger(tmp_path).read_bytes() == before
    authored = json.loads((run / "family_forecasts.json").read_text())
    authored["entries"][0]["probability"] = 0.8
    write(run / "family_forecasts.json", authored)
    with pytest.raises(ValueError, match="immutable"):
        append(run)
    assert ledger(tmp_path).read_bytes() == before


def test_skips_are_idempotent_and_can_be_superseded_by_a_later_call(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    entry = {"slot_id": "ipc_annual_up:2026-10-30", "decision": "skip", "skip_reason": "No defensible directional edge yet."}
    write(run / "family_forecasts.json", {"schema_version": "family_forecasts.v1", "run_date": run.name, "entries": [entry]})
    assert append(run)["skipped"]
    skips = tmp_path / "forecasts/family_skips.jsonl"
    before = skips.read_bytes()
    append(run)
    assert skips.read_bytes() == before
    assert not ledger(tmp_path).exists()
    prepare(run)
    append(run)
    score = build_scoreboard(tmp_path, as_of="2026-10-27")["families"][1]
    assert (score["events"], score["calls"], score["skips"]) == (1, 1, 0)


def test_every_due_slot_must_have_an_explicit_decision(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    write(run / "family_forecasts.json", {"schema_version": "family_forecasts.v1", "run_date": run.name, "entries": []})
    with pytest.raises(ValueError, match="Every enabled"):
        append(run)
    assert not ledger(tmp_path).exists()


@pytest.mark.parametrize("failure", ["blocked", "backdate", "late", "missing_evidence", "wrong_series", "disabled"])
def test_invalid_family_calls_write_nothing(tmp_path, failure):
    run = setup_run(tmp_path)
    planned = prepare(run)
    if failure == "blocked":
        write(run / "acceptance_report.json", {"status": "fail"})
    if failure == "backdate":
        with pytest.raises(ValueError, match="backdated"):
            append(run, now=NOW.replace(day=28))
        return
    if failure in ("late", "wrong_series", "disabled"):
        slot = planned["slots"][0]
        if failure == "late":
            slot["cutoff_at"] = "2026-10-27T08:00:00-05:00"
        elif failure == "wrong_series":
            slot["series_variant"] = None
        else:
            slot["enabled"] = False
        write(run / "family_forecast_slots.json", planned)
    if failure == "missing_evidence":
        (run / "indicator_watch.json").unlink()
    with pytest.raises(ValueError):
        append(run)
    assert not ledger(tmp_path).exists()


def test_preexisting_discretionary_banrep_call_covers_family_event(tmp_path):
    run = setup_run(tmp_path, cards=[card("policy_rate_ibr", period="2026-10-27", key="policy_rate_pct", value=12.25)], events=[calendar("banrep")])
    prepare(run)
    authored = json.loads((run / "family_forecasts.json").read_text())
    original_entry = authored["entries"][0]
    append(run)
    original = json.loads(ledger(tmp_path).read_text())
    original.pop("forecast_family")
    original.pop("slot_id")
    original.pop("family_event")
    original["forecast_id"] = "discretionary_october_rate"
    original["question"] = "Will Banco de la República set the policy rate above 12.25% on October 30?"
    ledger(tmp_path).write_text(json.dumps(original) + "\n")
    before = ledger(tmp_path).read_bytes()
    assert append(run)["covered_by_existing"] == ["discretionary_october_rate"]
    assert ledger(tmp_path).read_bytes() == before
    family_row = deepcopy(original)
    family_row["forecast_id"] = "different_id"
    family_row["question"] = "Will BanRep increase its policy rate at the October 30 decision?"
    with pytest.raises(ShadowForecastError, match="Duplicate family event"):
        append_shadow_forecast(ledger(tmp_path), family_row)
    assert original_entry["probability"] == 0.6


@pytest.mark.parametrize("value,expected", [(6.1, "YES"), (6.0, "NO"), (5.9, "NO")])
def test_resolver_exact_first_release_yes_no_and_ties(tmp_path, value, expected):
    run = setup_run(tmp_path)
    prepare(run)
    append(run)
    before = json.loads(ledger(tmp_path).read_text())
    first_release(tmp_path, value=value)
    result = resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of=EVENT, now=datetime(2026, 10, 30, 17, tzinfo=timezone.utc))
    after = json.loads(ledger(tmp_path).read_text())
    assert result["resolved"] == [before["forecast_id"]]
    assert after["outcome"] == expected
    assert after["probability"] == before["probability"]
    assert after["resolution_artifact"] == "runs/2026-10-30/indicator_watch.json"
    assert after["resolution_evidence"]["series_variant"] == "headline_annual"
    assert all(after[key] == value for key, value in before.items() if key != "status")
    # Later revisions can neither rescore nor replace the first official value.
    first_release(tmp_path, value=8.0, run_date="2026-11-02", first_release=False)
    assert not resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of="2026-11-02", now=datetime(2026, 11, 2, 17, tzinfo=timezone.utc))["resolved"]
    assert json.loads(ledger(tmp_path).read_text())["resolution_evidence"]["value"] == value


@pytest.mark.parametrize("override", [{"period": "2026-08"}, {"variant": "core_annual"}, {"published": "2026-10-29"}, {"first_release": False}, {"is_revision": True}, {"run_date": "2026-11-01"}])
def test_resolver_missing_or_mismatched_record_stays_open(tmp_path, override):
    run = setup_run(tmp_path)
    prepare(run)
    append(run)
    first_release(tmp_path, **override)
    result = resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of="2026-11-02", now=datetime(2026, 11, 2, 17, tzinfo=timezone.utc))
    assert result["waiting_for_official_records"]
    assert not result["resolved"]
    assert json.loads(ledger(tmp_path).read_text())["status"] == "open"


def test_review_flag_does_not_void_or_score_and_explicit_void_needs_approval(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    append(run)
    original = json.loads(ledger(tmp_path).read_text())
    assert not flag_shadow_forecasts_for_review(ledger(tmp_path), as_of="2026-11-22", now=datetime(2026, 11, 22, 17, tzinfo=timezone.utc))
    assert flag_shadow_forecasts_for_review(ledger(tmp_path), as_of="2026-11-23", now=datetime(2026, 11, 23, 17, tzinfo=timezone.utc)) == [original["forecast_id"]]
    flagged = json.loads(ledger(tmp_path).read_text())
    assert flagged["needs_review"]
    assert flagged["needs_review_at"] == "2026-11-23T17:00:00+00:00"
    assert flagged["status"] == "open"
    assert "outcome" not in flagged
    with pytest.raises(ShadowForecastError, match="approval"):
        void_shadow_forecast(ledger(tmp_path), forecast_id=original["forecast_id"], reason="Series withdrawn.", voided_at="2026-11-23T17:00:00Z")
    with pytest.raises(ShadowForecastError, match="21 days"):
        void_shadow_forecast(ledger(tmp_path), forecast_id=original["forecast_id"], reason="Series withdrawn.", voided_at="2026-11-22T17:00:00Z", human_approved=True)
    voided = void_shadow_forecast(ledger(tmp_path), forecast_id=original["forecast_id"], reason="Juan approved: official series withdrawn.", voided_at="2026-11-23T17:00:00Z", human_approved=True)
    assert not validate_shadow_forecast(voided)
    assert voided["probability"] == original["probability"]
    assert "brier_score" not in voided
    score = build_scoreboard(tmp_path, as_of="2026-11-23")
    assert len(score["internal"]["void"]) == 1
    assert score["internal"]["n_scored"] == 0


def test_scoreboard_excludes_a_review_flag_recorded_after_its_historical_cutoff(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    append(run)
    flag_shadow_forecasts_for_review(ledger(tmp_path), as_of="2026-11-25", now=datetime(2026, 11, 25, 17, tzinfo=timezone.utc))
    score = build_scoreboard(tmp_path, as_of="2026-11-24")
    assert not score["internal"]["needs_review"]
    with pytest.raises(ShadowForecastError, match="future"):
        flag_shadow_forecasts_for_review(ledger(tmp_path), as_of="2026-11-26", now=datetime(2026, 11, 25, 17, tzinfo=timezone.utc))


def test_banrep_requires_its_exact_board_communique_and_checks_rate_card(tmp_path):
    run = setup_run(tmp_path, cards=[card("policy_rate_ibr", period="2026-10-27", key="policy_rate_pct", value=12.25)], events=[calendar("banrep")])
    prepare(run)
    append(run)
    outcome = tmp_path / "runs" / EVENT
    outcome.mkdir()
    raw = {"id": "decision", "source_id": "banrep_junta_comunicados", "title": "La Junta Directiva aumentó la tasa de interés de política monetaria a 12,50%", "url": "https://banrep.gov.co/es/noticias/junta-directiva/junta-directiva-octubre-2026", "published_at": EVENT + "T13:50:00-05:00"}
    write(outcome / "raw_items.json", [raw])
    disagreeing = card("policy_rate_ibr", period=EVENT, key="policy_rate_pct", value=12.25)
    write(outcome / "indicator_watch.json", [disagreeing])
    result = resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of=EVENT, now=datetime(2026, 10, 30, 20, tzinfo=timezone.utc))
    assert not result["resolved"]
    disagreeing["values"]["policy_rate_pct"] = 12.5
    write(outcome / "indicator_watch.json", [disagreeing])
    assert resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of=EVENT, now=datetime(2026, 10, 30, 20, tzinfo=timezone.utc))["resolved"]


def test_scoreboard_separates_lanes_pairs_baselines_and_excludes_hindsight(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    append(run)
    first_release(tmp_path, value=6.1)
    resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of=EVENT, now=datetime(2026, 10, 30, 17, tzinfo=timezone.utc))
    public = {"forecast_id": "public", "run_date": "2026-10-20", "created_at": "2026-10-20T12:00:00Z", "probability": 0.5, "status": "resolved", "outcome": "NO", "resolved_at": "2026-10-23T12:00:00Z"}
    future = {**public, "forecast_id": "future", "run_date": "2026-11-02", "created_at": "2026-11-02T12:00:00Z"}
    (tmp_path / "forecasts/forecast_log.jsonl").write_text(json.dumps(public) + "\n" + json.dumps(future) + "\n")
    history = build_scoreboard(tmp_path, as_of="2026-10-27")
    assert history["internal"]["n_scored"] == 0
    assert len(history["internal"]["open"]) == 1
    assert "outcome" not in history["internal"]["open"][0]
    assert len(history["public"]["resolved"]) == 1
    assert history["public"]["directional_n"] == 0
    assert history["public"]["right_side_rate"] is None
    assert history["public"]["baseline_n"] == 0
    assert history["public"]["mean_brier"] == 0.25
    current = write_scoreboard(tmp_path, as_of=EVENT)
    assert not validate_artifact(current)
    assert current["internal"]["n_scored"] == current["internal"]["baseline_n"] == 1
    assert current["internal"]["mean_brier"] == 0.16
    assert current["internal"]["baseline_brier"] == 0.25
    assert current["internal"]["improvement"] == 0.09
    assert current["internal"]["family"]["n_scored"] == 1
    assert current["internal"]["discretionary"]["n_scored"] == 0
    assert "50/50" in render_scoreboard(current)
    assert (tmp_path / "runs" / EVENT / "scoreboard.json").exists()


def test_missed_events_are_counted_but_disabled_events_are_explained(tmp_path):
    run = setup_run(tmp_path, events=[calendar(), calendar("labor")])
    planned = plan_family_forecasts(run)
    write(run / "family_forecast_slots.json", planned)
    score = build_scoreboard(tmp_path, as_of=EVENT)
    inflation = next(row for row in score["families"] if row["family_id"] == "ipc_annual_up")
    labor = next(row for row in score["families"] if row["family_id"] == "unemployment_down_yoy")
    assert inflation["missed_events"] == 1
    assert labor["missed_events"] == 0
    assert labor["skips"] == labor["disabled_events"] == 1


def test_missing_calendars_never_imply_complete_coverage(tmp_path):
    run = setup_run(tmp_path, events=[])
    plan = plan_family_forecasts(run)
    assert not plan["slots"]
    assert "does not establish" in plan["notices"][0]
    score = build_scoreboard(tmp_path, as_of=run.name)
    assert score["coverage"]["complete_calendar_coverage"] is False
    assert "unavailable calendars remain unknown" in render_scoreboard(score)


def test_resolver_rejects_future_evidence_timestamps_and_pre_event_clock(tmp_path):
    run = setup_run(tmp_path)
    prepare(run)
    append(run)
    first_release(tmp_path, value=6.1)
    before = ledger(tmp_path).read_bytes()
    assert not resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of=EVENT, now=datetime(2026, 10, 30, 14, tzinfo=timezone.utc))["resolved"]
    assert ledger(tmp_path).read_bytes() == before
    observation = json.loads((tmp_path / "runs" / EVENT / "indicator_watch.json").read_text())
    observation[0]["release_date"] = EVENT + "T23:00:00Z"
    write(tmp_path / "runs" / EVENT / "indicator_watch.json", observation)
    assert not resolve_family_forecasts(tmp_path / "runs", ledger(tmp_path), as_of=EVENT, now=datetime(2026, 10, 30, 17, tzinfo=timezone.utc))["resolved"]
    assert ledger(tmp_path).read_bytes() == before


def load_cli(name):
    path = Path(__file__).parents[1] / "scripts" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_planner_and_scoreboard_clis_accept_explicit_roots(tmp_path):
    run = setup_run(tmp_path)
    assert load_cli("plan_family_forecasts").main(["--date", run.name, "--runs-dir", str(run.parent)]) == 0
    assert load_cli("build_scoreboard").main(["--date", run.name, "--repo-root", str(tmp_path)]) == 0
    assert json.loads((tmp_path / "forecasts/scoreboard.json").read_text())["as_of"] == run.name
