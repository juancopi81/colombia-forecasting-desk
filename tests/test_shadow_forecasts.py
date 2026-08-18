from __future__ import annotations

import json
from pathlib import Path

import pytest

from colombia_forecasting_desk.shadow_forecasts import (
    ShadowForecastError,
    append_shadow_forecast,
    resolve_shadow_forecast,
    shadow_forecast_from_analysis,
    validate_shadow_forecast,
    validate_shadow_forecast_ledger,
)


def _valid_forecast(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "schema_version": "shadow_forecast.v1",
        "forecast_id": "shadow_20260811_trm_five_day_direction",
        "created_at": "2026-08-11T16:00:00Z",
        "run_date": "2026-08-11",
        "question": "Will the official TRM be above 3,125 COP/USD on 2026-08-18?",
        "probability": 0.55,
        "status": "open",
        "resolution_deadline": "2026-08-18",
        "resolution_check_window_end": "2026-08-20",
        "resolution_source": "Superintendencia Financiera official TRM series",
        "resolution_criteria": "YES if the official TRM for 2026-08-18 is above 3,125 COP/USD; otherwise NO.",
        "baseline": {"label": "no_change_50_50", "probability": 0.5},
        "evidence_refs": [
            {
                "artifact": "market_pricing_watch.json",
                "locator": "series_id=trm",
                "note": "Official TRM context.",
            }
        ],
        "rationale_for": ["The peso has appreciated rapidly over seven days."],
        "rationale_against": ["Short-horizon exchange rates are noisy."],
        "falsifier": "A reversal in the broad dollar or a Colombia-specific risk shock.",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "visibility": "internal",
        "source_analysis": "runs/2026-08-11/agent_analysis.json",
    }
    row.update(overrides)
    return row


def test_valid_open_shadow_forecast_passes_contract() -> None:
    assert validate_shadow_forecast(_valid_forecast()) == []


@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "forecast_id",
        "created_at",
        "run_date",
        "question",
        "probability",
        "status",
        "resolution_deadline",
        "resolution_check_window_end",
        "resolution_source",
        "resolution_criteria",
        "baseline",
        "evidence_refs",
        "rationale_for",
        "rationale_against",
        "falsifier",
        "model",
        "reasoning_effort",
        "visibility",
        "source_analysis",
    ],
)
def test_contract_rejects_missing_required_fields(field: str) -> None:
    row = _valid_forecast()
    del row[field]

    issues = validate_shadow_forecast(row)

    assert any(issue.code == "missing_field" and field in issue.message for issue in issues)


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"schema_version": "shadow_forecast.v0"}, "invalid_schema_version"),
        ({"probability": -0.01}, "invalid_probability"),
        ({"probability": 1.01}, "invalid_probability"),
        ({"probability": True}, "invalid_probability"),
        ({"status": "pending"}, "invalid_status"),
        ({"visibility": "public"}, "invalid_visibility"),
        ({"baseline": {"label": "coin_flip", "probability": 1.1}}, "invalid_baseline"),
    ],
)
def test_contract_rejects_invalid_enums_and_probabilities(
    overrides: dict[str, object], code: str
) -> None:
    issues = validate_shadow_forecast(_valid_forecast(**overrides))

    assert any(issue.code == code for issue in issues)


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"created_at": "2026-08-11"}, "invalid_created_at"),
        ({"created_at": "2026-08-11T16:00:00"}, "invalid_created_at"),
        ({"run_date": "2026/08/11"}, "invalid_date"),
        ({"resolution_deadline": "next Tuesday"}, "invalid_date"),
        ({"resolution_check_window_end": "2026-02-30"}, "invalid_date"),
        ({"resolution_deadline": "2026-08-10"}, "deadline_before_run_date"),
        ({"resolution_check_window_end": "2026-08-17"}, "window_before_deadline"),
    ],
)
def test_contract_requires_ordered_iso_dates(
    overrides: dict[str, object], code: str
) -> None:
    issues = validate_shadow_forecast(_valid_forecast(**overrides))

    assert any(issue.code == code for issue in issues)


@pytest.mark.parametrize(
    "field",
    [
        "forecast_id",
        "question",
        "resolution_source",
        "resolution_criteria",
        "falsifier",
        "model",
        "reasoning_effort",
        "source_analysis",
    ],
)
def test_contract_rejects_blank_text(field: str) -> None:
    issues = validate_shadow_forecast(_valid_forecast(**{field: "   "}))

    assert any(issue.code == "empty_field" and field in issue.message for issue in issues)


@pytest.mark.parametrize("field", ["evidence_refs", "rationale_for", "rationale_against"])
def test_contract_requires_nonempty_string_lists(field: str) -> None:
    issues = validate_shadow_forecast(_valid_forecast(**{field: []}))

    assert any(issue.code == "empty_field" and field in issue.message for issue in issues)


def test_contract_rejects_evidence_ref_without_artifact_locator() -> None:
    issues = validate_shadow_forecast(
        _valid_forecast(evidence_refs=[{"artifact": "indicator_watch.json"}])
    )

    assert any(issue.code == "empty_field" for issue in issues)


def test_ledger_rejects_duplicate_forecast_ids(tmp_path: Path) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    first = _valid_forecast()
    second = _valid_forecast(question="A different question")
    ledger.write_text(
        "\n".join(json.dumps(row) for row in (first, second)) + "\n",
        encoding="utf-8",
    )

    issues = validate_shadow_forecast_ledger(ledger)

    assert any(
        issue.code == "duplicate_forecast_id" and issue.row_number == 2
        for issue in issues
    )


def test_ledger_reports_malformed_json_with_row_number(tmp_path: Path) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    ledger.write_text("{not-json}\n", encoding="utf-8")

    issues = validate_shadow_forecast_ledger(ledger)

    assert len(issues) == 1
    assert issues[0].code == "invalid_json"
    assert issues[0].row_number == 1


def test_append_creates_valid_separate_jsonl_ledger(tmp_path: Path) -> None:
    ledger = tmp_path / "forecasts" / "shadow_forecast_log.jsonl"
    row = _valid_forecast()

    append_shadow_forecast(ledger, row)

    assert json.loads(ledger.read_text(encoding="utf-8")) == row
    assert validate_shadow_forecast_ledger(ledger) == []
    assert list(ledger.parent.glob(".shadow_forecast_log.jsonl.*.tmp")) == []


def test_append_refuses_same_normalized_question_and_deadline(tmp_path: Path) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    first = _valid_forecast()
    append_shadow_forecast(ledger, first)
    original_bytes = ledger.read_bytes()
    semantic_duplicate = _valid_forecast(
        forecast_id="shadow_20260811_different_id",
        question="  WILL   THE OFFICIAL TRM BE ABOVE 3,125 COP/USD ON 2026-08-18?  ",
    )

    with pytest.raises(ShadowForecastError, match="Semantic duplicate"):
        append_shadow_forecast(ledger, semantic_duplicate)

    assert ledger.read_bytes() == original_bytes


def test_append_refuses_duplicate_id_and_preserves_ledger(tmp_path: Path) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    append_shadow_forecast(ledger, _valid_forecast())
    original_bytes = ledger.read_bytes()

    with pytest.raises(ShadowForecastError, match="Duplicate forecast_id"):
        append_shadow_forecast(
            ledger, _valid_forecast(question="A genuinely different question")
        )

    assert ledger.read_bytes() == original_bytes


def test_analysis_conversion_forces_open_internal_metadata() -> None:
    analysis = {
        "schema_version": "agent_analysis.v1",
        "run_date": "2026-08-11",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "strongest_changed_signal": {
            "signal": "The ex-post real policy rate is elevated.",
            "evidence_refs": [
                {
                    "artifact": "indicator_tension_cards.json",
                    "locator": "card_id=real_policy_rate",
                    "note": "Triggered card.",
                }
            ],
            "interpretation": "Policy is restrictive.",
            "alternative_explanations": ["Inflation expectations may remain high."],
            "falsifiers": ["Expectations rise materially."],
        },
        "tension_card_reviews": [
            {
                "card_id": "real_policy_rate",
                "assessment": "Useful lead.",
                "forecast_implication": "Inspect the next decision.",
                "disposition": "shadow_candidate",
            }
        ],
        "relationships": [
            {
                "type": "cross_bundle",
                "relationship": "Policy restraint and peso strength coincide.",
                "evidence_refs": [
                    {
                        "artifact": "cooccurrence_bundles.json",
                        "locator": "bundle_id=monetary_credit_transmission",
                        "note": "Cross-bundle context.",
                    }
                ],
                "interpretation": "FX strength may affect policy space.",
                "caveats": ["Global dollar weakness may dominate."],
            }
        ],
        "official_source_follow_up": {
            "candidate": "Next BanRep decision",
            "bounded_scope": "Checked the official calendar.",
            "sources_checked": [
                {
                    "source_id": "banrep_junta_calendar",
                    "url": "https://www.banrep.gov.co/es/calendario-junta-directiva",
                    "result": "Decision scheduled for 2026-09-30.",
                }
            ],
            "result": "A dated resolver exists.",
        },
        "public_interest_candidate": {
            "candidate": "Will BanRep change the policy rate?",
            "disposition": "research_more",
            "rationale": "Expectations evidence is incomplete.",
            "evidence_refs": [
                {
                    "artifact": "m3_preflight_opportunities.json",
                    "locator": "detector_id=banrep_policy_rate_decision",
                    "note": "Scheduled event.",
                }
            ],
            "missing_evidence": ["Expectations survey."],
        },
        "overall_disposition": "shadow_track",
        "shadow_forecast": {
            "id": "shadow_20260811_banrep_sep_rate_change",
            "visibility": "internal",
            "question": "Will BanRep change the 12% policy rate on 2026-09-30?",
            "probability": 0.62,
            "prediction_date": "2026-08-11",
            "resolution_date": "2026-09-30",
            "official_resolver": {
                "source_name": "Banco de la Republica",
                "source_url": "https://www.banrep.gov.co/es/estadisticas/tasas-interes-politica-monetaria",
            },
            "resolution_criteria": {
                "yes": "The official decision changes the 12% rate.",
                "no": "The official decision keeps the rate at 12%.",
            },
            "baseline": {
                "name": "Persistence / unchanged policy rate",
                "probability": 0.35,
                "rationale": "Most decisions leave the rate unchanged.",
            },
            "evidence": [
                {
                    "artifact": "indicator_tension_cards.json",
                    "locator": "card_id=real_policy_rate",
                    "note": "Triggered card.",
                }
            ],
            "rationale_for": ["The real policy rate is elevated."],
            "rationale_against": ["Inflation remains above target."],
            "falsifier": "Expectations rise materially before the meeting.",
        },
        "overall_rationale": "Track internally without publication.",
    }

    row = shadow_forecast_from_analysis(
        analysis,
        source_analysis="runs/2026-08-11/agent_analysis.json",
        created_at="2026-08-11T16:00:00Z",
    )

    assert row["forecast_id"] == "shadow_20260811_banrep_sep_rate_change"
    assert row["run_date"] == "2026-08-11"
    assert row["resolution_deadline"] == "2026-09-30"
    assert row["resolution_check_window_end"] == "2026-09-30"
    assert row["baseline"] == {
        "label": "Persistence / unchanged policy rate",
        "probability": 0.35,
        "rationale": "Most decisions leave the rate unchanged.",
    }
    assert row["evidence_refs"] == analysis["shadow_forecast"]["evidence"]
    assert row["status"] == "open"
    assert row["visibility"] == "internal"


def test_explicit_resolution_is_atomic_and_computes_brier_score(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    append_shadow_forecast(ledger, _valid_forecast())

    resolved = resolve_shadow_forecast(
        ledger,
        forecast_id="shadow_20260811_trm_five_day_direction",
        outcome="YES",
        resolved_at="2026-08-18T16:00:00-05:00",
        resolution_value="Official TRM was 3,130 COP/USD.",
        resolution_url="https://www.datos.gov.co/resource/32sa-8pi3.json",
    )

    stored = json.loads(ledger.read_text(encoding="utf-8"))
    assert stored == resolved
    assert resolved["status"] == "resolved"
    assert resolved["outcome"] == "YES"
    assert resolved["brier_score"] == 0.2025
    assert resolved["baseline_brier_score"] == 0.25
    assert resolved["brier_improvement_vs_baseline"] == 0.0475
    assert validate_shadow_forecast_ledger(ledger) == []
    assert list(tmp_path.glob(".shadow_forecast_log.jsonl.*.tmp")) == []


def test_resolved_score_comparison_must_match_probability_and_outcome() -> None:
    row = _valid_forecast(
        status="resolved",
        outcome="YES",
        resolved_at="2026-08-18T16:00:00-05:00",
        resolution_value="Official TRM was 3,130 COP/USD.",
        resolution_url="https://www.datos.gov.co/resource/32sa-8pi3.json",
        brier_score=0.2025,
        baseline_brier_score=0.25,
        brier_improvement_vs_baseline=0.99,
    )

    issues = validate_shadow_forecast(row)

    assert any(issue.code == "invalid_brier_improvement" for issue in issues)


def test_legacy_resolved_row_without_comparison_fields_remains_valid() -> None:
    row = _valid_forecast(
        status="resolved",
        outcome="YES",
        resolved_at="2026-08-18T16:00:00-05:00",
        resolution_value="Official TRM was 3,130 COP/USD.",
        resolution_url="https://www.datos.gov.co/resource/32sa-8pi3.json",
        brier_score=0.2025,
    )

    assert validate_shadow_forecast(row) == []


def test_resolution_refuses_to_overwrite_resolved_row(tmp_path: Path) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    append_shadow_forecast(ledger, _valid_forecast())
    kwargs = {
        "forecast_id": "shadow_20260811_trm_five_day_direction",
        "outcome": "NO",
        "resolved_at": "2026-08-18T16:00:00-05:00",
        "resolution_value": "Official TRM was 3,100 COP/USD.",
        "resolution_url": "https://www.datos.gov.co/resource/32sa-8pi3.json",
    }
    resolve_shadow_forecast(ledger, **kwargs)
    resolved_bytes = ledger.read_bytes()

    with pytest.raises(ShadowForecastError, match="already resolved"):
        resolve_shadow_forecast(
            ledger,
            **{
                **kwargs,
                "outcome": "YES",
                "resolution_value": "A conflicting result.",
            },
        )

    assert ledger.read_bytes() == resolved_bytes


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"outcome": "MAYBE"}, "invalid_outcome"),
        ({"resolved_at": "2026-08-18"}, "invalid_resolved_at"),
        ({"resolution_value": ""}, "invalid_resolution_value"),
        ({"resolution_url": "http://example.com/result"}, "invalid_resolution_url"),
    ],
)
def test_resolution_requires_explicit_valid_official_evidence(
    tmp_path: Path, override: dict[str, str], message: str
) -> None:
    ledger = tmp_path / "shadow_forecast_log.jsonl"
    append_shadow_forecast(ledger, _valid_forecast())
    original_bytes = ledger.read_bytes()
    kwargs = {
        "forecast_id": "shadow_20260811_trm_five_day_direction",
        "outcome": "YES",
        "resolved_at": "2026-08-18T16:00:00-05:00",
        "resolution_value": "Official TRM was 3,130 COP/USD.",
        "resolution_url": "https://www.datos.gov.co/resource/32sa-8pi3.json",
        **override,
    }

    with pytest.raises(ShadowForecastError, match=message):
        resolve_shadow_forecast(ledger, **kwargs)

    assert ledger.read_bytes() == original_bytes
