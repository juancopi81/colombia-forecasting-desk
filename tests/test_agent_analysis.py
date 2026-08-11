from __future__ import annotations

import json
from pathlib import Path

from colombia_forecasting_desk.agent_analysis import (
    render_agent_analysis,
    validate_agent_analysis,
)


def _evidence_ref(artifact: str, locator: str) -> dict:
    return {
        "artifact": artifact,
        "locator": locator,
        "note": "Source-backed input used by the analysis.",
    }


def _valid_analysis(**overrides) -> dict:
    base = {
        "schema_version": "agent_analysis.v1",
        "run_date": "2026-08-11",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "strongest_changed_signal": {
            "signal": "July IPC slowed to 6.03% while the policy rate stayed at 12%.",
            "evidence_refs": [
                _evidence_ref(
                    "indicator_watch.json",
                    "indicator_id=ipc_inflation",
                )
            ],
            "interpretation": "The ex-post policy stance became more restrictive.",
            "alternative_explanations": [
                "Headline disinflation may not represent core inflation pressure."
            ],
            "falsifiers": [
                "A renewed rise in core inflation or expectations would weaken the cut thesis."
            ],
        },
        "tension_card_reviews": [
            {
                "card_id": "real_policy_rate",
                "assessment": "Restrictive stance is a useful rate-decision lead.",
                "forecast_implication": "Review the next scheduled BanRep decision.",
                "disposition": "shadow_candidate",
            }
        ],
        "relationships": [
            {
                "type": "cross_bundle",
                "relationship": "Policy restraint coincides with peso appreciation.",
                "evidence_refs": [
                    _evidence_ref(
                        "cooccurrence_bundles.json",
                        "bundle_id=monetary_credit_transmission",
                    )
                ],
                "interpretation": "FX strength may give BanRep more room at a later meeting.",
                "caveats": ["Global dollar weakness may explain part of the move."],
            }
        ],
        "official_source_follow_up": {
            "candidate": "Next BanRep policy-rate decision",
            "bounded_scope": "Checked the official meeting calendar and policy-rate page.",
            "sources_checked": [
                {
                    "source_id": "banrep_junta_calendar",
                    "url": "https://www.banrep.gov.co/es/calendario-junta-directiva",
                    "result": "The next rate decision is scheduled for 2026-09-30.",
                }
            ],
            "result": "A dated official resolver exists, but the public case is still early.",
        },
        "public_interest_candidate": {
            "candidate": "Will BanRep change the 12% policy rate on 2026-09-30?",
            "disposition": "research_more",
            "rationale": "The resolver is clean, but expectations evidence is incomplete.",
            "evidence_refs": [
                _evidence_ref(
                    "m3_preflight_opportunities.json",
                    "detector_id=banrep_policy_rate_decision",
                )
            ],
            "missing_evidence": ["BanRep analyst expectations survey."],
        },
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
                "yes": "The official decision sets the policy rate above or below 12.00%.",
                "no": "The official decision keeps the policy rate at 12.00%.",
            },
            "baseline": {
                "name": "Persistence / unchanged policy rate",
                "probability": 0.35,
                "rationale": "Most scheduled decisions do not change the rate.",
            },
            "evidence": [
                _evidence_ref(
                    "indicator_tension_cards.json",
                    "card_id=real_policy_rate",
                )
            ],
            "rationale_for": ["The ex-post real policy rate is elevated."],
            "rationale_against": ["Inflation remains above target."],
            "falsifier": "A material rise in inflation expectations before the meeting.",
        },
        "overall_disposition": "shadow_track",
        "overall_rationale": "Track a clean internal case without recommending publication.",
    }
    base.update(overrides)
    return base


def test_valid_shadow_track_analysis_is_accepted() -> None:
    assert validate_agent_analysis(_valid_analysis()) == []


def test_analysis_requires_the_contract_top_level_fields() -> None:
    analysis = _valid_analysis()
    analysis.pop("model")
    analysis.pop("official_source_follow_up")

    issues = validate_agent_analysis(analysis)

    messages = {issue.message for issue in issues if issue.code == "missing_field"}
    assert "`model` is required." in messages
    assert "`official_source_follow_up` is required." in messages


def test_analysis_rejects_invalid_metadata_and_disposition() -> None:
    analysis = _valid_analysis(
        run_date="11-08-2026",
        model="",
        reasoning_effort="",
        overall_disposition="publish_now",
        overall_rationale="",
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "invalid_run_date" in codes
    assert "missing_model" in codes
    assert "missing_reasoning_effort" in codes
    assert "invalid_overall_disposition" in codes
    assert "missing_overall_rationale" in codes


def test_strongest_signal_requires_evidence_interpretation_challenge_and_falsifier() -> None:
    analysis = _valid_analysis(
        strongest_changed_signal={
            "signal": "",
            "evidence_refs": [],
            "interpretation": "",
            "alternative_explanations": [],
            "falsifiers": [],
        }
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "signal_missing_summary" in codes
    assert "signal_missing_evidence_refs" in codes
    assert "signal_missing_interpretation" in codes
    assert "signal_missing_alternative_explanations" in codes
    assert "signal_missing_falsifiers" in codes


def test_run_validation_requires_exact_triggered_tension_card_coverage(
    tmp_path: Path,
) -> None:
    (tmp_path / "indicator_tension_cards.json").write_text(
        json.dumps(
            [
                {"card_id": "real_policy_rate"},
                {"card_id": "construction_cost_vs_ipc"},
            ]
        ),
        encoding="utf-8",
    )
    analysis = _valid_analysis(
        tension_card_reviews=[
            {
                "card_id": "real_policy_rate",
                "assessment": "Reviewed.",
                "forecast_implication": "Possible shadow case.",
                "disposition": "shadow_candidate",
            },
            {
                "card_id": "not_triggered_today",
                "assessment": "Reviewed.",
                "forecast_implication": "None.",
                "disposition": "no_action",
            },
        ]
    )

    issues = validate_agent_analysis(analysis, run_dir=tmp_path)
    messages = {issue.message for issue in issues}

    assert "Triggered tension card `construction_cost_vs_ipc` was not reviewed." in messages
    assert "Tension card review `not_triggered_today` was not triggered in this run." in messages


def test_each_tension_card_review_requires_an_accountable_assessment() -> None:
    analysis = _valid_analysis(
        tension_card_reviews=[
            {
                "card_id": "real_policy_rate",
                "assessment": "",
                "forecast_implication": "",
                "disposition": "conclusion",
            },
            {
                "card_id": "real_policy_rate",
                "assessment": "Duplicate review.",
                "forecast_implication": "None.",
                "disposition": "no_action",
            },
        ]
    )

    codes = [issue.code for issue in validate_agent_analysis(analysis)]

    assert "tension_review_missing_assessment" in codes
    assert "tension_review_missing_forecast_implication" in codes
    assert "invalid_tension_review_disposition" in codes
    assert "duplicate_tension_card_review" in codes


def test_analysis_requires_at_least_one_cross_bundle_or_unbundled_relationship() -> None:
    analysis = _valid_analysis(relationships=[])

    issues = validate_agent_analysis(analysis)

    assert any(issue.code == "missing_relationship" for issue in issues)


def test_relationship_requires_type_evidence_interpretation_and_caveats() -> None:
    analysis = _valid_analysis(
        relationships=[
            {
                "type": "hard_coded_story",
                "relationship": "",
                "evidence_refs": [],
                "interpretation": "",
                "caveats": [],
            }
        ]
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "invalid_relationship_type" in codes
    assert "relationship_missing_summary" in codes
    assert "relationship_missing_evidence_refs" in codes
    assert "relationship_missing_interpretation" in codes
    assert "relationship_missing_caveats" in codes


def test_official_source_follow_up_must_be_bounded_and_report_what_was_checked() -> None:
    analysis = _valid_analysis(
        official_source_follow_up={
            "candidate": "",
            "bounded_scope": "",
            "sources_checked": [],
            "result": "",
        }
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "follow_up_missing_candidate" in codes
    assert "follow_up_missing_bounded_scope" in codes
    assert "follow_up_missing_sources_checked" in codes
    assert "follow_up_missing_result" in codes


def test_each_checked_official_source_requires_identity_url_and_result() -> None:
    analysis = _valid_analysis()
    analysis["official_source_follow_up"]["sources_checked"] = [
        {"source_id": "", "url": "", "result": ""}
    ]

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "checked_source_missing_source_id" in codes
    assert "checked_source_missing_url" in codes
    assert "checked_source_missing_result" in codes


def test_public_interest_candidate_requires_a_supported_explicit_disposition() -> None:
    analysis = _valid_analysis(
        public_interest_candidate={
            "candidate": "",
            "disposition": "publish",
            "rationale": "",
            "evidence_refs": [],
            "missing_evidence": [],
        }
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "public_candidate_missing_candidate" in codes
    assert "invalid_public_candidate_disposition" in codes
    assert "public_candidate_missing_rationale" in codes
    assert "public_candidate_missing_evidence_refs" in codes


def test_shadow_forecast_presence_must_match_overall_disposition() -> None:
    missing_shadow = _valid_analysis(
        overall_disposition="shadow_track",
        shadow_forecast=None,
    )
    unexpected_shadow = _valid_analysis(overall_disposition="insight_only")

    missing_codes = {issue.code for issue in validate_agent_analysis(missing_shadow)}
    unexpected_codes = {
        issue.code for issue in validate_agent_analysis(unexpected_shadow)
    }

    assert "shadow_track_without_forecast" in missing_codes
    assert "shadow_forecast_for_non_shadow_disposition" in unexpected_codes


def test_shadow_track_requires_a_clean_resolvable_internal_forecast() -> None:
    analysis = _valid_analysis(
        shadow_forecast={
            "id": "",
            "visibility": "public",
            "question": "",
            "probability": 62,
            "prediction_date": "2026-10-01",
            "resolution_date": "2026-09-30",
            "official_resolver": {"source_name": "", "source_url": ""},
            "resolution_criteria": {"yes": "", "no": ""},
            "baseline": {"name": "", "probability": -0.1, "rationale": ""},
            "evidence": [],
            "rationale_for": [],
            "rationale_against": [],
            "falsifier": "",
        }
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "shadow_missing_id" in codes
    assert "shadow_not_internal" in codes
    assert "shadow_invalid_probability" in codes
    assert "shadow_resolution_before_prediction" in codes
    assert "shadow_incomplete_official_resolver" in codes
    assert "shadow_incomplete_resolution_criteria" in codes
    assert "shadow_invalid_baseline" in codes
    assert "shadow_missing_evidence" in codes
    assert "shadow_missing_rationale_for" in codes
    assert "shadow_missing_rationale_against" in codes
    assert "shadow_missing_falsifier" in codes


def test_research_more_public_candidate_names_what_is_missing() -> None:
    analysis = _valid_analysis()
    analysis["public_interest_candidate"]["missing_evidence"] = []

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "public_candidate_research_more_without_missing_evidence" in codes


def test_promote_to_m3_requires_the_public_candidate_to_be_promoted() -> None:
    analysis = _valid_analysis(
        overall_disposition="promote_to_m3",
        shadow_forecast=None,
    )

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "promote_to_m3_without_promoted_candidate" in codes


def test_evidence_references_must_point_to_an_artifact_and_locator() -> None:
    analysis = _valid_analysis()
    analysis["strongest_changed_signal"]["evidence_refs"] = [
        {"artifact": "", "locator": ""}
    ]

    issues = validate_agent_analysis(analysis)

    assert any(issue.code == "invalid_evidence_ref" for issue in issues)


def test_shadow_prediction_date_must_match_the_analysis_run_date() -> None:
    analysis = _valid_analysis()
    analysis["shadow_forecast"]["prediction_date"] = "2026-08-10"

    codes = {issue.code for issue in validate_agent_analysis(analysis)}

    assert "shadow_prediction_date_mismatch" in codes


def test_render_agent_analysis_is_deterministic_and_human_readable() -> None:
    analysis = _valid_analysis()

    first = render_agent_analysis(analysis)
    second = render_agent_analysis(analysis)

    assert first == second
    assert first.startswith("# Agent Analysis - 2026-08-11\n")
    assert "Overall disposition: `shadow_track`" in first
    assert "## Strongest Changed Signal" in first
    assert "## Tension Card Reviews" in first
    assert "### `real_policy_rate`" in first
    assert "## Bounded Official-Source Follow-Up" in first
    assert "## Shadow Forecast" in first
    assert "Probability: 62.0%" in first
    assert first.endswith("\n")
