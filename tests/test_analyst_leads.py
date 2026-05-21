from __future__ import annotations

from colombia_forecasting_desk.analyst_leads import (
    OUTPUT_CONTRACT,
    build_analyst_leads,
    render_analyst_leads,
)
from colombia_forecasting_desk.models import RunSummary


def _summary() -> RunSummary:
    return RunSummary(
        run_date="2026-05-20",
        started_at="2026-05-20T12:00:00Z",
        finished_at="2026-05-20T12:00:30Z",
        sources_checked=14,
        sources_failed=0,
        raw_items=10,
        cleaned_items=8,
        clusters=3,
    )


def _excerpt() -> dict:
    return {
        "item_id": "gaceta-485-pl-560",
        "source_name": "Gacetas del Congreso",
        "url": "https://example.com/gaceta-485",
        "title": "Ponencia Proyecto de Ley 560 de 2025 Cámara",
        "content_kind": "parsed_content",
        "excerpt": "Official parsed text says the bill has a ponencia for debate.",
    }


def _required_fields_present(lead: dict) -> bool:
    required = OUTPUT_CONTRACT[lead["lead_type"]]["required_fields"]
    return all(field in lead for field in required)


def test_analyst_leads_promotes_only_ready_evidenced_items_to_forecast_questions() -> None:
    packet = {
        "review_items": [
            {
                "packet_item_id": "ready-bill",
                "item_type": "legislative_ranked_record",
                "origin_id": "bill:2025:camara:560",
                "question_seed": "Will Proyecto de Ley 560 advance before June 30?",
                "recommendation": "select_for_evidence_pack",
                "bucket": "ready_for_m3",
                "missing_evidence": ["final deadline check"],
                "heuristic_penalties": [],
                "heuristic_risk_flags": [],
                "llm_review_hint": "Build the M3 Case File and verify deadline.",
                "source_excerpts": [_excerpt()],
                "traceability": {
                    "artifact_refs": [
                        {
                            "artifact": "m2_ranked_questions.json",
                            "key": "rank_id",
                            "value": "ready-bill",
                        }
                    ],
                    "source_item_ids": ["gaceta-485-pl-560"],
                    "source_urls": ["https://example.com/gaceta-485"],
                },
            },
            {
                "packet_item_id": "cross-impact",
                "item_type": "cross_impact_hypothesis",
                "origin_id": "cross:bill:2026:camara:550:fiscal_tax_pulse",
                "question_seed": "Should the budget bill be reviewed alongside TES pressure?",
                "recommendation": "review_hypothesis",
                "bucket": "cross_domain_hypothesis",
                "missing_evidence": ["causal mechanism", "timing alignment"],
                "heuristic_penalties": ["This is not causal evidence."],
                "heuristic_risk_flags": ["advisory_cross_impact"],
                "llm_review_hint": "Research timing before M3.",
                "source_excerpts": [_excerpt()],
                "traceability": {
                    "artifact_refs": [
                        {
                            "artifact": "m2_review_packet.json",
                            "key": "packet_item_id",
                            "value": "cross-impact",
                        }
                    ],
                    "source_item_ids": ["gaceta-485-pl-560"],
                    "source_urls": ["https://example.com/gaceta-485"],
                },
            },
            {
                "packet_item_id": "generic-watchlist",
                "item_type": "legislative_ranked_record",
                "origin_id": "bill:2026:senado:360",
                "question_seed": "Could a symbolic commemorative bill advance?",
                "recommendation": "monitor",
                "bucket": "watchlist",
                "missing_evidence": ["human public-interest framing"],
                "heuristic_penalties": [],
                "heuristic_risk_flags": [
                    "possible_false_negative_structurally_strong"
                ],
                "llm_review_hint": "Check if this has a real public-interest hook.",
                "source_excerpts": [_excerpt()],
                "traceability": {},
            },
        ]
    }

    payload = build_analyst_leads(_summary(), packet, [])
    by_type = {
        lead_type: [
            lead for lead in payload["leads"] if lead["lead_type"] == lead_type
        ]
        for lead_type in OUTPUT_CONTRACT
    }

    assert payload["schema_version"] == "analyst_leads.v1"
    assert len(by_type["forecast_question"]) == 1
    assert len(by_type["investigation_lead"]) == 1
    assert by_type["forecast_question"][0]["claim_or_question"].startswith("Will")
    assert (
        by_type["investigation_lead"][0]["disposition"]
        == "research_more_before_m3"
    )
    assert "causal mechanism" in by_type["investigation_lead"][0]["caveats"]
    assert all(_required_fields_present(lead) for lead in payload["leads"])

    rendered = render_analyst_leads(payload)
    assert "## Forecast Questions" in rendered
    assert "## Analyst Insights" in rendered
    assert "## Investigation Leads" in rendered
    assert "not forecast-log entries" in rendered
    assert "Should the budget bill" in rendered
    assert "symbolic commemorative" not in rendered


def test_tension_cards_become_analyst_insights_not_forecasts() -> None:
    tension_card = {
        "schema_version": "indicator_tension_cards.v1",
        "card_id": "real_tax_revenue_squeeze",
        "family": "fiscal_capacity",
        "title": "Real tax revenue squeeze",
        "severity": "review",
        "trigger": "Nominal gross tax-revenue growth minus annual IPC is -4.24 pp.",
        "why_it_matters": "Tax collection below inflation can flag fiscal pressure.",
        "agent_prompt": "Check whether this connects to budget or debt questions.",
        "evidence": [
            {
                "label": "DIAN nominal gross tax revenue growth",
                "value": "1.44%",
                "source": "DIAN",
                "period": "2026-03",
                "url": "https://example.com/dian",
            }
        ],
        "caveats": ["Monthly tax collection can be affected by calendars."],
        "suggested_questions": [
            "Will the next DIAN release again trail annual IPC?"
        ],
        "review_policy": "Advisory screen only.",
        "source_refs": [],
    }

    payload = build_analyst_leads(_summary(), {"review_items": []}, [tension_card])

    assert payload["summary"]["forecast_question_count"] == 0
    assert payload["summary"]["analyst_insight_count"] == 1
    lead = payload["leads"][0]
    assert lead["lead_type"] == "analyst_insight"
    assert lead["claim_or_question"].startswith("Nominal gross tax-revenue")
    assert lead["review_context"]["family"] == "fiscal_capacity"
    assert _required_fields_present(lead)

    rendered = render_analyst_leads(payload)
    assert "## Forecast Questions\n\nNo leads in this class." in rendered
    assert "Real tax revenue squeeze" in rendered
