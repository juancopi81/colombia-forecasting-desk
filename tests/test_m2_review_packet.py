from __future__ import annotations

from colombia_forecasting_desk.m2_review_packet import (
    build_m2_review_packet,
    render_m2_review_packet,
)
from colombia_forecasting_desk.models import (
    CleanedItem,
    IndicatorObservation,
    RawItem,
    RunSummary,
    SourceHealth,
)


def _summary() -> RunSummary:
    return RunSummary(
        run_date="2026-05-18",
        started_at="2026-05-18T12:00:00Z",
        finished_at="2026-05-18T12:00:30Z",
        sources_checked=14,
        sources_failed=0,
        raw_items=2,
        cleaned_items=2,
        clusters=1,
    )


def _raw_item() -> RawItem:
    return RawItem(
        id="gaceta-485-pl-560",
        source_id="gacetas_congreso",
        source_name="Gacetas del Congreso",
        source_type="official_updates",
        url="https://example.com/gaceta-485",
        title="Ponencia Proyecto de Ley 560 de 2025 Cámara",
        fetched_at="2026-05-18T12:00:00Z",
        published_at="2026-05-18T00:00:00Z",
        raw_text=(
            "Proyecto de Ley 560 de 2025 Cámara. Subsidio al transporte de GLP "
            "para San Andrés, Providencia y Santa Catalina. Se publica ponencia "
            "para debate y se indican autores, objeto y trámite legislativo."
        ),
        metadata={"content_extraction": "gaceta_pdf_text"},
    )


def _cleaned_item() -> CleanedItem:
    return CleanedItem(
        id="gaceta-485-pl-560",
        source_id="gacetas_congreso",
        source_name="Gacetas del Congreso",
        source_type="official_updates",
        url="https://example.com/gaceta-485",
        title="Ponencia Proyecto de Ley 560 de 2025 Cámara",
        fetched_at="2026-05-18T12:00:00Z",
        published_at="2026-05-18T00:00:00Z",
        clean_text=(
            "Proyecto de Ley 560 de 2025 Cámara. Subsidio al transporte de GLP "
            "para San Andrés, Providencia y Santa Catalina. Se publica ponencia "
            "para debate y se indican autores, objeto y trámite legislativo."
        ),
        summary="Ponencia publicada para el Proyecto de Ley 560 de 2025 Cámara.",
        signal_type="official_update",
        country_relevance="high",
        quality_notes="",
        trust_role="official_signal",
        priority="high",
        metadata={"content_extraction": "gaceta_pdf_text"},
    )


def _legislative_record() -> dict:
    return {
        "schema_version": "legislative_reconciler.v1",
        "canonical_bill_id": "bill:2025:camara:560",
        "display_title": "Proyecto de Ley 560 de 2025 Cámara - subsidio GLP",
        "source_evidence": [
            {
                "source_id": "gacetas_congreso",
                "role": "movement",
                "date": "2026-05-18T00:00:00Z",
                "url": "https://example.com/gaceta-485",
                "summary": "Parsed Gaceta item with ponencia evidence.",
            }
        ],
        "decision_state": "unresolved",
        "m2_readiness": {"state": "ready", "reason": "ready", "missing": []},
    }


def _ranked_question() -> dict:
    return {
        "rank_id": "m2q_bill_560",
        "canonical_bill_id": "bill:2025:camara:560",
        "question_seed": "Will Proyecto de Ley 560 de 2025 Cámara advance?",
        "recommendation": "select_for_evidence_pack",
        "bucket": "ready_for_m3",
        "overall_score": 0.78,
        "score_reasons": ["Clean project identity and official movement."],
        "penalties": [],
        "heuristic_risk_flags": ["review_public_interest_angle"],
        "llm_review_hint": "Check if island fuel subsidy is public-interest enough.",
        "missing_evidence": ["Current Senado registry status."],
        "source_ids": ["gacetas_congreso"],
    }


def test_m2_review_packet_packages_source_excerpts_for_llm_review() -> None:
    duplicate_candidate = {
        "candidate_id": "m1c_legislative_560",
        "candidate_type": "legislative_bill",
        "origin_id": "bill:2025:camara:560",
        "question_seed": "Will Proyecto de Ley 560 de 2025 Cámara advance?",
        "decision_hint": "candidate",
        "m1_scores": {"forecastability_score": 0.8},
        "reasons": ["Clean legislative identity."],
        "noise_reasons": [],
        "missing_evidence": ["Current Senado registry status."],
        "source_ids": ["gacetas_congreso"],
        "evidence": {"item_ids": ["gaceta-485-pl-560"], "links": []},
    }
    packet = build_m2_review_packet(
        _summary(),
        [_raw_item()],
        [_cleaned_item()],
        {"candidates": [duplicate_candidate]},
        {
            "ranked_questions": [_ranked_question()],
            "heuristic_audit": {
                "possible_false_negatives": [],
                "possible_false_positives": [_ranked_question()],
            },
        },
        [_legislative_record()],
        [
            SourceHealth(
                source_id="gacetas_congreso",
                source_name="Gacetas del Congreso",
                url="https://example.com/gacetas",
                raw_count=1,
                cleaned_count=1,
                dated_count=1,
                rankable_count=1,
                failure_count=0,
            )
        ],
        [],
    )

    assert packet["schema_version"] == "m2_review_packet.v1"
    assert packet["summary"]["review_item_count"] == 1
    assert packet["summary"]["items_with_source_excerpts"] == 1
    assert packet["summary"]["heuristic_challenge_count"] == 1

    item = packet["review_items"][0]
    assert item["item_type"] == "legislative_ranked_record"
    assert item["heuristic_risk_flags"] == ["review_public_interest_angle"]
    assert item["source_excerpts"][0]["content_kind"] == "parsed_content"
    assert "Subsidio al transporte de GLP" in item["source_excerpts"][0]["excerpt"]

    rendered = render_m2_review_packet(packet)
    assert "content-first" in rendered
    assert "Source excerpts" in rendered
    assert "Will Proyecto de Ley 560" in rendered


def test_m2_review_packet_adds_structured_indicator_context() -> None:
    indicator = IndicatorObservation(
        indicator_id="ise_activity",
        name="DANE ISE",
        category="activity",
        status="observed",
        frequency="monthly",
        source_name="DANE",
        source_url="https://example.com/ise",
        period="2026-03",
        release_date="2026-05-15",
        headline="ISE grew 4.0% year over year.",
        values={"annual_growth_pct": 4.0},
        freshness_status="current",
        why_it_matters="Activity acceleration can shift fiscal and rates context.",
        next_step="Check next ISE release.",
    )
    candidate = {
        "candidate_id": "cand_ise_activity",
        "candidate_type": "indicator_seed",
        "origin_id": "ise_activity",
        "question_seed": "Will the next DANE ISE release show growth above 3%?",
        "decision_hint": "monitor",
        "m1_scores": {"forecastability_score": 0.6},
        "reasons": ["Observed current official indicator."],
        "noise_reasons": [],
        "missing_evidence": ["Sector contribution detail."],
        "source_ids": ["dane_ise"],
        "evidence": {"item_ids": [], "links": []},
    }

    packet = build_m2_review_packet(
        _summary(),
        [],
        [],
        {"candidates": [candidate]},
        {"ranked_questions": [], "heuristic_audit": {}},
        [],
        [],
        [indicator],
    )

    item = packet["review_items"][0]
    assert item["item_type"] == "indicator_seed"
    assert item["source_excerpts"][0]["content_kind"] == "structured_indicator"
    assert "ISE grew 4.0%" in item["source_excerpts"][0]["excerpt"]
