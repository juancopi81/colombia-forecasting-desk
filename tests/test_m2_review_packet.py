from __future__ import annotations

from colombia_forecasting_desk.cooccurrence_bundles import (
    attach_cooccurrence_bundles,
)
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


def _indicator_candidate(
    indicator_id: str = "ise_activity",
    question: str = "Will the next DANE ISE release show growth above 3%?",
) -> dict:
    return {
        "candidate_id": f"cand_{indicator_id}",
        "candidate_type": "indicator_seed",
        "origin_id": indicator_id,
        "question_seed": question,
        "decision_hint": "monitor",
        "m1_scores": {"forecastability_score": 0.6},
        "reasons": ["Observed current official indicator."],
        "noise_reasons": [],
        "missing_evidence": ["Sector contribution detail."],
        "source_ids": ["dane_ise"],
        "evidence": {"item_ids": [], "links": []},
    }


def _indicator_observation(
    indicator_id: str = "ise_activity",
    name: str = "DANE ISE",
    headline: str = "ISE grew 4.0% year over year.",
) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id=indicator_id,
        name=name,
        category="activity",
        status="observed",
        frequency="monthly",
        source_name="DANE",
        source_url=f"https://example.com/{indicator_id}",
        period="2026-03",
        release_date="2026-05-15",
        headline=headline,
        values={"annual_growth_pct": 4.0},
        freshness_status="current",
        why_it_matters="Activity acceleration can shift fiscal and rates context.",
        correlations=["activity + tax collection can reveal fiscal pressure"],
        next_step="Check next release.",
    )


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
    indicator = _indicator_observation()
    candidate = _indicator_candidate()

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


def test_m2_review_packet_surfaces_indicator_tension_cards() -> None:
    tension_card = {
        "schema_version": "indicator_tension_cards.v1",
        "card_id": "tes_policy_spread",
        "family": "sovereign_funding",
        "title": "TES-policy spread tension",
        "severity": "review",
        "trigger": "TES 5y minus policy rate is +3.23 pp.",
        "why_it_matters": "Government funding rates are elevated.",
        "agent_prompt": "Check whether this is fiscal risk or term premium.",
        "evidence": [
            {
                "label": "BanRep policy rate",
                "value": "11.25%",
                "source": "Banco de la República",
            }
        ],
        "caveats": ["This is not proof of fiscal stress."],
        "suggested_questions": [
            "Will the next COP TES auction clear above 14.0%?"
        ],
    }

    packet = build_m2_review_packet(
        _summary(),
        [],
        [],
        {"candidates": []},
        {"ranked_questions": [], "heuristic_audit": {}},
        [],
        [],
        [],
        [tension_card],
    )

    assert packet["summary"]["indicator_tension_card_count"] == 1
    assert packet["indicator_tension_cards"] == [tension_card]

    rendered = render_m2_review_packet(packet)
    assert "## Indicator Tension Cards" in rendered
    assert "TES-policy spread tension" in rendered
    assert "Family: `sovereign_funding`" in rendered
    assert "not conclusions or probability inputs" in rendered


def test_m2_review_packet_surfaces_cooccurrence_bundles() -> None:
    bundle = {
        "schema_version": "cooccurrence_bundles.v1",
        "bundle_id": "fiscal_sovereign_funding",
        "title": "Fiscal / sovereign funding bundle",
        "disposition": "review_context_only",
        "description": "Fiscal and TES inputs co-occurred.",
        "input_count": 2,
        "inputs": [
            {
                "kind": "tension_card",
                "input_id": "real_tax_revenue_squeeze",
                "title": "Real tax revenue squeeze",
                "summary": "Tax revenue growth below IPC.",
            }
        ],
        "review_questions": ["Are these inputs related or independent?"],
        "guardrails": ["Co-occurrence is not causality."],
    }
    packet = attach_cooccurrence_bundles(
        {
            "summary": {"review_item_count": 0},
            "inputs": {},
            "review_items": [],
        },
        [bundle],
    )

    rendered = render_m2_review_packet(packet)

    assert packet["summary"]["cooccurrence_bundle_count"] == 1
    assert "## Co-Occurrence Bundles" in rendered
    assert "Fiscal / sovereign funding bundle" in rendered
    assert "not conclusions, thesis labels" in rendered


def test_m2_review_packet_reserves_space_for_indicator_seeds() -> None:
    ranked_questions = []
    legislative_records = []
    for index in range(30):
        canonical_id = f"bill:2026:camara:{index}"
        ranked_questions.append(
            {
                **_ranked_question(),
                "rank_id": f"m2q_bill_{index}",
                "canonical_bill_id": canonical_id,
                "question_seed": f"Will Proyecto de Ley {index} advance?",
                "bucket": "watchlist",
                "overall_score": 0.6,
                "heuristic_risk_flags": ["possible_false_negative"],
            }
        )
        legislative_records.append(
            {
                **_legislative_record(),
                "canonical_bill_id": canonical_id,
                "display_title": f"Proyecto de Ley {index}",
                "source_evidence": [],
            }
        )

    packet = build_m2_review_packet(
        _summary(),
        [],
        [],
        {"candidates": [_indicator_candidate()]},
        {
            "ranked_questions": ranked_questions,
            "heuristic_audit": {"possible_false_negatives": ranked_questions},
        },
        legislative_records,
        [],
        [_indicator_observation()],
    )

    item_types = [item["item_type"] for item in packet["review_items"]]
    assert "indicator_seed" in item_types
    assert item_types.count("legislative_ranked_record") <= 10
    assert packet["summary"]["item_type_counts"]["indicator_seed"] == 1
    rendered = render_m2_review_packet(packet)
    assert "Composition:" in rendered
    assert "`indicator_seed`: 1" in rendered
    assert "`indicator_watch.json`: `indicator_id=ise_activity`" in rendered


def test_m2_review_packet_adds_advisory_cross_impact_hypothesis() -> None:
    ranked = {
        **_ranked_question(),
        "rank_id": "m2q_budget",
        "canonical_bill_id": "bill:2026:camara:550",
        "display_title": "Proyecto de Ley 550 de 2026 Cámara - adiciona el Presupuesto General de la Nación",
        "question_seed": "Could the budget-addition bill advance?",
        "bucket": "watchlist",
        "public_interest_signals": ["public_finance:fiscal/presupuesto"],
    }
    record = {
        **_legislative_record(),
        "canonical_bill_id": "bill:2026:camara:550",
        "display_title": ranked["display_title"],
        "title_normalized": "adiciona presupuesto general nacion 2026",
        "source_evidence": [],
    }
    indicator = _indicator_observation(
        indicator_id="fiscal_tax_pulse",
        name="Fiscal / tax pulse",
        headline="TES cutoff rates remain elevated.",
    )

    packet = build_m2_review_packet(
        _summary(),
        [],
        [],
        {"candidates": []},
        {"ranked_questions": [ranked], "heuristic_audit": {}},
        [record],
        [],
        [indicator],
    )

    cross_items = [
        item
        for item in packet["review_items"]
        if item["item_type"] == "cross_impact_hypothesis"
    ]
    assert len(cross_items) == 1
    cross = cross_items[0]
    assert cross["recommendation"] == "review_hypothesis"
    assert cross["heuristic_risk_flags"] == ["advisory_cross_impact"]
    assert "not_causal_evidence" in cross["structured_context"]["hypothesis"][
        "review_policy"
    ]
    assert any(
        excerpt["content_kind"] == "structured_indicator"
        for excerpt in cross["source_excerpts"]
    )
