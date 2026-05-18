from __future__ import annotations

import json

from colombia_forecasting_desk.m2_ranker import build_legislative_m2_ranking
from colombia_forecasting_desk.models import RunSummary


def _summary() -> RunSummary:
    return RunSummary(
        run_date="2026-05-18",
        started_at="2026-05-18T12:00:00Z",
        finished_at="2026-05-18T12:00:30Z",
        sources_checked=14,
        sources_failed=3,
        raw_items=443,
        cleaned_items=168,
        clusters=38,
    )


def _ready_record(**overrides) -> dict:
    record = {
        "schema_version": "legislative_reconciler.v1",
        "canonical_bill_id": "bill:2025:camara:560",
        "display_title": (
            "Proyecto de Ley 560 de 2025 Cámara - subsidio al transporte de "
            "GLP para San Andrés"
        ),
        "title_normalized": "subsidio transporte glp san andres",
        "origin_project": {"chamber": "camara", "number": "560", "year": "2025"},
        "linked_projects": [{"chamber": "camara", "number": "560", "year": "2025"}],
        "status": {
            "stage": "active",
            "label": "Pendiente designar ponentes en Senado",
            "as_of": "2026-05-18T00:00:00Z",
            "source_id": "camara_proyectos_ley_registry",
            "url": "https://example.com/camara-560",
        },
        "latest_movement": {
            "date": "2026-05-18T00:00:00Z",
            "action_type": "ponencia_publicada",
            "label": "Ponencia publicada en Gaceta del Congreso",
            "source_id": "gacetas_congreso",
            "source_name": "Gacetas del Congreso",
            "url": "https://example.com/gaceta-485",
        },
        "source_evidence": [
            {
                "source_id": "camara_proyectos_ley_registry",
                "role": "identity_status",
                "date": "2026-05-18T00:00:00Z",
                "url": "https://example.com/camara-560",
                "summary": "Registry row with project number and active status.",
            },
            {
                "source_id": "gacetas_congreso",
                "role": "movement",
                "date": "2026-05-18T00:00:00Z",
                "url": "https://example.com/gaceta-485",
                "summary": "Parsed Gaceta item with ponencia evidence.",
            },
        ],
        "contradiction": {"has_contradiction": False},
        "decision_state": "unresolved",
        "m2_readiness": {"state": "ready", "reason": "ready", "missing": []},
    }
    record.update(overrides)
    return record


def test_legislative_m2_ranking_keeps_scores_explainable_and_advisory(tmp_path) -> None:
    unready_public_interest = {
        **_ready_record(
            canonical_bill_id="bill:research:abc123",
            display_title=(
                "Proyecto sin número limpio sobre subsidio de energía, tarifa "
                "de gas GLP, transporte y servicios públicos para San Andrés"
            ),
            title_normalized=(
                "subsidio energia tarifa gas glp transporte servicios publicos "
                "san andres"
            ),
            origin_project=None,
            latest_movement={},
            source_evidence=[],
            m2_readiness={
                "state": "research_lead",
                "reason": "missing identity",
                "missing": ["clean project number/year/chamber"],
            },
        )
    }
    structurally_strong_low_keyword = _ready_record(
        canonical_bill_id="bill:2026:senado:1",
        display_title="Proyecto de Ley 1 de 2026 Senado - ajuste institucional",
        title_normalized="ajuste institucional",
        origin_project={"chamber": "senado", "number": "1", "year": "2026"},
    )

    ranking = build_legislative_m2_ranking(
        [_ready_record(), unready_public_interest, structurally_strong_low_keyword],
        _summary(),
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert ranking["schema_version"] == "m2_legislative_ranking.v1"
    assert ranking["inputs"]["policy"].startswith("Advisory deterministic triage")
    assert ranking["bucket_counts"]["ready_for_m3"] == 1
    assert ranking["bucket_counts"]["public_interest_but_unready"] == 1
    assert ranking["bucket_counts"]["watchlist"] == 1
    assert len(ranking["ranked_questions"]) == 3

    ready = ranking["ranked_questions"][0]
    assert ready["bucket"] == "ready_for_m3"
    assert ready["recommendation"] == "select_for_evidence_pack"
    assert ready["score_reasons"]
    assert ready["penalties"] == []
    assert ready["dimension_scores"]["forecastability"] >= 0.65
    assert ready["dimension_scores"]["source_quality"] >= 0.55

    audit = ranking["heuristic_audit"]
    assert audit["possible_false_negative_count"] >= 1
    assert any(
        item["canonical_bill_id"] == "bill:2026:senado:1"
        for item in audit["possible_false_negatives"]
    )
    assert any(
        item["canonical_bill_id"] == "bill:2026:senado:1"
        for item in ranking["review_queue"]
    )


def test_legislative_m2_ranking_penalizes_active_forecast_duplicates(tmp_path) -> None:
    forecast_log = tmp_path / "forecast_log.jsonl"
    forecast_log.write_text(
        json.dumps(
            {
                "forecast_id": "fcst_20260518_glp",
                "status": "draft_for_human_review",
                "question": "Will Proyecto de Ley 560 de 2025 Cámara advance?",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    ranking = build_legislative_m2_ranking(
        [_ready_record()],
        _summary(),
        forecast_log_path=forecast_log,
    )

    item = ranking["ranked_questions"][0]
    assert item["bucket"] == "watchlist"
    assert item["dimension_scores"]["novelty"] == 0.25
    assert "Likely duplicate of an active forecast-log item." in item["penalties"]
