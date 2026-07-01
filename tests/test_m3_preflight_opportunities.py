from __future__ import annotations

import json

from colombia_forecasting_desk.m3_preflight_opportunities import (
    build_m3_preflight_opportunities,
    render_m3_preflight_opportunities,
)
from colombia_forecasting_desk.models import IndicatorObservation, Metasource, RawItem


def _banrep_minutes(**overrides) -> RawItem:
    base = dict(
        id="banrep-minutes-april",
        source_id="banrep_junta_comunicados",
        source_name="Banco de la Republica - Comunicados Junta Directiva",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-abril-2026",
        title=(
            "Minutas BanRep: La Junta Directiva decidio mantener inalterada "
            "la tasa de interes de politica monetaria en 11,25%"
        ),
        fetched_at="2026-06-29T15:00:00Z",
        published_at="2026-05-06T00:00:00Z",
        raw_text="",
        metadata={
            "next_meeting_context": (
                "Asimismo, resaltaron que, en la sesion de Junta del proximo "
                "30 de junio, se contara con informacion adicional."
            )
        },
    )
    base.update(overrides)
    return RawItem(**base)


def _policy_rate(rate: float = 11.25) -> IndicatorObservation:
    return IndicatorObservation(
        indicator_id="policy_rate_ibr",
        name="Policy rate + IBR",
        category="monetary",
        status="observed",
        frequency="daily/monthly",
        source_name="Banco de la Republica",
        source_url=(
            "https://suameca.banrep.gov.co/estadisticas-economicas/"
            "informacionSerie/59/tasas_interes_politica_monetaria/"
        ),
        period="2026-06-29",
        release_date="2026-06-29T00:00:00Z",
        headline="BanRep 2026-06-29: policy rate 11.25%.",
        values={"policy_rate_pct": rate},
        freshness_status="current",
        components=[],
        why_it_matters="Shows monetary stance.",
        correlations=[],
        next_step="Review.",
    )


def _banrep_source(**overrides) -> Metasource:
    base = dict(
        id="banrep_junta_comunicados",
        name="Banco de la Republica - Comunicados Junta Directiva",
        url="https://www.banrep.gov.co/es/comunicados-junta",
        type="official_updates",
        country_relevance="high",
        access_status="html_public",
        fetch_method="html",
        priority="high",
        update_frequency="event_driven",
        trust_role="resolution_source",
        parsing_difficulty="medium",
        enabled=True,
    )
    base.update(overrides)
    return Metasource(**base)


def test_banrep_next_meeting_context_flags_m3_preflight(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate()],
        [
            {
                "card_id": "real_policy_rate",
                "family": "monetary_stance",
                "title": "High ex-post real policy rate",
                "trigger": "Policy rate minus annual IPC is high.",
            }
        ],
        run_date="2026-06-29",
        generated_at="2026-06-29T15:00:00Z",
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["schema_version"] == "m3_preflight_opportunities.v1"
    assert payload["summary"]["opportunity_count"] == 1
    opportunity = payload["opportunities"][0]
    assert opportunity["opportunity_id"] == "banrep_policy_rate_decision_2026-06-30"
    assert opportunity["event_date"] == "2026-06-30"
    assert opportunity["days_until_event"] == 1
    assert opportunity["urgency"] == "imminent"
    assert opportunity["disposition"] == "consider_m3_preflight"
    assert "above the current 11.25%" in opportunity["question_seed"]
    assert opportunity["linked_tension_cards"][0]["label"] == "High ex-post real policy rate"

    rendered = render_m3_preflight_opportunities(payload)
    assert "BanRep board policy-rate decision" in rendered
    assert "not forecasts" in rendered
    assert "30 de junio" in rendered


def test_banrep_preflight_fails_closed_outside_window(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate()],
        [],
        run_date="2026-06-20",
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["summary"]["opportunity_count"] == 0
    assert payload["opportunities"] == []


def test_banrep_preflight_does_not_keep_firing_after_event(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate(12.0)],
        [],
        run_date="2026-07-01",
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["summary"]["opportunity_count"] == 0


def test_banrep_preflight_marks_existing_active_forecast(tmp_path) -> None:
    forecast_log = tmp_path / "forecast_log.jsonl"
    forecast_log.write_text(
        json.dumps(
            {
                "forecast_id": "fcst_20260629_banrep_june30_policy_rate_hike",
                "status": "draft_for_human_review",
                "question": "Will Banco de la Republica raise its policy rate?",
                "resolution_source": "Banco de la Republica",
                "resolution_deadline": "2026-06-30",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate()],
        [],
        run_date="2026-06-29",
        forecast_log_path=forecast_log,
    )

    opportunity = payload["opportunities"][0]
    assert opportunity["disposition"] == "already_tracked"
    assert opportunity["active_forecast_ids"] == [
        "fcst_20260629_banrep_june30_policy_rate_hike"
    ]


def test_banrep_preflight_fails_closed_when_resolver_source_failed(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate()],
        [],
        run_date="2026-06-29",
        sources=[_banrep_source()],
        source_health=[
            {
                "source_id": "banrep_junta_comunicados",
                "status": "failed",
                "raw_count": 0,
                "failure_count": 1,
            }
        ],
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["summary"]["opportunity_count"] == 0
    assert payload["opportunities"] == []
    assert payload["caveats"] == [
        {
            "detector": "banrep_policy_rate_decision",
            "reason": "resolver_source_failed",
        }
    ]


def test_banrep_preflight_fails_closed_when_resolver_source_missing(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate()],
        [],
        run_date="2026-06-29",
        sources=[_banrep_source(id="other_source")],
        source_health=[
            {
                "source_id": "other_source",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            }
        ],
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["summary"]["opportunity_count"] == 0
    assert payload["opportunities"] == []
    assert payload["caveats"] == [
        {
            "detector": "banrep_policy_rate_decision",
            "reason": "resolver_source_missing_from_config",
        }
    ]
