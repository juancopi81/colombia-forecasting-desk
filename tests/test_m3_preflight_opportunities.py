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


def _banrep_calendar_source(**overrides) -> Metasource:
    base = dict(
        id="banrep_junta_calendar",
        name="Banco de la Republica - Calendario Junta Directiva",
        url="https://www.banrep.gov.co/es/calendario-junta-directiva",
        type="calendar",
        country_relevance="high",
        access_status="html_public",
        fetch_method="html",
        priority="high",
        update_frequency="daily",
        trust_role="agenda_signal",
        parsing_difficulty="medium",
        enabled=True,
    )
    base.update(overrides)
    return Metasource(**base)


def _banrep_calendar_event(
    scheduled_at: str = "2026-07-31T08:30:00-05:00",
) -> RawItem:
    event_date = scheduled_at[:10]
    return RawItem(
        id=f"banrep-calendar-{event_date}",
        source_id="banrep_junta_calendar",
        source_name="Banco de la Republica - Calendario Junta Directiva",
        source_type="calendar",
        url=(
            "https://www.banrep.gov.co/es/noticias/"
            "calendario-actividades-junta-directiva/reunion-julio-2026"
        ),
        title=(
            "Reunion de la Junta Directiva de julio de 2026 - "
            "Decision sobre la tasa de interes de intervencion"
        ),
        fetched_at="2026-07-30T15:00:00Z",
        published_at=scheduled_at,
        raw_text="Official BanRep policy-rate decision calendar event.",
        metadata={
            "content_extraction": "banrep_junta_calendar",
            "event_type": "banrep_policy_rate_decision",
            "scheduled_date": event_date,
            "scheduled_at_local": scheduled_at,
            "timezone": "America/Bogota",
        },
    )


def _dane_calendar_event(
    *,
    family: str = "pib",
    scheduled_at: str = "2026-08-18T10:00:00-05:00",
) -> RawItem:
    return RawItem(
        id=f"dane-calendar-{family}-{scheduled_at[:10]}",
        source_id="dane_publication_calendar",
        source_name="DANE - Calendario de publicaciones",
        source_type="calendar",
        url="https://www.dane.gov.co/index.php/calendario/evento-pib",
        title="18 Ago 2026 10:00 : Producto Interno Bruto (PIB)",
        fetched_at="2026-08-11T16:00:00Z",
        published_at=scheduled_at,
        raw_text="Official DANE publication calendar event.",
        metadata={
            "event_type": "dane_economic_release",
            "release_family": family,
            "scheduled_date": scheduled_at[:10],
            "scheduled_at_local": scheduled_at,
            "operation_url": (
                "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
                "cuentas-nacionales/cuentas-nacionales-trimestrales"
            ),
        },
    )


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

    assert payload["schema_version"] == "m3_preflight_opportunities.v2"
    assert payload["summary"]["opportunity_count"] == 1
    opportunity = payload["opportunities"][0]
    assert opportunity["opportunity_id"] == "banrep_policy_rate_decision_2026-06-30"
    assert opportunity["event_date"] == "2026-06-30"
    assert opportunity["days_until_event"] == 1
    assert opportunity["urgency"] == "imminent"
    assert opportunity["disposition"] == "consider_m3_preflight"
    assert "change its policy rate from 11.25%" in opportunity["question_seed"]
    assert opportunity["linked_tension_cards"][0]["label"] == "High ex-post real policy rate"

    rendered = render_m3_preflight_opportunities(payload)
    assert "BanRep board policy-rate decision" in rendered
    assert "not forecasts" in rendered
    assert "30 de junio" in rendered


def test_dane_calendar_surfaces_clean_internal_research_clock(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_dane_calendar_event()],
        [],
        [],
        run_date="2026-08-11",
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["summary"]["opportunity_count"] == 1
    opportunity = payload["opportunities"][0]
    assert opportunity["opportunity_id"] == "dane_pib_release_2026-08-18"
    assert opportunity["days_until_event"] == 7
    assert opportunity["urgency"] == "nearby"
    assert opportunity["m3_gate"] == "needs_human_review"
    assert opportunity["disposition"] == "consider_shadow_or_m3_preflight"
    assert "estimate it internally" in opportunity["question_seed"]
    assert opportunity["resolution_source"]["url"].startswith("https://www.dane.gov.co/")


def test_dane_calendar_early_event_stays_research_only(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_dane_calendar_event(scheduled_at="2026-08-31T10:00:00-05:00")],
        [],
        [],
        run_date="2026-08-11",
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    opportunity = payload["opportunities"][0]
    assert opportunity["days_until_event"] == 20
    assert opportunity["urgency"] == "research_horizon"
    assert opportunity["m3_gate"] == "research_only"
    assert opportunity["disposition"] == "shadow_research"


def test_banrep_calendar_recovers_july_31_preflight_miss(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes(), _banrep_calendar_event()],
        [_policy_rate(12.0)],
        [
            {
                "card_id": "real_policy_rate",
                "family": "monetary_stance",
                "title": "High ex-post real policy rate",
                "trigger": "Policy rate minus annual IPC is +5.86 pp.",
            }
        ],
        run_date="2026-07-30",
        sources=[_banrep_source(), _banrep_calendar_source()],
        source_health=[
            {
                "source_id": "banrep_junta_comunicados",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
            {
                "source_id": "banrep_junta_calendar",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
        ],
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["summary"]["opportunity_count"] == 1
    opportunity = payload["opportunities"][0]
    assert opportunity["event_date"] == "2026-07-31"
    assert opportunity["days_until_event"] == 1
    assert "change its policy rate from 12%" in opportunity["question_seed"]
    assert "official BanRep calendar" in opportunity["why_now"]
    assert opportunity["source_evidence"][0]["source_id"] == "banrep_junta_calendar"
    assert (
        opportunity["source_evidence"][0]["metadata_key"] == "scheduled_at_local"
    )
    assert opportunity["linked_tension_cards"][0]["label"] == (
        "High ex-post real policy rate"
    )
    assert payload["caveats"] == []


def test_banrep_calendar_coverage_gap_is_explicit(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes(), _banrep_calendar_event("2026-07-31T08:30:00-05:00")],
        [_policy_rate(12.0)],
        [],
        run_date="2026-08-04",
        sources=[_banrep_source(), _banrep_calendar_source()],
        source_health=[
            {
                "source_id": "banrep_junta_comunicados",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
            {
                "source_id": "banrep_junta_calendar",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
        ],
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["opportunities"] == []
    assert payload["caveats"] == [
        {
            "detector": "banrep_policy_rate_decision",
            "reason": "future_meeting_date_missing",
        }
    ]


def test_banrep_future_calendar_date_outside_window_is_not_a_coverage_gap(
    tmp_path,
) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes(), _banrep_calendar_event("2026-09-30T08:30:00-05:00")],
        [_policy_rate(12.0)],
        [],
        run_date="2026-08-04",
        sources=[_banrep_source(), _banrep_calendar_source()],
        source_health=[
            {
                "source_id": "banrep_junta_comunicados",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
            {
                "source_id": "banrep_junta_calendar",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
        ],
        research_window_days=7,
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["opportunities"] == []
    assert payload["caveats"] == []


def test_default_research_horizon_surfaces_later_banrep_decision(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes(), _banrep_calendar_event("2026-09-30T08:30:00-05:00")],
        [_policy_rate(12.0)],
        [],
        run_date="2026-08-11",
        sources=[_banrep_source(), _banrep_calendar_source()],
        source_health=[
            {
                "source_id": "banrep_junta_comunicados",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
            {
                "source_id": "banrep_junta_calendar",
                "status": "ok",
                "raw_count": 1,
                "failure_count": 0,
            },
        ],
        forecast_log_path=tmp_path / "forecast_log.jsonl",
    )

    assert payload["inputs"]["research_window_days"] == 60
    assert payload["inputs"]["imminent_window_days"] == 7
    assert payload["summary"]["opportunity_count"] == 1
    opportunity = payload["opportunities"][0]
    assert opportunity["event_date"] == "2026-09-30"
    assert opportunity["days_until_event"] == 50
    assert opportunity["urgency"] == "research_horizon"
    assert opportunity["status"] == "preflight_only"
    assert opportunity["m3_gate"] == "research_only"
    assert opportunity["disposition"] == "shadow_research"


def test_banrep_preflight_fails_closed_outside_window(tmp_path) -> None:
    payload = build_m3_preflight_opportunities(
        [_banrep_minutes()],
        [_policy_rate()],
        [],
        run_date="2026-06-20",
        research_window_days=7,
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
