from __future__ import annotations

from colombia_forecasting_desk.candidates import build_m1_candidates
from colombia_forecasting_desk.models import (
    Cluster,
    IndicatorObservation,
    RunSummary,
    SourceFailure,
    SourceHealth,
)


def _summary() -> RunSummary:
    return RunSummary(
        run_date="2026-05-06",
        started_at="2026-05-06T12:00:00Z",
        finished_at="2026-05-06T12:00:30Z",
        sources_checked=5,
        sources_failed=1,
        raw_items=40,
        cleaned_items=30,
        clusters=4,
    )


def _cluster(**overrides) -> Cluster:
    base = dict(
        cluster_id="c-banrep-decision",
        title="La Junta Directiva del Banco de la Republica mantiene la tasa",
        summary="BanRep mantiene la tasa de politica monetaria.",
        items=["item-1"],
        source_count=1,
        source_types=["official_updates"],
        latest_published_at="2026-05-06T10:00:00Z",
        signal_types=["official_update"],
        confidence="medium",
        score=9.5,
        member_urls=["https://banrep.gov.co/decision"],
        member_titles=["Decision BanRep"],
        member_source_names=["Banco de la Republica"],
        member_source_ids=["banrep_junta_comunicados"],
        priorities=["high"],
        why_it_matters="",
        possible_questions=[],
        missing_evidence=[],
        recommended_next_sources=[],
    )
    base.update(overrides)
    return Cluster(**base)


def test_build_m1_candidates_has_stable_event_ids() -> None:
    cluster = _cluster()
    first = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=["tasa", "banrep"],
        generated_at="2026-05-06T12:00:31Z",
    )
    second = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=["tasa", "banrep"],
        generated_at="2026-05-06T12:59:59Z",
    )

    assert first["schema_version"] == "m1_candidates.v1"
    assert first["run_date"] == "2026-05-06"
    assert first["candidates"][0]["candidate_id"] == second["candidates"][0][
        "candidate_id"
    ]


def test_event_candidate_shape_uses_forecastability_helpers() -> None:
    out = build_m1_candidates(
        _summary(),
        [_cluster()],
        [],
        topic_keywords=["banrep", "tasa"],
        generated_at="2026-05-06T12:00:31Z",
    )

    candidate = out["candidates"][0]
    assert candidate["candidate_type"] == "event_signal"
    assert candidate["origin_id"] == "c-banrep-decision"
    assert candidate["source_ids"] == ["banrep_junta_comunicados"]
    assert candidate["event_type"] == "official_update"
    assert candidate["actor"] == ""
    assert candidate["topic"] == "banrep"
    assert candidate["published_at"] == "2026-05-06T10:00:00Z"
    assert candidate["evidence_text"] == "BanRep mantiene la tasa de politica monetaria."
    assert candidate["question_seed"] == (
        "Will Banco de la Republica change the policy rate at its next board decision?"
    )
    assert candidate["resolution_source"] == (
        "Banco de la Republica board statement, minutes, and rate series."
    )
    assert candidate["resolution_source_hint"] == candidate["resolution_source"]
    assert candidate["deadline_or_window"] == "Next scheduled BanRep board decision."
    assert candidate["deadline_hint"] == candidate["deadline_or_window"]
    assert candidate["m1_scores"]["rank_score"] == 9.5
    assert candidate["m1_scores"]["forecastability_score"] >= 3
    assert "primary or structured source" in candidate["reasons"]
    assert candidate["noise_reasons"] == []
    assert candidate["topics"] == ["banrep", "tasa"]
    assert candidate["evidence"]["source_ids"] == ["banrep_junta_comunicados"]
    assert candidate["evidence"]["item_ids"] == ["item-1"]
    assert candidate["evidence"]["links"] == [
        {
            "title": "Decision BanRep",
            "url": "https://banrep.gov.co/decision",
            "source_name": "Banco de la Republica",
        }
    ]


def test_rejected_includes_opaque_gaceta_and_noisy_cluster() -> None:
    opaque = _cluster(
        cluster_id="c-gaceta-opaque",
        title="Gaceta del Congreso 421 — Senado de la República",
        summary="421 | Senado de la República | 05/05/2026",
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["gacetas_congreso"],
        member_source_names=["Gacetas del Congreso — Imprenta Nacional"],
        member_titles=["Gaceta del Congreso 421 — Senado de la República"],
        member_urls=["https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=421"],
    )
    noisy = _cluster(
        cluster_id="c-human-interest",
        title="Festival cultural abre convocatoria regional",
        summary="Una actividad cultural local anuncia sus invitados.",
        source_types=["news"],
        signal_types=["media_narrative"],
        member_source_ids=["eltiempo_colombia"],
        member_source_names=["El Tiempo"],
        member_titles=["Festival cultural abre convocatoria regional"],
        member_urls=["https://eltiempo.com/festival"],
    )

    out = build_m1_candidates(
        _summary(),
        [opaque, noisy],
        [],
        topic_keywords=[],
        generated_at="2026-05-06T12:00:31Z",
    )

    assert out["candidates"] == []
    rejected_by_id = {item["origin_id"]: item for item in out["rejected"]}
    assert rejected_by_id["c-gaceta-opaque"]["reason"] == (
        "official publication index lacks document title or parsed text"
    )
    assert rejected_by_id["c-human-interest"]["reason"] in {
        "low-forecastability human-interest/local story",
        "single-source media narrative without a clear resolution path",
    }


def test_indicator_seed_candidate_from_observed_trm_move() -> None:
    trm = IndicatorObservation(
        indicator_id="trm_usd_cop",
        name="TRM / USD-COP",
        category="markets",
        status="observed",
        frequency="daily",
        source_name="Superfinanciera",
        source_url="https://www.superfinanciera.gov.co/trm",
        period="2026-05-06",
        release_date="2026-05-06T00:00:00Z",
        headline="TRM vigente: 3900.00 COP/USD.",
        values={"seven_day_change_pct": 2.4, "trm_cop_per_usd": 3900.0},
        freshness_status="current",
    )

    out = build_m1_candidates(
        _summary(),
        [],
        [],
        topic_keywords=[],
        indicator_watch=[trm],
        generated_at="2026-05-06T12:00:31Z",
    )

    assert len(out["candidates"]) == 1
    candidate = out["candidates"][0]
    assert candidate["candidate_type"] == "indicator_seed"
    assert candidate["origin_id"] == "trm_usd_cop"
    assert candidate["event_type"] == "indicator_alert"
    assert candidate["published_at"] == "2026-05-06T00:00:00Z"
    assert candidate["evidence_text"] == "TRM vigente: 3900.00 COP/USD."
    assert candidate["topic"] == "external_trade"
    assert candidate["theme"] == "FX move persistence"
    assert candidate["reasons"] == ["indicator:material_move"]
    assert candidate["resolution_source"] == (
        "Superintendencia Financiera / datos.gov.co official TRM."
    )
    assert candidate["evidence"]["values"]["seven_day_change_pct"] == 2.4
    assert candidate["evidence"]["links"][0]["url"] == (
        "https://www.superfinanciera.gov.co/trm"
    )


def test_source_caveats_include_failures_and_link_only_sources() -> None:
    failure = SourceFailure(
        source_id="registraduria_noticias",
        source_name="Registraduria",
        url="https://www.registraduria.gov.co/-Noticias-.html",
        error_class="HTTPStatusError",
        error_message="403 Forbidden",
        occurred_at="2026-05-06T12:00:00Z",
    )
    health = SourceHealth(
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas",
        url="https://zf.mincit.gov.co/estadisticas",
        raw_count=3,
        cleaned_count=0,
        dated_count=3,
        rankable_count=0,
        failure_count=0,
        content_mode="pdf_links_only",
        document_link_count=3,
    )

    out = build_m1_candidates(
        _summary(),
        [],
        [failure],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-06T12:00:31Z",
    )

    reasons = {caveat["reason"] for caveat in out["source_caveats"]}
    assert "source failed during this run; silence is not evidence of no activity" in reasons
    assert "link-only source; ask for document contents before relying on it" in reasons


def test_link_only_source_cluster_is_rejected_even_when_forecastable() -> None:
    dane_cluster = _cluster(
        cluster_id="c-dane-pdf",
        title="DANE inicia tercera entrega de resultados del Censo Economico Nacional Urbano",
        summary="DANE inicia tercera entrega 4/05/2026 PDF Descargar",
        source_types=["official_updates"],
        signal_types=["official_update"],
        member_source_ids=["dane_comunicados_prensa"],
        member_source_names=["DANE"],
        member_titles=[
            "DANE inicia tercera entrega de resultados del Censo Economico Nacional Urbano"
        ],
        member_urls=["https://www.dane.gov.co/files/prensa/comunicados/cp.pdf"],
    )
    health = SourceHealth(
        source_id="dane_comunicados_prensa",
        source_name="DANE",
        url="https://example.com/dane",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="pdf_links_only",
        document_link_count=1,
        parsed_content_count=0,
    )

    out = build_m1_candidates(
        _summary(),
        [dane_cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-06T12:00:31Z",
    )

    assert out["candidates"] == []
    assert out["rejected"][0]["reason"] == (
        "source is link-only in this run; document contents are missing"
    )
    assert out["rejected"][0]["reject_reason"] == out["rejected"][0]["reason"]


def test_mixed_document_and_html_source_cluster_is_not_rejected_as_link_only() -> None:
    cluster = _cluster(
        member_source_ids=["dane_comunicados_prensa"],
        member_source_names=["DANE"],
        member_urls=["https://www.dane.gov.co/noticias/ipc.html"],
    )
    health = SourceHealth(
        source_id="dane_comunicados_prensa",
        source_name="DANE",
        url="https://example.com/dane",
        raw_count=2,
        cleaned_count=2,
        dated_count=2,
        rankable_count=1,
        failure_count=0,
        content_mode="mixed_document_and_html_links",
        document_link_count=1,
        parsed_content_count=0,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=["banrep", "tasa"],
        source_health=[health],
        generated_at="2026-05-06T12:00:31Z",
    )

    assert len(out["candidates"]) == 1
    assert out["rejected"] == []
