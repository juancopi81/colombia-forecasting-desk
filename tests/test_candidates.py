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


def test_legislative_reconciler_promotes_substantive_movement_only() -> None:
    registry_only = {
        "canonical_bill_id": "bill:2026:camara:550",
        "display_title": "Proyecto de Ley 550 de 2026 Cámara - presupuesto",
        "status": {
            "stage": "active",
            "label": "En trámite",
            "as_of": "2026-05-18T00:00:00Z",
            "source_id": "camara_proyectos_ley_registry",
            "url": "https://example.com/camara-550",
        },
        "latest_movement": {
            "date": "2026-05-18T00:00:00Z",
            "action_type": "registry_publication",
            "label": "Publication metadata listed in official registry",
            "source_id": "camara_proyectos_ley_registry",
            "source_name": "Cámara",
            "url": "https://example.com/camara-550",
        },
        "source_evidence": [
            {
                "source_id": "camara_proyectos_ley_registry",
                "role": "identity_status",
                "date": "2026-05-18T00:00:00Z",
                "url": "https://example.com/camara-550",
                "summary": "Registry row with project number and active status.",
            }
        ],
        "contradiction": {"has_contradiction": False},
        "decision_state": "unresolved",
        "m2_readiness": {"state": "ready", "reason": "ready", "missing": []},
    }
    ponencia = {
        **registry_only,
        "canonical_bill_id": "bill:2025:camara:560",
        "display_title": "Proyecto de Ley 560 de 2025 Cámara - subsidio GLP",
        "latest_movement": {
            "date": "2026-05-19T00:00:00Z",
            "action_type": "ponencia_publicada",
            "label": "Ponencia publicada en Gaceta del Congreso",
            "source_id": "gacetas_congreso",
            "source_name": "Gacetas del Congreso",
            "url": "https://example.com/gaceta-485",
        },
    }

    out = build_m1_candidates(
        _summary(),
        [],
        [],
        topic_keywords=[],
        legislative_reconciliations=[registry_only, ponencia],
    )

    legislative = [
        candidate
        for candidate in out["candidates"]
        if candidate["candidate_type"] == "legislative_bill"
    ]
    assert len(legislative) == 1
    assert legislative[0]["origin_id"] == "bill:2025:camara:560"
    assert out["inputs"]["legislative_reconciliations"]["record_count"] == 2
    assert out["inputs"]["legislative_reconciliations"]["m2_readiness_counts"] == {
        "ready": 2
    }


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


def test_mixed_diario_final_cluster_is_rejected_before_m2_promotion() -> None:
    cluster = _cluster(
        cluster_id="c-diario-cne-mixed",
        title="Diario Oficial 53.491 — Resolución 1002 de 2026",
        summary=(
            "Final Diario legal acts were clustered with a CNE polling item; "
            "this is resolution evidence, not a clean unresolved decision."
        ),
        source_count=2,
        source_types=["legal", "polling"],
        signal_types=["court_or_regulatory_movement", "poll"],
        member_source_ids=["diario_oficial", "cne_encuestas_2026"],
        member_source_names=[
            "Diario Oficial — Imprenta Nacional",
            "CNE — Encuestas Electorales 2026",
        ],
        member_titles=[
            "Diario Oficial 53.491 — Ordinaria — Resolución 1002 de 2026",
            "ANALIZAR & LOMBANA",
        ],
        member_urls=[
            "https://svrpubindc.imprenta.gov.co/diario?edicion=53.491#act-resolucion-1002-de-2026",
            "https://www.cne.gov.co/encuestas-2026/43-analizar-lombana",
        ],
        member_metadata=[
            {
                "content_extraction": "diario_oficial_pdf_text",
                "document_row_type": "diario_legal_act",
                "legal_act_records": [
                    {"kind": "Resolución", "number": "1002", "year": "2026"}
                ],
            },
            {},
        ],
        detected_entities=["cne", "presidencia"],
        detected_topics=["fiscal_tax", "electoral", "regulatory"],
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=["electoral", "regulatory"],
        generated_at="2026-05-18T16:25:26Z",
    )

    assert out["candidates"] == []
    assert out["rejected"][0]["reason"] == (
        "mixed cluster includes final Diario Oficial publication without a clean "
        "unresolved decision identity"
    )
    assert out["rejected"][0]["noise_reasons"] == [out["rejected"][0]["reason"]]


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


def test_indicator_seed_candidate_from_ise_activity_acceleration() -> None:
    ise = IndicatorObservation(
        indicator_id="ise_activity",
        name="ISE / monthly activity",
        category="macro_activity",
        status="observed",
        frequency="monthly",
        source_name="DANE",
        source_url=(
            "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
            "cuentas-nacionales/indicador-de-seguimiento-a-la-economia-ise"
        ),
        period="2026-03",
        release_date="2026-05-15T00:00:00Z",
        headline="DANE ISE 2026-03: index 128.91, activity +3.98% y/y.",
        values={"ise_index": 128.91, "annual_growth_pct": 3.98},
        freshness_status="current",
    )

    out = build_m1_candidates(
        _summary(),
        [],
        [],
        topic_keywords=[],
        indicator_watch=[ise],
        generated_at="2026-05-06T12:00:31Z",
    )

    assert len(out["candidates"]) == 1
    candidate = out["candidates"][0]
    assert candidate["candidate_type"] == "indicator_seed"
    assert candidate["origin_id"] == "ise_activity"
    assert candidate["topic"] == "economic_activity"
    assert candidate["theme"] == "Activity acceleration"
    assert candidate["reasons"] == ["indicator:activity_acceleration"]
    assert candidate["question_seed"] == (
        "Will the next DANE ISE release show annual growth of at least 3.0%?"
    )
    assert candidate["resolution_source"] == "DANE ISE next monthly release."
    assert candidate["evidence"]["values"]["annual_growth_pct"] == 3.98


def test_indicator_seed_candidate_from_tes_auction_cost() -> None:
    fiscal = IndicatorObservation(
        indicator_id="fiscal_tax_pulse",
        name="Fiscal / tax pulse",
        category="fiscal",
        status="observed",
        frequency="monthly",
        source_name="MinHacienda / IRC — Subastas TES 2026",
        source_url=(
            "https://www.irc.gov.co/documents/d/guest/"
            "subasta-9-cop-mayo-13-de-2026?download=true"
        ),
        period="2026-05-13",
        release_date="2026-05-13T00:00:00Z",
        headline="Fiscal / tax pulse has observed components: tes_auction.",
        values={
            "observed_components": 1,
            "total_components": 3,
            "components": {
                "tes_auction": {
                    "auction_type": "COP",
                    "auction_date": "2026-05-13T00:00:00Z",
                    "max_cutoff_rate_pct": 14.79,
                    "bid_to_cover": 4.1,
                    "source_pdf_url": (
                        "https://www.irc.gov.co/documents/d/guest/"
                        "subasta-9-cop-mayo-13-de-2026?download=true"
                    ),
                }
            },
        },
        freshness_status="current",
    )

    out = build_m1_candidates(
        _summary(),
        [],
        [],
        topic_keywords=[],
        indicator_watch=[fiscal],
        generated_at="2026-05-13T12:00:31Z",
    )

    assert len(out["candidates"]) == 1
    candidate = out["candidates"][0]
    assert candidate["candidate_type"] == "indicator_seed"
    assert candidate["origin_id"] == "fiscal_tax_pulse"
    assert candidate["theme"] == "TES auction funding cost"
    assert candidate["reasons"] == ["indicator:tes_funding_cost"]
    assert candidate["question_seed"] == (
        "Will the next official MinHacienda / IRC COP TES auction report show "
        "a maximum cutoff rate of at least 14.0%?"
    )
    assert candidate["resolution_source"] == (
        "MinHacienda / IRC official TES auction-result PDF for the next COP auction."
    )
    assert candidate["evidence"]["values"]["components"]["tes_auction"][
        "max_cutoff_rate_pct"
    ] == 14.79


def test_tes_raw_clusters_are_indicator_only() -> None:
    cluster = _cluster(
        cluster_id="c-tes-auction",
        title="Subasta 09 COP Mayo 13 de 2026",
        summary="Official MinHacienda TES auction report with cutoff rates.",
        member_source_ids=["minhacienda_tes_reports"],
        member_source_names=["MinHacienda / IRC — Subastas TES 2026"],
        source_types=["economic_indicator"],
        signal_types=["official_update"],
        member_urls=[
            "https://www.irc.gov.co/documents/d/guest/"
            "subasta-9-cop-mayo-13-de-2026?download=true"
        ],
        member_titles=["Subasta 09 COP Mayo 13 de 2026"],
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=["tes"],
        generated_at="2026-05-13T12:00:31Z",
    )

    assert out["candidates"] == []
    assert out["rejected"][0]["source_ids"] == ["minhacienda_tes_reports"]
    assert out["rejected"][0]["reject_reason"] == (
        "source is promoted through Indicator Watch; raw clusters would have a "
        "generic resolution path"
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
    mixed_health = SourceHealth(
        source_id="dian_proyectos_normas",
        source_name="DIAN — Proyectos de Normas",
        url="https://www.dian.gov.co/normatividad/Paginas/Inicio.aspx",
        raw_count=28,
        cleaned_count=0,
        dated_count=0,
        rankable_count=0,
        failure_count=0,
        content_mode="mixed_document_and_html_links",
        document_link_count=5,
        parsed_content_count=0,
    )

    out = build_m1_candidates(
        _summary(),
        [],
        [failure],
        topic_keywords=[],
        source_health=[health, mixed_health],
        generated_at="2026-05-06T12:00:31Z",
    )

    reasons = {caveat["reason"] for caveat in out["source_caveats"]}
    assert "source failed during this run; silence is not evidence of no activity" in reasons
    assert "link-only source; ask for document contents before relying on it" in reasons
    link_only_ids = {
        caveat["source_id"]
        for caveat in out["source_caveats"]
        if caveat["reason"]
        == "link-only source; ask for document contents before relying on it"
    }
    assert "dian_proyectos_normas" in link_only_ids


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


def test_parsed_senado_agenda_entry_is_m2_ready_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-senado-agenda-project",
        title=(
            "Senado agenda 2026-05-12 — primer debate: Proyecto de Ley "
            "312 de 2025 Senado / 463 de 2025 Camara — POR MEDIO DE "
            "LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO"
        ),
        summary=(
            "Extracted from official Senado agenda PDF. Agenda excerpt: "
            "presentacion en primer debate del Proyecto de Ley No. 312 del "
            "2025 Senado 463 del 2025 Camara, POR MEDIO DE LA CUAL SE "
            "MODIFICA EL REGIMEN TRIBUTARIO."
        ),
        source_types=["calendar"],
        signal_types=["calendar_event"],
        member_source_ids=["senado_agenda_legislativa"],
        member_source_names=["Senado — Agenda Legislativa Actual"],
        member_titles=[
            "Senado agenda 2026-05-12 — primer debate: Proyecto de Ley "
            "312 de 2025 Senado / 463 de 2025 Camara — POR MEDIO DE "
            "LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO"
        ],
        member_urls=[
            "https://www.senado.gov.co/index.php/documentos/agenda/file#project-1"
        ],
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )
    health = SourceHealth(
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        url="https://www.senado.gov.co/index.php/documentos/senado-prensa/agenda-legislativa-actual",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="parsed_content",
        parsed_content_count=1,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-15T15:10:31Z",
    )

    assert len(out["candidates"]) == 1
    candidate = out["candidates"][0]
    assert candidate["question_seed"] == (
        "Will the referenced legislative item advance to its next formal stage "
        "within the next 30-60 days?"
    )
    assert candidate["resolution_source"] == (
        "Congreso agenda, Gacetas del Congreso, and final legislative votes."
    )
    assert candidate["source_ids"] == ["senado_agenda_legislativa"]
    assert candidate["actor"] == "congreso"
    assert candidate["topic"] == "legislative"
    assert candidate["follow_up_sources"][0]["source_id"] == "gacetas_congreso"
    assert candidate["follow_up_sources"][0]["search_hint"] == (
        "Proyecto de Ley 312 de 2025 Senado / 463 de 2025 Camara"
    )
    assert out["rejected"] == []


def test_senado_candidate_uses_matched_gaceta_followup_source() -> None:
    cluster = _cluster(
        cluster_id="c-senado-agenda-project-linked",
        title=(
            "Senado agenda 2026-05-12 — primer debate: Proyecto de Ley "
            "550 de 2026 Senado — POR MEDIO DE LA CUAL SE ADOPTA UNA REFORMA"
        ),
        summary="Extracted from official Senado agenda PDF with a matched Gaceta.",
        source_types=["calendar"],
        signal_types=["calendar_event"],
        member_source_ids=["senado_agenda_legislativa"],
        member_source_names=["Senado — Agenda Legislativa Actual"],
        member_titles=[
            "Senado agenda 2026-05-12 — primer debate: Proyecto de Ley "
            "550 de 2026 Senado — POR MEDIO DE LA CUAL SE ADOPTA UNA REFORMA"
        ],
        member_urls=[
            "https://www.senado.gov.co/index.php/documentos/agenda/file#project-1"
        ],
        member_metadata=[
            {
                "official_followup_match_count": 1,
                "official_followup_matches": [
                    {
                        "source_id": "gacetas_congreso",
                        "source_name": "Gacetas del Congreso — Imprenta Nacional",
                        "url": "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=476",
                        "title": "Gaceta del Congreso 476 — Proyecto de Ley 550 DE 2026 Cámara y Senado",
                        "gaceta_number": "476",
                        "project_label": "Proyecto de Ley 550 DE 2026 Cámara y Senado",
                        "match_basis": "project_number_year_chamber",
                    }
                ],
            }
        ],
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )
    health = SourceHealth(
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        url="https://www.senado.gov.co/index.php/documentos/senado-prensa/agenda-legislativa-actual",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="parsed_content",
        parsed_content_count=1,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-15T15:10:31Z",
    )

    candidate = out["candidates"][0]
    assert "official follow-up matched" in candidate["reasons"]
    assert candidate["follow_up_sources"] == [
        {
            "source_id": "gacetas_congreso",
            "source_name": "Gacetas del Congreso — Imprenta Nacional",
            "url": "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=476",
            "search_hint": "Proyecto de Ley 550 DE 2026 Cámara y Senado",
            "purpose": (
                "Official follow-up matched by project number/year/chamber. "
                "Gaceta 476."
            ),
            "match_basis": "project_number_year_chamber",
        }
    ]


def test_registry_candidate_carries_registry_and_publication_followups() -> None:
    cluster = _cluster(
        cluster_id="c-senado-registry-project",
        title=(
            "Senado registry — Proyecto de Ley 1 de 2025 Senado — "
            "POR MEDIO DE LA CUAL SE ESTABLECEN LINEAMIENTOS EN SALUD"
        ),
        summary="Estado: pendiente discutir ponencia para primer debate en Senado.",
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["senado_leyes_registry"],
        member_source_names=["Senado — Sección de Leyes / Proyectos de Ley"],
        member_titles=[
            "Senado registry — Proyecto de Ley 1 de 2025 Senado — salud"
        ],
        member_urls=["https://leyes.senado.gov.co/api/get_detalle_pdly.php?id=9540"],
        member_metadata=[
            {
                "legislative_registry": "senado_leyes",
                "registry_detail_url": "https://leyes.senado.gov.co/api/get_detalle_pdly.php?id=9540",
                "project_label": "Proyecto de Ley 1 de 2025 Senado",
                "text_radicado_url": "https://leyes.senado.gov.co/p-ley/2025-2026/PL-001.pdf",
                "publication_links": [
                    {
                        "type": "Primera Ponencia",
                        "title": "Gaceta 1502/2025",
                        "url": "https://svrpubindc.imprenta.gov.co/senado/",
                    }
                ],
            }
        ],
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )
    health = SourceHealth(
        source_id="senado_leyes_registry",
        source_name="Senado — Sección de Leyes / Proyectos de Ley",
        url="https://leyes.senado.gov.co/",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="parsed_content",
        parsed_content_count=1,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-15T15:10:31Z",
    )

    candidate = out["candidates"][0]
    follow_up_ids = [item["source_id"] for item in candidate["follow_up_sources"]]
    assert follow_up_ids[:3] == [
        "senado_leyes_registry",
        "senado_text_radicado",
        "legislative_publication",
    ]
    assert "gacetas_congreso" in follow_up_ids


def test_senado_agenda_entry_without_clean_identity_is_research_only() -> None:
    cluster = _cluster(
        cluster_id="c-senado-lossy-agenda-project",
        title=(
            "Senado agenda 2026-05-11 — ponencia: Proyecto de Ley Senado — "
            "el cual se modifica el articulo de la ley de y se Dictan otras disposiciones"
        ),
        summary=(
            "Extracted from official Senado agenda PDF. Agenda excerpt: ponencia "
            "Proyecto de Ley Senado elacual se modificael articulo de laleydeyse "
            "Dictan otrasdisposiciones."
        ),
        source_types=["calendar"],
        signal_types=["calendar_event"],
        member_source_ids=["senado_agenda_legislativa"],
        member_source_names=["Senado — Agenda Legislativa Actual"],
        member_titles=[
            "Senado agenda 2026-05-11 — ponencia: Proyecto de Ley Senado — "
            "el cual se modifica el articulo de la ley de y se Dictan otras disposiciones"
        ],
        member_urls=[
            "https://www.senado.gov.co/index.php/documentos/agenda/file#project-1"
        ],
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )
    health = SourceHealth(
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        url="https://www.senado.gov.co/index.php/documentos/senado-prensa/agenda-legislativa-actual",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="parsed_content",
        parsed_content_count=1,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-15T15:10:31Z",
    )

    assert out["candidates"] == []
    assert out["rejected"][0]["reason"] == (
        "Senado agenda entry lacks a clean project number or bill title"
    )


def test_mincit_zona_franca_candidate_carries_legal_followups() -> None:
    cluster = _cluster(
        cluster_id="c-mincit-zf-change",
        title=(
            "MinCIT zona franca registry change — Zona Franca Permanente "
            "Especial De Servicios Rionegro MRO"
        ),
        summary=(
            "Official MinCIT approved-zones registry shows an updated "
            "resolution for a named zona franca."
        ),
        source_types=["regulatory"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["mincit_zonas_francas"],
        member_source_names=["MinCIT — Zonas Francas (Estadísticas)"],
        member_titles=[
            "MinCIT zona franca registry change — Rionegro MRO"
        ],
        member_urls=["https://zf.mincit.gov.co/estadisticas#zf-1"],
        member_metadata=[
            {
                "registry": "mincit_zonas_francas_aprobadas",
                "registry_change_type": "updated_registry_row",
                "zona_franca_name": (
                    "Zona Franca Permanente Especial De Servicios Rionegro MRO"
                ),
                "declaratory_resolution": (
                    "Res. No. 2118 del 26 de diciembre de 2025"
                ),
                "follow_up_sources": [
                    {
                        "source_id": "diario_oficial",
                        "source_name": "Diario Oficial — Imprenta Nacional",
                        "url": "https://svrpubindc.imprenta.gov.co/diario/index.xhtml",
                        "search_hint": "Rionegro MRO Res. No. 2118",
                        "purpose": "Verify official publication.",
                    }
                ],
            }
        ],
        detected_entities=["mincit"],
        detected_topics=["regulatory"],
    )
    health = SourceHealth(
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas (Estadísticas)",
        url="https://zf.mincit.gov.co/estadisticas",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="parsed_content",
        parsed_content_count=1,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-15T15:10:31Z",
    )

    candidate = out["candidates"][0]
    assert candidate["question_seed"] == (
        "Will the named zona-franca declaration or extension be confirmed "
        "in the next official follow-up window?"
    )
    assert candidate["resolution_source"] == (
        "MinCIT approved-zones registry, Diario Oficial, SUIN, or Gestor Normativo."
    )
    assert candidate["follow_up_sources"][0]["source_id"] == "diario_oficial"


def test_mincit_candidate_prefers_matched_official_resolution_followup() -> None:
    cluster = _cluster(
        cluster_id="c-mincit-zf-official-match",
        title="MinCIT zona franca registry change — Rionegro MRO",
        summary="Official MinCIT registry change has a Diario Oficial match.",
        source_types=["regulatory"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["mincit_zonas_francas"],
        member_source_names=["MinCIT — Zonas Francas (Estadísticas)"],
        member_titles=["MinCIT zona franca registry change — Rionegro MRO"],
        member_urls=["https://zf.mincit.gov.co/estadisticas#zf-1"],
        member_metadata=[
            {
                "registry": "mincit_zonas_francas_aprobadas",
                "registry_change_type": "new_registry_row",
                "zona_franca_name": (
                    "Zona Franca Permanente Especial De Servicios Rionegro MRO"
                ),
                "declaratory_resolution": (
                    "Res. No. 2118 del 26 de diciembre de 2025"
                ),
                "official_resolution_matches": [
                    {
                        "source_id": "diario_oficial",
                        "source_name": "Diario Oficial — Imprenta Nacional",
                        "url": "https://svrpubindc.imprenta.gov.co/diario?edicion=53.490",
                        "title": "Diario Oficial 53.490",
                        "legal_act_label": "Resolución 2118 de 2025",
                        "match_basis": (
                            "legal_act_number_year_with_mincit_context"
                        ),
                    }
                ],
            }
        ],
        detected_entities=["mincit"],
        detected_topics=["regulatory"],
    )
    health = SourceHealth(
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas (Estadísticas)",
        url="https://zf.mincit.gov.co/estadisticas",
        raw_count=1,
        cleaned_count=1,
        dated_count=1,
        rankable_count=1,
        failure_count=0,
        content_mode="parsed_content",
        parsed_content_count=1,
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        source_health=[health],
        generated_at="2026-05-15T15:10:31Z",
    )

    follow_up = out["candidates"][0]["follow_up_sources"][0]
    assert follow_up["source_id"] == "diario_oficial"
    assert follow_up["search_hint"] == "Resolución 2118 de 2025"
    assert follow_up["match_basis"] == "legal_act_number_year_with_mincit_context"


def test_generic_senado_agenda_pdf_is_not_m2_ready_without_entry_parse() -> None:
    cluster = _cluster(
        cluster_id="c-senado-generic-agenda",
        title="Agenda Legislativa del 4 al 8 de mayo de 2026 ( pdf, 1.03 MB )",
        summary="Agenda Legislativa del 4 al 8 de mayo de 2026",
        source_types=["calendar"],
        signal_types=["calendar_event"],
        member_source_ids=["senado_agenda_legislativa"],
        member_source_names=["Senado — Agenda Legislativa Actual"],
        member_titles=[
            "Agenda Legislativa del 4 al 8 de mayo de 2026 ( pdf, 1.03 MB )"
        ],
        member_urls=[
            "https://www.senado.gov.co/index.php/documentos/agenda/file"
        ],
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )

    out = build_m1_candidates(
        _summary(),
        [cluster],
        [],
        topic_keywords=[],
        generated_at="2026-05-15T15:10:31Z",
    )

    assert out["candidates"] == []
    assert out["rejected"][0]["reason"] == (
        "Senado agenda PDF lacks a parsed bill/action entry"
    )
