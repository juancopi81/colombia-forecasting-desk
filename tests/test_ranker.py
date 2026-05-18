from __future__ import annotations

from datetime import datetime, timedelta, timezone

from colombia_forecasting_desk.forecastability import (
    is_forecastable_candidate,
    noise_reasons,
)
from colombia_forecasting_desk.models import Cluster
from colombia_forecasting_desk.ranker import rank, score_cluster


def _cluster(**over) -> Cluster:
    base = dict(
        cluster_id="c-aaaaaa0001",
        title="t",
        summary="s",
        items=["i1"],
        source_count=1,
        source_types=["news"],
        latest_published_at=None,
        signal_types=["media_narrative"],
        confidence="low",
        score=0.0,
        member_urls=["https://e.com/a"],
        member_titles=["t"],
        member_source_names=["src"],
        member_source_ids=["src"],
        priorities=["medium"],
        why_it_matters="",
        possible_questions=[],
        missing_evidence=[],
        recommended_next_sources=[],
    )
    base.update(over)
    return Cluster(**base)


def test_multi_source_outscores_single_source() -> None:
    multi = _cluster(cluster_id="c-multi-0001", source_count=3, priorities=["medium", "medium", "medium"])
    single = _cluster(cluster_id="c-single-001", source_count=1, priorities=["medium"])
    assert score_cluster(multi) > score_cluster(single)


def test_fresh_outscores_stale() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    fresh = _cluster(
        cluster_id="c-fresh-00001",
        latest_published_at=(now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
    )
    stale = _cluster(
        cluster_id="c-stale-00001",
        latest_published_at=(now - timedelta(days=10)).isoformat().replace("+00:00", "Z"),
    )
    assert score_cluster(fresh, now) > score_cluster(stale, now)


def test_rank_sorts_descending_with_stable_ties() -> None:
    a = _cluster(cluster_id="c-aaaaaa0001", source_count=1)
    b = _cluster(cluster_id="c-bbbbbb0002", source_count=2)
    c = _cluster(cluster_id="c-cccccc0003", source_count=2)
    ranked = rank([a, b, c])
    # b and c tie on score; cluster_id breaks tie ascending
    assert [r.cluster_id for r in ranked] == [
        "c-bbbbbb0002", "c-cccccc0003", "c-aaaaaa0001",
    ]
    assert all(r.score >= 0 for r in ranked)


def test_priority_weight_applied() -> None:
    high = _cluster(cluster_id="c-high-000001", priorities=["high"])
    low = _cluster(cluster_id="c-low-0000001", priorities=["low"])
    assert score_cluster(high) > score_cluster(low)


def test_official_source_type_gets_bonus() -> None:
    official = _cluster(cluster_id="c-official", source_types=["official_updates"])
    media = _cluster(cluster_id="c-media", source_types=["news"])
    assert score_cluster(official) > score_cluster(media)


def test_rank_diversifies_top_sources() -> None:
    eltiempo = [
        _cluster(
            cluster_id=f"c-eltiempo-{i:02d}",
            member_source_ids=["eltiempo_colombia"],
            source_types=["news"],
            priorities=["high"],
        )
        for i in range(7)
    ]
    official = [
        _cluster(
            cluster_id=f"c-official-{i:02d}",
            member_source_ids=[f"official_{i}"],
            source_types=["official_updates"],
            priorities=["medium"],
        )
        for i in range(8)
    ]
    ranked = rank(eltiempo + official)
    top_sources = [
        source
        for cluster in ranked[:10]
        for source in set(cluster.member_source_ids)
    ]
    assert top_sources.count("eltiempo_colombia") <= 3


def test_single_source_local_incident_is_downgraded_against_strategic_news() -> None:
    now = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)
    local_incident = _cluster(
        cluster_id="c-local-incident",
        title="Lluvias causan desbordamiento y afectan viviendas en un municipio",
        summary="Autoridades atienden la emergencia local.",
        priorities=["high"],
        latest_published_at=now.isoformat().replace("+00:00", "Z"),
    )
    strategic = _cluster(
        cluster_id="c-strategic-news",
        title="Gobierno anuncia reforma fiscal ante presiones de recaudo",
        summary="La medida afecta el déficit y la política económica nacional.",
        priorities=["high"],
        latest_published_at=now.isoformat().replace("+00:00", "Z"),
    )
    assert score_cluster(strategic, now) > score_cluster(local_incident, now)


def test_forecastable_official_decision_beats_human_interest_news() -> None:
    now = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)
    official = _cluster(
        cluster_id="c-banrep-decision",
        title="La Junta Directiva del Banco de la Republica mantiene la tasa",
        summary="BanRep mantiene la tasa de politica monetaria.",
        source_types=["official_updates"],
        signal_types=["official_update"],
        priorities=["high"],
        latest_published_at=now.isoformat().replace("+00:00", "Z"),
    )
    curiosity = _cluster(
        cluster_id="c-curiosity-news",
        title="Organizacion evalua traslado de hipopotamos de Pablo Escobar",
        summary="La visita tecnica revisara el control de esta especie.",
        source_types=["news"],
        signal_types=["media_narrative"],
        priorities=["high"],
        latest_published_at=now.isoformat().replace("+00:00", "Z"),
    )

    assert score_cluster(official, now) > score_cluster(curiosity, now)


def test_opaque_gaceta_index_is_not_forecastable_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-gaceta-opaque",
        title="Gaceta del Congreso 421 — Senado de la República",
        summary="421 | Senado de la República | 05/05/2026",
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["gacetas_congreso"],
        member_source_names=["Gacetas del Congreso — Imprenta Nacional"],
        member_titles=["Gaceta del Congreso 421 — Senado de la República"],
        member_urls=["https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=421"],
        priorities=["high"],
    )

    assert not is_forecastable_candidate(cluster)
    assert "official publication index lacks document title or parsed text" in noise_reasons(
        cluster
    )


def test_generic_diario_ordinaria_title_is_not_forecastable_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-diario-ordinaria",
        title="Diario Oficial 53.491 — Ordinaria",
        summary=(
            "Diario Oficial legal-act identities: Decreto 502 de 2026, "
            "Decreto 500 de 2026."
        ),
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["diario_oficial"],
        member_source_names=["Diario Oficial — Imprenta Nacional"],
        member_titles=["Diario Oficial 53.491 — Ordinaria"],
        member_urls=["https://svrpubindc.imprenta.gov.co/diario?edicion=53.491"],
        priorities=["high"],
    )

    assert not is_forecastable_candidate(cluster)
    assert "official publication index lacks document title or parsed text" in noise_reasons(
        cluster
    )


def test_diario_final_act_row_is_resolution_evidence_not_forecast_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-diario-act",
        title="Diario Oficial 53.491 — Decreto 502 de 2026",
        summary=(
            "Diario Oficial 53.491 — Decreto 502 de 2026. "
            "Official Diario act excerpt."
        ),
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["diario_oficial"],
        member_source_names=["Diario Oficial — Imprenta Nacional"],
        member_titles=["Diario Oficial 53.491 — Decreto 502 de 2026"],
        member_urls=[
            "https://svrpubindc.imprenta.gov.co/diario?edicion=53.491#act-decreto-502-de-2026"
        ],
        priorities=["high"],
    )

    assert not is_forecastable_candidate(cluster)
    assert (
        "final Diario Oficial publication is resolution evidence, not an unresolved forecast"
        in noise_reasons(cluster)
    )


def test_mixed_diario_final_cluster_without_unresolved_identity_is_not_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-diario-cne-mixed",
        title="Diario Oficial 53.491 — Resolución 1002 de 2026",
        summary=(
            "Diario Oficial final act rows were clustered with a CNE polling "
            "filing because they shared broad electoral tags."
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
        detected_topics=["electoral", "regulatory"],
        priorities=["high", "high"],
    )

    assert not is_forecastable_candidate(cluster)
    assert (
        "mixed cluster includes final Diario Oficial publication without a clean unresolved decision identity"
        in noise_reasons(cluster)
    )


def test_mixed_diario_final_cluster_with_clean_gaceta_identity_can_remain_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-gaceta-diario-project",
        title=(
            "Gaceta del Congreso 485 — Proyecto de Ley 560 DE 2025 Cámara — "
            "subsidio de transporte del GLP"
        ),
        summary=(
            "Gaceta identifies Proyecto de Ley 560 DE 2025 Cámara; Diario "
            "publication is only follow-up context."
        ),
        source_count=2,
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["gacetas_congreso", "diario_oficial"],
        member_source_names=[
            "Gacetas del Congreso — Imprenta Nacional",
            "Diario Oficial — Imprenta Nacional",
        ],
        member_titles=[
            (
                "Gaceta del Congreso 485 — Proyecto de Ley 560 DE 2025 Cámara — "
                "subsidio de transporte del GLP"
            ),
            "Diario Oficial 53.491 — Ordinaria — Resolución 1002 de 2026",
        ],
        member_urls=[
            "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=485#project-560",
            "https://svrpubindc.imprenta.gov.co/diario?edicion=53.491#act-resolucion-1002-de-2026",
        ],
        member_metadata=[
            {
                "content_extraction": "gaceta_pdf_text",
                "document_row_type": "gaceta_bill_item",
                "project_label": "Proyecto de Ley 560 DE 2025 Cámara",
                "project_records": [
                    {"kind": "Proyecto de Ley", "number": "560", "year": "2025"}
                ],
            },
            {
                "content_extraction": "diario_oficial_pdf_text",
                "document_row_type": "diario_legal_act",
                "legal_act_records": [
                    {"kind": "Resolución", "number": "1002", "year": "2026"}
                ],
            },
        ],
        detected_entities=["congreso"],
        detected_topics=["legislative", "hydrocarbons"],
        priorities=["high", "medium"],
    )

    assert is_forecastable_candidate(cluster)
    assert noise_reasons(cluster) == []


def test_parsed_gaceta_project_is_forecastable_followup_evidence() -> None:
    cluster = _cluster(
        cluster_id="c-gaceta-parsed-project",
        title=(
            "Gaceta del Congreso 476 — Proyecto de Ley 550 DE 2026 "
            "Cámara y Senado — por la cual se adiciona el Presupuesto "
            "General de la Nación"
        ),
        summary=(
            "Extracted from official Gaceta PDF. AL PROYECTO DE LEY NÚMERO "
            "550 DE 2026 CÁMARA Y SENADO por la cual se adiciona el "
            "Presupuesto General de la Nación."
        ),
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["gacetas_congreso"],
        member_source_names=["Gacetas del Congreso — Imprenta Nacional"],
        member_titles=[
            "Gaceta del Congreso 476 — Proyecto de Ley 550 DE 2026 Cámara y Senado"
        ],
        member_urls=[
            "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=476"
        ],
        priorities=["high"],
    )

    assert is_forecastable_candidate(cluster)
    assert noise_reasons(cluster) == []


def test_clean_legislative_registry_project_is_forecastable_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-senado-registry",
        title=(
            "Senado registry — Proyecto de Ley 1 de 2025 Senado — "
            "por medio de la cual se establecen lineamientos en salud"
        ),
        summary=(
            "Proyecto de Ley 1 de 2025 Senado. Estado: pendiente discutir "
            "ponencia para primer debate en Senado."
        ),
        source_types=["legal"],
        signal_types=["court_or_regulatory_movement"],
        member_source_ids=["senado_leyes_registry"],
        member_source_names=["Senado — Sección de Leyes / Proyectos de Ley"],
        member_titles=[
            "Senado registry — Proyecto de Ley 1 de 2025 Senado — salud"
        ],
        member_urls=["https://leyes.senado.gov.co/api/get_detalle_pdly.php?id=9540"],
        priorities=["high"],
    )

    assert is_forecastable_candidate(cluster)
    assert noise_reasons(cluster) == []


def test_secop_only_cluster_is_not_forecastable_candidate() -> None:
    cluster = _cluster(
        cluster_id="c-secop-only",
        title="SECOP proceso de compraventa de materiales",
        summary="Proceso contractual publicado en SECOP.",
        source_types=["dataset"],
        signal_types=["new_data"],
        member_source_ids=["secop_i_procesos", "secop_ii_contratos"],
        member_source_names=["SECOP I", "SECOP II"],
        member_titles=["SECOP proceso", "SECOP contrato"],
        member_urls=["https://datos.gov.co/a", "https://datos.gov.co/b"],
        priorities=["high"],
    )

    assert not is_forecastable_candidate(cluster)
    assert "SECOP-only procurement rows belong in the procurement pulse unless they have a national hook" in noise_reasons(
        cluster
    )
