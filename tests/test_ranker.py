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
