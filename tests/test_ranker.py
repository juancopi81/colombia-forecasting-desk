from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
    assert top_sources.count("eltiempo_colombia") <= 4
