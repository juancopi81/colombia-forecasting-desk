from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .models import Cluster

_PRIORITY_WEIGHT = {"high": 2, "medium": 1, "low": 0}


def _max_priority_weight(priorities: list[str]) -> int:
    if not priorities:
        return 0
    return max(_PRIORITY_WEIGHT.get(p, 0) for p in priorities)


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        s = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _freshness_bonus(latest: str | None, now: datetime | None = None) -> float:
    dt = _parse_iso(latest)
    if dt is None:
        return 0.0
    current = now or datetime.now(timezone.utc)
    age = current - dt
    if age <= timedelta(hours=24):
        return 3.0
    if age <= timedelta(hours=72):
        return 1.0
    return 0.0


def score_cluster(cluster: Cluster, now: datetime | None = None) -> float:
    return (
        2.0 * cluster.source_count
        + 3.0 * _max_priority_weight(cluster.priorities)
        + _freshness_bonus(cluster.latest_published_at, now)
        + 1.0 * len(cluster.signal_types)
    )


def rank(clusters: list[Cluster], now: datetime | None = None) -> list[Cluster]:
    scored: list[Cluster] = []
    for c in clusters:
        s = score_cluster(c, now)
        scored.append(
            Cluster(
                cluster_id=c.cluster_id,
                title=c.title,
                summary=c.summary,
                items=c.items,
                source_count=c.source_count,
                source_types=c.source_types,
                latest_published_at=c.latest_published_at,
                signal_types=c.signal_types,
                confidence=c.confidence,
                score=round(s, 2),
                member_urls=c.member_urls,
                member_titles=c.member_titles,
                member_source_names=c.member_source_names,
                priorities=c.priorities,
                why_it_matters=c.why_it_matters,
                possible_questions=c.possible_questions,
                missing_evidence=c.missing_evidence,
                recommended_next_sources=c.recommended_next_sources,
            )
        )
    scored.sort(key=lambda c: (-c.score, c.cluster_id))
    return scored
