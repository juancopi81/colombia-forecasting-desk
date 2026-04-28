from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from .models import Cluster

_PRIORITY_WEIGHT = {"high": 2, "medium": 1, "low": 0}


def _max_priority_weight(priorities: list[str]) -> int:
    if not priorities:
        return 0
    return max(_PRIORITY_WEIGHT.get(p, 0) for p in priorities)


def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _freshness_bonus(latest: str | None, now: datetime | None = None) -> float:
    dt = parse_iso(latest)
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
    scored = [replace(c, score=round(score_cluster(c, now), 2)) for c in clusters]
    scored.sort(key=lambda c: (-c.score, c.cluster_id))
    return scored
