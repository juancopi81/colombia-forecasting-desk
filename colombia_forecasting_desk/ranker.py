from __future__ import annotations

import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from math import floor

from .forecastability import forecastability_score
from .models import Cluster

_PRIORITY_WEIGHT = {"high": 2, "medium": 1, "low": 0}
_PRIMARY_SOURCE_TYPES = {"official_updates", "legal", "calendar", "polling", "dataset"}
DEFAULT_DIVERSIFY_TOP_N = 10
DEFAULT_MAX_TOP_SOURCE_SHARE = 0.3

_STRATEGIC_TERMS = {
    "anh",
    "banrep",
    "candidato",
    "congreso",
    "corte",
    "dane",
    "deficit",
    "dian",
    "eleccion",
    "electoral",
    "eln",
    "farc",
    "fiscal",
    "gobierno",
    "inflacion",
    "jep",
    "minhacienda",
    "ministerio",
    "petro",
    "presidente",
    "reforma",
    "secop",
    "tasa",
    "trm",
}
_LOCAL_INCIDENT_TERMS = {
    "accidente",
    "aguacero",
    "asalto",
    "atraco",
    "captura",
    "capturaron",
    "colapso",
    "desbordamiento",
    "homicidio",
    "inundacion",
    "lluvia",
    "microtrafico",
    "robo",
    "viviendas",
}
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


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


def _primary_source_bonus(source_types: list[str]) -> float:
    return 1.5 if set(source_types) & _PRIMARY_SOURCE_TYPES else 0.0


def _text_terms(cluster: Cluster) -> set[str]:
    text = " ".join([cluster.title, cluster.summary]).lower()
    folded = (
        text.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    return set(_TOKEN_RE.findall(folded))


def _analyst_relevance_adjustment(cluster: Cluster) -> float:
    terms = _text_terms(cluster)
    adjustment = 0.0
    if terms & _STRATEGIC_TERMS:
        adjustment += 1.5
    if (
        cluster.source_count == 1
        and set(cluster.source_types) == {"news"}
        and terms & _LOCAL_INCIDENT_TERMS
        and not terms & _STRATEGIC_TERMS
    ):
        adjustment -= 2.5
    return adjustment


def score_cluster(cluster: Cluster, now: datetime | None = None) -> float:
    return (
        2.0 * cluster.source_count
        + 3.0 * _max_priority_weight(cluster.priorities)
        + _freshness_bonus(cluster.latest_published_at, now)
        + 1.0 * len(cluster.signal_types)
        + _primary_source_bonus(cluster.source_types)
        + _analyst_relevance_adjustment(cluster)
        + forecastability_score(cluster)
    )


def _source_keys(cluster: Cluster) -> set[str]:
    if cluster.member_source_ids:
        return set(cluster.member_source_ids)
    return set(cluster.member_source_names)


def _diversify_top(
    clusters: list[Cluster],
    top_n: int = DEFAULT_DIVERSIFY_TOP_N,
    max_source_share: float = DEFAULT_MAX_TOP_SOURCE_SHARE,
) -> list[Cluster]:
    if top_n <= 0 or not clusters:
        return clusters

    max_per_source = max(1, floor(top_n * max_source_share))
    selected: list[Cluster] = []
    deferred: list[Cluster] = []
    source_counts: dict[str, int] = {}

    for cluster in clusters:
        if len(selected) >= top_n:
            deferred.append(cluster)
            continue
        keys = _source_keys(cluster)
        if keys and all(source_counts.get(k, 0) >= max_per_source for k in keys):
            deferred.append(cluster)
            continue
        selected.append(cluster)
        for key in keys:
            source_counts[key] = source_counts.get(key, 0) + 1

    return selected + deferred


def rank(clusters: list[Cluster], now: datetime | None = None) -> list[Cluster]:
    scored = [replace(c, score=round(score_cluster(c, now), 2)) for c in clusters]
    scored.sort(key=lambda c: (-c.score, c.cluster_id))
    return _diversify_top(scored)
