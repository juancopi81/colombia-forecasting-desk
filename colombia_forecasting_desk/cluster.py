from __future__ import annotations

import hashlib
import re
from collections import Counter

from .models import CleanedItem, Cluster
from .stopwords_es import STOPWORDS_ES
from .tagger import fold_accents, sort_entity_tags, sort_topic_tags

JACCARD_THRESHOLD = 0.4
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)
_GENERIC_IMPRENTA_TITLE_RE = re.compile(
    r"^(?:gaceta del congreso\s+\d+|diario oficial\s+[\d.]+)"
    r"(?:\s+[-]\s+(?:senado de la republica|camara de representantes|"
    r"edicion ordinaria|edicion extraordinaria|ordinaria|extraordinaria))?$",
    re.IGNORECASE,
)
_STRONG_TAG_PAIRS = frozenset(
    {
        ("banrep", "monetary_policy"),
        ("banrep", "inflation"),
        ("dane", "inflation"),
        ("dane", "labor_market"),
        ("dane", "external_trade"),
        ("dane", "construction"),
        ("dian", "fiscal_tax"),
        ("dian", "external_trade"),
        ("dian", "regulatory"),
        ("congreso", "labor_market"),
        ("congreso", "fiscal_tax"),
        ("congreso", "external_trade"),
        ("congreso", "energy"),
        ("congreso", "hydrocarbons"),
        ("congreso", "electoral"),
        ("congreso", "regulatory"),
        ("congreso", "security"),
        ("registraduria", "electoral"),
        ("cne", "electoral"),
        ("corte_constitucional", "constitutional_court"),
        ("corte_constitucional", "regulatory"),
        ("corte_constitucional", "electoral"),
        ("corte_constitucional", "fiscal_tax"),
        ("minhacienda", "fiscal_tax"),
        ("mincit", "regulatory"),
        ("anh", "hydrocarbons"),
        ("anh", "energy"),
        ("secop", "procurement"),
        ("presidencia", "security"),
        ("presidencia", "fiscal_tax"),
        ("presidencia", "energy"),
        ("presidencia", "hydrocarbons"),
        ("presidencia", "electoral"),
        ("presidencia", "regulatory"),
    }
)


def tokenize_title(title: str) -> set[str]:
    folded = fold_accents(title.lower())
    return {
        t for t in _TOKEN_RE.findall(folded) if len(t) > 2 and t not in STOPWORDS_ES
    }


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _is_generic_imprenta_listing(item: CleanedItem) -> bool:
    if item.source_id not in {"diario_oficial", "gacetas_congreso"}:
        return False
    normalized = item.title.lower().replace("—", "-").replace("–", "-")
    normalized = fold_accents(normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return bool(_GENERIC_IMPRENTA_TITLE_RE.match(normalized))


def _can_union_by_title(a: CleanedItem, b: CleanedItem) -> bool:
    if (
        _is_generic_imprenta_listing(a)
        and _is_generic_imprenta_listing(b)
        and a.title != b.title
    ):
        return False
    return True


def _can_union_by_tags(a: CleanedItem, b: CleanedItem) -> bool:
    if a.source_id == b.source_id:
        return False
    if _is_generic_imprenta_listing(a) or _is_generic_imprenta_listing(b):
        return False
    shared_entities = set(a.detected_entities) & set(b.detected_entities)
    shared_topics = set(a.detected_topics) & set(b.detected_topics)
    if not shared_entities or not shared_topics:
        return False
    return any(
        (entity, topic) in _STRONG_TAG_PAIRS
        for entity in shared_entities
        for topic in shared_topics
    )


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # keep smaller index as root for stable group ordering
            root, other = (ra, rb) if ra < rb else (rb, ra)
            self.parent[other] = root


def _cluster_id(member_ids: list[str]) -> str:
    digest = hashlib.sha1("|".join(sorted(member_ids)).encode("utf-8")).hexdigest()
    return f"c-{digest[:10]}"


def _confidence(source_count: int, items_count: int) -> str:
    if source_count >= 3 or items_count >= 5:
        return "high"
    if source_count >= 2 or items_count >= 2:
        return "medium"
    return "low"


def _build_cluster(members: list[CleanedItem]) -> Cluster:
    ids = [m.id for m in members]
    cid = _cluster_id(ids)
    title_member = max(members, key=lambda m: (len(m.title), m.title))
    longest_title = title_member.title
    summary = title_member.summary
    source_ids = sorted({m.source_id for m in members})
    source_types = sorted({m.source_type for m in members})
    signal_types = sorted({m.signal_type for m in members})
    priorities = [m.priority for m in members]
    detected_entities = sort_entity_tags(
        {tag for m in members for tag in m.detected_entities}
    )
    detected_topics = sort_topic_tags(
        {tag for m in members for tag in m.detected_topics}
    )
    timestamps = [m.published_at for m in members if m.published_at]
    latest = max(timestamps) if timestamps else None
    return Cluster(
        cluster_id=cid,
        title=longest_title,
        summary=summary,
        items=ids,
        source_count=len(source_ids),
        source_types=source_types,
        latest_published_at=latest,
        signal_types=signal_types,
        confidence=_confidence(len(source_ids), len(members)),
        score=0.0,
        member_urls=[m.url for m in members],
        member_titles=[m.title for m in members],
        member_source_names=[m.source_name for m in members],
        member_source_ids=[m.source_id for m in members],
        member_metadata=[dict(m.metadata or {}) for m in members],
        priorities=priorities,
        detected_entities=detected_entities,
        detected_topics=detected_topics,
        why_it_matters="",
        possible_questions=[],
        missing_evidence=[],
        recommended_next_sources=[],
    )


def cluster(items: list[CleanedItem], threshold: float = JACCARD_THRESHOLD) -> list[Cluster]:
    n = len(items)
    if n == 0:
        return []
    token_sets = [tokenize_title(it.title) for it in items]
    uf = _UnionFind(n)
    for i in range(n):
        for j in range(i + 1, n):
            if (
                token_sets[i]
                and token_sets[j]
                and _can_union_by_title(items[i], items[j])
                and jaccard(token_sets[i], token_sets[j]) >= threshold
            ):
                uf.union(i, j)
            elif _can_union_by_tags(items[i], items[j]):
                uf.union(i, j)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        root = uf.find(i)
        groups.setdefault(root, []).append(i)

    clusters: list[Cluster] = []
    for indices in groups.values():
        members = [items[i] for i in sorted(indices, key=lambda k: items[k].id)]
        clusters.append(_build_cluster(members))
    clusters.sort(key=lambda c: c.cluster_id)
    return clusters


def topic_keywords(items: list[CleanedItem], top_n: int = 5) -> list[str]:
    counter: Counter[str] = Counter()
    for it in items:
        counter.update(tokenize_title(it.title))
    return [w for w, _ in counter.most_common(top_n)]
