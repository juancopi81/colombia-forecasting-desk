from __future__ import annotations

import hashlib
import re
from collections import Counter

from .cleaner import fold_accents
from .models import CleanedItem, Cluster
from .stopwords_es import STOPWORDS_ES

JACCARD_THRESHOLD = 0.4
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def tokenize_title(title: str) -> set[str]:
    folded = fold_accents(title.lower())
    return {
        t for t in _TOKEN_RE.findall(folded) if len(t) > 2 and t not in STOPWORDS_ES
    }


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


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
    longest_title = max((m.title for m in members), key=lambda t: (len(t), t))
    summary = members[0].summary
    source_ids = sorted({m.source_id for m in members})
    source_types = sorted({m.source_type for m in members})
    signal_types = sorted({m.signal_type for m in members})
    priorities = [m.priority for m in members]
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
        priorities=priorities,
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
        if not token_sets[i]:
            continue
        for j in range(i + 1, n):
            if not token_sets[j]:
                continue
            if jaccard(token_sets[i], token_sets[j]) >= threshold:
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
