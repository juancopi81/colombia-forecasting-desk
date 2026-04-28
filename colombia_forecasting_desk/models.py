from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Metasource:
    id: str
    name: str
    url: str
    type: str
    country_relevance: str
    access_status: str
    fetch_method: str
    priority: str
    update_frequency: str
    trust_role: str
    parsing_difficulty: str
    enabled: bool
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "url": self.url,
            "type": self.type,
            "country_relevance": self.country_relevance,
            "access_status": self.access_status,
            "fetch_method": self.fetch_method,
            "priority": self.priority,
            "update_frequency": self.update_frequency,
            "trust_role": self.trust_role,
            "parsing_difficulty": self.parsing_difficulty,
            "enabled": self.enabled,
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class RawItem:
    id: str
    source_id: str
    source_name: str
    source_type: str
    url: str
    title: str
    fetched_at: str
    published_at: str | None = None
    raw_text: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_type": self.source_type,
            "url": self.url,
            "title": self.title,
            "fetched_at": self.fetched_at,
            "published_at": self.published_at,
            "raw_text": self.raw_text,
            "metadata": self.metadata,
        }


@dataclass(frozen=True, slots=True)
class CleanedItem:
    id: str
    source_id: str
    source_name: str
    source_type: str
    url: str
    title: str
    fetched_at: str
    published_at: str | None
    clean_text: str
    summary: str
    signal_type: str
    country_relevance: str
    quality_notes: str
    detected_entities: list[str] = field(default_factory=list)
    detected_topics: list[str] = field(default_factory=list)
    trust_role: str = ""
    priority: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_type": self.source_type,
            "url": self.url,
            "title": self.title,
            "fetched_at": self.fetched_at,
            "published_at": self.published_at,
            "clean_text": self.clean_text,
            "summary": self.summary,
            "signal_type": self.signal_type,
            "country_relevance": self.country_relevance,
            "quality_notes": self.quality_notes,
            "detected_entities": list(self.detected_entities),
            "detected_topics": list(self.detected_topics),
            "trust_role": self.trust_role,
            "priority": self.priority,
        }


@dataclass(frozen=True, slots=True)
class Cluster:
    cluster_id: str
    title: str
    summary: str
    items: list[str]
    source_count: int
    source_types: list[str]
    latest_published_at: str | None
    signal_types: list[str]
    confidence: str
    score: float = 0.0
    member_urls: list[str] = field(default_factory=list)
    member_titles: list[str] = field(default_factory=list)
    member_source_names: list[str] = field(default_factory=list)
    priorities: list[str] = field(default_factory=list)
    why_it_matters: str = ""
    possible_questions: list[str] = field(default_factory=list)
    missing_evidence: list[str] = field(default_factory=list)
    recommended_next_sources: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "title": self.title,
            "summary": self.summary,
            "items": list(self.items),
            "source_count": self.source_count,
            "source_types": list(self.source_types),
            "latest_published_at": self.latest_published_at,
            "signal_types": list(self.signal_types),
            "confidence": self.confidence,
            "score": self.score,
            "member_urls": list(self.member_urls),
            "member_titles": list(self.member_titles),
            "member_source_names": list(self.member_source_names),
            "priorities": list(self.priorities),
            "why_it_matters": self.why_it_matters,
            "possible_questions": list(self.possible_questions),
            "missing_evidence": list(self.missing_evidence),
            "recommended_next_sources": list(self.recommended_next_sources),
        }


@dataclass(frozen=True, slots=True)
class SourceFailure:
    source_id: str
    source_name: str
    url: str
    error_class: str
    error_message: str
    occurred_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "url": self.url,
            "error_class": self.error_class,
            "error_message": self.error_message,
            "occurred_at": self.occurred_at,
        }


@dataclass(frozen=True, slots=True)
class RunSummary:
    run_date: str
    started_at: str
    finished_at: str
    sources_checked: int
    sources_failed: int
    raw_items: int
    cleaned_items: int
    clusters: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_date": self.run_date,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "sources_checked": self.sources_checked,
            "sources_failed": self.sources_failed,
            "raw_items": self.raw_items,
            "cleaned_items": self.cleaned_items,
            "clusters": self.clusters,
        }
