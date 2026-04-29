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
    max_items: int | None = None
    verify_ssl: bool = True


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
    member_source_ids: list[str] = field(default_factory=list)
    priorities: list[str] = field(default_factory=list)
    why_it_matters: str = ""
    possible_questions: list[str] = field(default_factory=list)
    missing_evidence: list[str] = field(default_factory=list)
    recommended_next_sources: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SourceFailure:
    source_id: str
    source_name: str
    url: str
    error_class: str
    error_message: str
    occurred_at: str


@dataclass(frozen=True, slots=True)
class SourceHealth:
    source_id: str
    source_name: str
    url: str
    raw_count: int
    cleaned_count: int
    dated_count: int
    rankable_count: int
    failure_count: int
    failures: list[str] = field(default_factory=list)


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
