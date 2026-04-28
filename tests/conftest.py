from __future__ import annotations

import pytest

from colombia_forecasting_desk.models import CleanedItem, Metasource, RawItem


@pytest.fixture
def sample_source() -> Metasource:
    return Metasource(
        id="banrep_rss",
        name="Banco de la República — RSS",
        url="https://www.banrep.gov.co/es/noticias-rss",
        type="official_updates",
        country_relevance="high",
        access_status="rss_public",
        fetch_method="rss",
        priority="high",
        update_frequency="daily",
        trust_role="official_signal",
        parsing_difficulty="easy",
        enabled=True,
        notes="",
    )


@pytest.fixture
def media_source() -> Metasource:
    return Metasource(
        id="eltiempo_colombia",
        name="El Tiempo — Colombia",
        url="https://www.eltiempo.com/rss/colombia.xml",
        type="news",
        country_relevance="high",
        access_status="rss_public",
        fetch_method="rss",
        priority="high",
        update_frequency="daily",
        trust_role="media_signal",
        parsing_difficulty="easy",
        enabled=True,
        notes="",
    )


def _raw(**overrides) -> RawItem:
    base = dict(
        id="abc123",
        source_id="banrep_rss",
        source_name="BanRep",
        source_type="official_updates",
        url="https://example.com/article",
        title="Some Title",
        fetched_at="2026-04-27T12:00:00Z",
        published_at="2026-04-27T11:00:00Z",
        raw_text="<p>Hello <b>world</b>.</p>",
        metadata={},
    )
    base.update(overrides)
    return RawItem(**base)


@pytest.fixture
def make_raw():
    return _raw


def _cleaned(**overrides) -> CleanedItem:
    base = dict(
        id="abc123",
        source_id="banrep_rss",
        source_name="BanRep",
        source_type="official_updates",
        url="https://example.com/article",
        title="Junta del Banco de la República mantiene la tasa de interés",
        fetched_at="2026-04-27T12:00:00Z",
        published_at="2026-04-27T11:00:00Z",
        clean_text="La junta directiva mantiene la tasa de interés en 9.5%.",
        summary="La junta directiva mantiene la tasa de interés en 9.5%.",
        signal_type="official_update",
        country_relevance="high",
        quality_notes="",
        trust_role="official_signal",
        priority="high",
    )
    base.update(overrides)
    return CleanedItem(**base)


@pytest.fixture
def make_cleaned():
    return _cleaned
