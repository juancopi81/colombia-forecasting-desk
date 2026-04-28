from __future__ import annotations

from colombia_forecasting_desk import pipeline
from colombia_forecasting_desk.models import Metasource, RawItem


def _source() -> Metasource:
    return Metasource(
        id="test_source",
        name="Test Source",
        url="https://example.com/feed.xml",
        type="news",
        country_relevance="high",
        access_status="rss_public",
        fetch_method="rss",
        priority="medium",
        update_frequency="daily",
        trust_role="media_signal",
        parsing_difficulty="easy",
        enabled=True,
        notes="",
    )


def _raw(**overrides) -> RawItem:
    base = dict(
        id="item-1",
        source_id="test_source",
        source_name="Test Source",
        source_type="news",
        url="https://example.com/item-1",
        title="Colombia policy update with enough words",
        fetched_at="2026-04-27T12:00:00Z",
        published_at="2026-04-27T11:00:00Z",
        raw_text="This item has enough text to avoid low-quality short-text flags.",
        metadata={},
    )
    base.update(overrides)
    return RawItem(**base)


def test_run_date_controls_age_filter_and_low_quality_stays_out_of_clusters(
    monkeypatch,
    tmp_path,
) -> None:
    source = _source()
    items = [
        _raw(
            id="edge",
            url="https://example.com/edge",
            title="Edge dated item still inside the fourteen day window",
            published_at="2026-04-13T23:59:59Z",
        ),
        _raw(
            id="old",
            url="https://example.com/old",
            title="Old item outside the fourteen day window",
            published_at="2026-04-12T23:59:59Z",
        ),
        _raw(
            id="future",
            url="https://example.com/future",
            title="Future item outside the requested as-of window",
            published_at="2026-04-28T00:00:00Z",
        ),
        _raw(
            id="undated",
            url="https://example.com/undated",
            title="Undated item with enough words to avoid short text flags",
            published_at=None,
            raw_text="This item has enough text to avoid low-quality short-text flags.",
        ),
        _raw(
            id="short",
            url="https://example.com/short",
            title="Short",
            published_at="2026-04-27T11:00:00Z",
            raw_text="",
        ),
    ]

    monkeypatch.setattr(pipeline, "load_metasources", lambda _: [source])
    monkeypatch.setattr(pipeline, "fetch_all", lambda _: (items, []))

    result = pipeline.run(date="2026-04-27", runs_root=tmp_path)

    cleaned_ids = {item.id for item in result.cleaned_items}
    clustered_ids = {item_id for cluster in result.clusters for item_id in cluster.items}

    assert "edge" in cleaned_ids
    assert "old" not in cleaned_ids
    assert "future" not in cleaned_ids
    assert "undated" in cleaned_ids
    assert "undated" not in clustered_ids
    assert "short" in cleaned_ids
    assert "short" not in clustered_ids
    assert result.run_dir == tmp_path / "2026-04-27"


def test_run_rejects_invalid_date(tmp_path) -> None:
    try:
        pipeline.run(date="2026/04/27", runs_root=tmp_path)
    except ValueError as exc:
        assert "--date must use YYYY-MM-DD format" in str(exc)
    else:
        raise AssertionError("expected invalid date to fail")
