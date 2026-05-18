from __future__ import annotations

import pytest

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
    assert result.source_health[0].source_id == "test_source"
    assert result.source_health[0].raw_count == 5
    assert result.source_health[0].dated_count == 4
    assert result.source_health[0].rankable_count == 1
    assert result.source_health[0].content_mode == "html_or_api"
    assert result.source_health[0].document_link_count == 0
    assert result.source_health[0].parsed_content_count == 0
    assert result.source_health[0].acceptance_status == "untagged"
    assert result.run_dir == tmp_path / "2026-04-27"
    assert (result.run_dir / "source_health.json").exists()
    assert (result.run_dir / "m2_handoff.md").exists()
    assert (result.run_dir / "m2_ranked_questions.json").exists()
    assert (result.run_dir / "m1_candidates.json").exists()
    assert (result.run_dir / "acceptance_report.json").exists()
    assert (result.run_dir / "run_manifest.json").exists()
    assert result.m2_ranked_questions["schema_version"] == "m2_legislative_ranking.v1"
    assert result.run_manifest["schema_version"] == "run_manifest.v1"
    assert result.run_manifest["capabilities"]["legislative_m2_ranking"] is True


def test_run_keeps_future_calendar_items_inside_planning_window(
    monkeypatch,
    tmp_path,
) -> None:
    source = Metasource(
        id="calendar_source",
        name="Calendar Source",
        url="https://example.com/calendar",
        type="calendar",
        country_relevance="high",
        access_status="html_public",
        fetch_method="html",
        priority="high",
        update_frequency="event_driven",
        trust_role="agenda_signal",
        parsing_difficulty="medium",
        enabled=True,
        notes="",
    )
    items = [
        RawItem(
            id="future-calendar",
            source_id="calendar_source",
            source_name="Calendar Source",
            source_type="calendar",
            url="https://example.com/calendar/future",
            title="Calendario electoral fecha limite de inscripcion",
            fetched_at="2026-05-06T12:00:00Z",
            published_at="2026-06-15T00:00:00Z",
            raw_text="Calendario electoral fecha limite de inscripcion presidencial.",
            metadata={},
        )
    ]

    monkeypatch.setattr(pipeline, "load_metasources", lambda _: [source])
    monkeypatch.setattr(pipeline, "fetch_all", lambda _: (items, []))

    result = pipeline.run(date="2026-05-06", runs_root=tmp_path)

    assert [item.id for item in result.cleaned_items] == ["future-calendar"]
    assert result.source_health[0].rankable_count == 1


def test_run_rejects_invalid_date(tmp_path) -> None:
    with pytest.raises(ValueError, match="--date must use YYYY-MM-DD format"):
        pipeline.run(date="2026/04/27", runs_root=tmp_path)


def test_run_single_source_writes_to_sandbox(monkeypatch, tmp_path) -> None:
    source = _source()
    items = [
        _raw(id="ok", url="https://example.com/ok"),
    ]

    captured: dict = {}

    def fake_fetch_all(sources):
        captured["sources"] = sources
        return items, []

    monkeypatch.setattr(pipeline, "load_metasources", lambda _: [source])
    monkeypatch.setattr(pipeline, "fetch_all", fake_fetch_all)

    result = pipeline.run_single_source(
        source_id="test_source",
        runs_root=tmp_path,
        date="2026-04-27",
    )

    assert [s.id for s in captured["sources"]] == ["test_source"]
    assert result.run_dir == tmp_path / "sandbox" / "test_source"
    assert (result.run_dir / "raw_items.json").exists()
    assert (result.run_dir / "source_health.json").exists()
    assert result.source_health[0].source_id == "test_source"
    assert result.source_health[0].rankable_count == 1
    assert (result.run_dir / "m2_ranked_questions.json").exists()
    assert (result.run_dir / "m1_candidates.json").exists()
    assert (result.run_dir / "acceptance_report.json").exists()
    assert (result.run_dir / "run_manifest.json").exists()


def test_run_single_source_unknown_id_raises(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(pipeline, "load_metasources", lambda _: [_source()])
    with pytest.raises(ValueError, match="not found"):
        pipeline.run_single_source(source_id="missing", runs_root=tmp_path)


@pytest.mark.parametrize(
    "raw,rankable,failures,expected",
    [
        (5, 3, 0, "ok"),
        (5, 0, 0, "no_rankable"),
        (0, 0, 0, "no_raw"),
        (3, 1, 1, "failed"),
        (0, 0, 1, "failed"),
    ],
)
def test_derive_status(raw, rankable, failures, expected) -> None:
    assert pipeline._derive_status(raw, rankable, failures) == expected


def test_derive_content_mode_identifies_document_link_only_sources() -> None:
    source_items = [
        _raw(
            id="pdf",
            url="https://example.com/reports/agenda.pdf",
            metadata={},
        ),
        _raw(
            id="xlsx",
            url="https://example.com/data/anexo.xlsx",
            metadata={},
        ),
    ]

    mode, document_links, parsed = pipeline._derive_content_mode(source_items, 0)

    assert mode == "document_links_only"
    assert document_links == 2
    assert parsed == 0


def test_derive_content_mode_treats_imprenta_rows_as_document_links() -> None:
    mode, document_links, parsed = pipeline._derive_content_mode(
        [
            _raw(
                url="https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=421",
                metadata={"extraction": "imprenta_nacional_jsf_table"},
            )
        ],
        0,
    )

    assert mode == "document_links_only"
    assert document_links == 1
    assert parsed == 0


def test_derive_content_mode_identifies_spreadsheet_link_only_sources() -> None:
    mode, document_links, parsed = pipeline._derive_content_mode(
        [_raw(url="https://example.com/data/anexo.xlsx")],
        0,
    )

    assert mode == "spreadsheet_links_only"
    assert document_links == 1
    assert parsed == 0


def test_derive_content_mode_identifies_parsed_document_content() -> None:
    mode, document_links, parsed = pipeline._derive_content_mode(
        [
            _raw(
                url="https://example.com/reports/agenda.pdf",
                metadata={"content_extraction": "pdf_text"},
            )
        ],
        0,
    )

    assert mode == "parsed_content"
    assert document_links == 0
    assert parsed == 1


def test_source_acceptance_status_helpers() -> None:
    assert pipeline._source_acceptance_status("failed", 0, 0, 0, 0) == "failed"
    assert (
        pipeline._source_acceptance_status("ok", 2, 0, 0, 0)
        == "document_unparsed"
    )
    assert pipeline._source_acceptance_status("no_raw", 0, 0, 0, 0) == "no_raw"
    assert pipeline._source_acceptance_status("ok", 0, 0, 0, 2) == "untagged"
    assert pipeline._source_acceptance_status("ok", 0, 0, 2, 0) == "ok"


def test_source_health_propagates_onboarding_status(monkeypatch, tmp_path) -> None:
    source = _source()
    object.__setattr__(source, "onboarding_status", "needs_parser")
    monkeypatch.setattr(pipeline, "load_metasources", lambda _: [source])
    monkeypatch.setattr(pipeline, "fetch_all", lambda _: ([], []))

    result = pipeline.run(date="2026-04-27", runs_root=tmp_path)

    health = result.source_health[0]
    assert health.onboarding_status == "needs_parser"
    assert health.status == "no_raw"
