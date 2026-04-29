from __future__ import annotations

from colombia_forecasting_desk.brief import render_brief
from colombia_forecasting_desk.models import Cluster, RunSummary, SourceFailure


def _cluster() -> Cluster:
    return Cluster(
        cluster_id="c-aaaaaa0001",
        title="BanRep mantiene tasa de interés",
        summary="La junta mantiene la tasa.",
        items=["i1", "i2"],
        source_count=2,
        source_types=["official_updates", "news"],
        latest_published_at="2026-04-27T11:00:00Z",
        signal_types=["official_update", "media_narrative"],
        confidence="medium",
        score=8.0,
        member_urls=["https://banrep.gov.co/a", "https://eltiempo.com/b"],
        member_titles=["Decisión BanRep", "BanRep mantiene tasa de interés"],
        member_source_names=["BanRep", "El Tiempo"],
        priorities=["high", "high"],
    )


def test_brief_has_all_sections(make_cleaned) -> None:
    summary = RunSummary(
        run_date="2026-04-27",
        started_at="2026-04-27T12:00:00Z",
        finished_at="2026-04-27T12:00:30Z",
        sources_checked=5,
        sources_failed=1,
        raw_items=120,
        cleaned_items=80,
        clusters=12,
    )
    failures = [
        SourceFailure(
            source_id="dane_prensa",
            source_name="DANE",
            url="https://www.dane.gov.co/",
            error_class="HTTPError",
            error_message="503 Service Unavailable",
            occurred_at="2026-04-27T12:00:10Z",
        )
    ]
    cleaned = [
        make_cleaned(id="x", quality_notes="low_quality:short_text"),
        make_cleaned(id="y"),
    ]
    out = render_brief(
        summary,
        [_cluster()],
        failures,
        cleaned,
        topic_keywords=["banrep", "tasa", "junta"],
    )
    for heading in (
        "# Metasource Brief — 2026-04-27",
        "## Run Summary",
        "## Top Signals",
        "## Emerging Questions",
        "## Topics to Monitor",
        "## Source Health",
        "## Noisy / Low-Confidence Items",
        "## Source Failures",
        "## Suggested Next Step",
    ):
        assert heading in out, f"missing section: {heading}"
    assert "BanRep mantiene tasa de interés" in out
    assert "dane_prensa" in out
    assert "503 Service Unavailable" in out
    assert "low_quality:short_text" in out
    assert "(populated in M2)" in out


def test_brief_handles_no_clusters_no_failures() -> None:
    summary = RunSummary(
        run_date="2026-04-27",
        started_at="2026-04-27T12:00:00Z",
        finished_at="2026-04-27T12:00:30Z",
        sources_checked=0,
        sources_failed=0,
        raw_items=0,
        cleaned_items=0,
        clusters=0,
    )
    out = render_brief(summary, [], [], [], topic_keywords=[])
    assert "_No clusters._" in out
    assert "_No source failures during this run._" in out
    assert "_None flagged._" in out
