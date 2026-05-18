from __future__ import annotations

from colombia_forecasting_desk.brief import render_brief, render_m2_handoff
from colombia_forecasting_desk.models import (
    Cluster,
    IndicatorComponent,
    IndicatorObservation,
    RunSummary,
    SourceFailure,
    SourceHealth,
)


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
        "## Analyst Attention",
        "## Indicator Watch",
        "## M2 Seed Questions",
        "## Candidate DB",
        "## Forecastable Signals",
        "## Top Signals",
        "## Emerging Questions",
        "## Topics to Monitor",
        "## Rejected / Noisy Top Signals",
        "## Source Health Actions",
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
    assert "Use `M2 Seed Questions`" in out


def test_brief_renders_onboarding_and_status_columns(make_cleaned) -> None:
    summary = RunSummary(
        run_date="2026-04-27",
        started_at="2026-04-27T12:00:00Z",
        finished_at="2026-04-27T12:00:30Z",
        sources_checked=2,
        sources_failed=0,
        raw_items=10,
        cleaned_items=10,
        clusters=1,
    )
    health = [
        SourceHealth(
            source_id="ok_src",
            source_name="OK",
            url="https://example.com/ok",
            raw_count=10,
            cleaned_count=10,
            dated_count=10,
            rankable_count=10,
            failure_count=0,
            onboarding_status="working",
            status="ok",
            content_mode="html_or_api",
        ),
        SourceHealth(
            source_id="needs_src",
            source_name="Needs",
            url="https://example.com/needs",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=0,
            onboarding_status="needs_parser",
            status="no_raw",
            content_mode="no_items",
        ),
    ]
    out = render_brief(
        summary, [], [], [make_cleaned()], topic_keywords=[], source_health=health
    )
    assert "Onboarding" in out
    assert "Content" in out
    assert "html_or_api" in out
    assert "needs_parser" in out
    assert "no_raw" in out


def test_brief_puts_indicator_watch_before_top_signals(make_cleaned) -> None:
    summary = RunSummary(
        run_date="2026-05-06",
        started_at="2026-05-06T12:00:00Z",
        finished_at="2026-05-06T12:00:30Z",
        sources_checked=2,
        sources_failed=0,
        raw_items=10,
        cleaned_items=10,
        clusters=1,
    )
    indicator = IndicatorObservation(
        indicator_id="trm_usd_cop",
        name="TRM / USD-COP",
        category="markets",
        status="observed",
        frequency="daily",
        source_name="SFC",
        source_url="https://example.com/trm",
        period="2026-05-06",
        release_date="2026-05-06T00:00:00Z",
        headline="TRM vigente: 3723.33 COP/USD.",
        values={"seven_day_change_pct": 2.46},
        freshness_status="current",
    )
    out = render_brief(
        summary,
        [_cluster()],
        [],
        [make_cleaned()],
        topic_keywords=[],
        indicator_watch=[indicator],
    )
    assert out.index("## Analyst Attention") < out.index("## Indicator Watch")
    assert out.index("## Indicator Watch") < out.index("## Top Signals")
    assert "`material_move`" in out


def test_brief_renders_cross_indicator_alerts_and_source_actions(make_cleaned) -> None:
    summary = RunSummary(
        run_date="2026-05-06",
        started_at="2026-05-06T12:00:00Z",
        finished_at="2026-05-06T12:00:30Z",
        sources_checked=3,
        sources_failed=0,
        raw_items=10,
        cleaned_items=10,
        clusters=1,
    )
    ipc = IndicatorObservation(
        indicator_id="ipc_inflation",
        name="IPC / inflation",
        category="prices",
        status="observed",
        frequency="monthly",
        source_name="DANE",
        source_url="https://example.com/ipc",
        period="2026-03",
        values={"annual_variation_pct": 5.56},
        freshness_status="current",
    )
    fiscal = IndicatorObservation(
        indicator_id="fiscal_tax_pulse",
        name="Fiscal / tax pulse",
        category="fiscal",
        status="observed",
        frequency="monthly",
        source_name="DIAN",
        source_url="https://example.com/dian",
        period="2026-03",
        values={"gross_tax_revenue_annual_variation_pct": 1.44},
        freshness_status="current",
    )
    trade = IndicatorObservation(
        indicator_id="external_trade",
        name="External trade",
        category="external",
        status="observed",
        frequency="monthly",
        source_name="DANE",
        source_url="https://example.com/trade",
        period="2026-03",
        freshness_status="current",
        components=[
            IndicatorComponent(
                component_id="exports",
                name="Exports",
                status="observed",
                source_name="DANE",
                source_url="https://example.com/exports",
                period="2026-03",
            ),
            IndicatorComponent(
                component_id="imports",
                name="Imports",
                status="observed",
                source_name="DANE",
                source_url="https://example.com/imports",
                period="2026-02",
            ),
        ],
    )
    health = [
        SourceHealth(
            source_id="registraduria_noticias",
            source_name="Registraduría",
            url="https://example.com/registraduria",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=1,
            onboarding_status="needs_parser",
            status="failed",
            content_mode="failed",
        ),
        SourceHealth(
            source_id="dane_comunicados_prensa",
            source_name="DANE",
            url="https://example.com/dane",
            raw_count=15,
            cleaned_count=15,
            dated_count=15,
            rankable_count=2,
            failure_count=0,
            onboarding_status="working",
            status="ok",
            content_mode="pdf_links_only",
            document_link_count=15,
            parsed_content_count=0,
        )
    ]
    out = render_brief(
        summary,
        [],
        [],
        [make_cleaned()],
        topic_keywords=[],
        source_health=health,
        indicator_watch=[ipc, fiscal, trade],
    )
    assert "`real_terms_warning`" in out
    assert "`mixed_period_components`" in out
    assert "document links but no parsed content" in out
    assert out.index("is failing") < out.index("document links but no parsed content")


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


def test_m2_handoff_is_paste_ready(make_cleaned) -> None:
    summary = RunSummary(
        run_date="2026-05-06",
        started_at="2026-05-06T12:00:00Z",
        finished_at="2026-05-06T12:00:30Z",
        sources_checked=2,
        sources_failed=0,
        raw_items=10,
        cleaned_items=10,
        clusters=1,
    )
    indicator = IndicatorObservation(
        indicator_id="trm_usd_cop",
        name="TRM / USD-COP",
        category="markets",
        status="observed",
        frequency="daily",
        source_name="SFC",
        source_url="https://example.com/trm",
        period="2026-05-06",
        headline="TRM vigente: 3723.33 COP/USD.",
        values={"trm_cop_per_usd": 3723.33, "seven_day_change_pct": 2.46},
        freshness_status="current",
    )
    health = [
        SourceHealth(
            source_id="eltiempo_colombia",
            source_name="El Tiempo",
            url="https://example.com/rss",
            raw_count=10,
            cleaned_count=10,
            dated_count=10,
            rankable_count=10,
            failure_count=0,
            onboarding_status="working",
            status="ok",
            content_mode="html_or_api",
        )
    ]

    out = render_m2_handoff(
        summary,
        [_cluster()],
        [],
        ["banrep"],
        source_health=health,
        indicator_watch=[indicator],
    )

    assert "# M2 Question Selection Handoff" in out
    assert "Task For The AI" in out
    assert "Indicator-Driven Seed Questions" in out
    assert "Forecastable Event Signals" in out
    assert "Candidate DB Snapshot" in out
    assert "Required M2 Output Schema" in out
    assert "combines RSS with Colombia section cards" in out
    assert "Will the official TRM remain" in out


def test_m2_handoff_prioritizes_document_caveats_before_cap() -> None:
    summary = RunSummary(
        run_date="2026-05-15",
        started_at="2026-05-15T12:00:00Z",
        finished_at="2026-05-15T12:00:30Z",
        sources_checked=10,
        sources_failed=3,
        raw_items=100,
        cleaned_items=90,
        clusters=8,
    )

    def health(
        source_id: str,
        *,
        status: str = "ok",
        content_mode: str = "html_or_api",
        failure_count: int = 0,
        onboarding_status: str = "working",
        document_link_count: int = 0,
        parsed_content_count: int = 0,
    ) -> SourceHealth:
        return SourceHealth(
            source_id=source_id,
            source_name=source_id,
            url=f"https://example.com/{source_id}",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=failure_count,
            onboarding_status=onboarding_status,
            status=status,
            content_mode=content_mode,
            document_link_count=document_link_count,
            parsed_content_count=parsed_content_count,
        )

    source_health = [
        health("eltiempo_colombia"),
        health("banrep_junta_comunicados", status="failed", content_mode="failed", failure_count=1),
        health("registraduria_noticias", status="failed", content_mode="failed", failure_count=1),
        health("minhacienda_proyectos_decreto", status="failed", content_mode="failed", failure_count=1),
        health("corte_constitucional_comunicados", status="no_raw", onboarding_status="needs_parser"),
        health("presidencia_noticias", status="no_raw", onboarding_status="needs_parser"),
        health("centro_nacional_consultoria", status="no_raw", onboarding_status="needs_parser"),
        health("diario_oficial", content_mode="document_links_only", document_link_count=10),
        health("gacetas_congreso", content_mode="document_links_only", document_link_count=10),
        SourceHealth(
            source_id="dian_proyectos_normas",
            source_name="DIAN proyectos normas",
            url="https://example.com/dian",
            raw_count=28,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=0,
            onboarding_status="needs_parser",
            status="no_rankable",
            content_mode="mixed_document_and_html_links",
            document_link_count=5,
            parsed_content_count=0,
        ),
    ]

    out = render_m2_handoff(
        summary,
        [],
        [],
        [],
        source_health=source_health,
    )

    assert "`gacetas_congreso` is link-only" in out
    assert "`dian_proyectos_normas` is link-only" in out


def test_m2_handoff_keeps_high_value_undercovered_sources_visible() -> None:
    summary = RunSummary(
        run_date="2026-05-15",
        started_at="2026-05-15T12:00:00Z",
        finished_at="2026-05-15T12:00:30Z",
        sources_checked=10,
        sources_failed=3,
        raw_items=100,
        cleaned_items=90,
        clusters=8,
    )

    def health(source_id: str, *, status: str = "no_raw") -> SourceHealth:
        return SourceHealth(
            source_id=source_id,
            source_name=source_id,
            url=f"https://example.com/{source_id}",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=0,
            onboarding_status="needs_parser",
            status=status,
            content_mode="no_items",
        )

    source_health = [
        SourceHealth(
            source_id="banrep_junta_comunicados",
            source_name="BanRep",
            url="https://example.com/banrep",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=1,
            status="failed",
            content_mode="failed",
        ),
        SourceHealth(
            source_id="registraduria_noticias",
            source_name="Registraduria",
            url="https://example.com/registraduria",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=1,
            status="failed",
            content_mode="failed",
        ),
        SourceHealth(
            source_id="minhacienda_proyectos_decreto",
            source_name="MinHacienda",
            url="https://example.com/minhacienda",
            raw_count=0,
            cleaned_count=0,
            dated_count=0,
            rankable_count=0,
            failure_count=1,
            status="failed",
            content_mode="failed",
        ),
        SourceHealth(
            source_id="diario_oficial",
            source_name="Diario Oficial",
            url="https://example.com/diario",
            raw_count=10,
            cleaned_count=10,
            dated_count=10,
            rankable_count=0,
            failure_count=0,
            content_mode="document_links_only",
            document_link_count=10,
        ),
        SourceHealth(
            source_id="gacetas_congreso",
            source_name="Gacetas",
            url="https://example.com/gacetas",
            raw_count=10,
            cleaned_count=10,
            dated_count=10,
            rankable_count=0,
            failure_count=0,
            content_mode="document_links_only",
            document_link_count=10,
        ),
        health("camara_agenda_consolidada", status="no_rankable"),
        health("corte_constitucional_comunicados"),
        health("presidencia_noticias"),
        health("centro_nacional_consultoria"),
        health("dian_proyectos_normas", status="no_rankable"),
    ]

    out = render_m2_handoff(
        summary,
        [],
        [],
        [],
        source_health=source_health,
    )

    assert "`dian_proyectos_normas` is undercovered" in out
