from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .models import (
    CleanedItem,
    Cluster,
    IndicatorObservation,
    RunSummary,
    SourceFailure,
    SourceHealth,
)

TOP_SIGNALS_LIMIT = 10
LOW_QUALITY_LIMIT = 10
ERROR_MSG_TRUNCATE = 200
INDICATOR_LIMIT = 12
ANALYST_ATTENTION_LIMIT = 8
SOURCE_ACTION_LIMIT = 8
MONTHLY_LAG_WARNING_DAYS = 120


def _bullet_or_none(items: list[str], empty_text: str = "_None._") -> str:
    if not items:
        return empty_text
    return "\n".join(f"- {x}" for x in items)


def _max_priority(priorities: list[str]) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    if not priorities:
        return "low"
    return max(priorities, key=lambda p: order.get(p, 0))


def _render_cluster(idx: int, cluster: Cluster) -> str:
    priority = _max_priority(cluster.priorities)
    source_types = ", ".join(cluster.source_types) or "n/a"
    latest = cluster.latest_published_at or "n/a"
    questions = (
        "\n".join(f"- {q}" for q in cluster.possible_questions)
        if cluster.possible_questions
        else "- _(populated in M2)_"
    )
    missing = _bullet_or_none(cluster.missing_evidence, "- _(populated in M2)_")
    next_sources = _bullet_or_none(
        cluster.recommended_next_sources, "- _(populated in M2)_"
    )
    seen: set[str] = set()
    link_lines: list[str] = []
    for url, title, src in zip(
        cluster.member_urls, cluster.member_titles, cluster.member_source_names
    ):
        if url in seen:
            continue
        seen.add(url)
        title_safe = title.strip() or "(no title)"
        link_lines.append(f"- [{title_safe}]({url}) — {src}")
    links_block = "\n".join(link_lines) if link_lines else "- _None._"

    why = cluster.why_it_matters or "_TBD by analyst (populated in M2)._"

    return (
        f"### {idx}. {cluster.title}\n\n"
        f"- Priority: {priority}\n"
        f"- Confidence: {cluster.confidence}\n"
        f"- Source count: {cluster.source_count}\n"
        f"- Source types: {source_types}\n"
        f"- Signal types: {', '.join(cluster.signal_types) or 'unknown'}\n"
        f"- Latest update: {latest}\n"
        f"- Score: {cluster.score}\n\n"
        f"**Summary:** {cluster.summary or '_(no summary)_'}\n\n"
        f"**Why it may matter:** {why}\n\n"
        f"**Possible forecastable questions:**\n{questions}\n\n"
        f"**Missing evidence:**\n{missing}\n\n"
        f"**Recommended next sources:**\n{next_sources}\n\n"
        f"**Links:**\n{links_block}\n"
    )


def _render_failures(failures: list[SourceFailure]) -> str:
    if not failures:
        return "_No source failures during this run._"
    lines = []
    for f in failures:
        msg = f.error_message[:ERROR_MSG_TRUNCATE]
        lines.append(f"- `{f.source_id}`: {f.error_class}: {msg}")
    return "\n".join(lines)


def _render_low_quality(items: list[CleanedItem]) -> str:
    flagged = [it for it in items if it.quality_notes]
    if not flagged:
        return "_None flagged._"
    lines = []
    for it in flagged[:LOW_QUALITY_LIMIT]:
        lines.append(
            f"- `{it.source_id}`: {it.title or '(no title)'} — {it.quality_notes}"
        )
    return "\n".join(lines)


def _render_source_health(source_health: list[SourceHealth]) -> str:
    if not source_health:
        return "_No source health report generated._"
    lines = [
        "| Source | Onboarding | Status | Content | Raw | Dated | Rankable | Doc links | Parsed | Failures |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for health in source_health:
        lines.append(
            f"| `{health.source_id}` | {health.onboarding_status} | "
            f"{health.status} | {health.content_mode} | {health.raw_count} | "
            f"{health.dated_count} | {health.rankable_count} | "
            f"{health.document_link_count} | {health.parsed_content_count} | "
            f"{health.failure_count} |"
        )
    return "\n".join(lines)


def _source_health_actions(source_health: list[SourceHealth]) -> list[str]:
    actions: list[tuple[int, str]] = []
    for health in source_health:
        if health.failure_count:
            actions.append(
                (
                    0,
                    f"`{health.source_id}` is failing "
                    f"({health.failure_count} failure"
                    f"{'s' if health.failure_count != 1 else ''}); check access "
                    "or alternate endpoints.",
                )
            )
            continue
        if health.document_link_count and health.parsed_content_count == 0:
            actions.append(
                (
                    1,
                    f"`{health.source_id}` exposes {health.document_link_count} "
                    "document links but no parsed content; evaluate a lightweight "
                    "parser or structured substitute.",
                )
            )
            continue
        if health.onboarding_status == "needs_parser" and health.status in {
            "no_raw",
            "no_rankable",
        }:
            actions.append(
                (
                    2,
                    f"`{health.source_id}` is marked `needs_parser` with "
                    f"`{health.status}`; prioritize only if this source is "
                    "decision-useful.",
                )
            )
            continue
        if health.onboarding_status == "working" and health.status == "no_rankable":
            actions.append(
                (
                    3,
                    f"`{health.source_id}` is enabled as working but produced no "
                    "rankable signal; either improve extraction or downgrade "
                    "onboarding.",
                )
            )
    return [message for _, message in sorted(actions)[:SOURCE_ACTION_LIMIT]]


def _render_source_health_actions(source_health: list[SourceHealth]) -> str:
    actions = _source_health_actions(source_health)
    if not actions:
        return "_No immediate source-health actions._"
    return "\n".join(f"- {action}" for action in actions)


def _format_indicator_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, dict):
        parts = [f"{k}={v}" for k, v in sorted(value.items())]
        return ", ".join(parts)
    if isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            rows = []
            for item in value:
                parts = [f"{k}={v}" for k, v in sorted(item.items())]
                rows.append("{" + ", ".join(parts) + "}")
            return "; ".join(rows)
        return ", ".join(str(item) for item in value)
    return str(value)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None


def _period_start(period: str) -> date | None:
    if not period:
        return None
    if len(period) == 10:
        return _parse_date(period)
    if len(period) == 7 and period[4] == "-":
        try:
            return date(int(period[:4]), int(period[5:7]), 1)
        except ValueError:
            return None
    if "-Q" in period:
        year_text, quarter_text = period.split("-Q", 1)
        try:
            quarter = int(quarter_text)
            return date(int(year_text), 1 + (quarter - 1) * 3, 1)
        except ValueError:
            return None
    return None


def _indicator_by_id(
    indicators: list[IndicatorObservation], indicator_id: str
) -> IndicatorObservation | None:
    for indicator in indicators:
        if indicator.indicator_id == indicator_id:
            return indicator
    return None


def _indicator_alerts(
    indicator: IndicatorObservation,
    indicators: list[IndicatorObservation],
    run_date: str,
) -> list[str]:
    alerts: list[str] = []
    if indicator.status == "observed" and indicator.freshness_status not in {
        "current",
        "unknown",
    }:
        alerts.append(
            f"`stale_observation`: freshness is `{indicator.freshness_status}`."
        )

    run_day = _parse_date(run_date)
    period_day = _period_start(indicator.period)
    if (
        run_day
        and period_day
        and indicator.frequency.startswith("monthly")
        and (run_day - period_day).days > MONTHLY_LAG_WARNING_DAYS
    ):
        alerts.append(
            "`observation_lag`: latest observed period is more than four months "
            "behind the run date."
        )

    if indicator.indicator_id == "trm_usd_cop":
        move = indicator.values.get("seven_day_change_pct")
        if isinstance(move, int | float) and abs(move) >= 2:
            direction = "depreciation" if move > 0 else "appreciation"
            alerts.append(
                f"`material_move`: seven-day USD/COP move is {move:.2f}% "
                f"({direction})."
            )

    if indicator.indicator_id == "policy_rate_ibr":
        spread = indicator.values.get("ibr_policy_spread_pp")
        if isinstance(spread, int | float) and abs(spread) >= 0.5:
            alerts.append(
                f"`liquidity_spread`: IBR-policy spread is {spread:.2f} pp."
            )

    if indicator.indicator_id == "external_trade":
        periods = {
            component.period
            for component in indicator.components
            if component.status == "observed" and component.period
        }
        if len(periods) > 1:
            alerts.append(
                "`mixed_period_components`: exports/imports are observed for "
                "different periods, so the trade balance should not be forced."
            )

    if indicator.indicator_id == "fiscal_tax_pulse":
        ipc = _indicator_by_id(indicators, "ipc_inflation")
        nominal_tax = indicator.values.get("gross_tax_revenue_annual_variation_pct")
        annual_ipc = ipc.values.get("annual_variation_pct") if ipc else None
        if isinstance(nominal_tax, int | float) and isinstance(annual_ipc, int | float):
            if nominal_tax < annual_ipc:
                alerts.append(
                    "`real_terms_warning`: nominal tax collection growth is below "
                    "annual IPC."
                )

    if indicator.indicator_id == "manufacturing":
        retail = _indicator_by_id(indicators, "retail_sales")
        manufacturing_sales = indicator.values.get("real_sales_annual_variation_pct")
        retail_sales = (
            retail.values.get("real_retail_sales_annual_variation_pct")
            if retail
            else None
        )
        if (
            isinstance(manufacturing_sales, int | float)
            and isinstance(retail_sales, int | float)
            and retail_sales >= 5
            and manufacturing_sales < 0
        ):
            alerts.append(
                "`cross_indicator_tension`: retail sales are strong while "
                "manufacturing real sales are negative."
            )

    return alerts


def _all_indicator_alerts(
    indicators: list[IndicatorObservation], run_date: str
) -> list[tuple[str, str]]:
    alerts: list[tuple[str, str]] = []
    for indicator in indicators:
        for alert in _indicator_alerts(indicator, indicators, run_date):
            alerts.append((indicator.name, alert))
    return alerts


def _render_analyst_attention(
    indicators: list[IndicatorObservation],
    source_health: list[SourceHealth],
    run_date: str,
) -> str:
    lines = [
        f"- **{name}:** {alert}"
        for name, alert in _all_indicator_alerts(indicators, run_date)[
            :ANALYST_ATTENTION_LIMIT
        ]
    ]
    if not lines:
        observed = sum(1 for item in indicators if item.status == "observed")
        lines.append(
            f"- Indicator Watch has {observed}/{len(indicators)} observed cards; "
            "no deterministic alert fired."
        )
    source_actions = _source_health_actions(source_health)
    if source_actions:
        lines.append(f"- **Source health:** {source_actions[0]}")
    return "\n".join(lines)


def _render_indicator_watch(
    indicators: list[IndicatorObservation], run_date: str
) -> str:
    if not indicators:
        return "_No indicator watch generated._"

    blocks: list[str] = []
    for indicator in indicators[:INDICATOR_LIMIT]:
        alerts = _indicator_alerts(indicator, indicators, run_date)
        alert_block = (
            "\n".join(f"- {alert}" for alert in alerts)
            if alerts
            else "- _No deterministic alert._"
        )
        display_values = {
            key: value for key, value in indicator.values.items() if key != "components"
        }
        values = (
            "\n".join(
                f"- `{key}`: {_format_indicator_value(value)}"
                for key, value in display_values.items()
            )
            if display_values
            else "- _Pending structured value._"
        )
        correlations = (
            "\n".join(f"- {item}" for item in indicator.correlations[:2])
            if indicator.correlations
            else "- _None defined._"
        )
        components = (
            "\n".join(
                f"- `{component.component_id}`: {component.status}"
                f"/{component.freshness_status}; period={component.period or 'n/a'}; "
                f"release={component.release_date or 'n/a'}"
                f"{' — ' + component.headline if component.headline else ''}"
                for component in indicator.components
            )
            if indicator.components
            else "- _No subcomponents._"
        )
        headline = indicator.headline or "_Not wired yet._"
        release = indicator.release_date or "n/a"
        period = indicator.period or "n/a"
        blocks.append(
            f"### {indicator.name}\n\n"
            f"- Status: {indicator.status}\n"
            f"- Freshness: {indicator.freshness_status}\n"
            f"- Category: {indicator.category}\n"
            f"- Frequency: {indicator.frequency}\n"
            f"- Period: {period}\n"
            f"- Latest release: {release}\n"
            f"- Source: [{indicator.source_name}]({indicator.source_url})\n\n"
            f"**Headline:** {headline}\n\n"
            f"**Alerts:**\n{alert_block}\n\n"
            f"**Values:**\n{values}\n\n"
            f"**Components:**\n{components}\n\n"
            f"**Why it matters:** {indicator.why_it_matters}\n\n"
            f"**Useful correlations:**\n{correlations}\n\n"
            f"**M1 next step:** {indicator.next_step}\n"
        )
    return "\n\n---\n\n".join(blocks)


def render_brief(
    run_summary: RunSummary,
    ranked_clusters: list[Cluster],
    failures: list[SourceFailure],
    cleaned_items: list[CleanedItem],
    topic_keywords: list[str],
    source_health: list[SourceHealth] | None = None,
    indicator_watch: list[IndicatorObservation] | None = None,
) -> str:
    top = ranked_clusters[:TOP_SIGNALS_LIMIT]
    top_blocks = [
        _render_cluster(i + 1, c) for i, c in enumerate(top)
    ]
    top_section = "\n---\n\n".join(top_blocks) if top_blocks else "_No clusters._"

    keywords_section = (
        "\n".join(f"- {w}" for w in topic_keywords) if topic_keywords else "_None._"
    )

    return (
        f"# Metasource Brief — {run_summary.run_date}\n\n"
        "## Run Summary\n\n"
        f"- Run date: {run_summary.run_date}\n"
        f"- Started at: {run_summary.started_at}\n"
        f"- Finished at: {run_summary.finished_at}\n"
        f"- Sources checked: {run_summary.sources_checked}\n"
        f"- Sources failed: {run_summary.sources_failed}\n"
        f"- Raw items collected: {run_summary.raw_items}\n"
        f"- Cleaned items retained: {run_summary.cleaned_items}\n"
        f"- Clusters created: {run_summary.clusters}\n\n"
        "## Analyst Attention\n\n"
        f"{_render_analyst_attention(indicator_watch or [], source_health or [], run_summary.run_date)}\n\n"
        "## Indicator Watch\n\n"
        f"{_render_indicator_watch(indicator_watch or [], run_summary.run_date)}\n\n"
        "## Top Signals\n\n"
        f"{top_section}\n\n"
        "## Emerging Questions\n\n"
        "- _(populated in M2)_\n\n"
        "## Topics to Monitor\n\n"
        f"{keywords_section}\n\n"
        "## Source Health Actions\n\n"
        f"{_render_source_health_actions(source_health or [])}\n\n"
        "## Source Health\n\n"
        f"{_render_source_health(source_health or [])}\n\n"
        "## Noisy / Low-Confidence Items\n\n"
        f"{_render_low_quality(cleaned_items)}\n\n"
        "## Source Failures\n\n"
        f"{_render_failures(failures)}\n\n"
        "## Suggested Next Step\n\n"
        "- Review top 3 clusters; propose forecastable questions in M2.\n"
    )
