from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .forecastability import (
    deadline_hint,
    forecastability_reasons,
    forecastability_score,
    is_forecastable_candidate,
    noise_reasons,
    question_seed,
    resolution_hint,
)
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
INDICATOR_LIMIT = 14
ANALYST_ATTENTION_LIMIT = 8
SOURCE_ACTION_LIMIT = 8
HIGH_VALUE_UNDERCOVERED_SOURCES = frozenset({"dian_proyectos_normas"})
MONTHLY_LAG_WARNING_DAYS = 120
FORECASTABLE_SIGNAL_LIMIT = 8
REJECTED_SIGNAL_LIMIT = 6
M2_SEED_LIMIT = 8


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


def _cluster_links(cluster: Cluster, limit: int = 3) -> str:
    seen: set[str] = set()
    lines: list[str] = []
    for url, title, src in zip(
        cluster.member_urls, cluster.member_titles, cluster.member_source_names
    ):
        if url in seen:
            continue
        seen.add(url)
        lines.append(f"- [{title.strip() or '(no title)'}]({url}) — {src}")
        if len(lines) >= limit:
            break
    return "\n".join(lines) if lines else "- _None._"


def _forecastable_clusters(
    clusters: list[Cluster],
    limit: int = FORECASTABLE_SIGNAL_LIMIT,
) -> list[Cluster]:
    candidates = [cluster for cluster in clusters if is_forecastable_candidate(cluster)]
    candidates.sort(
        key=lambda c: (forecastability_score(c), c.score, c.source_count),
        reverse=True,
    )
    return candidates[:limit]


def _render_forecastable_signal(idx: int, cluster: Cluster) -> str:
    reasons = forecastability_reasons(cluster)
    caveats = noise_reasons(cluster)
    caveat_text = "; ".join(caveats) if caveats else "none"
    return (
        f"### {idx}. {cluster.title}\n\n"
        f"- Forecastability score: {forecastability_score(cluster):.1f}\n"
        f"- Source count: {cluster.source_count}\n"
        f"- Source types: {', '.join(cluster.source_types) or 'n/a'}\n"
        f"- Latest update: {cluster.latest_published_at or 'n/a'}\n"
        f"- Why this is forecastable: {', '.join(reasons) or 'needs analyst review'}\n"
        f"- Caveats: {caveat_text}\n\n"
        f"**Signal:** {cluster.summary or cluster.title}\n\n"
        f"**Question seed:** {question_seed(cluster)}\n\n"
        f"**Likely resolution source:** {resolution_hint(cluster)}\n\n"
        f"**Deadline/window hint:** {deadline_hint(cluster)}\n\n"
        "**Missing evidence to ask for in M2:** primary-source details, current "
        "status, exact deadline, and whether the event is already resolved.\n\n"
        f"**Links:**\n{_cluster_links(cluster)}\n"
    )


def _render_forecastable_signals(clusters: list[Cluster]) -> str:
    candidates = _forecastable_clusters(clusters)
    if not candidates:
        return "_No deterministic forecastable event candidates passed the M1 filter._"
    return "\n---\n\n".join(
        _render_forecastable_signal(i + 1, cluster)
        for i, cluster in enumerate(candidates)
    )


def _render_rejected_signals(clusters: list[Cluster]) -> str:
    rejected: list[tuple[float, Cluster, list[str]]] = []
    for cluster in clusters:
        reasons = noise_reasons(cluster)
        if reasons or not is_forecastable_candidate(cluster):
            rejected.append((cluster.score, cluster, reasons or ["weak forecastability"]))
    if not rejected:
        return "_No obvious rejected/noisy top signals._"
    lines = []
    for _, cluster, reasons in rejected[:REJECTED_SIGNAL_LIMIT]:
        lines.append(f"- **{cluster.title}:** {'; '.join(reasons)}.")
    return "\n".join(lines)


def _render_candidate_db_summary(
    m1_candidates: dict | None,
    acceptance_report: dict | None = None,
) -> str:
    if not m1_candidates:
        return "- `m1_candidates.json`: _not generated for this render._"
    candidates = m1_candidates.get("candidates", [])
    rejected = m1_candidates.get("rejected", [])
    caveats = m1_candidates.get("source_caveats", [])
    status = (acceptance_report or {}).get("status", "unknown")
    errors = (acceptance_report or {}).get("error_count", "n/a")
    warnings = (acceptance_report or {}).get("warning_count", "n/a")
    return (
        f"- `m1_candidates.json`: {len(candidates)} candidates, "
        f"{len(rejected)} rejected signals, {len(caveats)} source caveats.\n"
        f"- `acceptance_report.json`: status={status}; errors={errors}; "
        f"warnings={warnings}."
    )


def _candidate_links(candidate: dict, limit: int = 3) -> str:
    evidence = candidate.get("evidence") if isinstance(candidate, dict) else {}
    links = evidence.get("links") if isinstance(evidence, dict) else []
    if not isinstance(links, list) or not links:
        return "- _None._"
    lines: list[str] = []
    seen: set[str] = set()
    for link in links:
        if not isinstance(link, dict):
            continue
        url = str(link.get("url") or "")
        if not url or url in seen:
            continue
        seen.add(url)
        title = str(link.get("title") or "(no title)")
        source = str(link.get("source_name") or "source")
        lines.append(f"- [{title}]({url}) — {source}")
        if len(lines) >= limit:
            break
    return "\n".join(lines) if lines else "- _None._"


def _render_candidate_event_signals(m1_candidates: dict | None) -> str | None:
    if not m1_candidates:
        return None
    candidates = [
        candidate
        for candidate in m1_candidates.get("candidates", [])
        if isinstance(candidate, dict)
        and candidate.get("candidate_type") == "event_signal"
    ][:FORECASTABLE_SIGNAL_LIMIT]
    if not candidates:
        return "_No deterministic forecastable event candidates passed the M1 filter._"
    blocks: list[str] = []
    for idx, candidate in enumerate(candidates, 1):
        evidence = candidate.get("evidence") or {}
        entities = ", ".join(candidate.get("entities") or []) or "n/a"
        topics = ", ".join(candidate.get("topics") or []) or "n/a"
        blocks.append(
            f"### {idx}. {candidate.get('trigger') or candidate.get('question_seed')}\n\n"
            f"- Candidate ID: `{candidate.get('candidate_id')}`\n"
            f"- Forecastability score: "
            f"{(candidate.get('m1_scores') or {}).get('forecastability_score')}\n"
            f"- Entities: {entities}\n"
            f"- Topics: {topics}\n"
            f"- Why this is forecastable: "
            f"{', '.join(candidate.get('reasons') or []) or 'needs analyst review'}\n"
            f"- Caveats: "
            f"{'; '.join(candidate.get('noise_reasons') or []) or 'none'}\n\n"
            f"**Signal:** {evidence.get('starting_evidence') or candidate.get('trigger') or 'n/a'}\n\n"
            f"**Question seed:** {candidate.get('question_seed')}\n\n"
            f"**Likely resolution source:** {candidate.get('resolution_source')}\n\n"
            f"**Deadline/window hint:** {candidate.get('deadline_or_window')}\n\n"
            f"**Missing evidence to ask for in M2:** "
            f"{'; '.join(candidate.get('missing_evidence') or []) or 'n/a'}\n\n"
            f"**Links:**\n{_candidate_links(candidate)}\n"
        )
    return "\n---\n\n".join(blocks)


def _render_rejected_candidates(m1_candidates: dict | None) -> str | None:
    if not m1_candidates:
        return None
    rejected = [
        item for item in m1_candidates.get("rejected", []) if isinstance(item, dict)
    ][:REJECTED_SIGNAL_LIMIT]
    if not rejected:
        return "_No obvious rejected/noisy top signals._"
    return "\n".join(
        f"- **{item.get('title') or item.get('origin_id')}:** {item.get('reason')}."
        for item in rejected
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
        "| Source | Onboarding | Status | Acceptance | Content | Raw | Dated | Rankable | Tagged | Untagged | Doc links | Parsed | Failures |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for health in source_health:
        lines.append(
            f"| `{health.source_id}` | {health.onboarding_status} | "
            f"{health.status} | {health.acceptance_status} | "
            f"{health.content_mode} | {health.raw_count} | "
            f"{health.dated_count} | {health.rankable_count} | "
            f"{health.tagged_count} | {health.untagged_rankable_count} | "
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

    if indicator.indicator_id == "ise_activity":
        annual_growth = indicator.values.get("annual_growth_pct")
        if isinstance(annual_growth, int | float) and annual_growth >= 3:
            alerts.append(
                f"`activity_acceleration`: ISE annual growth is "
                f"{annual_growth:.2f}%."
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


def _indicator_seed_questions(
    indicators: list[IndicatorObservation],
    run_date: str,
) -> list[dict[str, str]]:
    seeds: list[dict[str, str]] = []
    by_id = {indicator.indicator_id: indicator for indicator in indicators}

    trm = by_id.get("trm_usd_cop")
    if trm and any("`material_move`" in a for a in _indicator_alerts(trm, indicators, run_date)):
        value = trm.values.get("trm_cop_per_usd")
        value_text = f"{value:.2f} COP/USD" if isinstance(value, int | float) else "the latest official TRM"
        seeds.append(
            {
                "theme": "FX move persistence",
                "trigger": trm.headline,
                "question": (
                    "Will the official TRM remain at least 2% weaker than its "
                    "seven-day-ago level seven calendar days after this run?"
                ),
                "resolution": "Superintendencia Financiera / datos.gov.co official TRM.",
                "deadline": "Seven calendar days after the run date.",
                "missing": f"Market context for why TRM moved; current reference level is {value_text}.",
            }
        )

    policy = by_id.get("policy_rate_ibr")
    if policy and any("`liquidity_spread`" in a for a in _indicator_alerts(policy, indicators, run_date)):
        seeds.append(
            {
                "theme": "BanRep policy/liquidity",
                "trigger": policy.headline,
                "question": (
                    "Will Banco de la Republica change the policy rate at the "
                    "next board decision?"
                ),
                "resolution": "BanRep board communique and official policy-rate series.",
                "deadline": "Next scheduled BanRep board decision.",
                "missing": "Next meeting date, inflation expectations, board guidance, and market pricing.",
            }
        )

    ise = by_id.get("ise_activity")
    if ise and any(
        "`activity_acceleration`" in a
        for a in _indicator_alerts(ise, indicators, run_date)
    ):
        seeds.append(
            {
                "theme": "Activity acceleration",
                "trigger": ise.headline,
                "question": (
                    "Will the next DANE ISE release show annual growth of at "
                    "least 3.0%?"
                ),
                "resolution": "DANE ISE next monthly release.",
                "deadline": "Next DANE ISE release.",
                "missing": (
                    "Activity-group contribution details, base effects, and "
                    "confirmation from retail, manufacturing, electricity, and "
                    "tax collection."
                ),
            }
        )

    manufacturing = by_id.get("manufacturing")
    if manufacturing and any(
        "`cross_indicator_tension`" in a
        for a in _indicator_alerts(manufacturing, indicators, run_date)
    ):
        seeds.append(
            {
                "theme": "Activity divergence",
                "trigger": manufacturing.headline,
                "question": (
                    "Will the next DANE EMMET release still show negative real "
                    "manufacturing sales year over year?"
                ),
                "resolution": "DANE EMMET next monthly release.",
                "deadline": "Next DANE manufacturing release.",
                "missing": "Subsector drivers, electricity demand trend, inventories, and import/capital-goods context.",
            }
        )

    fiscal = by_id.get("fiscal_tax_pulse")
    if fiscal and any("`real_terms_warning`" in a for a in _indicator_alerts(fiscal, indicators, run_date)):
        seeds.append(
            {
                "theme": "Fiscal revenue stress",
                "trigger": fiscal.headline,
                "question": (
                    "Will the next DIAN monthly tax-collection release again "
                    "show nominal gross revenue growth below annual IPC?"
                ),
                "resolution": "DIAN monthly tax-collection XLSX and DANE IPC.",
                "deadline": "Next DIAN monthly collection release.",
                "missing": "Withholding, VAT, customs, fiscal-plan assumptions, and whether calendar effects explain the miss.",
            }
        )

    trade = by_id.get("external_trade")
    if trade and any("`mixed_period_components`" in a for a in _indicator_alerts(trade, indicators, run_date)):
        seeds.append(
            {
                "theme": "External trade alignment",
                "trigger": trade.headline,
                "question": (
                    "When exports and imports are observed for the same period, "
                    "will Colombia's goods trade balance improve year over year?"
                ),
                "resolution": "DANE/DIAN exports and imports releases for the same reference month.",
                "deadline": "Next import release that aligns with the latest export period.",
                "missing": "Same-period import data, oil/fuel export detail, and capital-goods import drivers.",
            }
        )

    oil = by_id.get("oil_gas_production")
    if oil and any("`observation_lag`" in a for a in _indicator_alerts(oil, indicators, run_date)):
        seeds.append(
            {
                "theme": "Hydrocarbon data lag",
                "trigger": oil.headline,
                "question": (
                    "Will ANH publish a newer consolidated oil/gas production "
                    "period within the next 30 days?"
                ),
                "resolution": "ANH official production statistics or datos.gov.co Socrata mirrors.",
                "deadline": "30 days after the run date.",
                "missing": "Normal ANH publication lag and whether the current dataset mirror is delayed.",
            }
        )

    return seeds[:M2_SEED_LIMIT]


def _render_m2_seed_questions(
    indicators: list[IndicatorObservation],
    run_date: str,
) -> str:
    seeds = _indicator_seed_questions(indicators, run_date)
    if not seeds:
        return "_No deterministic indicator-driven seed questions fired._"
    blocks = []
    for idx, seed in enumerate(seeds, 1):
        blocks.append(
            f"### {idx}. {seed['theme']}\n\n"
            f"- Trigger: {seed['trigger'] or 'n/a'}\n"
            f"- Question seed: {seed['question']}\n"
            f"- Likely resolution source: {seed['resolution']}\n"
            f"- Deadline/window hint: {seed['deadline']}\n"
            f"- Missing evidence: {seed['missing']}\n"
        )
    return "\n---\n\n".join(blocks)


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
    m1_candidates: dict | None = None,
    acceptance_report: dict | None = None,
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
        "## M2 Seed Questions\n\n"
        f"{_render_m2_seed_questions(indicator_watch or [], run_summary.run_date)}\n\n"
        "## Candidate DB\n\n"
        f"{_render_candidate_db_summary(m1_candidates, acceptance_report)}\n\n"
        "## Forecastable Signals\n\n"
        f"{_render_candidate_event_signals(m1_candidates) or _render_forecastable_signals(ranked_clusters)}\n\n"
        "## Top Signals\n\n"
        f"{top_section}\n\n"
        "## Emerging Questions\n\n"
        "- Use `M2 Seed Questions` and `Forecastable Signals` as the first-pass queue.\n\n"
        "## Topics to Monitor\n\n"
        f"{keywords_section}\n\n"
        "## Rejected / Noisy Top Signals\n\n"
        f"{_render_rejected_candidates(m1_candidates) or _render_rejected_signals(ranked_clusters)}\n\n"
        "## Source Health Actions\n\n"
        f"{_render_source_health_actions(source_health or [])}\n\n"
        "## Source Health\n\n"
        f"{_render_source_health(source_health or [])}\n\n"
        "## Noisy / Low-Confidence Items\n\n"
        f"{_render_low_quality(cleaned_items)}\n\n"
        "## Source Failures\n\n"
        f"{_render_failures(failures)}\n\n"
        "## Suggested Next Step\n\n"
        "- Paste `m2_handoff.md` plus `prompts/question_selection.md` into an AI to run M2 question selection.\n"
    )


def _render_handoff_instructions() -> str:
    return (
        "You are running M2 question selection for Colombia Forecasting Desk. "
        "Use only the evidence in this handoff unless you explicitly label a "
        "gap as missing evidence. Do not browse, estimate probabilities, write "
        "investment/trading/betting advice, or draft posts. Produce 5-10 "
        "candidate forecast questions, reject weak items, score candidates, "
        "and select the top 1-3 for evidence-pack research. Each selected "
        "question must have clear resolution criteria, a likely resolution "
        "source, a deadline/window, and missing evidence."
    )


def _render_source_caveats(source_health: list[SourceHealth]) -> str:
    if not source_health:
        return "- No source-health report was available."
    caveats: list[tuple[int, int, str]] = []
    by_id = {health.source_id: health for health in source_health}
    if "eltiempo_colombia" in by_id:
        caveats.append(
            (
                4,
                0,
                "- `eltiempo_colombia` is a rolling RSS media pulse, not guaranteed "
                "full-day coverage unless a local cache/scheduler is running.",
            )
        )
    for index, health in enumerate(source_health):
        if health.failure_count:
            caveats.append(
                (
                    0,
                    index,
                    f"- `{health.source_id}` failed during this run; absence of "
                    "signals from it is not evidence of no activity.",
                )
            )
        elif health.document_link_count and health.parsed_content_count == 0:
            caveats.append(
                (
                    1,
                    index,
                    f"- `{health.source_id}` is link-only in this run; M2 should "
                    "ask for document contents before relying on the signal.",
                )
            )
        elif health.status in {"no_raw", "no_rankable"} and health.onboarding_status == "needs_parser":
            priority = (
                2
                if health.source_id in HIGH_VALUE_UNDERCOVERED_SOURCES
                else 3
            )
            caveats.append(
                (
                    priority,
                    index,
                    f"- `{health.source_id}` is undercovered (`{health.status}`); "
                    "treat silence from this domain as unknown.",
                )
            )
    lines = [line for _, _, line in sorted(caveats)]
    return "\n".join(lines[:SOURCE_ACTION_LIMIT]) if lines else "- No major caveats."


def render_m2_handoff(
    run_summary: RunSummary,
    ranked_clusters: list[Cluster],
    failures: list[SourceFailure],
    topic_keywords: list[str],
    source_health: list[SourceHealth] | None = None,
    indicator_watch: list[IndicatorObservation] | None = None,
    m1_candidates: dict | None = None,
    acceptance_report: dict | None = None,
) -> str:
    indicators = indicator_watch or []
    health = source_health or []
    forecastable = _render_forecastable_signals(ranked_clusters)
    seeds = _render_m2_seed_questions(indicators, run_summary.run_date)
    keywords = (
        "\n".join(f"- {keyword}" for keyword in topic_keywords)
        if topic_keywords
        else "- _None._"
    )
    failures_text = _render_failures(failures)

    return (
        f"# M2 Question Selection Handoff — {run_summary.run_date}\n\n"
        "## Task For The AI\n\n"
        f"{_render_handoff_instructions()}\n\n"
        "## Run Snapshot\n\n"
        f"- Run date: {run_summary.run_date}\n"
        f"- Sources checked: {run_summary.sources_checked}\n"
        f"- Sources failed: {run_summary.sources_failed}\n"
        f"- Raw items: {run_summary.raw_items}\n"
        f"- Cleaned items: {run_summary.cleaned_items}\n"
        f"- Clusters: {run_summary.clusters}\n\n"
        "## Analyst Attention\n\n"
        f"{_render_analyst_attention(indicators, health, run_summary.run_date)}\n\n"
        "## Indicator-Driven Seed Questions\n\n"
        f"{seeds}\n\n"
        "## Candidate DB Snapshot\n\n"
        f"{_render_candidate_db_summary(m1_candidates, acceptance_report)}\n\n"
        "## Forecastable Event Signals\n\n"
        f"{_render_candidate_event_signals(m1_candidates) or forecastable}\n\n"
        "## Rejected / Noisy Signals\n\n"
        f"{_render_rejected_candidates(m1_candidates) or _render_rejected_signals(ranked_clusters)}\n\n"
        "## Source Coverage Caveats\n\n"
        f"{_render_source_caveats(health)}\n\n"
        "## Source Failures\n\n"
        f"{failures_text}\n\n"
        "## Topics To Monitor\n\n"
        f"{keywords}\n\n"
        "## Required M2 Output Schema\n\n"
        "For each candidate question, return:\n\n"
        "- `question`: precise yes/no or numeric-threshold forecast question\n"
        "- `why_now`: one sentence tied to a signal above\n"
        "- `interest_score`: 1-5\n"
        "- `forecastability_score`: 1-5\n"
        "- `evidence_score`: 1-5\n"
        "- `freshness_score`: 1-5\n"
        "- `risk_score`: 1-5 where 5 is highest risk\n"
        "- `resolution_source`: primary source that decides the outcome\n"
        "- `deadline_or_window`: date or explicit window\n"
        "- `missing_evidence`: what must be checked before probability estimation\n"
        "- `decision`: `select_for_evidence_pack` or `reject`\n\n"
        "Select the top 1-3 questions for evidence packs and briefly explain "
        "why rejected questions were rejected.\n"
    )
