from __future__ import annotations

import hashlib
from datetime import date
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
    Cluster,
    IndicatorObservation,
    RunSummary,
    SourceFailure,
    SourceHealth,
)

SCHEMA_VERSION = "m1_candidates.v1"
DEFAULT_MISSING_EVIDENCE = [
    "primary-source details",
    "current status",
    "exact deadline",
    "whether the event is already resolved",
]
INDICATOR_SEED_LIMIT = 8
MONTHLY_LAG_WARNING_DAYS = 120


def build_m1_candidates(
    run_summary: RunSummary,
    ranked_clusters: list[Cluster],
    failures: list[SourceFailure],
    topic_keywords: list[str],
    source_health: list[SourceHealth] | None = None,
    indicator_watch: list[IndicatorObservation] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the deterministic M1-to-M2 candidate evidence contract."""
    health = source_health or []
    indicators = indicator_watch or []
    generated = generated_at or run_summary.finished_at

    event_candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    link_only_sources = _link_only_source_ids(health)
    for cluster in ranked_clusters:
        blocked_sources = sorted(set(cluster.member_source_ids) & link_only_sources)
        if blocked_sources:
            rejected.append(
                _rejected_cluster(
                    cluster,
                    topic_keywords,
                    reason="source is link-only in this run; document contents are missing",
                )
            )
        elif is_forecastable_candidate(cluster):
            event_candidates.append(
                _event_candidate(run_summary, cluster, topic_keywords)
            )
        else:
            rejected.append(_rejected_cluster(cluster, topic_keywords))

    indicator_candidates = _indicator_candidates(run_summary, indicators)

    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "generated_at": generated,
        "inputs": {
            "clusters_artifact": "clusters.json",
            "indicator_watch_artifact": "indicator_watch.json",
            "source_health_artifact": "source_health.json",
            "topic_keywords": _unique_sorted(topic_keywords),
            "failure_count": len(failures),
        },
        "candidates": indicator_candidates + event_candidates,
        "rejected": rejected,
        "source_caveats": _source_caveats(failures, health),
    }


def _event_candidate(
    run_summary: RunSummary, cluster: Cluster, topic_keywords: list[str]
) -> dict[str, Any]:
    question = question_seed(cluster)
    missing = cluster.missing_evidence or DEFAULT_MISSING_EVIDENCE
    source_ids = _unique_preserve_order(cluster.member_source_ids)
    source_names = _unique_preserve_order(cluster.member_source_names)
    links = _cluster_links(cluster)
    evidence_text = _evidence_text(cluster)
    entities = list(cluster.detected_entities)
    topics = list(cluster.detected_topics) or _matched_topic_keywords(
        cluster, topic_keywords
    )
    resolution = resolution_hint(cluster)
    deadline = deadline_hint(cluster)
    return {
        "candidate_id": _candidate_id(
            "event", run_summary.run_date, cluster.cluster_id, question
        ),
        "candidate_type": "event_signal",
        "origin_id": cluster.cluster_id,
        "source_ids": source_ids,
        "source_names": source_names,
        "event_type": _event_type(cluster.signal_types),
        "actor": entities[0] if entities else "",
        "topic": topics[0] if topics else "",
        "published_at": cluster.latest_published_at,
        "evidence_text": evidence_text,
        "question_seed": question,
        "trigger": evidence_text,
        "why_now": "Ranked M1 forecastable event signal.",
        "resolution_source": resolution,
        "resolution_source_hint": resolution,
        "deadline_or_window": deadline,
        "deadline_hint": deadline,
        "missing_evidence": list(missing),
        "m1_scores": {
            "rank_score": cluster.score,
            "forecastability_score": forecastability_score(cluster),
        },
        "reasons": forecastability_reasons(cluster),
        "noise_reasons": noise_reasons(cluster),
        "entities": entities,
        "topics": topics,
        "freshness": {
            "latest_published_at": cluster.latest_published_at,
            "freshness_status": "current" if cluster.latest_published_at else "unknown",
        },
        "evidence": {
            "source_ids": source_ids,
            "source_types": list(cluster.source_types),
            "item_ids": list(cluster.items),
            "links": links,
            "evidence_text": evidence_text,
            "starting_evidence": evidence_text,
        },
        "decision_hint": "candidate",
    }


def _rejected_cluster(
    cluster: Cluster,
    topic_keywords: list[str],
    reason: str | None = None,
) -> dict[str, Any]:
    cluster_noise = noise_reasons(cluster)
    reasons = forecastability_reasons(cluster)
    reject_reason = reason or (cluster_noise[0] if cluster_noise else "weak forecastability")
    entities = list(cluster.detected_entities)
    topics = list(cluster.detected_topics) or _matched_topic_keywords(
        cluster, topic_keywords
    )
    return {
        "origin_id": cluster.cluster_id,
        "candidate_type": "event_signal",
        "title": cluster.title,
        "reason": reject_reason,
        "reject_reason": reject_reason,
        "reasons": reasons,
        "noise_reasons": cluster_noise,
        "entities": entities,
        "topics": topics,
        "event_type": _event_type(cluster.signal_types),
        "actor": entities[0] if entities else "",
        "topic": topics[0] if topics else "",
        "published_at": cluster.latest_published_at,
        "evidence_text": _evidence_text(cluster),
        "m1_scores": {
            "rank_score": cluster.score,
            "forecastability_score": forecastability_score(cluster),
        },
        "source_ids": _unique_preserve_order(cluster.member_source_ids),
        "source_types": list(cluster.source_types),
    }


def _indicator_candidates(
    run_summary: RunSummary, indicators: list[IndicatorObservation]
) -> list[dict[str, Any]]:
    by_id = {indicator.indicator_id: indicator for indicator in indicators}
    seeds: list[tuple[IndicatorObservation, dict[str, str], list[str]]] = []

    trm = by_id.get("trm_usd_cop")
    if trm and trm.status == "observed":
        move = trm.values.get("seven_day_change_pct")
        if isinstance(move, int | float) and abs(move) >= 2:
            value = trm.values.get("trm_cop_per_usd")
            value_text = (
                f"{value:.2f} COP/USD"
                if isinstance(value, int | float)
                else "the latest official TRM"
            )
            seeds.append(
                (
                    trm,
                    {
                        "theme": "FX move persistence",
                        "trigger": trm.headline,
                        "question": (
                            "Will the official TRM remain at least 2% weaker than "
                            "its seven-day-ago level seven calendar days after this run?"
                        ),
                        "resolution": "Superintendencia Financiera / datos.gov.co official TRM.",
                        "deadline": "Seven calendar days after the run date.",
                        "missing": (
                            "Market context for why TRM moved; current reference "
                            f"level is {value_text}."
                        ),
                    },
                    ["material_move"],
                )
            )

    policy = by_id.get("policy_rate_ibr")
    if policy and policy.status == "observed":
        spread = policy.values.get("ibr_policy_spread_pp")
        if isinstance(spread, int | float) and abs(spread) >= 0.5:
            seeds.append(
                (
                    policy,
                    {
                        "theme": "BanRep policy/liquidity",
                        "trigger": policy.headline,
                        "question": (
                            "Will Banco de la Republica change the policy rate at "
                            "the next board decision?"
                        ),
                        "resolution": (
                            "BanRep board communique and official policy-rate series."
                        ),
                        "deadline": "Next scheduled BanRep board decision.",
                        "missing": (
                            "Next meeting date, inflation expectations, board guidance, "
                            "and market pricing."
                        ),
                    },
                    ["liquidity_spread"],
                )
            )

    manufacturing = by_id.get("manufacturing")
    retail = by_id.get("retail_sales")
    if manufacturing and manufacturing.status == "observed":
        manufacturing_sales = manufacturing.values.get("real_sales_annual_variation_pct")
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
            seeds.append(
                (
                    manufacturing,
                    {
                        "theme": "Activity divergence",
                        "trigger": manufacturing.headline,
                        "question": (
                            "Will the next DANE EMMET release still show negative "
                            "real manufacturing sales year over year?"
                        ),
                        "resolution": "DANE EMMET next monthly release.",
                        "deadline": "Next DANE manufacturing release.",
                        "missing": (
                            "Subsector drivers, electricity demand trend, inventories, "
                            "and import/capital-goods context."
                        ),
                    },
                    ["cross_indicator_tension"],
                )
            )

    fiscal = by_id.get("fiscal_tax_pulse")
    ipc = by_id.get("ipc_inflation")
    if fiscal and fiscal.status == "observed":
        nominal_tax = fiscal.values.get("gross_tax_revenue_annual_variation_pct")
        annual_ipc = ipc.values.get("annual_variation_pct") if ipc else None
        if isinstance(nominal_tax, int | float) and isinstance(annual_ipc, int | float):
            if nominal_tax < annual_ipc:
                seeds.append(
                    (
                        fiscal,
                        {
                            "theme": "Fiscal revenue stress",
                            "trigger": fiscal.headline,
                            "question": (
                                "Will the next DIAN monthly tax-collection release "
                                "again show nominal gross revenue growth below annual IPC?"
                            ),
                            "resolution": "DIAN monthly tax-collection XLSX and DANE IPC.",
                            "deadline": "Next DIAN monthly collection release.",
                            "missing": (
                                "Withholding, VAT, customs, fiscal-plan assumptions, "
                                "and whether calendar effects explain the miss."
                            ),
                        },
                        ["real_terms_warning"],
                    )
                )

    trade = by_id.get("external_trade")
    if trade and trade.status == "observed":
        periods = {
            component.period
            for component in trade.components
            if component.status == "observed" and component.period
        }
        if len(periods) > 1:
            seeds.append(
                (
                    trade,
                    {
                        "theme": "External trade alignment",
                        "trigger": trade.headline,
                        "question": (
                            "When exports and imports are observed for the same period, "
                            "will Colombia's goods trade balance improve year over year?"
                        ),
                        "resolution": (
                            "DANE/DIAN exports and imports releases for the same "
                            "reference month."
                        ),
                        "deadline": (
                            "Next import release that aligns with the latest export period."
                        ),
                        "missing": (
                            "Same-period import data, oil/fuel export detail, and "
                            "capital-goods import drivers."
                        ),
                    },
                    ["mixed_period_components"],
                )
            )

    oil = by_id.get("oil_gas_production")
    if oil and oil.status == "observed" and _monthly_lagged(oil, run_summary.run_date):
        seeds.append(
            (
                oil,
                {
                    "theme": "Hydrocarbon data lag",
                    "trigger": oil.headline,
                    "question": (
                        "Will ANH publish a newer consolidated oil/gas production "
                        "period within the next 30 days?"
                    ),
                    "resolution": (
                        "ANH official production statistics or datos.gov.co Socrata mirrors."
                    ),
                    "deadline": "30 days after the run date.",
                    "missing": (
                        "Normal ANH publication lag and whether the current dataset "
                        "mirror is delayed."
                    ),
                },
                ["observation_lag"],
            )
        )

    candidates: list[dict[str, Any]] = []
    for indicator, seed, alerts in seeds[:INDICATOR_SEED_LIMIT]:
        question = seed["question"]
        entities = _indicator_entities(indicator)
        topics = _indicator_topics(indicator)
        candidates.append(
            {
                "candidate_id": _candidate_id(
                    "indicator", run_summary.run_date, indicator.indicator_id, question
                ),
                "candidate_type": "indicator_seed",
                "origin_id": indicator.indicator_id,
                "source_ids": [],
                "source_names": [indicator.source_name] if indicator.source_name else [],
                "event_type": "indicator_alert",
                "actor": entities[0] if entities else "",
                "topic": topics[0] if topics else "",
                "published_at": indicator.release_date,
                "evidence_text": indicator.headline,
                "question_seed": question,
                "trigger": seed["trigger"] or indicator.headline,
                "theme": seed["theme"],
                "why_now": "Deterministic Indicator Watch seed fired.",
                "resolution_source": seed["resolution"],
                "resolution_source_hint": seed["resolution"],
                "deadline_or_window": seed["deadline"],
                "deadline_hint": seed["deadline"],
                "missing_evidence": [seed["missing"]],
                "m1_scores": {
                    "rank_score": None,
                    "forecastability_score": None,
                },
                "reasons": [f"indicator:{alert}" for alert in alerts],
                "noise_reasons": [],
                "entities": entities,
                "topics": topics,
                "freshness": {
                    "latest_published_at": indicator.release_date,
                    "freshness_status": indicator.freshness_status,
                    "period": indicator.period,
                },
                "evidence": {
                    "indicator_id": indicator.indicator_id,
                    "source_name": indicator.source_name,
                    "source_url": indicator.source_url,
                    "links": [
                        {
                            "title": indicator.source_name or indicator.name,
                            "url": indicator.source_url,
                            "source_name": indicator.source_name,
                        }
                    ]
                    if indicator.source_url
                    else [],
                    "evidence_text": indicator.headline,
                    "starting_evidence": indicator.headline,
                    "values": dict(indicator.values),
                },
                "decision_hint": "candidate",
            }
        )
    return candidates


def _link_only_source_ids(source_health: list[SourceHealth]) -> set[str]:
    return {
        health.source_id
        for health in source_health
        if health.document_link_count > 0 and health.parsed_content_count == 0
    }


def _indicator_entities(indicator: IndicatorObservation) -> list[str]:
    text = " ".join(
        [
            indicator.indicator_id,
            indicator.name,
            indicator.source_name,
            indicator.source_url,
        ]
    ).lower()
    entities: list[str] = []
    for needle, entity in (
        ("banrep", "banrep"),
        ("banco de la republica", "banrep"),
        ("dane", "dane"),
        ("dian", "dian"),
        ("anh", "anh"),
    ):
        if needle in text and entity not in entities:
            entities.append(entity)
    return entities


def _indicator_topics(indicator: IndicatorObservation) -> list[str]:
    mapped = {
        "markets": ["external_trade"],
        "monetary": ["monetary_policy"],
        "activity": ["labor_market"],
        "fiscal": ["fiscal_tax"],
        "external": ["external_trade"],
        "energy_fiscal": ["hydrocarbons", "energy", "fiscal_tax"],
    }.get(indicator.category)
    if mapped:
        return mapped
    return [indicator.category] if indicator.category else []


def _event_type(signal_types: list[str]) -> str:
    for signal_type in signal_types:
        if signal_type:
            return signal_type
    return "event_signal"


def _source_caveats(
    failures: list[SourceFailure], source_health: list[SourceHealth]
) -> list[dict[str, Any]]:
    caveats: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def append(source_id: str, severity: str, reason: str, **extra: Any) -> None:
        key = (source_id, reason)
        if key in seen:
            return
        seen.add(key)
        caveats.append(
            {
                "source_id": source_id,
                "severity": severity,
                "reason": reason,
                **extra,
            }
        )

    for health in source_health:
        if health.source_id == "eltiempo_colombia":
            append(
                health.source_id,
                "warning",
                "rolling RSS media pulse; not guaranteed full-day coverage",
                source_name=health.source_name,
            )
        if health.failure_count:
            append(
                health.source_id,
                "warning",
                "source failed during this run; silence is not evidence of no activity",
                source_name=health.source_name,
                failure_count=health.failure_count,
            )
        if health.content_mode in {"pdf_links_only", "document_links_only"}:
            append(
                health.source_id,
                "warning",
                "link-only source; ask for document contents before relying on it",
                source_name=health.source_name,
                content_mode=health.content_mode,
            )
        if (
            health.status in {"no_raw", "no_rankable"}
            and health.onboarding_status == "needs_parser"
        ):
            append(
                health.source_id,
                "warning",
                "undercovered source; treat silence from this domain as unknown",
                source_name=health.source_name,
                status=health.status,
            )

    health_source_ids = {health.source_id for health in source_health}
    for failure in failures:
        if failure.source_id in health_source_ids:
            continue
        append(
            failure.source_id,
            "warning",
            "source failed during this run; silence is not evidence of no activity",
            source_name=failure.source_name,
            error_class=failure.error_class,
        )
    return caveats


def _candidate_id(
    kind: str, run_date: str, origin_id: str, question: str
) -> str:
    digest = hashlib.sha1(
        "\x1f".join([run_date, kind, origin_id, question]).encode("utf-8")
    ).hexdigest()[:12]
    return f"m1c_{kind}_{digest}"


def _cluster_links(cluster: Cluster) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for idx, url in enumerate(cluster.member_urls):
        if not url or url in seen:
            continue
        seen.add(url)
        links.append(
            {
                "title": _list_get(cluster.member_titles, idx) or cluster.title,
                "url": url,
                "source_name": _list_get(cluster.member_source_names, idx),
            }
        )
    return links


def _evidence_text(cluster: Cluster) -> str:
    return cluster.summary or cluster.title


def _matched_topic_keywords(
    cluster: Cluster, topic_keywords: list[str]
) -> list[str]:
    haystack = f"{cluster.title} {cluster.summary}".casefold()
    return [
        keyword
        for keyword in _unique_sorted(topic_keywords)
        if keyword.casefold() in haystack
    ]


def _monthly_lagged(indicator: IndicatorObservation, run_date: str) -> bool:
    if not indicator.frequency.startswith("monthly"):
        return False
    run_day = _parse_date(run_date)
    period_day = _period_start(indicator.period)
    if not run_day or not period_day:
        return False
    return (run_day - period_day).days > MONTHLY_LAG_WARNING_DAYS


def _period_start(period: str) -> date | None:
    if not period:
        return None
    if len(period) == 7 and period[4] == "-":
        year_text, month_text = period.split("-", 1)
        try:
            return date(int(year_text), int(month_text), 1)
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


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _list_get(items: list[str], index: int) -> str:
    return items[index] if index < len(items) else ""


def _unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _unique_sorted(items: list[str]) -> list[str]:
    return sorted({item for item in items if item})
