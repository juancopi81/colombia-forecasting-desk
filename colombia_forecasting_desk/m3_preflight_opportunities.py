from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .config_loader import load_metasources
from .models import Metasource

SCHEMA_VERSION = "m3_preflight_opportunities.v2"
JSON_FILENAME = "m3_preflight_opportunities.json"
MARKDOWN_FILENAME = "m3_preflight_opportunities.md"
DEFAULT_WINDOW_DAYS = 60
IMMINENT_WINDOW_DAYS = 7

BANREP_RESOLVER_SOURCE_ID = "banrep_junta_comunicados"
BANREP_CALENDAR_SOURCE_ID = "banrep_junta_calendar"
BANREP_EVENT_TYPE = "banrep_policy_rate_decision"
DANE_CALENDAR_SOURCE_ID = "dane_publication_calendar"
DANE_EVENT_TYPE = "dane_economic_release"

_DANE_RELEASE_LABELS = {
    "ipc": "consumer inflation (IPC)",
    "pib": "gross domestic product (PIB)",
    "ise": "economic activity (ISE)",
    "labor": "labor-market statistics (GEIH)",
    "icoced": "building construction costs (ICOCED)",
    "emmet": "manufacturing activity (EMMET)",
    "services_trade": "external trade in services (EMCES)",
    "retail": "retail activity (EMC)",
    "imports": "imports",
    "exports": "exports",
    "construction_licenses": "construction licenses",
}
_DANE_TENSION_CARD_IDS = {
    "ipc": {"real_policy_rate", "construction_cost_vs_ipc"},
    "ise": {"activity_composition_divergence", "activity_labor_tension"},
    "labor": {"activity_labor_tension"},
    "icoced": {"construction_cost_vs_ipc"},
    "construction_licenses": {"construction_cost_vs_ipc"},
}

_SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
_SPANISH_DATE_RE = re.compile(
    r"\b(?:pr[oó]ximo\s+)?(?P<day>\d{1,2})\s+de\s+"
    r"(?P<month>enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|setiembre|octubre|noviembre|diciembre)"
    r"(?:\s+de\s+(?P<year>\d{4}))?\b",
    flags=re.IGNORECASE,
)


class PreflightInputError(ValueError):
    """Raised when a run folder cannot support preflight artifact generation."""


def build_m3_preflight_opportunities(
    raw_items: list[Any] | None = None,
    indicator_watch: list[Any] | None = None,
    indicator_tension_cards: list[dict[str, Any]] | None = None,
    *,
    run_date: str,
    sources: list[Metasource] | None = None,
    source_health: list[Any] | None = None,
    generated_at: str | None = None,
    research_window_days: int = DEFAULT_WINDOW_DAYS,
    forecast_log_path: str | Path = "forecasts/forecast_log.jsonl",
) -> dict[str, Any]:
    """Build a deterministic preflight artifact from existing run evidence.

    This is a low-authority pre-M3 surface. It can flag scheduled, nearby,
    forecastable events for human review, but it does not create evidence packs,
    probabilities, or forecast-log entries.
    """
    run_day = _parse_date(run_date, field_name="run_date")
    if research_window_days < 0:
        raise ValueError("research_window_days must be non-negative")

    raw_records = [_record(item) for item in raw_items or []]
    health_records = [_record(item) for item in source_health or []]
    source_by_id = {source.id: source for source in sources or []}
    if not source_by_id:
        source_by_id[BANREP_RESOLVER_SOURCE_ID] = _default_banrep_source()
    health_by_id = _health_by_source_id(health_records)
    caveats: list[dict[str, Any]] = []

    should_enforce_resolver_health = sources is not None or source_health is not None
    banrep_check = {"passed": True, "caveat": None}
    if should_enforce_resolver_health:
        banrep_check = _resolver_check(
            source_by_id=source_by_id,
            health_by_id=health_by_id,
            source_health_present=source_health is not None,
            resolver_source_id=BANREP_RESOLVER_SOURCE_ID,
            detector=BANREP_EVENT_TYPE,
        )
    if not banrep_check["passed"]:
        caveats.append(banrep_check["caveat"])
        opportunities: list[dict[str, Any]] = []
    else:
        if sources is not None:
            calendar_check = _schedule_source_check(
                source_by_id=source_by_id,
                health_by_id=health_by_id,
                source_health_present=source_health is not None,
            )
            if not calendar_check["passed"]:
                caveats.append(calendar_check["caveat"])
            elif not _has_current_or_future_calendar_event(raw_records, run_day):
                caveats.append(
                    {
                        "detector": BANREP_EVENT_TYPE,
                        "reason": "future_meeting_date_missing",
                    }
                )
        opportunities = _banrep_policy_rate_opportunities(
            run_day=run_day,
            raw_items=raw_records,
            indicator_watch=indicator_watch or [],
            indicator_tension_cards=indicator_tension_cards or [],
            resolver_source=source_by_id[BANREP_RESOLVER_SOURCE_ID],
            source_health_by_id=health_by_id,
            window_days=research_window_days,
            forecast_log_path=forecast_log_path,
        )

    opportunities.extend(
        _dane_release_opportunities(
            run_day=run_day,
            raw_items=raw_records,
            indicator_tension_cards=indicator_tension_cards or [],
            window_days=research_window_days,
        )
    )

    opportunities = sorted(
        opportunities,
        key=lambda item: (item["event_date"], item["opportunity_id"]),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_date,
        "generated_at": generated_at or f"{run_date}T23:59:59Z",
        "status": "opportunities_found" if opportunities else "no_opportunities",
        "policy": (
            "Preflight-only artifact. It exposes scheduled opportunities across "
            "an early research horizon and labels only the final seven days as "
            "imminent. It does not write forecast_log.jsonl, create evidence "
            "packs, set probabilities, or mark any opportunity ready_for_m3."
        ),
        "inputs": {
            "raw_items_artifact": "raw_items.json",
            "source_health_artifact": "source_health.json",
            "config_artifact": "config/metasources.yaml",
            "research_window_days": research_window_days,
            "imminent_window_days": IMMINENT_WINDOW_DAYS,
        },
        "summary": {
            "detectors": [BANREP_EVENT_TYPE, DANE_EVENT_TYPE],
            "opportunity_count": len(opportunities),
            "imminent_count": sum(
                1
                for item in opportunities
                if item.get("urgency") in {"imminent", "nearby"}
            ),
            "research_horizon_count": sum(
                1
                for item in opportunities
                if item.get("urgency") == "research_horizon"
            ),
            "caveat_count": len(caveats),
        },
        "opportunities": opportunities,
        "caveats": caveats,
    }


def write_m3_preflight_opportunities(
    run_dir: str | Path,
    *,
    config_path: str | Path = "config/metasources.yaml",
    research_window_days: int = DEFAULT_WINDOW_DAYS,
) -> tuple[dict[str, Any], Path, Path]:
    run_path = Path(run_dir)
    artifact = build_m3_preflight_opportunities_from_run_dir(
        run_path,
        config_path=config_path,
        research_window_days=research_window_days,
    )
    json_path = run_path / JSON_FILENAME
    markdown_path = run_path / MARKDOWN_FILENAME
    json_path.write_text(
        json.dumps(artifact, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_m3_preflight_opportunities(artifact),
        encoding="utf-8",
    )
    return artifact, json_path, markdown_path


def build_m3_preflight_opportunities_from_run_dir(
    run_dir: str | Path,
    *,
    config_path: str | Path = "config/metasources.yaml",
    research_window_days: int = DEFAULT_WINDOW_DAYS,
) -> dict[str, Any]:
    run_path = Path(run_dir)
    if not run_path.is_dir():
        raise PreflightInputError(f"Missing run directory: {run_path}")
    raw_items_path = run_path / "raw_items.json"
    if not raw_items_path.is_file():
        raise PreflightInputError(f"Missing required artifact: {raw_items_path}")

    raw_items = _read_json_list(raw_items_path)
    source_health_path = run_path / "source_health.json"
    source_health = (
        _read_json_list(source_health_path) if source_health_path.is_file() else None
    )
    indicator_watch_path = run_path / "indicator_watch.json"
    indicator_watch = (
        _read_json_list(indicator_watch_path) if indicator_watch_path.is_file() else []
    )
    tension_cards_path = run_path / "indicator_tension_cards.json"
    indicator_tension_cards = (
        _read_json_list(tension_cards_path) if tension_cards_path.is_file() else []
    )
    run_date = _run_date_from_dir(run_path)
    generated_at = _generated_at_from_run_summary(run_path, run_date)
    sources = load_metasources(config_path)
    return build_m3_preflight_opportunities(
        raw_items,
        indicator_watch,
        indicator_tension_cards,
        run_date=run_date,
        sources=sources,
        source_health=source_health,
        generated_at=generated_at,
        research_window_days=research_window_days,
    )


def render_m3_preflight_opportunities(artifact: dict[str, Any]) -> str:
    """Render a deterministic Markdown companion for the JSON artifact."""
    lines = [
        f"# M3 Preflight Opportunities - {artifact.get('run_date', 'unknown')}",
        "",
        f"Schema: `{artifact.get('schema_version', SCHEMA_VERSION)}`",
        f"Status: `{artifact.get('status', 'no_opportunities')}`",
        f"Opportunity count: {artifact.get('summary', {}).get('opportunity_count', 0)}",
        "",
        (
            "This artifact flags scheduled opportunities for early M3 research. "
            "Only events within seven days are imminent; these are not forecasts. "
            "It does not write `forecast_log.jsonl`, create evidence packs, "
            "assign probabilities, or mark items `ready_for_m3`."
        ),
        "",
    ]

    opportunities = [
        item for item in artifact.get("opportunities") or [] if isinstance(item, dict)
    ]
    if not opportunities:
        lines.extend(["## Opportunities", "", "No deterministic opportunities found.", ""])
    else:
        lines.extend(["## Opportunities", ""])
        for item in opportunities:
            lines.extend(_render_opportunity(item))

    caveats = [item for item in artifact.get("caveats") or [] if isinstance(item, dict)]
    if caveats:
        lines.extend(["## Fail-Closed Caveats", ""])
        for caveat in caveats:
            lines.append(
                "- "
                f"{caveat.get('detector', 'unknown')}: "
                f"{caveat.get('reason', 'unknown')}"
            )
        lines.append("")

    return "\n".join(lines)


def _dane_release_opportunities(
    *,
    run_day: date,
    raw_items: list[dict[str, Any]],
    indicator_tension_cards: list[dict[str, Any]],
    window_days: int,
) -> list[dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in raw_items:
        if item.get("source_id") != DANE_CALENDAR_SOURCE_ID:
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        event_day = _parse_optional_date(str(metadata.get("scheduled_date") or ""))
        release_family = str(metadata.get("release_family") or "").strip()
        if event_day is None or release_family not in _DANE_RELEASE_LABELS:
            continue
        days_until = (event_day - run_day).days
        if days_until < 0 or days_until > window_days:
            continue
        key = (event_day.isoformat(), release_family)
        if key in seen:
            continue
        seen.add(key)

        is_imminent = days_until <= IMMINENT_WINDOW_DAYS
        calendar_url = str(metadata.get("calendar_event_url") or item.get("url") or "")
        operation_url = str(metadata.get("operation_url") or item.get("url") or "")
        source_title = str(item.get("title") or "").strip()
        release_label = re.sub(
            r"^\d{1,2}\s+[A-Za-zÁÉÍÓÚáéíóú]+\s+\d{4}\s+\d{1,2}:\d{2}\s*:\s*",
            "",
            source_title,
        ).strip() or _DANE_RELEASE_LABELS[release_family]
        title = source_title or release_label
        scheduled_at_local = str(metadata.get("scheduled_at_local") or "")
        linked_cards = _linked_tension_cards_for_ids(
            indicator_tension_cards,
            _DANE_TENSION_CARD_IDS.get(release_family, set()),
        )
        opportunities.append(
            {
                "opportunity_id": (
                    f"dane_{release_family}_release_{event_day.isoformat()}"
                ),
                "event_type": DANE_EVENT_TYPE,
                "status": "preflight_only",
                "m3_gate": "needs_human_review" if is_imminent else "research_only",
                "title": f"DANE {release_label} release on {event_day.isoformat()}",
                "entity": "DANE",
                "run_date": run_day.isoformat(),
                "event_date": event_day.isoformat(),
                "days_until_event": days_until,
                "urgency": (
                    "imminent"
                    if days_until <= 1
                    else "nearby"
                    if is_imminent
                    else "research_horizon"
                ),
                "disposition": (
                    "consider_shadow_or_m3_preflight"
                    if is_imminent
                    else "shadow_research"
                ),
                "active_forecast_ids": [],
                "question_seed": (
                    "Before publication, define one bounded, thresholded outcome "
                    f"for DANE's {release_label} release and estimate it internally "
                    "against a named baseline."
                ),
                "why_now": (
                    f"DANE scheduled {title} for {event_day.isoformat()}, giving "
                    "the desk a clean official event clock and resolver."
                ),
                "resolution_source": {
                    "source_id": DANE_CALENDAR_SOURCE_ID,
                    "source_name": str(item.get("source_name") or "DANE"),
                    "url": operation_url,
                    "trust_role": "agenda_signal",
                    "criteria": (
                        "Resolve against the first official DANE release for the "
                        "scheduled statistical operation and predeclared threshold."
                    ),
                },
                "resolution_sources": [
                    {
                        "label": f"DANE {release_label} publication",
                        "value": "Official scheduled release and operation page.",
                        "source": str(item.get("source_name") or "DANE"),
                        "url": operation_url,
                    }
                ],
                "linked_tension_cards": linked_cards,
                "checks": {
                    "scheduled_date": {
                        "passed": True,
                        "value": event_day.isoformat(),
                    },
                    "official_resolver": {
                        "passed": bool(operation_url),
                        "value": operation_url,
                    },
                },
                "source_evidence": [
                    {
                        "artifact": "raw_items.json",
                        "item_id": str(item.get("id") or ""),
                        "source_id": DANE_CALENDAR_SOURCE_ID,
                        "title": source_title,
                        "url": calendar_url,
                        "published_at": item.get("published_at"),
                        "metadata_key": "scheduled_at_local",
                        "value": scheduled_at_local,
                        "excerpt": title,
                    }
                ],
                "evidence": [
                    {
                        "label": "Scheduled release",
                        "value": (
                            f"{title} — scheduled at {scheduled_at_local} (local)"
                            if scheduled_at_local
                            else title
                        ),
                        "source": str(item.get("source_name") or "DANE"),
                        "url": calendar_url,
                    }
                ],
                "missing_evidence": [
                    "Choose the exact outcome and threshold before publication.",
                    "Record a simple historical, consensus, or persistence baseline.",
                ],
                "guardrails": [
                    "This prompt is not a forecast until the agent authors a valid shadow case.",
                    "Do not infer the release value from the event calendar.",
                ],
            }
        )
    return opportunities


def _banrep_policy_rate_opportunities(
    *,
    run_day: date,
    raw_items: list[dict[str, Any]],
    indicator_watch: list[Any],
    indicator_tension_cards: list[dict[str, Any]],
    resolver_source: Metasource,
    source_health_by_id: dict[str, dict[str, Any]],
    window_days: int,
    forecast_log_path: str | Path,
) -> list[dict[str, Any]]:
    by_event_date: dict[date, dict[str, Any]] = {}
    policy_rate_from_indicators = _policy_rate_from_indicators(indicator_watch)

    # Parsed minutes remain a fallback schedule signal.
    for item in raw_items:
        if item.get("source_id") != BANREP_RESOLVER_SOURCE_ID:
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        context = str(metadata.get("next_meeting_context") or "").strip()
        policy_rate = _format_rate(
            policy_rate_from_indicators or metadata.get("policy_rate_pct")
        )
        if not context or not policy_rate:
            continue
        for event_day in _extract_spanish_dates(
            context,
            base_date=_item_reference_date(item, run_day),
        ):
            days_until = (event_day - run_day).days
            if days_until < 0 or days_until > window_days:
                continue
            current = by_event_date.get(event_day)
            candidate = _banrep_opportunity(
                run_day=run_day,
                event_day=event_day,
                days_until=days_until,
                policy_rate=policy_rate,
                item=item,
                context=context,
                evidence_label="BanRep next meeting context",
                metadata_key="next_meeting_context",
                why_now=(
                    f"The previous BanRep minutes point to the "
                    f"{_spanish_display_date(event_day)} board session, which is "
                    "inside the preflight window."
                ),
                resolver_source=resolver_source,
                resolver_source_health=source_health_by_id.get(
                    BANREP_RESOLVER_SOURCE_ID, {}
                ),
                schedule_source_health=source_health_by_id.get(
                    BANREP_RESOLVER_SOURCE_ID, {}
                ),
                linked_tension_cards=_linked_tension_cards(indicator_tension_cards),
                active_forecast_ids=_active_forecast_ids(
                    forecast_log_path,
                    event_date=event_day.isoformat(),
                ),
            )
            if current is None or _item_sort_key(item) > _item_sort_key(
                current["source_evidence"][0]
            ):
                by_event_date[event_day] = candidate

    # The official calendar is the primary schedule clock and overrides a
    # same-day minutes fallback. A current policy-rate observation is required
    # so the generated binary question has an explicit baseline.
    if not policy_rate_from_indicators:
        return list(by_event_date.values())
    for item in raw_items:
        if item.get("source_id") != BANREP_CALENDAR_SOURCE_ID:
            continue
        metadata = item.get("metadata")
        if (
            not isinstance(metadata, dict)
            or metadata.get("event_type") != BANREP_EVENT_TYPE
        ):
            continue
        scheduled_at = str(
            metadata.get("scheduled_at_local")
            or metadata.get("scheduled_date")
            or item.get("published_at")
            or ""
        )
        event_day = _parse_optional_date(scheduled_at)
        if event_day is None:
            continue
        days_until = (event_day - run_day).days
        if days_until < 0 or days_until > window_days:
            continue
        context = str(item.get("title") or "").strip() or scheduled_at
        by_event_date[event_day] = _banrep_opportunity(
            run_day=run_day,
            event_day=event_day,
            days_until=days_until,
            policy_rate=policy_rate_from_indicators,
            item=item,
            context=context,
            evidence_label="Official BanRep policy-rate calendar",
            metadata_key="scheduled_at_local",
            why_now=(
                "The official BanRep calendar schedules a policy-rate decision "
                f"for {_spanish_display_date(event_day)}, inside the preflight window."
            ),
            resolver_source=resolver_source,
            resolver_source_health=source_health_by_id.get(
                BANREP_RESOLVER_SOURCE_ID, {}
            ),
            schedule_source_health=source_health_by_id.get(
                BANREP_CALENDAR_SOURCE_ID, {}
            ),
            linked_tension_cards=_linked_tension_cards(indicator_tension_cards),
            active_forecast_ids=_active_forecast_ids(
                forecast_log_path,
                event_date=event_day.isoformat(),
            ),
        )
    return list(by_event_date.values())


def _banrep_opportunity(
    *,
    run_day: date,
    event_day: date,
    days_until: int,
    policy_rate: str,
    item: dict[str, Any],
    context: str,
    evidence_label: str,
    metadata_key: str,
    why_now: str,
    resolver_source: Metasource,
    resolver_source_health: dict[str, Any],
    schedule_source_health: dict[str, Any],
    linked_tension_cards: list[dict[str, str]],
    active_forecast_ids: list[str],
) -> dict[str, Any]:
    event_date = event_day.isoformat()
    display_date = _display_date(event_day)
    question_seed = (
        "Will Banco de la Republica change its policy rate from "
        f"{policy_rate}% at the {display_date} board decision?"
    )
    resolver_criteria = (
        f"Resolve YES if the official Junta Directiva communique sets a policy "
        f"rate different from {policy_rate}%, and NO if it leaves the rate "
        "unchanged; cross-check the official policy-rate series if needed."
    )
    evidence = {
        "label": evidence_label,
        "value": context,
        "source": str(item.get("source_name") or "BanRep"),
        "url": str(item.get("url") or ""),
    }
    resolution_sources = [
        {
            "label": "BanRep Junta Directiva communique",
            "value": resolver_criteria,
            "source": resolver_source.name,
            "url": resolver_source.url,
        }
    ]
    missing = [
        "Human/LLM review must confirm the directional threshold before M3.",
        "Create a full M3 evidence pack only after explicit selection.",
    ]
    guardrails = [
        "Preflight opportunities are not forecasts.",
        "Do not write forecast_log.jsonl from this artifact alone.",
    ]
    is_imminent = days_until <= IMMINENT_WINDOW_DAYS
    disposition = (
        "already_tracked"
        if active_forecast_ids
        else "consider_m3_preflight"
        if is_imminent
        else "shadow_research"
    )
    return {
        "opportunity_id": f"banrep_policy_rate_decision_{event_date}",
        "event_type": BANREP_EVENT_TYPE,
        "status": "preflight_only",
        "m3_gate": "needs_human_review" if is_imminent else "research_only",
        "title": f"BanRep board policy-rate decision on {event_date}",
        "entity": "Banco de la Republica",
        "run_date": run_day.isoformat(),
        "event_date": event_date,
        "days_until_event": days_until,
        "urgency": (
            "imminent"
            if days_until <= 1
            else "nearby"
            if days_until <= IMMINENT_WINDOW_DAYS
            else "research_horizon"
        ),
        "disposition": disposition,
        "active_forecast_ids": active_forecast_ids,
        "question_seed": question_seed,
        "why_now": why_now,
        "resolution_source": {
            "source_id": resolver_source.id,
            "source_name": resolver_source.name,
            "url": resolver_source.url,
            "trust_role": resolver_source.trust_role,
            "criteria": resolver_criteria,
        },
        "resolution_sources": resolution_sources,
        "linked_tension_cards": linked_tension_cards,
        "checks": {
            "nearby_date": {
                "status": "pass",
                "event_date": event_date,
                "days_until_event": days_until,
            },
            "official_resolver": {
                "status": "pass",
                "source_id": resolver_source.id,
                "trust_role": resolver_source.trust_role,
            },
            "source_health": {
                "status": "pass",
                "source_id": resolver_source.id,
                "failure_count": _int(resolver_source_health.get("failure_count")),
                "raw_count": _int(resolver_source_health.get("raw_count")),
                "acceptance_status": resolver_source_health.get(
                    "acceptance_status", "unknown"
                ),
            },
            "schedule_source_health": {
                "status": "pass",
                "source_id": str(item.get("source_id") or ""),
                "failure_count": _int(schedule_source_health.get("failure_count")),
                "raw_count": _int(schedule_source_health.get("raw_count")),
                "acceptance_status": schedule_source_health.get(
                    "acceptance_status", "unknown"
                ),
            },
            "current_policy_rate_context": {
                "status": "pass",
                "policy_rate_pct": policy_rate,
            },
        },
        "evidence": [evidence],
        "source_evidence": [
            {
                "artifact": "raw_items.json",
                "item_id": str(item.get("id") or ""),
                "source_id": str(item.get("source_id") or ""),
                "title": str(item.get("title") or ""),
                "url": str(item.get("url") or ""),
                "published_at": item.get("published_at"),
                "metadata_key": metadata_key,
                "excerpt": context,
            }
        ],
        "missing_evidence": missing,
        "guardrails": guardrails,
        "missing_before_m3": missing,
        "next_step": (
            "Review the BanRep policy context and decide whether to promote a "
            "separate M3 evidence pack. This artifact is not a forecast."
        ),
    }


def _render_opportunity(item: dict[str, Any]) -> list[str]:
    resolver = item.get("resolution_source") or {}
    evidence = [
        ref for ref in item.get("source_evidence") or [] if isinstance(ref, dict)
    ]
    lines = [
        f"### {item.get('title', 'Untitled opportunity')}",
        "",
        f"- Opportunity ID: `{item.get('opportunity_id', 'unknown')}`",
        f"- Event type: `{item.get('event_type', 'unknown')}`",
        f"- Event date: `{item.get('event_date', 'unknown')}`",
        f"- Days until event: {item.get('days_until_event', 'unknown')}",
        f"- Gate: `{item.get('m3_gate', 'needs_human_review')}`",
        f"- Question seed: {item.get('question_seed', 'not_recorded')}",
        (
            "- Resolver: "
            f"`{resolver.get('source_id', 'unknown')}` - "
            f"{resolver.get('criteria', 'not_recorded')}"
        ),
    ]
    if evidence:
        first = evidence[0]
        lines.extend(
            [
                (
                    "- Evidence: "
                    f"`{first.get('artifact', 'raw_items.json')}` "
                    f"item `{first.get('item_id', 'unknown')}` "
                    f"from `{first.get('source_id', 'unknown')}`"
                ),
                f"- Trigger excerpt: {first.get('excerpt', 'not_recorded')}",
            ]
        )
        if item.get("event_type") == DANE_EVENT_TYPE:
            if first.get("url"):
                lines.append(
                    f"- Calendar: [DANE publication calendar]({first['url']})"
                )
            if first.get("value"):
                lines.append(f"- Scheduled time (local): `{first['value']}`")
    missing = item.get("missing_before_m3") or []
    if missing:
        lines.append("- Missing before M3:")
        for entry in missing:
            lines.append(f"  - {entry}")
    lines.append("")
    return lines


def _resolver_check(
    *,
    source_by_id: dict[str, Metasource],
    health_by_id: dict[str, dict[str, Any]],
    source_health_present: bool,
    resolver_source_id: str,
    detector: str,
) -> dict[str, Any]:
    source = source_by_id.get(resolver_source_id)
    if source is None:
        return _failed_check(detector, "resolver_source_missing_from_config")
    if source.trust_role != "resolution_source":
        return _failed_check(detector, "resolver_source_not_marked_resolution_source")
    if not source_health_present:
        return _failed_check(detector, "source_health_artifact_missing")
    health = health_by_id.get(resolver_source_id)
    if health is None:
        return _failed_check(detector, "resolver_source_missing_from_source_health")
    if _int(health.get("failure_count")) > 0 or health.get("status") == "failed":
        return _failed_check(detector, "resolver_source_failed")
    if _int(health.get("raw_count")) <= 0:
        return _failed_check(detector, "resolver_source_had_no_raw_items")
    return {"passed": True, "caveat": None}


def _schedule_source_check(
    *,
    source_by_id: dict[str, Metasource],
    health_by_id: dict[str, dict[str, Any]],
    source_health_present: bool,
) -> dict[str, Any]:
    source = source_by_id.get(BANREP_CALENDAR_SOURCE_ID)
    if source is None:
        return _failed_check(BANREP_EVENT_TYPE, "schedule_source_missing_from_config")
    if source.trust_role != "agenda_signal":
        return _failed_check(BANREP_EVENT_TYPE, "schedule_source_not_agenda_signal")
    if not source_health_present:
        return _failed_check(BANREP_EVENT_TYPE, "source_health_artifact_missing")
    health = health_by_id.get(BANREP_CALENDAR_SOURCE_ID)
    if health is None:
        return _failed_check(
            BANREP_EVENT_TYPE,
            "schedule_source_missing_from_source_health",
        )
    if _int(health.get("failure_count")) > 0 or health.get("status") == "failed":
        return _failed_check(BANREP_EVENT_TYPE, "schedule_source_failed")
    if _int(health.get("raw_count")) <= 0:
        return _failed_check(BANREP_EVENT_TYPE, "schedule_source_had_no_raw_items")
    return {"passed": True, "caveat": None}


def _has_current_or_future_calendar_event(
    raw_items: list[dict[str, Any]],
    run_day: date,
) -> bool:
    for item in raw_items:
        if item.get("source_id") != BANREP_CALENDAR_SOURCE_ID:
            continue
        metadata = item.get("metadata")
        if (
            not isinstance(metadata, dict)
            or metadata.get("event_type") != BANREP_EVENT_TYPE
        ):
            continue
        scheduled = str(
            metadata.get("scheduled_at_local")
            or metadata.get("scheduled_date")
            or item.get("published_at")
            or ""
        )
        event_day = _parse_optional_date(scheduled)
        if event_day is not None and event_day >= run_day:
            return True
    return False


def _failed_check(detector: str, reason: str) -> dict[str, Any]:
    return {
        "passed": False,
        "caveat": {
            "detector": detector,
            "reason": reason,
        },
    }


def _record(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return item
    fields = (
        "id",
        "source_id",
        "source_name",
        "source_type",
        "url",
        "title",
        "fetched_at",
        "published_at",
        "raw_text",
        "metadata",
        "indicator_id",
        "name",
        "category",
        "status",
        "frequency",
        "source_url",
        "period",
        "release_date",
        "headline",
        "values",
        "freshness_status",
        "components",
        "why_it_matters",
        "correlations",
        "next_step",
        "raw_count",
        "cleaned_count",
        "dated_count",
        "rankable_count",
        "failure_count",
        "failures",
        "acceptance_status",
    )
    return {field: getattr(item, field) for field in fields if hasattr(item, field)}


def _default_banrep_source() -> Metasource:
    return Metasource(
        id=BANREP_RESOLVER_SOURCE_ID,
        name="Banco de la Republica - Comunicados Junta Directiva",
        url="https://www.banrep.gov.co/es/comunicados-junta",
        type="official_updates",
        country_relevance="high",
        access_status="html_public",
        fetch_method="html",
        priority="high",
        update_frequency="event_driven",
        trust_role="resolution_source",
        parsing_difficulty="medium",
        enabled=True,
    )


def _policy_rate_from_indicators(indicator_watch: list[Any]) -> str:
    for item in indicator_watch:
        record = _record(item)
        values = record.get("values")
        if not isinstance(values, dict):
            continue
        rate = _format_rate(values.get("policy_rate_pct"))
        if rate:
            return rate
    return ""


def _format_rate(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return f"{float(value):g}"
    text = str(value).strip().replace(",", ".")
    if not text:
        return ""
    try:
        return f"{float(text):g}"
    except ValueError:
        return text


def _linked_tension_cards(cards: list[dict[str, Any]]) -> list[dict[str, str]]:
    linked: list[dict[str, str]] = []
    for card in cards:
        if not isinstance(card, dict):
            continue
        label = str(card.get("title") or card.get("card_id") or "").strip()
        value = str(card.get("trigger") or card.get("summary") or "").strip()
        if not label:
            continue
        linked.append(
            {
                "label": label,
                "value": value,
                "source": "indicator_tension_cards.json",
                "url": "",
            }
        )
    return linked


def _linked_tension_cards_for_ids(
    cards: list[dict[str, Any]], card_ids: set[str]
) -> list[dict[str, str]]:
    if not card_ids:
        return []
    return _linked_tension_cards(
        [
            card
            for card in cards
            if isinstance(card, dict) and str(card.get("card_id") or "") in card_ids
        ]
    )


def _active_forecast_ids(
    forecast_log_path: str | Path,
    *,
    event_date: str,
) -> list[str]:
    path = Path(forecast_log_path)
    if not path.is_file():
        return []
    active_ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").lower()
        if status in {"resolved", "rejected", "cancelled", "canceled"}:
            continue
        deadline = str(row.get("resolution_deadline") or "")
        if deadline[:10] != event_date:
            continue
        haystack = " ".join(
            str(row.get(key) or "")
            for key in ("question", "resolution_source", "forecast_id")
        ).lower()
        if "banrep" not in haystack and "banco de la republica" not in haystack:
            continue
        forecast_id = str(row.get("forecast_id") or "").strip()
        if forecast_id:
            active_ids.append(forecast_id)
    return active_ids


def _extract_spanish_dates(text: str, *, base_date: date) -> list[date]:
    dates: list[date] = []
    for match in _SPANISH_DATE_RE.finditer(text):
        day = int(match.group("day"))
        month = _SPANISH_MONTHS[match.group("month").lower()]
        year_text = match.group("year")
        year = int(year_text) if year_text else base_date.year
        try:
            parsed = date(year, month, day)
        except ValueError:
            continue
        if not year_text and parsed < base_date:
            try:
                parsed = date(year + 1, month, day)
            except ValueError:
                continue
        if parsed not in dates:
            dates.append(parsed)
    return dates


def _item_reference_date(item: dict[str, Any], fallback: date) -> date:
    parsed = _parse_optional_date(str(item.get("published_at") or ""))
    return parsed or fallback


def _parse_optional_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _parse_date(value: str, *, field_name: str) -> date:
    parsed = _parse_optional_date(value)
    if parsed is None:
        raise ValueError(f"{field_name} must be an ISO date")
    return parsed


def _run_date_from_dir(run_path: Path) -> str:
    try:
        _parse_date(run_path.name, field_name="run_dir.name")
    except ValueError as exc:
        raise PreflightInputError(
            f"Run directory name must be YYYY-MM-DD: {run_path}"
        ) from exc
    return run_path.name


def _generated_at_from_run_summary(run_path: Path, run_date: str) -> str:
    summary_path = run_path / "run_summary.json"
    if not summary_path.is_file():
        return f"{run_date}T23:59:59Z"
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return f"{run_date}T23:59:59Z"
    if isinstance(payload, dict) and payload.get("finished_at"):
        return str(payload["finished_at"])
    return f"{run_date}T23:59:59Z"


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PreflightInputError(f"Could not read {path}: {exc}") from exc
    if not isinstance(payload, list):
        raise PreflightInputError(f"{path} must contain a JSON list")
    return [item for item in payload if isinstance(item, dict)]


def _health_by_source_id(
    source_health: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    if source_health is None:
        return {}
    return {
        str(item.get("source_id")): item
        for item in source_health
        if isinstance(item, dict) and item.get("source_id")
    }


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _item_sort_key(item: dict[str, Any]) -> tuple[str, str]:
    return (str(item.get("published_at") or ""), str(item.get("id") or ""))


def _display_date(value: date) -> str:
    return f"{value.strftime('%B')} {value.day}, {value.year}"


def _spanish_display_date(value: date) -> str:
    months = {
        1: "enero",
        2: "febrero",
        3: "marzo",
        4: "abril",
        5: "mayo",
        6: "junio",
        7: "julio",
        8: "agosto",
        9: "septiembre",
        10: "octubre",
        11: "noviembre",
        12: "diciembre",
    }
    month = months.get(value.month)
    return f"{value.day} de {month}" if month else value.isoformat()
