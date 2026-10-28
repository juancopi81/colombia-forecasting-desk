"""Prospective, internally scored calls on scheduled official events.

Planning and rendering are deterministic and offline. Appenders use the actual
clock, preserve existing predictions and never write the public ledger.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import yaml

from .schemas import require_valid
from .shadow_forecasts import (
    ShadowForecastError, append_shadow_forecast, read_shadow_forecast_ledger,
    resolve_shadow_forecast, flag_shadow_forecasts_for_review, forecast_event_key,
)

BOGOTA = ZoneInfo("America/Bogota")
DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "forecast_families.yaml"
FAMILY_BY_RELEASE = {"ipc": "ipc_annual_up", "ise": "ise_annual_up", "labor": "unemployment_down_yoy", "geih": "unemployment_down_yoy"}


def _read(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {path}: {exc}") from exc


def _rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {path}: {exc}") from exc
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError(f"{path} must contain JSON objects")
    return rows


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("an exact clock must include its timezone")
    return parsed


def _month_shift(period: str, months: int) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", period):
        raise ValueError(f"Invalid reference period: {period}")
    year, month = map(int, period.split("-"))
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid reference period: {period}")
    index = year * 12 + month - 1 + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def _official_url(url: Any, family: str) -> bool:
    try:
        parsed = urlsplit(str(url))
    except ValueError:
        return False
    domain = "banrep.gov.co" if family == "banrep_rate_decision" else "dane.gov.co"
    host = parsed.hostname or ""
    return parsed.scheme == "https" and (host == domain or host.endswith("." + domain)) and not parsed.username and not parsed.password


def load_forecast_families(path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict) or config.get("schema_version") != "forecast_families.v1" or not isinstance(config.get("families"), dict):
        raise ValueError("Invalid forecast family configuration")
    if config.get("window_days") != [1, 7]:
        raise ValueError("Family forecasts require the prospective 1–7 day window")
    return config


def _observed(card: dict[str, Any]) -> bool:
    return card.get("status") == "observed" and card.get("freshness_status") not in ("failed", "stale", "unparsed")


def _events(preflight: dict[str, Any], raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Use the official raw calendar even if preflight lacked a current rate."""
    events: dict[tuple[str, str], dict[str, Any]] = {}
    for item in raw_items:
        metadata = item.get("metadata") or {}
        if not isinstance(metadata, dict):
            continue
        if item.get("source_id") == "banrep_junta_calendar" and metadata.get("event_type") == "banrep_policy_rate_decision":
            family = "banrep_rate_decision"
        elif item.get("source_id") == "dane_publication_calendar":
            family = FAMILY_BY_RELEASE.get(metadata.get("release_family"))
        else:
            continue
        if family == "ipc_annual_up" and any(token in str(item.get("title", "")).casefold() for token in ("sin alimentos", "core", "excluyendo")):
            continue
        day = metadata.get("scheduled_date")
        if family and day:
            events[(family, day)] = {
                "family_id": family, "event_date": day,
                "event_at": metadata.get("scheduled_at_local"),
                "reference_period": metadata.get("reference_period"),
                "calendar_url": metadata.get("calendar_event_url") or item.get("url"),
                "title": item.get("title", ""),
                "calendar_ref": {"artifact": "raw_items.json", "locator": f"id={item.get('id', '')}"},
            }
    for opportunity in preflight.get("opportunities", []):
        identity = str(opportunity.get("opportunity_id", ""))
        family = "banrep_rate_decision" if opportunity.get("event_type") == "banrep_policy_rate_decision" or identity.startswith("banrep_policy_rate_decision") else None
        if family is None:
            family = next((candidate for release, candidate in FAMILY_BY_RELEASE.items() if identity.startswith(f"dane_{release}_release_")), None)
        day = opportunity.get("event_date")
        if not family or not day or (family, day) in events:
            continue
        evidence = opportunity.get("source_evidence") or []
        clock = next((ref.get("value") for ref in evidence if "T" in str(ref.get("value", ""))), None)
        events[(family, day)] = {
            "family_id": family, "event_date": day, "event_at": opportunity.get("event_at") or clock,
            "reference_period": opportunity.get("reference_period"),
            "calendar_url": evidence[0].get("url") if evidence else "",
            "title": opportunity.get("title", ""),
            "calendar_ref": {"artifact": "m3_preflight_opportunities.json", "locator": f"opportunity_id={identity}"},
        }
    return [events[key] for key in sorted(events)]


def _verified_policy_fallback(value: dict[str, Any] | None, as_of: date) -> tuple[float | None, dict[str, Any] | None]:
    if not isinstance(value, dict):
        return None, None
    rate, decision, verified = value.get("policy_rate_pct"), value.get("decision_date"), value.get("verified_at")
    try:
        day = date.fromisoformat(str(decision))
        checked = _instant(str(verified)).astimezone(BOGOTA).date()
    except (ValueError, TypeError):
        return None, None
    if not _number(rate) or not 0 <= rate <= 100 or not _official_url(value.get("source_url"), "banrep_rate_decision"):
        return None, None
    if value.get("verified") is not True or not day <= checked <= as_of or (as_of - day).days > 62 or (as_of - checked).days > 7:
        return None, None
    return float(rate), deepcopy(value)


def _history(card: dict[str, Any], key: str, as_of: date) -> list[dict[str, Any]]:
    """Only explicit official, dated pre-forecast history may form a baseline."""
    values = card.get("values") or {}
    rows = values.get("history") or []
    url = values.get("history_source_url") or card.get("source_url")
    family = "banrep_rate_decision" if card.get("indicator_id") == "policy_rate_ibr" else "ipc_annual_up"
    if not _official_url(url, family) or not isinstance(rows, list):
        return []
    result = []
    for row in rows:
        if not isinstance(row, dict) or not _number(row.get(key)):
            continue
        period = str(row.get("period") or row.get("date") or "")
        try:
            observed = date.fromisoformat(period if len(period) == 10 else period + "-01")
            published = date.fromisoformat(str(row.get("published_at", ""))[:10])
        except ValueError:
            continue
        if observed < as_of and published <= as_of:
            result.append({**row, "period": period})
    return sorted(result, key=lambda row: row["period"])


def _unemployment_momentum(rows: list[dict[str, Any]], card: dict[str, Any], reference_period: str | None) -> dict[str, Any] | None:
    """Condition next month's outcome on the latest observed annual rise."""
    if not rows:
        return None
    key = "national_unemployment_rate_pct"
    by_period = {row["period"]: row for row in rows}
    # No gap may silently turn an older condition into the target's momentum.
    latest = _month_shift(reference_period, -1) if reference_period else str(card.get("period") or rows[-1]["period"])
    deltas = {period: Decimal(str(row[key])) - Decimal(str(by_period[prior][key]))
              for period, row in by_period.items() if (prior := _month_shift(period, -12)) in by_period}
    rise = deltas.get(latest)
    minimum = Decimal("0.5")
    if rise is None or rise < minimum:
        return None
    comparisons = [
        {"condition_period": period, "outcome_period": following, "condition_change_pp": float(delta),
         "outcome_change_pp": float(deltas[following]), "yes": deltas[following] < 0}
        for period, delta in sorted(deltas.items())
        if delta >= minimum and (following := _month_shift(period, 1)) in deltas and following <= latest
    ]
    if not comparisons:
        return None
    yes = sum(pair["yes"] for pair in comparisons)
    values = card.get("values") or {}
    return {"label": "official_momentum_conditional_yes_frequency", "probability": yes / len(comparisons),
            "rationale": f"After a year-on-year national unemployment rise of at least 0.5 percentage points, the next month's rate fell year-on-year in {yes} of {len(comparisons)} comparable observed pairs. The latest applicable annual change ({latest}) is +{rise} percentage points. This is a historical comparator, not the forecast probability.",
            "rule": "momentum_conditional_frequency", "inputs": rows,
            "condition": {"period": latest, "year_on_year_change_pp": float(rise), "minimum_rise_pp": 0.5},
            "comparisons": comparisons,
            "window": {"start": comparisons[0]["condition_period"], "end": comparisons[-1]["outcome_period"], "n": len(comparisons), "yes": yes},
            "source_url": values.get("history_source_url") or card.get("source_url"),
            "caveat": values.get("history_caveat") or "Official historical observations may include revisions and methodology changes; this is not an archived first-release estimate."}


def baseline_for_slot(family: str, card: dict[str, Any], *, threshold: float | None, as_of: date, survey: dict[str, Any] | None = None, reference_period: str | None = None) -> dict[str, Any]:
    fallback = {"label": "uninformative_50_50", "probability": 0.5, "rationale": "No published outcome distribution or comparable official historical frequency is available; 50/50 is explicitly uninformative.", "rule": "uninformative_50_50", "inputs": [], "window": None}
    if family == "banrep_rate_decision" and isinstance(survey, dict) and _observed(survey) and _official_url(survey.get("source_url"), family):
        values = survey.get("values") or {}
        distribution = values.get("policy_rate_distribution")
        try:
            published = date.fromisoformat(str(survey.get("release_date"))[:10])
        except ValueError:
            published = as_of + timedelta(days=1)
        if isinstance(distribution, list) and threshold is not None and published <= as_of:
            valid = all(isinstance(row, dict) and _number(row.get("rate_pct")) and isinstance(row.get("count"), int) and not isinstance(row["count"], bool) and row["count"] >= 0 for row in distribution)
            total = sum(row["count"] for row in distribution) if valid else 0
            horizon = values.get("policy_rate_distribution_event_date")
            if total and horizon:
                yes = sum(row["count"] for row in distribution if row["rate_pct"] > threshold)
                return {"label": "published_analyst_yes_share", "probability": yes / total, "rationale": f"{yes} of {total} surveyed analysts expect the stated YES outcome; no probability is inferred from a median.", "rule": "survey_share", "inputs": distribution, "window": {"published_at": survey["release_date"], "event_date": horizon}, "source_url": survey["source_url"]}
    key = {"banrep_rate_decision": "policy_rate_pct", "ipc_annual_up": "annual_variation_pct", "ise_annual_up": "annual_growth_pct", "unemployment_down_yoy": "national_unemployment_rate_pct"}[family]
    rows = _history(card, key, as_of)
    if family == "unemployment_down_yoy":
        momentum = _unemployment_momentum(rows, card, reference_period)
        if momentum:
            return momentum
    comparisons: list[bool] = []
    if family == "banrep_rate_decision":
        # Persistence only estimates P(hike) when the latest decision was a hike.
        # A persistence-of-cuts rate would estimate the wrong binary event.
        directions = [1 if b[key] > a[key] else -1 if b[key] < a[key] else 0 for a, b in zip(rows, rows[1:])]
        if directions and directions[-1] == 1:
            comparisons = [b == 1 for a, b in zip(directions, directions[1:]) if a == 1]
    elif family == "unemployment_down_yoy":
        by_period = {row["period"]: row for row in rows}
        comparisons = [row[key] < by_period[_month_shift(row["period"], -12)][key] for row in rows if _month_shift(row["period"], -12) in by_period]
    else:
        comparisons = [b[key] > a[key] for a, b in zip(rows, rows[1:]) if _month_shift(a["period"], 1) == b["period"]]
    if comparisons:
        yes = sum(comparisons)
        return {"label": "official_historical_yes_frequency", "probability": yes / len(comparisons), "rationale": f"The declared YES event occurred {yes} times in {len(comparisons)} comparable observations in the stated pre-forecast official history.", "rule": "empirical_frequency", "inputs": rows, "window": {"start": rows[0]["period"], "end": rows[-1]["period"], "n": len(comparisons)}, "source_url": (card.get("values") or {}).get("history_source_url") or card.get("source_url")}
    return fallback


def _survey_context(raw_items: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Adapt the existing EME raw-item metadata without inventing a distribution."""
    candidates = []
    for item in raw_items:
        if item.get("source_id") != "banrep_eme_expectations":
            continue
        metadata = item.get("metadata") or {}
        expectations = metadata.get("expectations") or {}
        policy = expectations.get("policy_rate") or {}
        distribution = metadata.get("policy_rate_distribution") or policy.get("distribution")
        if not distribution or metadata.get("publication_date_known") is False:
            continue
        candidates.append({"status": "observed", "freshness_status": metadata.get("freshness_status"), "source_url": item.get("url"), "release_date": metadata.get("release_date") or item.get("published_at"), "values": {"policy_rate_distribution": distribution, "policy_rate_distribution_event_date": metadata.get("policy_rate_distribution_event_date") or policy.get("horizon_date")}})
    return max(candidates, key=lambda row: str(row.get("release_date", ""))) if candidates else None


def plan_family_forecasts(run_dir: Path, *, config_path: Path = DEFAULT_CONFIG, policy_fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    run_day = date.fromisoformat(run_dir.name)
    config = load_forecast_families(config_path)
    cards = _read(run_dir / "indicator_watch.json", [])
    if not isinstance(cards, list):
        raise ValueError("indicator_watch.json must contain a list")
    by_id = {card.get("indicator_id"): card for card in cards if isinstance(card, dict)}
    preflight = _read(run_dir / "m3_preflight_opportunities.json", {"opportunities": []})
    raw_items = _read(run_dir / "raw_items.json", [])
    if not isinstance(preflight, dict) or not isinstance(raw_items, list):
        raise ValueError("Invalid preflight or raw items artifact")
    from .family_evidence import reference_history_for_run
    reference_history = reference_history_for_run(run_dir)
    if reference_history:
        labor = deepcopy(by_id.get("labor_market", {}))
        labor_values = labor.setdefault("values", {})
        labor_values["history"] = reference_history["rows"]
        labor_values["history_source_url"] = reference_history["source_url"]
        labor_values["history_caveat"] = reference_history["caveat"]
        by_id["labor_market"] = labor
    survey = by_id.get("banrep_eme_expectations") or _survey_context(raw_items)
    slots, notices = [], []
    events = _events(preflight, raw_items)
    if not events:
        notices.append("No usable official calendar events were captured; zero slots does not establish complete calendar coverage.")
    for event in events:
        family, event_day = event["family_id"], date.fromisoformat(event["event_date"])
        days = (event_day - run_day).days
        if not 1 <= days <= 7 or family not in config["families"]:
            continue
        definition = config["families"][family]
        card = by_id.get(definition["indicator_id"], {})
        values = card.get("values") or {}
        missing = []
        threshold: float | None = None
        provenance: dict[str, Any] | None = None
        reference = event.get("reference_period") or (_month_shift(event_day.strftime("%Y-%m"), -definition.get("reference_lag_months", 0)) if family != "banrep_rate_decision" else event_day.isoformat())
        if family == "banrep_rate_decision":
            if _observed(card) and _number(values.get("policy_rate_pct")) and _official_url(card.get("source_url"), family):
                threshold = float(values["policy_rate_pct"])
                provenance = {"source_url": card["source_url"], "period": card.get("period"), "artifact": "indicator_watch.json"}
            else:
                threshold, provenance = _verified_policy_fallback(policy_fallback, run_day)
            if threshold is None:
                notices.append("BanRep slot unavailable: no current policy rate")
                continue
        elif family == "unemployment_down_yoy":
            last_year = _month_shift(reference, -12)
            history = _history(card, definition["value_key"], run_day)
            match = next((row for row in history if row["period"] == last_year), None)
            if match:
                threshold = float(match[definition["value_key"]])
            elif card.get("period") == reference and _number(values.get("national_unemployment_previous_year_pct")):
                threshold = float(values["national_unemployment_previous_year_pct"])
            elif (values.get("national_unemployment_previous_year_period") or values.get("comparison_period")) == last_year and _number(values.get("national_unemployment_previous_year_pct")):
                threshold = float(values["national_unemployment_previous_year_pct"])
            else:
                missing.append(f"The national unemployment comparator for {last_year} is unavailable.")
            if not _observed(card):
                missing.append("The national unemployment feed is not observed.")
        else:
            expected = _month_shift(reference, -1)
            if _observed(card) and card.get("period") == expected and _number(values.get(definition["value_key"])):
                threshold = float(values[definition["value_key"]])
            else:
                missing.append(f"A current comparable official {expected} observation is unavailable.")
        if "sin alimentos" in str(event.get("title", "")).lower() or "core" in str(event.get("title", "")).lower():
            notices.append("The calendar item is a core-inflation release, outside the headline-inflation family.")
            continue
        clock = event.get("event_at")
        if clock:
            try:
                parsed_clock = _instant(str(clock))
                if parsed_clock.astimezone(BOGOTA).date() != event_day:
                    raise ValueError("event clock disagrees with event date")
            except ValueError:
                missing.append("The official event time is invalid.")
                clock = None
        # Date-only official calendars use a conservative midnight cutoff,
        # explicitly distinguished from a claimed official publication time.
        cutoff = clock or datetime.combine(event_day, time.min, BOGOTA).isoformat()
        enabled = (definition.get("enabled") or (definition.get("enable_when_observed") and _observed(card))) and not missing
        if not definition.get("enabled") and not enabled and not missing:
            missing.append("This forecast family is disabled in configuration.")
        threshold_text = f"{threshold:g}" if threshold is not None else "the unavailable official comparator"
        question = definition["question"].format(threshold=threshold_text, reference_period=reference, event_date=event_day.isoformat())
        direction = "strictly above" if definition["comparison"] == "gt" else "strictly below"
        opposite = "at or below" if definition["comparison"] == "gt" else "at or above"
        baseline = baseline_for_slot(family, card, threshold=threshold, as_of=run_day, survey=survey, reference_period=reference)
        if baseline["rule"] == "survey_share" and baseline["window"]["event_date"] != event_day.isoformat():
            baseline = baseline_for_slot(family, card, threshold=threshold, as_of=run_day, survey=None)
        slot = {
            "slot_id": f"{family}:{event_day.isoformat()}", "family_id": family,
            "question": question,
            "resolution_source": definition["resolver_url"],
            "resolution_criteria": f"YES: The first comparable official release for {reference}, series {definition['series_variant']}, reports a value {direction} {threshold_text}%. NO: That release expressly reports a value {opposite} {threshold_text}%. Missing, postponed, contradictory or incomparable records remain unresolved. Later revisions do not change the first-release outcome.",
            "resolution_deadline": event_day.isoformat(),
            "resolution_check_window_end": (event_day + timedelta(days=config["resolution_check_days"])).isoformat(),
            "event_date": event_day.isoformat(), "event_at": clock,
            "cutoff_at": cutoff, "cutoff_basis": "official_event_clock" if clock else "conservative_start_of_official_event_date",
            "reference_period": reference, "series_variant": definition["series_variant"],
            "indicator_id": definition["indicator_id"], "value_key": definition["value_key"],
            "comparison": definition["comparison"], "threshold": threshold,
            "days_until_event": days, "enabled": bool(enabled), "missing_evidence": missing,
            "baseline": baseline, "calendar_url": event.get("calendar_url"),
            "calendar_ref": event["calendar_ref"], "policy_rate_provenance": provenance,
            "comparator_provenance": {"source_url": reference_history["source_url"], "source_sha256": reference_history["source_sha256"], "precision_decimal_places": 1, "caveat": reference_history["caveat"]} if reference_history and family == "unemployment_down_yoy" else None,
        }
        slots.append(slot)
    artifact = {"schema_version": "family_forecast_slots.v1", "run_date": run_day.isoformat(), "slots": slots, "notices": sorted(set(notices)), "calendar_coverage": "observed_artifacts_only"}
    require_valid(artifact)
    return artifact


def _evidence_exists(ref: dict[str, Any], run_dir: Path) -> bool:
    artifact = str(ref.get("artifact", ""))
    if Path(artifact).is_absolute() or ".." in Path(artifact).parts:
        return False
    path = (run_dir.parent.parent / artifact) if artifact.startswith("runs/") else run_dir / artifact
    if not path.is_file():
        return False
    locator = str(ref.get("locator", ""))
    content = path.read_text(encoding="utf-8")
    if "=" not in locator:
        return bool(locator) and locator in content
    key, expected = locator.split("=", 1)
    if key == "heading":
        return expected.casefold() in content.casefold()
    try:
        value = json.loads(content)
    except json.JSONDecodeError:
        return False
    def contains(node: Any) -> bool:
        if isinstance(node, dict):
            return str(node.get(key, "")) == expected or any(contains(item) for item in node.values())
        return isinstance(node, list) and any(contains(item) for item in node)
    return contains(value)


def append_family_forecasts(run_dir: Path, ledger_path: Path, skips_path: Path, *, model: str, reasoning_effort: str, now: datetime | None = None, public_ledger_path: Path | None = None) -> dict[str, list[str]]:
    slots_artifact = _read(run_dir / "family_forecast_slots.json")
    authored = _read(run_dir / "family_forecasts.json")
    require_valid(slots_artifact, "family_forecast_slots.v1")
    require_valid(authored, "family_forecasts.v1")
    run_date = run_dir.name
    if slots_artifact["run_date"] != run_date or authored["run_date"] != run_date:
        raise ValueError("Family artifact dates must equal the run directory")
    acceptance = _read(run_dir / "acceptance_report.json", {})
    if not isinstance(acceptance, dict) or acceptance.get("status") != "pass":
        raise ValueError("Family forecasts require a decision-grade accepted run")
    clock = now or datetime.now(timezone.utc)
    if clock.tzinfo is None or clock.astimezone(BOGOTA).date().isoformat() != run_date:
        raise ValueError("Family forecasts cannot be backdated: actual creation date must equal run_date")
    if not model.strip() or not reasoning_effort.strip():
        raise ValueError("Record the actual model and reasoning effort")
    slots = {slot["slot_id"]: slot for slot in slots_artifact["slots"]}
    entries = {entry["slot_id"]: entry for entry in authored["entries"]}
    if entries.keys() - slots.keys():
        raise ValueError("Authored forecast contains an unknown slot_id")
    existing, issues = read_shadow_forecast_ledger(ledger_path) if ledger_path.exists() else ([], [])
    if issues:
        raise ValueError("Existing shadow ledger is invalid")
    public = _rows(public_ledger_path or ledger_path.with_name("forecast_log.jsonl"))
    by_event = {key: row for row in existing + public if (key := forecast_event_key(row))}
    required = {identity for identity, slot in slots.items() if slot["enabled"] and identity not in by_event}
    if required - entries.keys():
        raise ValueError("Every enabled due slot requires a forecast or a written skip")
    pending_rows, pending_skips = [], []
    result: dict[str, list[str]] = {"appended": [], "already_recorded": [], "skipped": [], "covered_by_existing": []}
    previous_skips = _rows(skips_path)
    for identity, entry in entries.items():
        slot = slots[identity]
        if identity not in by_event:
            if clock >= _instant(slot.get("cutoff_at", "")):
                raise ValueError("Forecast or skip was created after the pre-event cutoff")
            days = (date.fromisoformat(slot["event_date"]) - clock.astimezone(BOGOTA).date()).days
            if not 1 <= days <= 7:
                raise ValueError("Family calls and skips must be created 1–7 days before the event")
        if entry["decision"] == "skip":
            skip = {"slot_id": identity, "family_id": slot["family_id"], "run_date": run_date, "event_date": slot["event_date"], "reason": entry["skip_reason"]}
            prior = next((row for row in previous_skips if row.get("slot_id") == identity and row.get("run_date") == run_date), None)
            if prior and any(prior.get(key) != value for key, value in skip.items()):
                raise ValueError("An existing same-day skip is immutable")
            if not prior:
                pending_skips.append({**skip, "created_at": clock.isoformat()})
            result["skipped"].append(identity)
            continue
        if identity in by_event:
            row = by_event[identity]
            if row.get("slot_id") == identity and (row.get("probability") != entry["probability"] or row.get("rationale_for") != entry["rationale_for"] or row.get("rationale_against") != entry["rationale_against"] or row.get("falsifier") != entry["falsifier"]):
                raise ValueError("An existing forecast's prediction fields are immutable")
            result["already_recorded" if row.get("slot_id") == identity else "covered_by_existing"].append(row["forecast_id"])
            continue
        if not slot["enabled"]:
            raise ValueError("A disabled slot cannot receive a forecast")
        if not _number(slot.get("threshold")) or not slot.get("reference_period") or not slot.get("series_variant") or slot.get("comparison") not in ("gt", "lt"):
            raise ValueError("Enabled slot lacks an exact comparison, reference period or series")
        if not _official_url(slot.get("resolution_source"), slot["family_id"]):
            raise ValueError("Resolver is not the declared official institution")
        if not all(_evidence_exists(ref, run_dir) for ref in entry["evidence_refs"]):
            raise ValueError("Forecast evidence reference does not exist in the preserved run artifacts")
        row = {
            "schema_version": "shadow_forecast.v1", "forecast_id": "family_" + identity.replace(":", "_"),
            "created_at": clock.isoformat(), "run_date": run_date, "status": "open", "visibility": "internal",
            "question": slot["question"], "probability": entry["probability"],
            "resolution_deadline": slot["resolution_deadline"], "resolution_check_window_end": slot["resolution_check_window_end"],
            "resolution_source": slot["resolution_source"], "resolution_criteria": slot["resolution_criteria"],
            "baseline": slot["baseline"], "evidence_refs": entry["evidence_refs"],
            "rationale_for": entry["rationale_for"], "rationale_against": entry["rationale_against"], "falsifier": entry["falsifier"],
            "model": model, "reasoning_effort": reasoning_effort, "source_analysis": f"runs/{run_date}/family_forecasts.json",
            "forecast_family": slot["family_id"], "slot_id": identity,
            "family_event": {key: deepcopy(slot[key]) for key in ("event_date", "event_at", "cutoff_at", "cutoff_basis", "reference_period", "series_variant", "indicator_id", "value_key", "comparison", "threshold")},
        }
        pending_rows.append(row)
    # Validate the entire input before any writes; individual ledger writes are
    # locked and atomic. Rerunning after an interruption is safe and idempotent.
    for row in pending_rows:
        append_shadow_forecast(ledger_path, row)
        result["appended"].append(row["forecast_id"])
    if pending_skips:
        skips_path.parent.mkdir(parents=True, exist_ok=True)
        with skips_path.with_name("." + skips_path.name + ".lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            known = {(row.get("slot_id"), row.get("run_date")) for row in _rows(skips_path)}
            with skips_path.open("a", encoding="utf-8") as output:
                for row in pending_skips:
                    if (row["slot_id"], row["run_date"]) not in known:
                        output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    return result


def _card_resolution(row: dict[str, Any], run_dir: Path) -> dict[str, Any] | None:
    event = row["family_event"]
    if run_dir.name < event["event_date"]:
        return None
    cards = _read(run_dir / "indicator_watch.json", [])
    if not isinstance(cards, list):
        return None
    matches = [card for card in cards if isinstance(card, dict) and card.get("indicator_id") == event["indicator_id"]]
    observations = []
    for card in matches:
        values = card.get("values") or {}
        if not _observed(card) or card.get("period") != event["reference_period"]:
            continue
        if str(card.get("release_date", ""))[:10] != event["event_date"]:
            continue
        variant = values.get("series_variant") or card.get("series_variant") or event["series_variant"]
        if variant == "national_total_original":
            variant = "national_original_unemployment"
        if variant != event["series_variant"] or values.get("is_revision") is True or values.get("first_release") is False:
            continue
        # A same-day archived official observation pins the first release. A
        # later capture needs an explicit verified first-release document,
        # otherwise a revision could silently masquerade as the initial value.
        if run_dir.name != event["event_date"] and not (values.get("first_release") is True and _official_url(values.get("first_release_source_url"), row["forecast_family"])):
            continue
        value = values.get(event["value_key"])
        url = values.get("first_release_source_url") or card.get("source_url")
        if not _number(value) or not _official_url(url, row["forecast_family"]):
            continue
        observations.append({"value": float(value), "source_url": url, "published_at": card["release_date"], "artifact": f"runs/{run_dir.name}/indicator_watch.json", "locator": f"indicator_id={event['indicator_id']}", "sha256": hashlib.sha256(json.dumps(card, sort_keys=True).encode()).hexdigest(), "period": card["period"], "series_variant": variant, "event_date": event["event_date"]})
    if not observations or len({observation["value"] for observation in observations}) != 1:
        return None
    return observations[0]


def _banrep_resolution(row: dict[str, Any], run_dir: Path) -> dict[str, Any] | None:
    event = row["family_event"]
    raw = _read(run_dir / "raw_items.json", [])
    if run_dir.name < event["event_date"] or not isinstance(raw, list):
        return None
    candidates = []
    for item in raw:
        if not isinstance(item, dict) or item.get("source_id") not in ("banrep_junta_comunicados", "banrep_rss"):
            continue
        url = item.get("url", "")
        if not _official_url(url, row["forecast_family"]) or "minuta" in str(url).lower() or str(item.get("published_at", ""))[:10] != event["event_date"]:
            continue
        text = str(item.get("title", ""))
        if "junta" not in text.lower() or "política monetaria" not in text.lower() and "policy" not in text.lower():
            continue
        rate = re.search(r"(?:\ba|\ben|\bat|\bto)\s+(\d+(?:[.,]\d+)?)\s*%", text, re.I)
        if not rate:
            continue
        candidates.append({"value": float(rate.group(1).replace(",", ".")), "source_url": url, "published_at": item["published_at"], "artifact": f"runs/{run_dir.name}/raw_items.json", "locator": f"id={item.get('id', '')}", "sha256": hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest(), "period": event["reference_period"], "series_variant": "policy_rate", "event_date": event["event_date"]})
    if not candidates or len({candidate["value"] for candidate in candidates}) != 1:
        return None
    candidate = candidates[0]
    for card in _read(run_dir / "indicator_watch.json", []):
        values = card.get("values") or {}
        if card.get("indicator_id") == "policy_rate_ibr" and _observed(card) and str(card.get("period", ""))[:10] >= event["event_date"] and _number(values.get("policy_rate_pct")) and values["policy_rate_pct"] != candidate["value"]:
            return None
    return candidate


def resolve_family_forecasts(runs_root: Path, ledger_path: Path, *, as_of: str, now: datetime | None = None) -> dict[str, Any]:
    """Resolve only exact archived first releases; flag absent records for review."""
    day = date.fromisoformat(as_of)
    clock = now or datetime.now(timezone.utc)
    if clock.tzinfo is None or day > clock.astimezone(BOGOTA).date():
        raise ValueError("A resolver cannot manufacture future resolution timestamps")
    rows, issues = read_shadow_forecast_ledger(ledger_path) if ledger_path.exists() else ([], [])
    if issues:
        raise ValueError("Existing shadow ledger is invalid")
    runs = sorted(path for path in runs_root.iterdir() if path.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name) and path.name <= as_of) if runs_root.exists() else []
    resolved, waiting = [], []
    from .family_evidence import verified_release_for_row
    for row in rows:
        if row.get("status") != "open" or not row.get("forecast_family") or row["resolution_deadline"] > as_of:
            continue
        if clock < _instant(row["family_event"]["cutoff_at"]):
            waiting.append(row["forecast_id"])
            continue
        observation = None
        for run in runs:
            observation = _banrep_resolution(row, run) if row["forecast_family"] == "banrep_rate_decision" else verified_release_for_row(row, run) or _card_resolution(row, run)
            if observation:
                break
        if observation is None:
            waiting.append(row["forecast_id"])
            continue
        try:
            if _instant(observation["published_at"]) > clock:
                waiting.append(row["forecast_id"])
                continue
            if observation.get("captured_at") and _instant(observation["captured_at"]) > clock:
                waiting.append(row["forecast_id"])
                continue
        except (ValueError, TypeError):
            waiting.append(row["forecast_id"])
            continue
        event = row["family_event"]
        yes = observation["value"] > event["threshold"] if event["comparison"] == "gt" else observation["value"] < event["threshold"]
        resolve_shadow_forecast(
            ledger_path, forecast_id=row["forecast_id"], outcome="YES" if yes else "NO",
            resolved_at=clock.isoformat(),
            resolution_value=f"First official {observation['period']} {observation['series_variant']} value: {observation['value']:g}%",
            resolution_url=observation["source_url"],
            resolution_artifact=observation["artifact"], resolution_evidence=observation,
        )
        resolved.append(row["forecast_id"])
    flagged = flag_shadow_forecasts_for_review(ledger_path, as_of=as_of, now=clock)
    return {"resolved": resolved, "waiting_for_official_records": waiting, "needs_review": flagged}


_RESOLUTION_KEYS = {"outcome", "resolved_at", "resolution_value", "resolution_url", "resolution_artifact", "resolution_evidence", "brier_score", "baseline_brier_score", "brier_improvement_vs_baseline", "voided_at", "void_reason", "void_human_approved"}


def _as_of_rows(rows: list[dict[str, Any]], as_of: str) -> list[dict[str, Any]]:
    cutoff = datetime.combine(date.fromisoformat(as_of) + timedelta(days=1), time.min, BOGOTA)
    output = []
    for original in rows:
        row = deepcopy(original)
        created = row.get("created_at")
        try:
            if created and _instant(created) >= cutoff:
                continue
        except (ValueError, TypeError):
            continue
        if str(row.get("run_date", "")) > as_of:
            continue
        recorded = row.get("voided_at") if row.get("status") == "void" else row.get("resolved_at")
        try:
            is_future = recorded and _instant(recorded) >= cutoff
        except (ValueError, TypeError):
            is_future = True
        # A terminal status without a known observation time is not admissible
        # to a historical scoreboard. Its original prediction is still open.
        if is_future or row.get("status") in ("resolved", "void") and not recorded:
            row = {key: value for key, value in row.items() if key not in _RESOLUTION_KEYS}
            row["status"] = "open"
        try:
            future_flag = bool(row.get("needs_review_at")) and _instant(row["needs_review_at"]) >= cutoff
        except (ValueError, TypeError):
            future_flag = True
        if str(row.get("needs_review_since", "")) > as_of or future_flag:
            row.pop("needs_review", None)
            row.pop("needs_review_since", None)
            row.pop("needs_review_at", None)
        output.append(row)
    return output


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _summarize(rows: list[dict[str, Any]], as_of: str) -> dict[str, Any]:
    open_rows = [row for row in rows if row.get("status") == "open"]
    resolved = [row for row in rows if row.get("status") == "resolved"]
    scored = [row for row in resolved if _number(row.get("probability")) and 0 <= row["probability"] <= 1 and row.get("outcome") in ("YES", "NO")]
    scores = [round((row["probability"] - (row["outcome"] == "YES")) ** 2, 4) for row in scored]
    directional = [row for row in scored if row["probability"] != 0.5]
    right = sum((row["probability"] > 0.5) == (row["outcome"] == "YES") for row in directional)
    paired = [row for row in scored if isinstance(row.get("baseline"), dict) and _number(row["baseline"].get("probability")) and 0 <= row["baseline"]["probability"] <= 1]
    model = [round((row["probability"] - (row["outcome"] == "YES")) ** 2, 4) for row in paired]
    baseline = [round((row["baseline"]["probability"] - (row["outcome"] == "YES")) ** 2, 4) for row in paired]
    return {
        "open": open_rows, "resolved": resolved, "void": [row for row in rows if row.get("status") == "void"],
        "needs_review": [row for row in open_rows if row.get("needs_review")],
        "waiting_for_records": [row for row in open_rows if str(row.get("resolution_deadline", ""))[:10] <= as_of],
        "n_scored": len(scored), "directional_n": len(directional), "right_side_n": right,
        "right_side_rate": right / len(directional) if directional else None,
        "no_direction_n": len(scored) - len(directional), "mean_brier": _mean(scores),
        "baseline_n": len(paired), "model_brier_paired": _mean(model), "baseline_brier": _mean(baseline),
        "improvement": _mean([b - m for b, m in zip(baseline, model)]),
    }


def build_scoreboard(repo_root: Path, *, as_of: str) -> dict[str, Any]:
    date.fromisoformat(as_of)
    public = _as_of_rows(_rows(repo_root / "forecasts" / "forecast_log.jsonl"), as_of)
    internal = _as_of_rows(_rows(repo_root / "forecasts" / "shadow_forecast_log.jsonl"), as_of)
    skips = [row for row in _rows(repo_root / "forecasts" / "family_skips.jsonl") if row.get("run_date", "") <= as_of]
    events: dict[str, dict[str, Any]] = {}
    runs_root = repo_root / "runs"
    run_dirs = sorted(path for path in runs_root.iterdir() if path.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name) and path.name <= as_of) if runs_root.exists() else []
    for run in run_dirs:
        artifact = _read(run / "family_forecast_slots.json")
        if artifact is None:
            continue
        require_valid(artifact, "family_forecast_slots.v1")
        for slot in artifact["slots"]:
            events[slot["slot_id"]] = slot
    by_event = {key: row for row in public + internal if (key := forecast_event_key(row))}
    family_ids = list(load_forecast_families()["families"])
    family_summaries = []
    for family in family_ids:
        known = {identity: slot for identity, slot in events.items() if slot["family_id"] == family}
        # Explicitly tagged calls remain visible when their plan artifact is
        # unavailable; do not silently drop ledger history from the scoreboard.
        for identity, row in by_event.items():
            if row.get("forecast_family") == family and identity not in known:
                known[identity] = {"slot_id": identity, "family_id": family, "event_date": row["family_event"]["event_date"], "enabled": True}
        called = {identity for identity in known if identity in by_event}
        skipped = {row["slot_id"] for row in skips if row.get("family_id") == family and row["slot_id"] in known and row["slot_id"] not in called}
        disabled = {identity for identity, slot in known.items() if not slot["enabled"] and identity not in called}
        missed = {identity for identity, slot in known.items() if slot["event_date"] <= as_of and identity not in called | skipped | disabled}
        family_summaries.append({"family_id": family, "events": len(known), "calls": len(called), "skips": len(skipped | disabled), "disabled_events": len(disabled), "missed_events": len(missed), "missed_slot_ids": sorted(missed), "upcoming_slot_ids": sorted(identity for identity, slot in known.items() if slot["event_date"] > as_of), "scores": _summarize([by_event[identity] for identity in called], as_of)})
    internal_group = _summarize(internal, as_of)
    internal_group["discretionary"] = _summarize([row for row in internal if not row.get("forecast_family")], as_of)
    internal_group["family"] = _summarize([row for row in internal if row.get("forecast_family")], as_of)
    planned_dates = [run.name for run in run_dirs if (run / "family_forecast_slots.json").exists()]
    artifact = {"schema_version": "scoreboard.v1", "as_of": as_of, "public": _summarize(public, as_of), "internal": internal_group, "families": family_summaries, "coverage": {"scope": "observed_calendar_slots", "planned_run_dates": planned_dates, "complete_calendar_coverage": False, "notice": "Missed-event counts cover captured family calendar slots only. Zero detected misses does not establish complete calendar coverage; unavailable calendars remain unknown."}, "caveat": "Descriptive scores only; these samples do not establish calibration. Public and internal forecasts remain separate. A 50/50 call has no directional success or failure."}
    require_valid(artifact)
    return artifact


def render_scoreboard(scoreboard: dict[str, Any]) -> str:
    require_valid(scoreboard, "scoreboard.v1")
    lines = [f"# Forecast scoreboard — {scoreboard['as_of']}", ""]
    for name in ("public", "internal"):
        group = scoreboard[name]
        rate = "unavailable" if group["right_side_rate"] is None else f"{group['right_side_rate']:.0%}"
        lines.extend([f"## {'Public' if name == 'public' else 'Internal'} forecasts", "", f"- Open: {len(group['open'])}; resolved: {len(group['resolved'])}; void and unscored: {len(group['void'])}.", f"- Right side: {group['right_side_n']}/{group['directional_n']} directional calls ({rate}); {group['no_direction_n']} calls at 50/50.", f"- Mean Brier score: {group['mean_brier']} (n={group['n_scored']}).", f"- Paired model / baseline Brier: {group['model_brier_paired']} / {group['baseline_brier']} (n={group['baseline_n']}); baseline minus model: {group['improvement']}."])
        waiting = group.get("waiting_for_records", [])
        if waiting:
            lines.append(f"- Waiting for official records: {len(waiting)}.")
            for row in waiting:
                lines.append(f"  - {row['question']} ({row['forecast_id']})")
        if group["needs_review"]:
            lines.append(f"- Needs review after the record-check window plus 21 days: {len(group['needs_review'])}.")
        lines.append("")
    lines.extend(["## Scheduled families", ""])
    for row in scoreboard["families"]:
        lines.append(f"- {row['family_id']}: {row['events']} events, {row['calls']} calls, {row['skips']} skips ({row['disabled_events']} disabled), {row['missed_events']} missed.")
    lines.extend(["", scoreboard["caveat"], ""])
    lines.extend([scoreboard["coverage"]["notice"], ""])
    return "\n".join(lines)


def write_scoreboard(repo_root: Path, *, as_of: str) -> dict[str, Any]:
    artifact = build_scoreboard(repo_root, as_of=as_of)
    _write(repo_root / "forecasts" / "scoreboard.json", artifact)
    (repo_root / "forecasts" / "scoreboard.md").write_text(render_scoreboard(artifact), encoding="utf-8")
    # A daily frozen copy is what future historical weekly briefs should read.
    run_dir = repo_root / "runs" / as_of
    if run_dir.exists():
        _write(run_dir / "scoreboard.json", artifact)
    return artifact
