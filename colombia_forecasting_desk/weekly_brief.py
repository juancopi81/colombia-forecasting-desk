"""Dated weekly evidence, validated briefs and explicit usefulness feedback."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta
import hashlib
import html
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import yaml

from .plain_language import validate_plain_text, word_count
from .schemas.case_file import validate as validate_case
from .schemas._common import iso_date, probability, text as nonempty_text
from .schemas.scoreboard import validate as validate_scoreboard
from .schemas.watchlist_hits import validate as validate_watchlist
from .schemas.weekly_brief import validate as validate_brief_schema
from .schemas.weekly_facts import validate as validate_facts_schema

BOGOTA = ZoneInfo("America/Bogota")
RESOLUTION_FIELDS = {
    "outcome", "resolved_at", "resolution_value", "resolution_url", "resolution_artifact",
    "brier_score", "baseline_brier_score", "brier_improvement_vs_baseline", "void_reason",
    "voided_at", "needs_review", "needs_review_at", "needs_review_since",
}


def week_bounds(week: str) -> tuple[date, date]:
    match = re.fullmatch(r"(\d{4})-W(\d{2})", week)
    if not match:
        raise ValueError("week must use YYYY-Www")
    start = date.fromisocalendar(int(match[1]), int(match[2]), 1)
    return start, start + timedelta(days=4)


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        if len(value) == 10:
            return datetime.combine(date.fromisoformat(value), time.max, BOGOTA)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except ValueError:
        return None


def _json(path: Path, gaps: list[str], *, missing: bool = False) -> Any:
    if not path.exists():
        if missing:
            gaps.append(f"Unavailable: {path}")
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        gaps.append(f"Unreadable {path}: {exc}")
        return None


def _known(payload: Any, cutoff: datetime) -> bool:
    if not isinstance(payload, dict):
        return True
    for field in ("generated_at", "fetched_at", "recorded_at", "created_at", "review_completed_at",
                  "review_completed_at_utc", "retrieved_at", "retrieved_at_utc", "finished_at", "ended_at"):
        stamp = _timestamp(payload.get(field))
        if stamp and stamp > cutoff:
            return False
    return True


def _ref(path: Path, locator: str = "$") -> dict[str, str]:
    return {"artifact": str(path.resolve()), "locator": locator}


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_receipts(run: Path, cutoff: datetime, gaps: list[str]) -> list[dict[str, Any]]:
    receipts = []
    paths = sorted(set(run.glob("sources/**/metadata.json")) | set(run.glob("sources/**/receipts.json")) | set(run.glob("sources/**/*_receipt.json")))
    for path in paths:
        metadata = _json(path, gaps)
        if not isinstance(metadata, (dict, list)) or not _known(metadata, cutoff):
            continue
        context = metadata if isinstance(metadata, dict) else (_json(path.parent / "scope.json", gaps) or {})
        if not _known(context, cutoff):
            continue
        sources = metadata.get("sources", [metadata]) if isinstance(metadata, dict) else metadata
        if not isinstance(sources, list):
            continue
        for source in sources:
            if not isinstance(source, dict) or not _known(source, cutoff):
                continue
            url = source.get("canonical_url") or source.get("url") or source.get("source_url")
            if not isinstance(url, str) or not url.startswith("https://"):
                continue
            filename = source.get("file") or source.get("path") or source.get("artifact") or source.get("saved_path")
            original = None
            if filename:
                relative = Path(str(filename))
                original = (run.parent.parent / relative) if relative.parts[:2] == ("runs", run.name) else (run / relative)
                if not original.resolve().is_relative_to(run.resolve()):
                    original = None
            verified_hash = False
            if original is not None and original.is_file() and original.resolve().is_relative_to(run.resolve()) and source.get("sha256"):
                verified_hash = _digest(original) == source["sha256"]
                if not verified_hash:
                    gaps.append(f"Source capture hash mismatch: {original}")
            receipts.append({
                "source_url": url, "metadata_ref": _ref(path),
                "source_ref": _ref(original) if original is not None else None,
                "status_code": source.get("status_code", source.get("http_status", source.get("status"))),
                "tls_verification": source.get("tls_verification", source.get("verify_ssl", source.get("tls_verified"))),
                "sha256": source.get("sha256"), "hash_verified": verified_hash,
                "reviewed_pages": context.get("fully_reviewed_pages_text_and_visual") or context.get("pdf_pages_text_and_visually_checked") or context.get("inspected_pdf_pages") or context.get("complete_relevant_pages_text_and_visual", []),
            })
    return receipts


def _asof_forecast(row: dict[str, Any], cutoff: datetime, gaps: list[str]) -> dict[str, Any] | None:
    created = _timestamp(row.get("created_at"))
    if not created:
        gaps.append(f"Forecast lacks a usable creation timestamp: {row.get('forecast_id', 'unknown')}")
        return None
    if created > cutoff:
        return None
    projected = deepcopy(row)
    if row.get("status") in {"resolved", "void"}:
        resolved = _timestamp(row.get("resolved_at") or row.get("voided_at"))
        if not resolved or resolved > cutoff:
            projected = {key: value for key, value in projected.items()
                         if key not in RESOLUTION_FIELDS - {"needs_review", "needs_review_at", "needs_review_since"}}
            projected["status"] = "open"
            if not resolved:
                gaps.append(f"Resolution timestamp unavailable: {row.get('forecast_id', 'unknown')}")
    flagged = _timestamp(row.get("needs_review_at")) or _timestamp(row.get("needs_review_since"))
    if projected.get("needs_review") and (not flagged or flagged > cutoff):
        projected.pop("needs_review", None)
        projected.pop("needs_review_at", None)
        projected.pop("needs_review_since", None)
    return projected


def _ledger(path: Path, cutoff: datetime, gaps: list[str]) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    seen = set()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            gaps.append(f"Malformed ledger row: {path}:{number}")
            continue
        if not isinstance(row, dict) or not isinstance(row.get("forecast_id"), str) or row["forecast_id"] in seen:
            gaps.append(f"Invalid or duplicate ledger identity: {path}:{number}")
            continue
        seen.add(row["forecast_id"])
        value = _asof_forecast(row, cutoff, gaps)
        if value is not None:
            value["ledger_ref"] = _ref(path, f"line={number}")
            rows.append(value)
    return rows


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("status") == "resolved" and row.get("outcome") in ("YES", "NO")]
    scored = [row for row in resolved if type(row.get("probability")) in (int, float) and 0 <= row["probability"] <= 1]
    directional = [row for row in scored if row["probability"] != 0.5]
    right = sum((row["probability"] > 0.5) == (row["outcome"] == "YES") for row in directional)
    briers = [(row["probability"] - int(row["outcome"] == "YES")) ** 2 for row in scored]
    paired = [row for row in scored if isinstance(row.get("baseline"), dict) and type(row["baseline"].get("probability")) in (int, float) and 0 <= row["baseline"]["probability"] <= 1]
    paired_model = [(row["probability"] - int(row["outcome"] == "YES")) ** 2 for row in paired]
    paired_baseline = [(row["baseline"]["probability"] - int(row["outcome"] == "YES")) ** 2 for row in paired]
    mean = lambda values: sum(values) / len(values) if values else None
    return {
        "open": [row for row in rows if row.get("status") == "open"], "resolved": resolved,
        "void": [row for row in rows if row.get("status") == "void"],
        "needs_review": [row for row in rows if row.get("status") == "open" and row.get("needs_review")],
        "n_scored": len(scored), "n_directional": len(directional), "neutral_n": len(scored) - len(directional),
        "right_side_n": right, "right_side_rate": right / len(directional) if directional else None,
        "mean_brier": mean(briers), "baseline_n": len(paired), "model_brier_paired": mean(paired_model),
        "baseline_brier": mean(paired_baseline),
        "improvement": mean([baseline - model for model, baseline in zip(paired_model, paired_baseline)]),
    }


def _forecasts(repo: Path, last_run: Path, cutoff: datetime, start: date, gaps: list[str]) -> dict[str, Any]:
    frozen = _json(last_run / "scoreboard.json", gaps)
    groups = {}
    snapshot = isinstance(frozen, dict) and not validate_scoreboard(frozen) and frozen.get("as_of") == last_run.name and _known(frozen, cutoff)
    for name, filename in (("public", "forecast_log.jsonl"), ("internal", "shadow_forecast_log.jsonl")):
        if snapshot and isinstance(frozen.get(name), dict):
            raw = [row for key in ("open", "resolved", "void") for row in frozen[name].get(key, [])]
            rows = []
            for row in raw:
                if isinstance(row, dict):
                    projected = _asof_forecast(row, cutoff, gaps)
                    if projected is not None:
                        rows.append(projected)
        else:
            rows = _ledger(repo / "forecasts" / filename, cutoff, gaps)
        groups[name] = _summary(rows)
    rows = [row for group in groups.values() for key in ("open", "resolved", "void") for row in group[key]]
    skips = []
    path = repo / "forecasts" / "family_skips.jsonl"
    if path.exists():
        seen = set()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            try:
                row = json.loads(line)
            except ValueError:
                gaps.append(f"Malformed skip row: {path}:{number}")
                continue
            stamp = _timestamp(row.get("created_at") or row.get("recorded_at") or row.get("run_date")) if isinstance(row, dict) else None
            if not stamp:
                gaps.append(f"Skip lacks a usable timestamp: {path}:{number}")
            elif start <= stamp.astimezone(BOGOTA).date() and stamp <= cutoff:
                key = (row.get("slot_id"), row.get("run_date"), row.get("skip_reason"))
                if key not in seen:
                    skips.append({**row, "artifact_ref": _ref(path, f"line={number}")})
                    seen.add(key)
    return {**groups,
            "made_this_week": [row for row in rows if start <= _timestamp(row["created_at"]).astimezone(BOGOTA).date()],
            "resolved_this_week": [row for row in rows if row.get("status") == "resolved" and start <= _timestamp(row["resolved_at"]).astimezone(BOGOTA).date()],
            "skips": skips, "scoreboard_source": str(last_run / "scoreboard.json") if snapshot else "dated projection of protected ledgers"}


def _normal_dates(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normal_dates(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normal_dates(item) for item in value]
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def _cases(run: Path, as_of: str, gaps: list[str]) -> list[dict[str, Any]]:
    folder = run / "cases_snapshot"
    manifest = _json(folder / "manifest.json", gaps)
    if not isinstance(manifest, dict) or manifest.get("run_date") != run.name or not isinstance(manifest.get("files"), dict):
        gaps.append(f"Case snapshots unavailable for {run.name}; live cases were not used.")
        return []
    cases = []
    for filename, expected in sorted(manifest["files"].items()):
        path = folder / filename
        if not path.resolve().is_relative_to(folder.resolve()) or path.suffix not in (".yaml", ".yml") or not path.is_file() or _digest(path) != expected:
            gaps.append(f"Invalid case snapshot: {path}")
            continue
        try:
            case = _normal_dates(yaml.safe_load(path.read_text(encoding="utf-8")))
        except (OSError, ValueError, yaml.YAMLError):
            gaps.append(f"Unreadable case snapshot: {path}")
            continue
        if validate_case(case) or case["current_view"]["date"] > as_of or any(update["date"] > as_of for update in case["updates"]):
            gaps.append(f"Invalid or future-dated case snapshot: {path}")
            continue
        cases.append({**case, "artifact_ref": _ref(path)})
    return cases


def _family(value: str) -> str:
    text = value.lower()
    if "banrep" in text or "interest-rate" in text:
        return "banrep_rate_decision"
    if "ipc" in text or "inflation" in text:
        return "ipc_annual_up"
    if "ise" in text:
        return "ise_annual_up"
    if any(token in text for token in ("geih", "labor", "unemployment", "empleo")):
        return "unemployment_down_yoy"
    return text


def build_weekly_facts(
    runs_dir: Path, week: str, *, repo_root: Path | None = None,
    as_of: str | None = None, cutoff_at: str | None = None,
) -> dict[str, Any]:
    """Collect a deterministic, hindsight-safe evidence set for an ISO workweek.

    The default cutoff is the end of the last available run's date in Bogotá.
    Supply cutoff_at to reproduce an exact intraday publication boundary.
    """
    start, friday = week_bounds(week)
    limit = date.fromisoformat(as_of) if as_of else friday
    if not start <= limit <= friday:
        raise ValueError("as_of must be a weekday within the requested ISO week")
    dirs = [path for path in sorted(runs_dir.iterdir()) if path.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name) and start.isoformat() <= path.name <= limit.isoformat()]
    if not dirs:
        raise ValueError(f"No runs available for {week}")
    last = dirs[-1]
    cutoff = _timestamp(cutoff_at) if cutoff_at else datetime.combine(date.fromisoformat(last.name), time.max, BOGOTA)
    if (cutoff_at and "T" not in cutoff_at) or cutoff is None or cutoff.astimezone(BOGOTA).date() != date.fromisoformat(last.name):
        raise ValueError("cutoff_at must be timezone-aware and fall on the last run's date")
    gaps: list[str] = []
    facts: dict[str, Any] = {"schema_version": "weekly_facts.v1", "week": week, "as_of": last.name,
                             "cutoff_at": cutoff.isoformat(), "run_dates": [run.name for run in dirs],
                             "coverage_gaps": gaps, "indicator_changes": [], "agent_signals": [],
                             "official_followups": [], "watchlist_hits": [], "patterns": [], "events": [], "data_problems": []}
    indicator_index: dict[str, dict[str, Any]] = {}
    signals_seen, hits_seen, patterns_seen = set(), set(), set()
    problems: dict[tuple[str, str], dict[str, Any]] = {}
    events: dict[tuple[str, str], dict[str, Any]] = {}

    def add_event(event: dict[str, Any], artifact: Path, locator: str, family: str, day: str, title: str, url: str) -> None:
        try:
            event_day = date.fromisoformat(day[:10])
        except (TypeError, ValueError):
            return
        if not date.fromisoformat(last.name) < event_day <= date.fromisoformat(last.name) + timedelta(days=14) or not _known(event, cutoff):
            return
        key = (event_day.isoformat(), _family(family or title))
        events[key] = {"date": event_day.isoformat(), "family_id": key[1], "event": title,
                       "source_url": url, "artifact_ref": _ref(artifact, locator), "details": event}

    for run in dirs:
        metadata_path = run / "sources" / "followup" / "metadata.json"
        receipts_path = run / "sources" / "followup" / "receipts.json"
        metadata = _json(metadata_path if metadata_path.exists() else receipts_path, gaps)
        source_records = metadata if isinstance(metadata, list) else metadata.get("sources", []) if isinstance(metadata, dict) else []
        scope = _json(run / "sources" / "followup" / "scope.json", gaps)
        followup_known = _known(metadata, cutoff) and _known(scope, cutoff) and all(_known(source, cutoff) for source in source_records)
        acceptance = _json(run / "acceptance_report.json", gaps)
        if isinstance(acceptance, dict) and acceptance.get("status") != "pass":
            gaps.append(f"Run {run.name} did not pass strict acceptance; source silence is not negative evidence.")
        health = _json(run / "source_health.json", gaps)
        for source in health if isinstance(health, list) else []:
            if not isinstance(source, dict) or not _known(source, cutoff):
                continue
            if source.get("status") in ("failed", "stale", "unparsed") or (type(source.get("failure_count")) is int and source["failure_count"] > 0):
                key = (str(source.get("source_id", "unknown")), str(source.get("status", "failed")))
                problem = problems.setdefault(key, {"source_id": key[0], "status": source.get("status"),
                                                    "detail": source.get("failure_reason") or source.get("error") or "Official source coverage is incomplete.", "run_dates": []})
                problem["run_dates"].append(run.name)
        indicator_path = run / "indicator_watch.json"
        indicators = _json(indicator_path, gaps, missing=True)
        for number, card in enumerate(indicators if isinstance(indicators, list) else []):
            if not isinstance(card, dict) or not _known(card, cutoff):
                continue
            identity = str(card.get("indicator_id", "unknown"))
            if card.get("status") == "observed":
                key = json.dumps([identity, card.get("period"), card.get("values")], sort_keys=True)
                if key in indicator_index:
                    indicator_index[key]["last_seen"] = run.name
                else:
                    entry = {"indicator_id": identity, "period": card.get("period"), "headline": card.get("headline"),
                             "values": card.get("values"), "source_url": card.get("source_url"), "first_seen": run.name,
                             "last_seen": run.name, "artifact_ref": _ref(indicator_path, f"indicator_id={identity}")}
                    prior = next((item for item in reversed(facts["indicator_changes"]) if item["indicator_id"] == identity), None)
                    if prior and prior["period"] == card.get("period"):
                        entry["change_kind"] = "same_period_correction_or_revision"
                        entry["previous_values"] = prior["values"]
                    facts["indicator_changes"].append(entry)
                    indicator_index[key] = entry
            else:
                key = (identity, str(card.get("headline", "")))
                problem = problems.setdefault(key, {"source_id": identity, "status": card.get("status"), "detail": card.get("headline"), "source_url": card.get("source_url"), "run_dates": []})
                problem["run_dates"].append(run.name)
        analysis_path = run / "agent_analysis.json"
        analysis = _json(analysis_path, gaps, missing=True)
        if isinstance(analysis, dict) and analysis.get("run_date") == run.name and _known(analysis, cutoff) and followup_known:
            signal = analysis.get("strongest_changed_signal")
            if isinstance(signal, dict) and signal.get("signal") not in signals_seen:
                facts["agent_signals"].append({**signal, "run_date": run.name, "artifact_ref": _ref(analysis_path, "strongest_changed_signal"),
                                              "official_source_follow_up": analysis.get("official_source_follow_up")})
                signals_seen.add(signal.get("signal"))
        followup = run / "official_followup.md"
        if followup.is_file() and followup_known:
            receipts = _source_receipts(run, cutoff, gaps)
            facts["official_followups"].append({"run_date": run.name, "artifact_ref": _ref(followup),
                                                "text": followup.read_text(encoding="utf-8"), "source_receipts": receipts})
        elif followup.is_file():
            gaps.append(f"Follow-up postdates the exact cutoff and was excluded: {followup}")
        watch_path = run / "watchlist_hits.json"
        watch = _json(watch_path, gaps)
        if isinstance(watch, dict) and not validate_watchlist(watch) and watch.get("run_date") == run.name and _known(watch, cutoff):
            for number, hit in enumerate(watch.get("hits", [])):
                key = (hit.get("watch_id"), hit.get("source_id"), hit.get("url"))
                if key not in hits_seen and _known(hit, cutoff):
                    facts["watchlist_hits"].append({**hit, "artifact_ref": _ref(watch_path, f"hits[{number}]")})
                    hits_seen.add(key)
            for number, pattern in enumerate(watch.get("patterns", [])):
                key = pattern.get("pattern_id")
                if key not in patterns_seen and _known(pattern, cutoff):
                    facts["patterns"].append({**pattern, "artifact_ref": _ref(watch_path, f"patterns[{number}]")})
                    patterns_seen.add(key)
        elif watch is not None:
            gaps.append(f"Invalid or future watchlist snapshot excluded: {watch_path}")
        preflight_path = run / "m3_preflight_opportunities.json"
        preflight = _json(preflight_path, gaps)
        if isinstance(preflight, dict) and _known(preflight, cutoff):
            for entry in preflight.get("opportunities", []):
                add_event(entry, preflight_path, f"opportunity_id={entry.get('opportunity_id')}", str(entry.get("release_family") or entry.get("opportunity_id", "")),
                          entry.get("event_date", ""), entry.get("title", "Scheduled official release"), entry.get("resolution_source_url") or entry.get("resolver_url") or "")
        raw_path = run / "raw_items.json"
        raw = _json(raw_path, gaps)
        for entry in raw if isinstance(raw, list) else []:
            metadata = entry.get("metadata", {})
            if isinstance(metadata, dict) and metadata.get("scheduled_date") and _known(entry, cutoff):
                add_event(entry, raw_path, f"id={entry.get('id')}", str(metadata.get("release_family") or metadata.get("event_type") or entry.get("source_id", "")),
                          metadata["scheduled_date"], entry.get("title", "Scheduled official release"), entry.get("url", ""))
        slots_path = run / "family_forecast_slots.json"
        slots = _json(slots_path, gaps)
        if isinstance(slots, dict) and slots.get("run_date") == run.name and _known(slots, cutoff):
            for entry in slots.get("slots", []):
                add_event(entry, slots_path, f"slot_id={entry.get('slot_id')}", entry.get("family_id", ""), entry.get("event_date", ""), entry.get("question", "Scheduled official release"), entry.get("resolution_source", ""))
    facts["cases"] = _cases(last, last.name, gaps)
    facts["events"] = [events[key] for key in sorted(events)]
    facts["data_problems"] = list(problems.values())
    if not any((run / "watchlist_hits.json").is_file() for run in dirs):
        gaps.append("Watchlist snapshots unavailable for this week.")
    facts["forecasts"] = _forecasts(repo_root or runs_dir.parent, last, cutoff, start, gaps)
    facts["coverage_gaps"] = list(dict.fromkeys(gaps))
    issues = validate_facts_schema(facts)
    if issues:
        raise ValueError("; ".join(issues))
    return facts


def _md(value: str) -> str:
    return re.sub(r"([\\`*_{}\[\]()#+!<>])", r"\\\1", " ".join(value.split()))


def _markdown(brief: dict[str, Any]) -> str:
    start, friday = week_bounds(brief["week"])
    lines = [f"# The week in Colombia — {start:%b %d}–{friday:%d}, {start.year}", "", f"**Big picture:** {_md(brief['big_picture'])}"]
    for number, story in enumerate(brief["top_stories"], 1):
        url = quote(story["source_url"], safe=":/?#[]@!$&'*+,;=%")
        lines.extend(["", f"**{number}. {_md(story['title'])}.** {_md(story['what_happened'])} "
                      f"{_md(story['why_it_matters'])} Affected: {_md(story['who_is_affected'])} [Source]({url})."])
    lines.extend(["", f"**Our calls:** {_md(brief['our_calls'])}"])
    if brief.get("open_forecasts"):
        lines.extend(["", "**Open forecasts**"])
        lines.extend(f"- {_md(item['plain_question'])} {item['probability'] * 100:g}%; official record due {item['resolves_by']}."
                     for item in brief["open_forecasts"])
    if brief["next_week"]:
        lines.extend(["", "**Next week**"])
        for event in brief["next_week"]:
            call = event.get("our_call")
            view = f"Our call: {call['probability'] * 100:g}% — {_md(call['outcome'])}." if call else _md(event["no_call_reason"])
            lines.append(f"- {event['date']} — {_md(event['event'])}. {view}")
    if brief["patterns"]:
        lines.extend(["", "**Patterns worth a look**"])
        lines.extend(f"- {_md(item['plain_text'])} {_md(item['caveat'])}" for item in brief["patterns"])
    if brief["data_problems"]:
        lines.extend(["", "**Data problems**"])
        lines.extend(f"- {_md(problem)}" for problem in brief["data_problems"])
    lines.extend(["", f"**One question for you:** {_md(brief['question'])}"])
    return "\n".join(lines) + "\n"


def _urls(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set().union(*(_urls(v) for v in value.values())) if value else set()
    if isinstance(value, list):
        return set().union(*(_urls(v) for v in value)) if value else set()
    return {value} if isinstance(value, str) and value.startswith("https://") else set()


def validate_weekly_brief(brief: Any, *, facts: dict[str, Any] | None = None) -> list[str]:
    issues = validate_brief_schema(brief)
    if issues:
        return issues
    try:
        start, friday = week_bounds(brief["week"])
        if not start.isoformat() <= brief["as_of"] <= friday.isoformat():
            issues.append("as_of must fall within the requested ISO workweek")
    except ValueError as exc:
        issues.append(str(exc))
    texts = [brief["big_picture"], brief["our_calls"], brief["question"], *brief["data_problems"]]
    for story in brief["top_stories"]:
        texts.extend(story[key] for key in ("title", "what_happened", "why_it_matters", "who_is_affected"))
        if any(char.isspace() for char in story["source_url"]) or any(char in story["source_url"] for char in '<>"'):
            issues.append("source_url contains unsafe URL characters")
    for event in brief["next_week"]:
        texts.append(event["event"])
        if not brief["as_of"] < event["date"] <= (date.fromisoformat(brief["as_of"]) + timedelta(days=14)).isoformat():
            issues.append("next_week events must be within the next fourteen days")
        if event.get("our_call"):
            texts.append(event["our_call"]["outcome"])
        else:
            texts.append(event["no_call_reason"])
            if len(re.findall(r"[.!?](?:\s|$)", event["no_call_reason"])) > 1:
                issues.append("no_call_reason must be one sentence")
    for pattern in brief["patterns"]:
        texts.extend([pattern["plain_text"], pattern["caveat"]])
    open_forecasts = brief.get("open_forecasts", [])
    if not isinstance(open_forecasts, list) or any(not isinstance(item, dict) for item in open_forecasts):
        issues.append("open_forecasts must be a list of forecast objects")
        open_forecasts = []
    for item in open_forecasts:
        if not nonempty_text(item.get("forecast_id")) or not nonempty_text(item.get("plain_question")) or not probability(item.get("probability")) or not iso_date(item.get("resolves_by")):
            issues.append("open_forecasts needs forecast_id, plain_question, probability and resolves_by")
        elif item["plain_question"]:
            texts.append(item["plain_question"])
    for text in texts:
        issues.extend(validate_plain_text(text))
        if re.search(r"<[^>]+>", text):
            issues.append("reader fields must be plain text without HTML tags")
    if not issues and word_count(_markdown(brief)) > 450:
        issues.append("rendered weekly brief exceeds 450 words")
    if facts is not None:
        if validate_facts_schema(facts):
            issues.append("weekly facts are invalid")
            return issues
        if brief["week"] != facts["week"] or brief["as_of"] != facts["as_of"]:
            issues.append("brief week and as_of must match the frozen facts")
        known_urls = _urls(facts)
        for story in brief["top_stories"]:
            if story["source_url"] not in known_urls:
                issues.append(f"story source is not in the dated facts: {story['source_url']}")
        rows = [row for name in ("public", "internal") for key in ("open", "resolved") for row in facts.get("forecasts", {}).get(name, {}).get(key, [])]
        by_id = {row["forecast_id"]: row for row in rows}
        known_events = {(event["date"], event["family_id"]) for event in facts.get("events", [])}
        for event in brief["next_week"]:
            family = event.get("family_id") or _family(event["event"])
            if (event["date"], family) not in known_events:
                issues.append(f"event is not established by the dated facts: {event['event']}")
            call = event.get("our_call")
            if call and (call["forecast_id"] not in by_id or by_id[call["forecast_id"]].get("probability") != call["probability"]):
                issues.append("weekly call must match a forecast recorded by the cutoff")
            elif call:
                target = by_id[call["forecast_id"]]
                target_day = target.get("event_date") or str(target.get("resolution_deadline", ""))[:10]
                if target_day != event["date"]:
                    issues.append("weekly call must target the exact upcoming event")
        open_by_id = {row["forecast_id"]: row for row in rows if row.get("status") == "open"}
        shown = set()
        for item in open_forecasts:
            row = open_by_id.get(item.get("forecast_id"))
            if row is None or row.get("probability") != item.get("probability") or (row.get("resolution_check_window_end") or str(row.get("resolution_deadline", ""))[:10]) != item.get("resolves_by"):
                issues.append("open forecast must match the dated ledger record")
            shown.add(item.get("forecast_id"))
        if set(open_by_id) - shown:
            issues.append("all open forecasts awaiting official records must be listed")
        if facts.get("coverage_gaps") and not brief["data_problems"]:
            issues.append("weekly coverage gaps must be disclosed")
    return list(dict.fromkeys(issues))


def render_weekly_brief(brief: dict[str, Any]) -> str:
    issues = validate_weekly_brief(brief)
    if issues:
        raise ValueError("; ".join(issues))
    return _markdown(brief)


def render_weekly_html(brief: dict[str, Any]) -> str:
    issues = validate_weekly_brief(brief)
    if issues:
        raise ValueError("; ".join(issues))
    esc = lambda value: html.escape(" ".join(str(value).split()), quote=True)
    start, friday = week_bounds(brief["week"])
    parts = ['<main><p class="kicker">Colombia Forecasting Desk</p>', f'<h1>The week in Colombia</h1><p class="dates">{start:%B %d}–{friday:%d}, {start.year} · As of {esc(brief["as_of"])}</p>',
             f'<p class="big-picture">{esc(brief["big_picture"])}</p>']
    for number, story in enumerate(brief["top_stories"], 1):
        parts.append(f'<section><h2>{number}. {esc(story["title"])}</h2><p>{esc(story["what_happened"])} '
                     f'{esc(story["why_it_matters"])} Affected: {esc(story["who_is_affected"])} '
                     f'<a href="{esc(story["source_url"])}">Source</a>.</p></section>')
    parts.append(f'<section><h2>Our calls</h2><p>{esc(brief["our_calls"])}</p></section>')
    if brief.get("open_forecasts"):
        parts.append("<section><h2>Open forecasts</h2><ul>")
        parts.extend(f'<li>{esc(item["plain_question"])} {item["probability"] * 100:g}%; official record due {esc(item["resolves_by"])}.</li>'
                     for item in brief["open_forecasts"])
        parts.append("</ul></section>")
    if brief["next_week"]:
        parts.append("<section><h2>Next week</h2><ul>")
        for event in brief["next_week"]:
            call = event.get("our_call")
            view = f"Our call: {call['probability'] * 100:g}% — {esc(call['outcome'])}." if call else esc(event["no_call_reason"])
            parts.append(f'<li>{esc(event["date"])} — {esc(event["event"])}. {view}</li>')
        parts.append("</ul></section>")
    if brief["patterns"]:
        parts.append("<section><h2>Patterns worth a look</h2><ul>")
        parts.extend(f'<li>{esc(item["plain_text"])} {esc(item["caveat"])}</li>' for item in brief["patterns"])
        parts.append("</ul></section>")
    if brief["data_problems"]:
        parts.append("<aside><h2>Data problems</h2><ul>")
        parts.extend(f"<li>{esc(problem)}</li>" for problem in brief["data_problems"])
        parts.append("</ul></aside>")
    parts.append(f'<footer><h2>One question for you</h2><p>{esc(brief["question"])}</p></footer></main>')
    return '<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' + f'<title>Colombia — {esc(brief["week"])}</title><style>{WEEKLY_CSS}</style></head><body>' + "".join(parts) + "</body></html>\n"


WEEKLY_CSS = """
:root{color-scheme:light dark;--paper:#f5f1e7;--ink:#24231f;--muted:#686454;--rule:#d8d0bd;--accent:#805b1c;--aside:#ece5d6}
*{box-sizing:border-box;min-width:0}html,body{margin:0;width:100%;overflow-wrap:anywhere}
body{background:var(--paper);color:var(--ink);font:18px/1.6 Georgia,serif}
main{width:100%;max-width:820px;margin:0 auto;padding:52px 30px 64px}
.kicker,.dates,h2{font-family:system-ui,sans-serif}.kicker{color:var(--accent);font-size:12px;text-transform:uppercase;letter-spacing:.14em}
h1{font-size:clamp(30px,6vw,48px);line-height:1.1;margin:18px 0}h2{font-size:19px;line-height:1.35;margin:0 0 10px}
.dates{font-size:13px;color:var(--muted)}.big-picture{font-size:22px;line-height:1.5;border-bottom:2px solid var(--rule);padding-bottom:24px}
section{margin:28px 0}p{margin:10px 0}ul{padding-left:24px}li{margin:8px 0}a{color:var(--accent);text-underline-offset:3px}
aside{padding:18px 20px;background:var(--aside);border-left:3px solid var(--accent);margin:28px 0}footer{margin-top:30px;padding-top:24px;border-top:1px solid var(--rule)}
@media(prefers-color-scheme:dark){:root{--paper:#1e211f;--ink:#e9e5da;--muted:#b6b3a7;--rule:#4a4d43;--accent:#e2ba75;--aside:#2a2f29}}
@media(max-width:480px){body{font-size:17px}main{padding:28px 19px 44px}.big-picture{font-size:20px}aside{padding:14px}}
@media print{main{padding:0;max-width:none}body{font-size:11pt}section,aside,footer{break-inside:avoid}a{color:inherit}}
"""


def append_usefulness_feedback(path: Path, feedback: dict[str, Any]) -> bool:
    """Append only an explicitly supplied user answer; never inspect chat state."""
    required = ("week", "date", "answer", "source", "source_ref")
    if not isinstance(feedback, dict) or any(not isinstance(feedback.get(key), str) or not feedback[key].strip() for key in required):
        raise ValueError("feedback needs week, date, answer, source and source_ref")
    week_bounds(feedback["week"])
    date.fromisoformat(feedback["date"])
    if feedback["source"] not in ("user_message", "user_file"):
        raise ValueError("feedback must identify an explicit user_message or user_file")
    identity = hashlib.sha256(json.dumps(feedback, sort_keys=True).encode()).hexdigest()[:20]
    marker = f"<!-- usefulness:{identity} -->"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as stream:
        import fcntl
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        stream.seek(0)
        if marker in stream.read():
            return False
        answer = "\n".join("> " + line for line in feedback["answer"].splitlines())
        stream.write(f"\n{marker}\n## {feedback['date']} — {feedback['week']}\n\n{answer}\n\nSource: {feedback['source']} ({feedback['source_ref']}).\n")
    return True
