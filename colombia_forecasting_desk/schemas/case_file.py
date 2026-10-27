from __future__ import annotations
from typing import Any
from ._common import base, reader_fields, text, iso_date, strings, objects, https
from ..plain_language import validate_plain_text


def validate(payload: Any) -> list[str]:
    issues = base(payload, "case_file", ("case_id", "plain_title", "plain_summary", "why_it_matters", "who_is_affected", "identifiers", "current_view", "next_trigger", "forecast_ids", "status", "park_after_days", "updates"), dated=False)
    if not isinstance(payload, dict):
        return issues
    if not text(payload.get("case_id")):
        issues.append("case_id is required")
    reader_fields(payload, ("plain_title", "plain_summary", "why_it_matters", "who_is_affected"), issues)
    if text(payload.get("plain_summary")):
        issues.extend(validate_plain_text(payload["plain_summary"], max_words=60))
    identifiers = payload.get("identifiers")
    if not isinstance(identifiers, dict) or any(not strings(identifiers.get(k)) for k in ("bills", "decrees", "indicator_ids", "nits", "keywords")):
        issues.append("identifiers needs bills, decrees, indicator_ids, nits and keywords lists")
    view = payload.get("current_view")
    if not isinstance(view, dict) or not iso_date(view.get("date")) or view.get("confidence") not in ("confirmed", "likely", "uncertain"):
        issues.append("current_view needs date and confidence")
    else:
        reader_fields(view, ("text",), issues)
        issues.extend(validate_plain_text(view.get("text", ""), max_words=80))
    trigger = payload.get("next_trigger")
    if not isinstance(trigger, dict) or not text(trigger.get("event")) or (trigger.get("date") is not None and not iso_date(trigger["date"])):
        issues.append("next_trigger needs event and a date or null")
    if payload.get("status") not in ("active", "parked", "closed"):
        issues.append("status must be active, parked or closed")
    days = payload.get("park_after_days")
    if not isinstance(days, int) or isinstance(days, bool) or days < 1:
        issues.append("park_after_days must be a positive integer")
    if not strings(payload.get("forecast_ids")):
        issues.append("forecast_ids must be a text list")
    for update in objects(payload, "updates", issues):
        if not iso_date(update.get("date")) or not https(update.get("source_url")):
            issues.append("updates needs a date and https source_url")
        reader_fields(update, ("change",), issues)
    return issues
