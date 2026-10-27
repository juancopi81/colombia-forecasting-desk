from __future__ import annotations
from typing import Any
from ._common import base, objects, reader_fields, upcoming, https, iso_date, probability, strings, text
from ..plain_language import validate_plain_text


def validate(payload: Any) -> list[str]:
    issues = base(payload, "reader_brief", ("needs_you", "headline", "changes", "coming_up", "open_forecasts", "data_problems"))
    if not isinstance(payload, dict):
        return issues
    needs = payload.get("needs_you")
    if not isinstance(needs, dict) or not isinstance(needs.get("required"), bool):
        issues.append("needs_you requires a boolean required")
    elif needs["required"]:
        reader_fields(needs, ("action",), issues)
        if needs.get("by") is not None and not iso_date(needs["by"]):
            issues.append("needs_you.by must be an ISO date or null")
    elif needs.get("action") is not None or needs.get("by") is not None:
        issues.append("needs_you action and by must be null when no action is required")
    reader_fields(payload, ("headline",), issues)
    for item in objects(payload, "changes", issues, cap=3):
        reader_fields(item, ("title", "what_happened", "why_it_matters", "who_is_affected"), issues)
        if not https(item.get("source_url")):
            issues.append("changes.source_url must use https")
        if item.get("confidence") not in ("confirmed", "likely", "uncertain"):
            issues.append("changes.confidence must be confirmed, likely or uncertain")
    upcoming(payload, "coming_up", issues)
    for item in objects(payload, "open_forecasts", issues):
        reader_fields(item, ("plain_question",), issues)
        if not text(item.get("forecast_id")) or not probability(item.get("probability")) or not iso_date(item.get("resolves_by")):
            issues.append("open_forecasts needs forecast_id, probability and resolves_by")
    problems = payload.get("data_problems")
    if not strings(problems) or len(problems) > 2:
        issues.append("data_problems allows at most two text entries")
    else:
        for problem in problems:
            issues.extend(validate_plain_text(problem))
    return issues
