from __future__ import annotations
import re
from typing import Any
from ._common import base, reader_fields, objects, upcoming, https, iso_date, strings, text
from ..plain_language import validate_plain_text


def validate(payload: Any) -> list[str]:
    issues = base(payload, "weekly_brief", ("week", "as_of", "big_picture", "top_stories", "our_calls", "next_week", "patterns", "data_problems", "question"), dated=False)
    if not isinstance(payload, dict):
        return issues
    if not re.fullmatch(r"\d{4}-W\d{2}", str(payload.get("week", ""))) or not iso_date(payload.get("as_of")):
        issues.append("week and as_of must identify the ISO week and cutoff date")
    reader_fields(payload, ("big_picture", "our_calls", "question"), issues)
    for story in objects(payload, "top_stories", issues, cap=3):
        reader_fields(story, ("title", "what_happened", "why_it_matters", "who_is_affected"), issues)
        if not https(story.get("source_url")):
            issues.append("top_stories.source_url must use https")
    upcoming(payload, "next_week", issues, cap=10)
    for pattern in objects(payload, "patterns", issues, cap=3):
        reader_fields(pattern, ("plain_text", "caveat"), issues)
    problems = payload.get("data_problems")
    if not strings(problems) or len(problems) > 2:
        issues.append("data_problems allows at most two text entries")
    else:
        for problem in problems:
            issues.extend(validate_plain_text(problem))
    if text(payload.get("big_picture")) and len(re.findall(r"[.!?](?:\s|$)", payload["big_picture"])) > 2:
        issues.append("big_picture allows at most two sentences")
    return issues
