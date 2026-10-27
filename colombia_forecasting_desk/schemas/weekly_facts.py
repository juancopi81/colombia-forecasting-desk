from __future__ import annotations
import re
from typing import Any
from ._common import base, iso_date, strings


def validate(payload: Any) -> list[str]:
    issues = base(payload, "weekly_facts", ("week", "as_of", "run_dates", "coverage_gaps"), dated=False)
    if not isinstance(payload, dict):
        return issues
    if not re.fullmatch(r"\d{4}-W\d{2}", str(payload.get("week", ""))):
        issues.append("week must be an ISO week")
    if not iso_date(payload.get("as_of")):
        issues.append("as_of must be an ISO date")
    days = payload.get("run_dates")
    if not isinstance(days, list) or not all(iso_date(d) for d in days):
        issues.append("run_dates must contain ISO dates")
    elif iso_date(payload.get("as_of")) and any(d > payload["as_of"] for d in days):
        issues.append("run_dates may not contain future knowledge")
    if not strings(payload.get("coverage_gaps")):
        issues.append("coverage_gaps must be a list of text")
    return issues
