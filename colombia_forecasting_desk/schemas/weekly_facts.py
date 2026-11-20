from __future__ import annotations
import re
from typing import Any
from ._common import base, iso_date, strings
from ..brief_charts import _stamp, validate_frozen_chart_data


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
    if "chart_data" in payload:
        if not isinstance(payload["chart_data"], list):
            issues.append("chart_data must be a list of frozen datasets")
        elif payload["chart_data"]:
            try:
                cutoff = _stamp(payload.get("cutoff_at"))
                for data in payload["chart_data"]:
                    issues.extend(validate_frozen_chart_data(data, cutoff=cutoff))
                ids = [data.get("data_id") for data in payload["chart_data"] if isinstance(data, dict)]
                if len(ids) != len(set(ids)):
                    issues.append("frozen chart ids must be unique")
            except (ValueError, TypeError, KeyError):
                issues.append("frozen charts require a timezone-aware cutoff_at")
    return issues
