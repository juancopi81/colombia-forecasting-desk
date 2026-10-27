from __future__ import annotations
from typing import Any
from ._common import base, objects, text, iso_date, probability, https


def validate(payload: Any) -> list[str]:
    issues = base(payload, "family_forecast_slots", ("slots",))
    if not isinstance(payload, dict):
        return issues
    seen = set()
    for slot in objects(payload, "slots", issues):
        for field in ("slot_id", "family_id", "question", "resolution_criteria"):
            if not text(slot.get(field)):
                issues.append(f"slots.{field} must be nonempty text")
        identity = slot.get("slot_id")
        if text(identity):
            if identity in seen:
                issues.append("duplicate slot_id")
            seen.add(identity)
        for field in ("event_date", "resolution_deadline", "resolution_check_window_end"):
            if not iso_date(slot.get(field)):
                issues.append(f"slots.{field} must be an ISO date")
        if not https(slot.get("resolution_source")):
            issues.append("slots.resolution_source must use https")
        if not isinstance(slot.get("enabled"), bool):
            issues.append("slots.enabled must be boolean")
        if not isinstance(slot.get("days_until_event"), int) or isinstance(slot.get("days_until_event"), bool):
            issues.append("slots.days_until_event must be an integer")
        baseline = slot.get("baseline")
        if not isinstance(baseline, dict) or not text(baseline.get("label")) or not probability(baseline.get("probability")) or not text(baseline.get("rationale")):
            issues.append("slots.baseline needs label, probability and rationale")
    return issues
