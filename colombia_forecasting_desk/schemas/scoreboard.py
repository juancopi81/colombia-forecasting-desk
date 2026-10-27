from __future__ import annotations
from typing import Any
from ._common import base, iso_date, objects, probability


def validate(payload: Any) -> list[str]:
    issues = base(payload, "scoreboard", ("as_of", "public", "internal", "families"), dated=False)
    if not isinstance(payload, dict):
        return issues
    if not iso_date(payload.get("as_of")):
        issues.append("as_of must be an ISO date")
    for key in ("public", "internal"):
        group = payload.get(key)
        if not isinstance(group, dict):
            issues.append(f"{key} must be an object")
            continue
        for field in ("open", "resolved", "void", "needs_review"):
            objects(group, field, issues)
        for field in ("n_scored", "right_side_n", "baseline_n"):
            if not isinstance(group.get(field), int) or isinstance(group.get(field), bool) or group[field] < 0:
                issues.append(f"{key}.{field} must be a nonnegative integer")
        for field in ("right_side_rate", "mean_brier", "model_brier_paired", "baseline_brier"):
            if group.get(field) is not None and not probability(group[field]):
                issues.append(f"{key}.{field} must be a score or null")
    for family in objects(payload, "families", issues):
        for field in ("events", "calls", "skips", "missed_events"):
            if not isinstance(family.get(field), int) or isinstance(family.get(field), bool) or family[field] < 0:
                issues.append(f"families.{field} must be a nonnegative integer")
    return issues
