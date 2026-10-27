from __future__ import annotations
from typing import Any
from ._common import base, objects, text, probability, strings, references


def validate(payload: Any) -> list[str]:
    issues = base(payload, "family_forecasts", ("entries",))
    if not isinstance(payload, dict):
        return issues
    seen = set()
    for entry in objects(payload, "entries", issues):
        identity = entry.get("slot_id")
        if not text(identity):
            issues.append("entries.slot_id is required")
        elif identity in seen:
            issues.append("duplicate entry slot_id")
        else:
            seen.add(identity)
        if entry.get("decision") == "forecast":
            if not probability(entry.get("probability")):
                issues.append("forecast probability must be between zero and one")
            for key in ("rationale_for", "rationale_against"):
                if not strings(entry.get(key)) or not entry[key]:
                    issues.append(f"forecast {key} must contain evidence")
            if not text(entry.get("falsifier")) or not references(entry.get("evidence_refs")):
                issues.append("forecast needs a falsifier and artifact evidence_refs")
            if entry.get("skip_reason") is not None:
                issues.append("a forecast cannot also have a skip_reason")
        elif entry.get("decision") == "skip":
            if not text(entry.get("skip_reason")) or entry.get("probability") is not None:
                issues.append("skip needs a written reason and no probability")
        else:
            issues.append("entries.decision must be forecast or skip")
    return issues
