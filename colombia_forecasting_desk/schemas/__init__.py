"""Versioned shared artifact contracts. Validators perform no I/O."""
from __future__ import annotations

from importlib import import_module
from typing import Any

SCHEMAS = (
    "reader_brief", "family_forecast_slots", "family_forecasts", "scoreboard",
    "case_file", "watchlist_hits", "weekly_facts", "weekly_brief",
)


def validate_artifact(payload: Any, schema_version: str | None = None) -> list[str]:
    version = schema_version or (payload.get("schema_version") if isinstance(payload, dict) else None)
    if not isinstance(version, str) or version not in {f"{name}.v1" for name in SCHEMAS}:
        return [f"unsupported schema_version: {version}"]
    return import_module(f"{__name__}.{version.split('.')[0]}").validate(payload)


def require_valid(payload: Any, schema_version: str | None = None) -> None:
    issues = validate_artifact(payload, schema_version)
    if issues:
        raise ValueError("; ".join(issues))
