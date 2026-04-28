from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from .models import Metasource

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = (
    "id",
    "name",
    "url",
    "type",
    "country_relevance",
    "access_status",
    "fetch_method",
    "priority",
    "update_frequency",
    "trust_role",
    "parsing_difficulty",
    "enabled",
)


class ConfigError(ValueError):
    pass


def _validate(entry: dict[str, Any], index: int) -> None:
    missing = [f for f in REQUIRED_FIELDS if f not in entry]
    if missing:
        raise ConfigError(
            f"metasource at index {index} (id={entry.get('id', '?')}) "
            f"is missing required fields: {missing}"
        )


def load_metasources(path: str | Path) -> list[Metasource]:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "metasources" not in raw:
        raise ConfigError(f"{path} must be a mapping with a top-level 'metasources' key")

    entries = raw["metasources"]
    if not isinstance(entries, list):
        raise ConfigError("'metasources' must be a list")

    sources: list[Metasource] = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ConfigError(f"metasource at index {i} must be a mapping")
        _validate(entry, i)
        if not entry["enabled"]:
            logger.info("Skipping disabled source: %s", entry["id"])
            continue
        sources.append(
            Metasource(
                id=entry["id"],
                name=entry["name"],
                url=entry["url"],
                type=entry["type"],
                country_relevance=entry["country_relevance"],
                access_status=entry["access_status"],
                fetch_method=entry["fetch_method"],
                priority=entry["priority"],
                update_frequency=entry["update_frequency"],
                trust_role=entry["trust_role"],
                parsing_difficulty=entry["parsing_difficulty"],
                enabled=bool(entry["enabled"]),
                notes=entry.get("notes", ""),
            )
        )
    return sources
