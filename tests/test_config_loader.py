from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from colombia_forecasting_desk.config_loader import ConfigError, load_metasources


def _write_yaml(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "metasources.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


def _entry(**overrides):
    base = {
        "id": "s1",
        "name": "Source One",
        "url": "https://example.com/feed",
        "type": "news",
        "country_relevance": "high",
        "access_status": "rss_public",
        "fetch_method": "rss",
        "priority": "high",
        "update_frequency": "daily",
        "trust_role": "media_signal",
        "parsing_difficulty": "easy",
        "enabled": True,
        "notes": "x",
    }
    base.update(overrides)
    return base


def test_loads_enabled_sources(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        {"metasources": [_entry(max_items=12, verify_ssl=False)]},
    )
    sources = load_metasources(path)
    assert len(sources) == 1
    assert sources[0].id == "s1"
    assert sources[0].url == "https://example.com/feed"
    assert sources[0].max_items == 12
    assert sources[0].verify_ssl is False


def test_drops_disabled_sources(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        {"metasources": [_entry(id="a"), _entry(id="b", enabled=False)]},
    )
    sources = load_metasources(path)
    assert [s.id for s in sources] == ["a"]


def test_missing_required_field_raises(tmp_path: Path) -> None:
    bad = _entry()
    del bad["url"]
    path = _write_yaml(tmp_path, {"metasources": [bad]})
    with pytest.raises(ConfigError, match="url"):
        load_metasources(path)


def test_real_config_loads() -> None:
    repo_config = (
        Path(__file__).resolve().parent.parent / "config" / "metasources.yaml"
    )
    sources = load_metasources(repo_config)
    assert len(sources) >= 5
    assert all(s.enabled for s in sources)
    assert {s.fetch_method for s in sources} <= {"rss", "html"}
