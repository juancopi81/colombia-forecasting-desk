from __future__ import annotations

from datetime import date
import math
from typing import Any
from urllib.parse import urlsplit

from ..plain_language import validate_plain_text


def text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def probability(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and 0 <= value <= 1


def iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def https(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        url = urlsplit(value)
        return url.scheme == "https" and bool(url.hostname) and not url.username and not url.password
    except ValueError:
        return False


def base(payload: Any, name: str, fields: tuple[str, ...], *, dated: bool = True) -> list[str]:
    if not isinstance(payload, dict):
        return ["artifact must be an object"]
    issues = []
    if payload.get("schema_version") != f"{name}.v1":
        issues.append(f"schema_version must be {name}.v1")
    issues.extend(f"{field} is required" for field in fields if field not in payload)
    if dated and not iso_date(payload.get("run_date")):
        issues.append("run_date must be an ISO date")
    return issues


def objects(payload: dict, key: str, issues: list[str], *, cap: int | None = None) -> list[dict]:
    values = payload.get(key)
    if not isinstance(values, list) or any(not isinstance(v, dict) for v in values):
        issues.append(f"{key} must be a list of objects")
        return []
    if cap is not None and len(values) > cap:
        issues.append(f"{key} allows at most {cap} entries")
    return values


def strings(value: Any) -> bool:
    return isinstance(value, list) and all(text(x) for x in value)


def reader_fields(row: dict, names: tuple[str, ...], issues: list[str]) -> None:
    for name in names:
        if not text(row.get(name)):
            issues.append(f"{name} must be nonempty reader text")
        else:
            issues.extend(f"{name}: {issue}" for issue in validate_plain_text(row[name]))


def references(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(
        isinstance(ref, dict) and text(ref.get("artifact")) and text(ref.get("locator")) for ref in value
    )


def upcoming(payload: dict, key: str, issues: list[str], *, cap: int = 5) -> None:
    for item in objects(payload, key, issues, cap=cap):
        reader_fields(item, ("event",), issues)
        if not iso_date(item.get("date")):
            issues.append(f"{key}.date must be an ISO date")
        call, reason = item.get("our_call"), item.get("no_call_reason")
        if (call is not None) == text(reason):
            issues.append(f"{key} needs exactly one of our_call or no_call_reason")
        if call is not None and (not isinstance(call, dict) or not probability(call.get("probability")) or not text(call.get("forecast_id")) or not text(call.get("outcome"))):
            issues.append(f"{key}.our_call needs forecast_id, outcome and probability")
        if text(reason):
            issues.extend(validate_plain_text(reason))
