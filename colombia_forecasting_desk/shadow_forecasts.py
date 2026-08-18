from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any
import unicodedata
from urllib.parse import urlparse

SCHEMA_VERSION = "shadow_forecast.v1"
ALLOWED_STATUSES = {"open", "resolved"}
RESOLUTION_FIELDS = (
    "outcome",
    "resolved_at",
    "resolution_value",
    "resolution_url",
    "brier_score",
)
REQUIRED_FIELDS = (
    "schema_version",
    "forecast_id",
    "created_at",
    "run_date",
    "question",
    "probability",
    "status",
    "resolution_deadline",
    "resolution_check_window_end",
    "resolution_source",
    "resolution_criteria",
    "baseline",
    "evidence_refs",
    "rationale_for",
    "rationale_against",
    "falsifier",
    "model",
    "reasoning_effort",
    "visibility",
    "source_analysis",
)
@dataclass(frozen=True, slots=True)
class ShadowForecastIssue:
    code: str
    message: str
    row_number: int | None = None


class ShadowForecastError(ValueError):
    """Raised when a ledger operation would violate the shadow contract."""


def validate_shadow_forecast(row: dict[str, Any]) -> list[ShadowForecastIssue]:
    """Validate one internal shadow-forecast row."""
    issues = [
        ShadowForecastIssue("missing_field", f"`{field}` is required.")
        for field in REQUIRED_FIELDS
        if field not in row
    ]
    if row.get("schema_version") != SCHEMA_VERSION:
        issues.append(
            ShadowForecastIssue(
                "invalid_schema_version",
                f"`schema_version` must be `{SCHEMA_VERSION}`.",
            )
        )
    if not _is_probability(row.get("probability")):
        issues.append(
            ShadowForecastIssue(
                "invalid_probability", "`probability` must be between 0 and 1."
            )
        )
    if row.get("status") not in ALLOWED_STATUSES:
        issues.append(
            ShadowForecastIssue(
                "invalid_status",
                f"`status` must be one of: {', '.join(sorted(ALLOWED_STATUSES))}.",
            )
        )
    if row.get("visibility") != "internal":
        issues.append(
            ShadowForecastIssue(
                "invalid_visibility", "`visibility` must be `internal`."
            )
        )
    baseline = row.get("baseline")
    if not (
        isinstance(baseline, dict)
        and isinstance(baseline.get("label"), str)
        and baseline["label"].strip()
        and _is_probability(baseline.get("probability"))
    ):
        issues.append(
            ShadowForecastIssue(
                "invalid_baseline",
                "`baseline` needs a non-empty label and probability between 0 and 1.",
            )
        )
    if not _is_aware_datetime(row.get("created_at")):
        issues.append(
            ShadowForecastIssue(
                "invalid_created_at",
                "`created_at` must be an ISO 8601 datetime with a timezone.",
            )
        )

    parsed_dates: dict[str, date] = {}
    for field in (
        "run_date",
        "resolution_deadline",
        "resolution_check_window_end",
    ):
        parsed = _parse_iso_date(row.get(field))
        if parsed is None:
            issues.append(
                ShadowForecastIssue(
                    "invalid_date", f"`{field}` must be an ISO date (YYYY-MM-DD)."
                )
            )
        else:
            parsed_dates[field] = parsed

    run_date = parsed_dates.get("run_date")
    deadline = parsed_dates.get("resolution_deadline")
    window_end = parsed_dates.get("resolution_check_window_end")
    if run_date and deadline and deadline < run_date:
        issues.append(
            ShadowForecastIssue(
                "deadline_before_run_date",
                "`resolution_deadline` cannot be before `run_date`.",
            )
        )
    if deadline and window_end and window_end < deadline:
        issues.append(
            ShadowForecastIssue(
                "window_before_deadline",
                "`resolution_check_window_end` cannot be before `resolution_deadline`.",
            )
        )
    for field in (
        "forecast_id",
        "question",
        "resolution_source",
        "resolution_criteria",
        "falsifier",
        "model",
        "reasoning_effort",
        "source_analysis",
    ):
        if field in row and not _nonempty_text(row.get(field)):
            issues.append(
                ShadowForecastIssue(
                    "empty_field", f"`{field}` must be a non-empty string."
                )
            )
    if "evidence_refs" in row and not _valid_evidence_refs(row.get("evidence_refs")):
        issues.append(
            ShadowForecastIssue(
                "empty_field",
                "`evidence_refs` must contain artifact and locator references.",
            )
        )
    for field in ("rationale_for", "rationale_against"):
        if field in row and not _nonempty_string_list(row.get(field)):
            issues.append(
                ShadowForecastIssue(
                    "empty_field",
                    f"`{field}` must contain at least one non-empty string.",
                )
            )
    if row.get("status") == "resolved":
        issues.extend(_resolved_row_issues(row))
    return issues


def validate_shadow_forecast_ledger(path: Path) -> list[ShadowForecastIssue]:
    """Validate every JSONL row and enforce unique forecast identifiers."""
    _, issues = _read_ledger(path)
    return issues


def read_shadow_forecast_ledger(
    path: Path,
) -> tuple[list[dict[str, Any]], list[ShadowForecastIssue]]:
    """Read parsed rows together with all ledger issues for reporting."""
    return _read_ledger(path)


def append_shadow_forecast(path: Path, row: dict[str, Any]) -> None:
    """Atomically append one new open forecast without altering prior rows."""
    row_issues = validate_shadow_forecast(row)
    if row_issues:
        raise ShadowForecastError(_issue_summary(row_issues))
    if row["status"] != "open":
        raise ShadowForecastError("New shadow forecasts must have status=open.")

    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f".{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        rows, ledger_issues = _read_ledger(path, missing_is_empty=True)
        if ledger_issues:
            raise ShadowForecastError(
                f"Existing shadow ledger is invalid: {_issue_summary(ledger_issues)}"
            )
        forecast_id = row["forecast_id"]
        if any(existing["forecast_id"] == forecast_id for existing in rows):
            raise ShadowForecastError(f"Duplicate forecast_id: {forecast_id}")
        semantic_key = _semantic_key(row)
        if any(_semantic_key(existing) == semantic_key for existing in rows):
            raise ShadowForecastError(
                "Semantic duplicate: normalized question and resolution_deadline "
                "already exist in the shadow ledger."
            )
        _atomic_append_json_line(path, row)


def shadow_forecast_from_analysis(
    analysis: dict[str, Any],
    *,
    source_analysis: str,
    created_at: str | None = None,
    run_dir: Path | None = None,
) -> dict[str, Any]:
    """Build one immutable-on-append ledger row from an analysis artifact."""
    from colombia_forecasting_desk.agent_analysis import validate_agent_analysis

    analysis_issues = validate_agent_analysis(analysis, run_dir=run_dir)
    if analysis_issues:
        messages = "; ".join(
            f"{issue.code}: {issue.message}" for issue in analysis_issues
        )
        raise ShadowForecastError(f"Agent analysis is invalid: {messages}")
    if analysis.get("overall_disposition") != "shadow_track":
        raise ShadowForecastError(
            "Analysis overall_disposition must be shadow_track before append."
        )
    candidate = analysis.get("shadow_forecast")
    if not isinstance(candidate, dict):
        raise ShadowForecastError("Analysis must contain a shadow_forecast object.")
    if candidate.get("prediction_date") != analysis.get("run_date"):
        raise ShadowForecastError(
            "shadow_forecast.prediction_date must equal analysis run_date."
        )

    resolver = candidate["official_resolver"]
    criteria = candidate["resolution_criteria"]
    baseline = candidate["baseline"]
    resolution_date = candidate["resolution_date"]

    row: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "forecast_id": candidate["id"],
        "created_at": created_at or _utc_now_iso(),
        "run_date": analysis.get("run_date"),
        "question": candidate["question"],
        "probability": candidate["probability"],
        "status": "open",
        "resolution_deadline": resolution_date,
        "resolution_check_window_end": candidate.get(
            "resolution_check_window_end", resolution_date
        ),
        "resolution_source": (
            f"{resolver['source_name']}: {resolver['source_url']}"
        ),
        "resolution_criteria": (
            f"YES: {criteria['yes']} NO: {criteria['no']}"
        ),
        "baseline": {
            "label": baseline["name"],
            "probability": baseline["probability"],
            "rationale": baseline["rationale"],
        },
        "evidence_refs": candidate["evidence"],
        "rationale_for": candidate["rationale_for"],
        "rationale_against": candidate["rationale_against"],
        "falsifier": candidate["falsifier"],
        "model": analysis.get("model"),
        "reasoning_effort": analysis.get("reasoning_effort"),
        "visibility": "internal",
        "source_analysis": source_analysis,
    }
    issues = validate_shadow_forecast(row)
    if issues:
        raise ShadowForecastError(
            f"Analysis shadow_forecast is invalid: {_issue_summary(issues)}"
        )
    return row


def resolve_shadow_forecast(
    path: Path,
    *,
    forecast_id: str,
    outcome: str,
    resolved_at: str,
    resolution_value: str,
    resolution_url: str,
) -> dict[str, Any]:
    """Explicitly resolve one open row using only caller-supplied evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f".{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        rows, ledger_issues = _read_ledger(path)
        if ledger_issues:
            raise ShadowForecastError(
                f"Existing shadow ledger is invalid: {_issue_summary(ledger_issues)}"
            )
        matches = [
            index
            for index, row in enumerate(rows)
            if row["forecast_id"] == forecast_id
        ]
        if not matches:
            raise ShadowForecastError(f"Unknown forecast_id: {forecast_id}")
        index = matches[0]
        current = rows[index]
        if current["status"] != "open":
            raise ShadowForecastError(
                f"Forecast {forecast_id} is already {current['status']}; "
                "resolution is immutable."
            )

        resolved = dict(current)
        model_brier = _brier_score(current["probability"], outcome)
        baseline_brier = _brier_score(
            current["baseline"]["probability"], outcome
        )
        resolved.update(
            {
                "status": "resolved",
                "outcome": outcome,
                "resolved_at": resolved_at,
                "resolution_value": resolution_value,
                "resolution_url": resolution_url,
                "brier_score": model_brier,
                "baseline_brier_score": baseline_brier,
                "brier_improvement_vs_baseline": _brier_improvement(
                    model_brier, baseline_brier
                ),
            }
        )
        issues = validate_shadow_forecast(resolved)
        if issues:
            raise ShadowForecastError(
                f"Resolution is invalid: {_issue_summary(issues)}"
            )
        rows[index] = resolved
        _atomic_write_json_rows(path, rows)
        return resolved


def _read_ledger(
    path: Path, *, missing_is_empty: bool = False
) -> tuple[list[dict[str, Any]], list[ShadowForecastIssue]]:
    if not path.exists():
        if missing_is_empty:
            return [], []
        return [], [
            ShadowForecastIssue("ledger_not_found", f"Ledger not found: {path}")
        ]
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return [], [ShadowForecastIssue("read_error", str(exc))]

    rows: list[dict[str, Any]] = []
    issues: list[ShadowForecastIssue] = []
    seen_ids: set[str] = set()
    for row_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as exc:
            issues.append(
                ShadowForecastIssue("invalid_json", str(exc), row_number=row_number)
            )
            continue
        if not isinstance(parsed, dict):
            issues.append(
                ShadowForecastIssue(
                    "invalid_row",
                    "Each JSONL row must be an object.",
                    row_number=row_number,
                )
            )
            continue
        rows.append(parsed)
        for issue in validate_shadow_forecast(parsed):
            issues.append(
                ShadowForecastIssue(issue.code, issue.message, row_number=row_number)
            )
        forecast_id = parsed.get("forecast_id")
        if isinstance(forecast_id, str) and forecast_id:
            if forecast_id in seen_ids:
                issues.append(
                    ShadowForecastIssue(
                        "duplicate_forecast_id",
                        f"Duplicate forecast_id: {forecast_id}",
                        row_number=row_number,
                    )
                )
            seen_ids.add(forecast_id)
    return rows, issues


def _atomic_append_json_line(path: Path, row: dict[str, Any]) -> None:
    existing = path.read_bytes() if path.exists() else b""
    if existing and not existing.endswith(b"\n"):
        existing += b"\n"
    encoded_row = (
        json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")

    _atomic_replace(path, existing + encoded_row)


def _atomic_write_json_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = "".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        for row in rows
    ).encode("utf-8")
    _atomic_replace(path, payload)


def _atomic_replace(path: Path, payload: bytes) -> None:
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(file_descriptor, "wb") as temporary:
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _issue_summary(issues: list[ShadowForecastIssue]) -> str:
    return "; ".join(f"{issue.code}: {issue.message}" for issue in issues)


def _semantic_key(row: dict[str, Any]) -> tuple[str, str]:
    question = unicodedata.normalize("NFKC", str(row.get("question") or ""))
    normalized_question = " ".join(question.casefold().split())
    return normalized_question, str(row.get("resolution_deadline") or "")


def _resolved_row_issues(row: dict[str, Any]) -> list[ShadowForecastIssue]:
    issues: list[ShadowForecastIssue] = []
    for field in RESOLUTION_FIELDS:
        if field not in row:
            issues.append(
                ShadowForecastIssue(
                    "missing_resolution_field", f"`{field}` is required."
                )
            )
    if row.get("outcome") not in {"YES", "NO"}:
        issues.append(
            ShadowForecastIssue("invalid_outcome", "`outcome` must be YES or NO.")
        )
    if not _is_aware_datetime(row.get("resolved_at")):
        issues.append(
            ShadowForecastIssue(
                "invalid_resolved_at",
                "`resolved_at` must be an ISO 8601 datetime with a timezone.",
            )
        )
    if not _nonempty_text(row.get("resolution_value")):
        issues.append(
            ShadowForecastIssue(
                "invalid_resolution_value", "`resolution_value` must be non-empty."
            )
        )
    if not _is_https_url(row.get("resolution_url")):
        issues.append(
            ShadowForecastIssue(
                "invalid_resolution_url",
                "`resolution_url` must be an explicit HTTPS official-source URL.",
            )
        )
    expected = _brier_score(row.get("probability"), row.get("outcome"))
    if (
        not _is_probability(row.get("brier_score"))
        or row.get("brier_score") != expected
    ):
        issues.append(
            ShadowForecastIssue(
                "invalid_brier_score",
                "`brier_score` must match probability and outcome.",
            )
        )
    has_baseline_brier = "baseline_brier_score" in row
    has_improvement = "brier_improvement_vs_baseline" in row
    if has_baseline_brier != has_improvement:
        issues.append(
            ShadowForecastIssue(
                "incomplete_brier_comparison",
                "Resolved rows must record both baseline Brier score and "
                "improvement when either comparison field is present.",
            )
        )
    elif has_baseline_brier:
        baseline = row.get("baseline")
        baseline_probability = (
            baseline.get("probability") if isinstance(baseline, dict) else None
        )
        expected_baseline = _brier_score(baseline_probability, row.get("outcome"))
        if (
            not _is_probability(row.get("baseline_brier_score"))
            or row.get("baseline_brier_score") != expected_baseline
        ):
            issues.append(
                ShadowForecastIssue(
                    "invalid_baseline_brier_score",
                    "`baseline_brier_score` must match the named baseline "
                    "probability and outcome.",
                )
            )
        expected_improvement = _brier_improvement(expected, expected_baseline)
        if (
            not _is_score_difference(row.get("brier_improvement_vs_baseline"))
            or row.get("brier_improvement_vs_baseline") != expected_improvement
        ):
            issues.append(
                ShadowForecastIssue(
                    "invalid_brier_improvement",
                    "`brier_improvement_vs_baseline` must equal baseline Brier "
                    "minus model Brier; positive values mean the model scored better.",
                )
            )
    return issues


def _brier_score(probability: Any, outcome: Any) -> float | None:
    if not _is_probability(probability) or outcome not in {"YES", "NO"}:
        return None
    observed = 1.0 if outcome == "YES" else 0.0
    return round((float(probability) - observed) ** 2, 4)


def _brier_improvement(
    model_brier: Any, baseline_brier: Any
) -> float | None:
    if not _is_probability(model_brier) or not _is_probability(baseline_brier):
        return None
    return round(float(baseline_brier) - float(model_brier), 4)


def _is_score_difference(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and -1 <= value <= 1
    )


def _is_https_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc)


def _is_probability(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and 0 <= value <= 1
    )


def _parse_iso_date(value: Any) -> date | None:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _is_aware_datetime(value: Any) -> bool:
    if not isinstance(value, str) or "T" not in value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _nonempty_string_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(_nonempty_text(item) for item in value)
    )


def _valid_evidence_refs(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(
            isinstance(item, dict)
            and _nonempty_text(item.get("artifact"))
            and _nonempty_text(item.get("locator"))
            for item in value
        )
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
