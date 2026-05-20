from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import yaml

SCHEMA_VERSION = "m3_case_file.v1"
ALLOWED_GATES = {"ready_for_m3", "research_more", "reject"}
ALLOWED_DUPLICATE_STATUSES = {
    "no_active_duplicate",
    "possible_duplicate",
    "duplicate",
    "not_checked",
}
REQUIRED_FIELDS = (
    "question",
    "resolution_source",
    "resolution_criteria",
    "deadline_or_window",
    "source_excerpts",
    "missing_evidence",
    "duplicate_check",
    "m3_gate",
)

_M3_HEADING_RE = re.compile(r"(?m)^##\s+M3 Case File\s*$")
_H2_RE = re.compile(r"(?m)^##\s+(.+?)\s*$")
_FENCED_YAML_RE = re.compile(r"```(?:yaml|yml)\s*\n(.*?)\n```", re.DOTALL)


@dataclass(frozen=True, slots=True)
class M3CaseIssue:
    code: str
    message: str
    severity: str = "error"


def extract_m3_case_file(markdown_text: str) -> dict[str, Any] | None:
    """Extract the YAML case-file block from an evidence-pack Markdown file."""
    heading = _M3_HEADING_RE.search(markdown_text)
    if heading is None:
        return None
    match = _FENCED_YAML_RE.search(markdown_text, heading.end())
    if match is None:
        return None
    parsed = yaml.safe_load(match.group(1))
    return parsed if isinstance(parsed, dict) else None


def validate_evidence_pack_markdown(markdown_text: str) -> list[M3CaseIssue]:
    """Validate that an evidence pack starts with a usable M3 case file."""
    issues: list[M3CaseIssue] = []
    first_h2 = _H2_RE.search(markdown_text)
    if first_h2 is None:
        return [
            M3CaseIssue(
                "missing_m3_case_file",
                "Evidence pack has no level-2 M3 Case File section.",
            )
        ]
    if first_h2.group(1).strip() != "M3 Case File":
        issues.append(
            M3CaseIssue(
                "m3_case_file_not_first",
                "The first level-2 section must be `M3 Case File`.",
            )
        )

    case_file = extract_m3_case_file(markdown_text)
    if case_file is None:
        issues.append(
            M3CaseIssue(
                "missing_m3_case_file",
                "Evidence pack must include a fenced yaml M3 Case File block.",
            )
        )
        return issues
    return issues + validate_m3_case_file(case_file)


def validate_m3_case_file(case_file: dict[str, Any]) -> list[M3CaseIssue]:
    """Validate the M3 readiness contract before probability or draft work."""
    issues: list[M3CaseIssue] = []
    schema_version = str(case_file.get("schema_version") or "")
    if schema_version != SCHEMA_VERSION:
        issues.append(
            M3CaseIssue(
                "invalid_schema_version",
                f"`schema_version` must be `{SCHEMA_VERSION}`.",
            )
        )

    for field in REQUIRED_FIELDS:
        if field not in case_file:
            issues.append(
                M3CaseIssue("missing_field", f"`{field}` is required.")
            )

    gate = str(case_file.get("m3_gate") or "")
    if gate not in ALLOWED_GATES:
        issues.append(
            M3CaseIssue(
                "invalid_m3_gate",
                "`m3_gate` must be ready_for_m3, research_more, or reject.",
            )
        )
        return issues

    duplicate_check = case_file.get("duplicate_check")
    duplicate_status = _duplicate_status(duplicate_check)
    if duplicate_status and duplicate_status not in ALLOWED_DUPLICATE_STATUSES:
        issues.append(
            M3CaseIssue(
                "invalid_duplicate_status",
                "`duplicate_check.status` has an unsupported value.",
            )
        )

    missing_evidence = _as_list(case_file.get("missing_evidence"))
    if gate == "ready_for_m3":
        issues.extend(_ready_gate_issues(case_file, duplicate_status))
    elif gate == "research_more" and not missing_evidence:
        issues.append(
            M3CaseIssue(
                "research_more_without_missing_evidence",
                "`research_more` must name the missing evidence.",
            )
        )
    elif gate == "reject" and not _text(case_file.get("gate_reason")):
        issues.append(
            M3CaseIssue(
                "reject_without_gate_reason",
                "`reject` must explain why the candidate is not usable.",
            )
        )

    return issues


def _ready_gate_issues(
    case_file: dict[str, Any],
    duplicate_status: str,
) -> list[M3CaseIssue]:
    issues: list[M3CaseIssue] = []
    readiness_fields = (
        "question",
        "resolution_source",
        "resolution_criteria",
        "deadline_or_window",
        "source_excerpts",
        "duplicate_check",
    )
    for field in readiness_fields:
        if _is_empty(case_file.get(field)):
            issues.append(
                M3CaseIssue(
                    f"ready_missing_{field}",
                    f"`ready_for_m3` requires non-empty `{field}`.",
                )
            )

    if duplicate_status != "no_active_duplicate":
        issues.append(
            M3CaseIssue(
                "ready_duplicate_check_not_clear",
                "`ready_for_m3` requires duplicate_check.status=no_active_duplicate.",
            )
        )

    source_excerpts = case_file.get("source_excerpts")
    if isinstance(source_excerpts, list):
        for index, excerpt in enumerate(source_excerpts, 1):
            issues.extend(_source_excerpt_issues(excerpt, index))

    return issues


def _source_excerpt_issues(value: Any, index: int) -> list[M3CaseIssue]:
    if not isinstance(value, dict):
        return [
            M3CaseIssue(
                "invalid_source_excerpt",
                f"`source_excerpts[{index}]` must be a mapping.",
            )
        ]
    issues: list[M3CaseIssue] = []
    if not (_text(value.get("source_id")) or _text(value.get("source_name"))):
        issues.append(
            M3CaseIssue(
                "source_excerpt_missing_source",
                f"`source_excerpts[{index}]` needs source_id or source_name.",
            )
        )
    for field in ("url", "excerpt"):
        if not _text(value.get(field)):
            issues.append(
                M3CaseIssue(
                    f"source_excerpt_missing_{field}",
                    f"`source_excerpts[{index}].{field}` is required.",
                )
            )
    return issues


def _duplicate_status(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("status") or "")
    return ""


def _is_empty(value: Any) -> bool:
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return value is None


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()
