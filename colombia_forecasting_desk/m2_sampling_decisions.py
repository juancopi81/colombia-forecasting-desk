from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .cleaner import normalize_whitespace
from .tagger import fold_accents

SCHEMA_VERSION = "m2_sampling_decisions.v1"
CANDIDATE_QUESTIONS = "candidate_questions.md"
RANKED_QUESTIONS = "m2_ranked_questions.json"
JSON_FILENAME = "m2_sampling_decisions.json"
MARKDOWN_FILENAME = "m2_sampling_decisions.md"

FIELD_ALIASES = {
    "candidate_question": "question_considered",
    "deadline_or_window": "deadline_or_window",
    "deadline_window": "deadline_or_window",
    "decision": "decision",
    "duplicate_check": "duplicate_relationship",
    "duplicate_relationship": "duplicate_relationship",
    "evidence": "evidence_available_now",
    "evidence_available_now": "evidence_available_now",
    "gate_reason": "why_not_m3",
    "items": "evidence_available_now",
    "market_context": "evidence_available_now",
    "missing_evidence": "missing_evidence",
    "missing_m3_fields": "missing_m3_fields",
    "public_interest_hook": "public_interest_hook",
    "question": "question_considered",
    "question_considered": "question_considered",
    "rationale": "why_not_m3",
    "resolution_criteria": "resolution_criteria",
    "resolution_source": "resolution_source",
    "why_not_m3": "why_not_m3",
}

SECTION_DEFAULT_DECISIONS = {
    "monitor_no_new_m3": "monitor_no_new_m3",
    "monitor": "monitor_no_new_m3",
    "monitor_queue": "",
    "monitor_no_new_m3_candidates": "monitor_no_new_m3",
    "monitor_no_new_m3_items": "monitor_no_new_m3",
    "reject": "reject",
    "rejected": "reject",
    "research_more_before_m3": "research_more_before_m3",
    "selected_for_evidence_pack": "select_for_evidence_pack",
}

M3_REQUIRED_FIELDS = (
    "question_considered",
    "resolution_source",
    "resolution_criteria",
    "deadline_or_window",
    "evidence_available_now",
)

BILL_RE = re.compile(
    r"\b(?:Proyecto\s+de\s+Ley|PL)\s+(\d+)\s+"
    r"(?:de\s+)?(\d{4})\s+(C[aá]mara|Camara|Senado)\b",
    flags=re.IGNORECASE,
)


class MissingCandidateQuestionsError(FileNotFoundError):
    """Raised when the post-editorial candidate file has not been written."""


def build_m2_sampling_decisions(run_dir: str | Path) -> dict[str, Any]:
    """Build a durable bridge from sampled M2 leads to M3 gate decisions.

    This is intentionally post-editorial. It reads ``candidate_questions.md``
    after the human/LLM review has recorded which serious candidates were
    sampled. Missing ``candidate_questions.md`` is a hard error so callers do
    not mistake an absent editorial artifact for an empty review.
    """
    run_path = Path(run_dir)
    candidate_path = run_path / CANDIDATE_QUESTIONS
    if not candidate_path.exists():
        raise MissingCandidateQuestionsError(
            f"Missing required {candidate_path}. Run this after "
            "candidate_questions.md has been written."
        )

    candidate_text = candidate_path.read_text(encoding="utf-8")
    ranked_path = run_path / RANKED_QUESTIONS
    ranked = _read_json(ranked_path) if ranked_path.exists() else None
    ranked_records = _ranked_records(ranked or {})

    candidates = _parse_candidate_questions(candidate_text)
    for candidate in candidates:
        refs = _match_ranked_refs(candidate, ranked_records)
        candidate["m2_ranked_refs"] = refs
        candidate["duplicate_relationship"] = _duplicate_relationship(candidate, refs)
        candidate["missing_m3_fields"] = _missing_m3_fields(candidate)

    missing_inputs = []
    if not ranked_path.exists():
        missing_inputs.append(RANKED_QUESTIONS)

    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_path.name,
        "status": "recorded" if candidates else "no_candidates_recorded",
        "inputs": {
            "candidate_questions_artifact": CANDIDATE_QUESTIONS,
            "m2_ranked_questions_artifact": RANKED_QUESTIONS,
            "missing_inputs": missing_inputs,
            "policy": (
                "Post-editorial bridge from sampled M2 candidates to M3 gate "
                "decisions. It records what candidate_questions.md already "
                "reviewed; it does not promote leads or create forecasts."
            ),
            "matching_policy": (
                "M2 links are attached only for exact rank_id matches, exact "
                "question_seed matches, or canonical bill IDs parsed from "
                "explicit Proyecto de Ley/PL number-year-chamber text."
            ),
        },
        "overall_decision": _extract_overall_decision(candidate_text),
        "decision_counts": _decision_counts(candidates),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }


def write_m2_sampling_decisions(run_dir: str | Path) -> tuple[dict[str, Any], Path, Path]:
    """Build and write the JSON and Markdown sampling-decision artifacts."""
    run_path = Path(run_dir)
    artifact = build_m2_sampling_decisions(run_path)
    json_path = run_path / JSON_FILENAME
    markdown_path = run_path / MARKDOWN_FILENAME
    json_path.write_text(
        json.dumps(artifact, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_m2_sampling_decisions(artifact), encoding="utf-8")
    return artifact, json_path, markdown_path


def render_m2_sampling_decisions(artifact: dict[str, Any]) -> str:
    """Render a deterministic Markdown companion for the JSON artifact."""
    lines = [
        f"# M2 Sampling Decisions - {artifact.get('run_date', 'unknown')}",
        "",
        f"Schema: `{artifact.get('schema_version', SCHEMA_VERSION)}`",
        "",
        (
            "This post-editorial artifact records candidates sampled in "
            "`candidate_questions.md` and links them back to deterministic M2 "
            "ranker rows only when the match is exact enough to cite."
        ),
        "",
        f"Overall decision: `{artifact.get('overall_decision', 'not_recorded')}`",
        f"Candidate count: {artifact.get('candidate_count', 0)}",
        "",
    ]

    candidates = [
        candidate
        for candidate in artifact.get("candidates") or []
        if isinstance(candidate, dict)
    ]
    if not candidates:
        lines.extend(
            [
                "## Candidates",
                "",
                "No sampled or reviewed candidates were recorded.",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(["## Candidates", ""])
    for candidate in candidates:
        lines.extend(_render_candidate(candidate))
    return "\n".join(lines)


def _render_candidate(candidate: dict[str, Any]) -> list[str]:
    title = str(candidate.get("title") or "Untitled candidate")
    index = candidate.get("index", "")
    prefix = f"{index}. " if index else ""
    lines = [
        f"### {prefix}{title}",
        "",
        f"- Source section: {candidate.get('source_section', 'not_recorded')}",
        f"- Decision: `{candidate.get('decision', 'not_recorded')}`",
        f"- Question considered: {candidate.get('question_considered', 'not_recorded')}",
        f"- Resolution source: {candidate.get('resolution_source', 'not_recorded')}",
        f"- Resolution criteria: {candidate.get('resolution_criteria', 'not_recorded')}",
        f"- Deadline/window: {candidate.get('deadline_or_window', 'not_recorded')}",
        (
            "- Evidence available now: "
            f"{candidate.get('evidence_available_now', 'not_recorded')}"
        ),
        f"- Missing evidence: {candidate.get('missing_evidence', 'not_recorded')}",
        f"- Missing M3 fields: {_list_text(candidate.get('missing_m3_fields'))}",
    ]
    why = candidate.get("why_not_m3")
    if why and why != "not_recorded":
        lines.append(f"- Why not M3: {why}")

    duplicate = candidate.get("duplicate_relationship") or {}
    lines.append(f"- Duplicate relationship: {duplicate.get('status', 'unknown')}")
    notes = duplicate.get("notes")
    if notes:
        lines.append(f"  - Notes: {notes}")

    refs = [
        ref for ref in candidate.get("m2_ranked_refs") or [] if isinstance(ref, dict)
    ]
    if refs:
        lines.append("- M2 ranked refs:")
        for ref in refs:
            label = ref.get("rank_id") or ref.get("canonical_bill_id") or "unknown"
            lines.append(
                "  - "
                f"`{label}` "
                f"bucket=`{ref.get('bucket', 'unknown')}` "
                f"recommendation=`{ref.get('recommendation', 'unknown')}` "
                f"match=`{ref.get('match_method', 'unknown')}`"
            )
    else:
        lines.append("- M2 ranked refs: none")
    lines.append("")
    return lines


def _parse_candidate_questions(text: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    section = ""
    section_key = ""
    current_heading = ""
    current_lines: list[str] = []
    candidates: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal current_heading, current_lines
        if not current_heading:
            return
        parsed = _candidate_from_block(
            index=len(candidates) + 1,
            heading=current_heading,
            source_section=section,
            section_key=section_key,
            lines=current_lines,
        )
        candidates.append(parsed)
        current_heading = ""
        current_lines = []

    for line in lines:
        heading = re.match(r"^(#{2,6})\s+(.+?)\s*$", line)
        if heading:
            level = len(heading.group(1))
            label = heading.group(2).strip()
            if level == 2:
                flush()
                section = label
                section_key = _key(label)
                continue
            if level == 3:
                flush()
                if _is_candidate_section(section_key):
                    current_heading = label
                    current_lines = [line]
                continue
        if current_heading:
            current_lines.append(line)

    flush()
    return candidates


def _candidate_from_block(
    *,
    index: int,
    heading: str,
    source_section: str,
    section_key: str,
    lines: list[str],
) -> dict[str, Any]:
    title = re.sub(r"^\d+[.)]\s+", "", heading).strip()
    fields = _parse_field_bullets(lines[1:])
    decision = _clean_decision(
        fields.get("decision") or SECTION_DEFAULT_DECISIONS.get(section_key, "")
    )
    consumed = {
        "decision",
        "question_considered",
        "resolution_source",
        "resolution_criteria",
        "deadline_or_window",
        "evidence_available_now",
        "missing_evidence",
        "missing_m3_fields",
        "why_not_m3",
        "duplicate_relationship",
    }
    return {
        "index": index,
        "title": title,
        "sample_status": "reviewed_in_candidate_questions",
        "source_section": source_section or "not_recorded",
        "decision": decision or "not_recorded",
        "question_considered": fields.get("question_considered", "not_recorded"),
        "resolution_source": fields.get("resolution_source", "not_recorded"),
        "resolution_criteria": fields.get("resolution_criteria", "not_recorded"),
        "deadline_or_window": fields.get("deadline_or_window", "not_recorded"),
        "evidence_available_now": fields.get(
            "evidence_available_now", "not_recorded"
        ),
        "missing_evidence": fields.get("missing_evidence", "not_recorded"),
        "why_not_m3": fields.get("why_not_m3", "not_recorded"),
        "public_interest_hook": fields.get("public_interest_hook", "not_recorded"),
        "recorded_duplicate_relationship": fields.get(
            "duplicate_relationship", "not_recorded"
        ),
        "other_recorded_fields": {
            key: value for key, value in fields.items() if key not in consumed
        },
        "canonical_bill_ids_detected": _candidate_bill_ids("\n".join(lines)),
        "raw_markdown": "\n".join(lines).strip(),
    }


def _parse_field_bullets(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    current_key = ""
    current_values: list[str] = []

    def flush() -> None:
        nonlocal current_key, current_values
        if current_key:
            fields[current_key] = normalize_whitespace(" ".join(current_values))
        current_key = ""
        current_values = []

    for line in lines:
        match = re.match(r"^\s*-\s+([^:]{1,90}):\s*(.*)$", line)
        if match:
            flush()
            current_key = FIELD_ALIASES.get(_key(match.group(1)), _key(match.group(1)))
            current_values = [match.group(2).strip()] if match.group(2).strip() else []
            continue
        if current_key and (line.startswith((" ", "\t")) or not line.strip()):
            stripped = line.strip()
            if stripped:
                current_values.append(stripped)
    flush()
    return fields


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _ranked_records(ranked: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_key in ("ranked_questions", "review_queue"):
        for row in ranked.get(source_key) or []:
            if not isinstance(row, dict):
                continue
            identity = str(row.get("rank_id") or row.get("canonical_bill_id") or id(row))
            if identity in seen:
                continue
            seen.add(identity)
            records.append(row)
    return records


def _match_ranked_refs(
    candidate: dict[str, Any],
    ranked_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    raw_text = str(candidate.get("raw_markdown") or "")
    question = str(candidate.get("question_considered") or "")
    normalized_question = normalize_whitespace(question)
    detected_ids = set(candidate.get("canonical_bill_ids_detected") or [])
    refs: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(record: dict[str, Any], method: str) -> None:
        key = str(record.get("rank_id") or record.get("canonical_bill_id") or "")
        if not key or key in seen:
            return
        seen.add(key)
        refs.append(_rank_ref(record, method))

    for record in ranked_records:
        rank_id = str(record.get("rank_id") or "")
        if rank_id and rank_id in raw_text:
            add(record, "rank_id_exact_text")

    for record in ranked_records:
        canonical = str(record.get("canonical_bill_id") or "")
        if canonical and canonical in detected_ids:
            add(record, "canonical_bill_id_from_candidate_text")

    if normalized_question and normalized_question != "not_recorded":
        for record in ranked_records:
            seed = normalize_whitespace(str(record.get("question_seed") or ""))
            if seed and seed == normalized_question:
                add(record, "question_seed_exact")

    return refs


def _rank_ref(record: dict[str, Any], match_method: str) -> dict[str, Any]:
    risk_flags = record.get("heuristic_risk_flags")
    if risk_flags is None:
        risk_flags = record.get("risk_flags")
    return {
        "artifact": RANKED_QUESTIONS,
        "rank_id": record.get("rank_id", ""),
        "canonical_bill_id": record.get("canonical_bill_id", ""),
        "display_title": record.get("display_title", ""),
        "question_seed": record.get("question_seed", ""),
        "bucket": record.get("bucket", ""),
        "recommendation": record.get("recommendation", ""),
        "overall_score": record.get("overall_score", ""),
        "match_method": match_method,
        "heuristic_risk_flags": risk_flags or [],
        "missing_evidence": record.get("missing_evidence", []),
        "source_ids": record.get("source_ids", []),
        "duplicate_signals": _duplicate_signals(record),
    }


def _duplicate_relationship(
    candidate: dict[str, Any],
    refs: list[dict[str, Any]],
) -> dict[str, str]:
    explicit = str(candidate.get("recorded_duplicate_relationship") or "")
    if explicit == "not_recorded":
        explicit = ""
    if explicit:
        return {
            "status": "recorded_in_candidate_text",
            "source": CANDIDATE_QUESTIONS,
            "notes": explicit,
        }

    raw = str(candidate.get("raw_markdown") or "")
    folded_raw = fold_accents(raw).lower()
    if re.search(
        r"\bduplicate\b|\bduplicad|existing draft lane|active forecast|forecast-log",
        folded_raw,
    ):
        return {
            "status": "possible_duplicate",
            "source": CANDIDATE_QUESTIONS,
            "notes": (
                "Candidate text explicitly mentions a duplicate, active forecast, "
                "or adjacent draft relationship."
            ),
        }

    for ref in refs:
        signals = ref.get("duplicate_signals") or []
        if signals:
            return {
                "status": "possible_duplicate",
                "source": RANKED_QUESTIONS,
                "notes": "; ".join(str(signal) for signal in signals),
            }

    if refs:
        return {
            "status": "not_recorded",
            "source": RANKED_QUESTIONS,
            "notes": "Matched M2 row did not record a duplicate signal.",
        }
    return {
        "status": "unknown",
        "source": "not_recorded",
        "notes": "No deterministic M2 row match was available.",
    }


def _duplicate_signals(record: dict[str, Any]) -> list[str]:
    signals: list[str] = []
    for key in ("score_reasons", "penalties"):
        values = record.get(key) or []
        for value in values if isinstance(values, list) else []:
            text = str(value)
            folded = fold_accents(text).lower()
            if "duplicate" in folded or "duplicad" in folded:
                signals.append(text)
    return signals


def _missing_m3_fields(candidate: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in M3_REQUIRED_FIELDS:
        value = str(candidate.get(field) or "")
        if not value or value == "not_recorded":
            missing.append(field)
            continue
        folded = fold_accents(value).lower()
        if field == "deadline_or_window" and folded.startswith(("not ready", "needs")):
            missing.append(field)
    duplicate = candidate.get("duplicate_relationship") or {}
    if duplicate.get("status") in {"unknown", "not_recorded", ""}:
        missing.append("duplicate_check")
    return _unique(missing)


def _candidate_bill_ids(text: str) -> list[str]:
    ids: list[str] = []
    for number, year, chamber in BILL_RE.findall(text):
        chamber_key = (
            "camara" if fold_accents(chamber).lower().startswith("cam") else "senado"
        )
        ids.append(f"bill:{year}:{chamber_key}:{int(number)}")
    return _unique(ids)


def _extract_overall_decision(text: str) -> str:
    match = re.search(
        r"Overall decision:\s*`?([A-Za-z0-9_]+)`?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return "not_recorded"
    return _clean_decision(match.group(1)) or "not_recorded"


def _decision_counts(candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        decision = str(candidate.get("decision") or "not_recorded")
        counts[decision] = counts.get(decision, 0) + 1
    return counts


def _is_candidate_section(section_key: str) -> bool:
    if not section_key:
        return False
    blocked = {"decision_summary", "bottom_line", "m3_decision", "monitor_queue"}
    if section_key in blocked:
        return False
    return (
        "candidate" in section_key
        or "research_more" in section_key
        or "selected_for_evidence_pack" in section_key
        or section_key.startswith("monitor")
        or section_key.startswith("reject")
    )


def _clean_decision(value: str) -> str:
    cleaned = normalize_whitespace(value.replace("`", "")).rstrip(".")
    return cleaned


def _key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", fold_accents(text).lower()).strip("_")


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _list_text(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "none"
    return str(value or "none")
