"""Scaffold a partial M3 evidence-pack DRAFT from ``m2_sampling_decisions.json``.

The M2 -> M3 handoff stalls on a blank page: a reviewer must hand-author the
whole ``## M3 Case File`` block even though most of its fields already exist in
``m2_sampling_decisions.json``. This script reuses those fields to emit one
pre-filled ``<NN>_<slug>.draft.md`` evidence-pack stub per *selected* candidate.

It is deliberately conservative:

- It writes exactly ONE draft per invocation (pick with ``--candidate-index`` or
  ``--rank-id``; use ``--list`` to see the options).
- It only ever writes ``*.draft.md`` files under
  ``runs/<date>/evidence_packs/`` and never overwrites an existing file: if the
  target already exists it fails and asks you to delete or rename it, so both a
  hand-authored ``<slug>.md`` pack and a prior ``.draft.md`` are always safe.
- The gate is mapped to ``research_more`` (or ``reject``) and is NEVER set to
  ``ready_for_m3``. No probability is assigned and ``forecast_log.jsonl`` is not
  touched. A human/LLM must complete the checklist and paste real source
  excerpts before promoting the draft.

    uv run python scripts/scaffold_m3_case_file.py --date 2026-06-03 --list
    uv run python scripts/scaffold_m3_case_file.py --date 2026-06-03 --candidate-index 2
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.m3_case_file import (  # noqa: E402
    ALLOWED_DUPLICATE_STATUSES,
    ALLOWED_GATES,
    SCHEMA_VERSION,
)

RUNS_DIR = REPO_ROOT / "runs"
DRAFT_SUFFIX = ".draft.md"

# m2_sampling_decisions decision vocabulary -> m3 case-file gate.
# Anything unknown falls back to research_more; ready_for_m3 is never produced.
_GATE_BY_DECISION = {
    "research_more_before_m3": "research_more",
    "monitor_no_new_m3": "research_more",
    "select_for_evidence_pack": "research_more",
    "reject": "reject",
}

# duplicate_relationship.status -> contract duplicate_check.status.
_DUPLICATE_STATUS_MAP = {
    "no_active_duplicate": "no_active_duplicate",
    "possible_duplicate": "possible_duplicate",
    "duplicate": "duplicate",
    "not_checked": "not_checked",
    "unknown": "not_checked",
    "not_recorded": "not_checked",
}

_PLACEHOLDER_TEXT = {"", "not_recorded", "not recorded"}

# missing_m3_field name -> human-facing checklist line.
_CHECKLIST_BY_FIELD = {
    "resolution_criteria": "Write explicit YES/NO `resolution_criteria` (scaffold left it blank).",
    "deadline_or_window": "Confirm a concrete `deadline_or_window` (currently a 'not ready' placeholder).",
    "duplicate_check": "Resolve `duplicate_check.status` after checking `forecasts/forecast_log.jsonl`.",
    "resolution_source": "Confirm the official `resolution_source`.",
    "source_excerpts": "Paste verbatim `source_excerpts` (source_id, url, excerpt).",
    "question": "Sharpen `question` into one resolvable YES/NO.",
}

_EXCERPT_CHECK = _CHECKLIST_BY_FIELD["source_excerpts"]


class ScaffoldError(Exception):
    """User-facing scaffold failure (bad selection, missing or invalid input)."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _is_placeholder(value: Any) -> bool:
    return _text(value).lower() in _PLACEHOLDER_TEXT


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def load_sampling_decisions(run_dir: Path) -> dict[str, Any]:
    """Read and lightly validate ``<run_dir>/m2_sampling_decisions.json``."""
    path = run_dir / "m2_sampling_decisions.json"
    if not path.is_file():
        raise ScaffoldError(
            f"Missing {path}. Run "
            "`scripts/write_m2_sampling_decisions.py --date <date>` first."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ScaffoldError(f"Could not read {path}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("candidates"), list):
        raise ScaffoldError(f"{path} is not a valid m2_sampling_decisions artifact.")
    return data


def select_candidate(
    sampling: dict[str, Any],
    *,
    index: int | None = None,
    rank_id: str | None = None,
) -> dict[str, Any]:
    """Return the single candidate selected by index or by a linked rank_id."""
    candidates: list[dict[str, Any]] = sampling.get("candidates") or []
    if index is not None:
        for candidate in candidates:
            if candidate.get("index") == index:
                return candidate
        raise ScaffoldError(
            f"No candidate with index {index}. Use --list to see options."
        )
    if rank_id:
        matches = [
            candidate
            for candidate in candidates
            if any(
                ref.get("rank_id") == rank_id
                for ref in candidate.get("m2_ranked_refs") or []
            )
        ]
        if len(matches) > 1:
            indices = ", ".join(str(candidate.get("index")) for candidate in matches)
            raise ScaffoldError(
                f"rank_id {rank_id!r} is referenced by {len(matches)} candidates "
                f"(indices {indices}). Select one with --candidate-index instead."
            )
        if not matches:
            raise ScaffoldError(
                f"No candidate references rank_id {rank_id!r}. Use --list to see options."
            )
        return matches[0]
    raise ScaffoldError("No candidate selector provided.")


def gate_for_decision(decision: str) -> str:
    """Map a sampling decision to an allowed gate; never ``ready_for_m3``."""
    gate = _GATE_BY_DECISION.get(_text(decision), "research_more")
    if gate not in ALLOWED_GATES or gate == "ready_for_m3":
        return "research_more"
    return gate


def _source_ids(candidate: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for ref in candidate.get("m2_ranked_refs") or []:
        for source_id in ref.get("source_ids") or []:
            ids.append(_text(source_id))
    return _dedupe(ids)


def _source_excerpts(candidate: dict[str, Any]) -> list[dict[str, str]]:
    """Template excerpt rows (one per cited source) for the human to fill in."""
    return [
        {"source_id": source_id, "source_name": "", "url": "", "date": "", "excerpt": ""}
        for source_id in _source_ids(candidate)
    ]


def _resolution_criteria(candidate: dict[str, Any]) -> list[str]:
    raw = candidate.get("resolution_criteria")
    if _is_placeholder(raw):
        return []
    return [_text(raw)]


def _missing_evidence(candidate: dict[str, Any]) -> list[str]:
    """Always-non-empty list so a ``research_more`` gate stays validator-clean."""
    items: list[str] = []
    if not _is_placeholder(candidate.get("missing_evidence")):
        items.append(_text(candidate.get("missing_evidence")))
    for ref in candidate.get("m2_ranked_refs") or []:
        for evidence in ref.get("missing_evidence") or []:
            items.append(_text(evidence))
    for field in candidate.get("missing_m3_fields") or []:
        field = _text(field)
        if field:
            items.append(f"Provide {field} before M3.")
    items = _dedupe(items)
    if not items:
        items.append(
            "Confirm resolution source, criteria, deadline, and source excerpts before M3."
        )
    return items


def _duplicate_check(candidate: dict[str, Any]) -> dict[str, Any]:
    relationship = candidate.get("duplicate_relationship") or {}
    status = _DUPLICATE_STATUS_MAP.get(_text(relationship.get("status")).lower(), "not_checked")
    if status not in ALLOWED_DUPLICATE_STATUSES:
        status = "not_checked"
    check: dict[str, Any] = {"status": status, "matched_forecast_ids": []}
    notes = _text(relationship.get("notes"))
    if notes:
        check["notes"] = notes
    return check


def _reasons_to_challenge(candidate: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for ref in candidate.get("m2_ranked_refs") or []:
        for flag in ref.get("heuristic_risk_flags") or []:
            flag = _text(flag)
            if flag:
                reasons.append(f"M2 heuristic risk flag to weigh: {flag}")
    return _dedupe(reasons)


def _artifact_refs(candidate: dict[str, Any], run_date: str) -> list[dict[str, Any]]:
    sampling_artifact = f"runs/{run_date}/m2_sampling_decisions.json"
    refs: list[dict[str, Any]] = [
        {
            "artifact": sampling_artifact,
            "key": "candidate_index",
            "value": candidate.get("index"),
        }
    ]
    linked_bills: set[str] = set()
    for ref in candidate.get("m2_ranked_refs") or []:
        artifact = _text(ref.get("artifact")) or "m2_ranked_questions.json"
        rank_id = _text(ref.get("rank_id"))
        if rank_id:
            refs.append({"artifact": artifact, "key": "rank_id", "value": rank_id})
        canonical = _text(ref.get("canonical_bill_id"))
        if canonical:
            refs.append({"artifact": artifact, "key": "canonical_bill_id", "value": canonical})
            linked_bills.add(canonical)
    for canonical in candidate.get("canonical_bill_ids_detected") or []:
        canonical = _text(canonical)
        if canonical and canonical not in linked_bills:
            refs.append(
                {
                    "artifact": sampling_artifact,
                    "key": "canonical_bill_id_detected",
                    "value": canonical,
                }
            )
    return refs


def build_case_file(candidate: dict[str, Any], run_date: str) -> dict[str, Any]:
    """Assemble the M3 case-file mapping (contract field order)."""
    gate = gate_for_decision(candidate.get("decision") or "")
    gate_reason = _text(candidate.get("why_not_m3"))
    if gate == "reject" and not gate_reason:
        gate_reason = "Recorded as reject in m2_sampling_decisions.json."
    return {
        "schema_version": SCHEMA_VERSION,
        "question": _text(candidate.get("question_considered")),
        "resolution_source": _text(candidate.get("resolution_source")),
        "resolution_criteria": _resolution_criteria(candidate),
        "deadline_or_window": _text(candidate.get("deadline_or_window")),
        "source_excerpts": _source_excerpts(candidate),
        "source_health_caveats": [],
        "missing_evidence": _missing_evidence(candidate),
        "duplicate_check": _duplicate_check(candidate),
        "m3_gate": gate,
        "gate_reason": gate_reason,
        "reasons_to_challenge": _reasons_to_challenge(candidate),
        "artifact_refs": _artifact_refs(candidate, run_date),
    }


def _checklist(candidate: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for field in candidate.get("missing_m3_fields") or []:
        field = _text(field)
        if not field:
            continue
        lines.append(_CHECKLIST_BY_FIELD.get(field, f"Provide `{field}`."))
    if _EXCERPT_CHECK not in lines:
        lines.append(_EXCERPT_CHECK)
    lines = _dedupe(lines)
    lines.append("Add `source_health_caveats` from `source_failures.json` if a cited source failed.")
    lines.append(
        "When every gap is filled, set `m3_gate: ready_for_m3` and re-run "
        "`scripts/validate_m3_case_file.py`."
    )
    return lines


def _bullets(items: list[Any], empty: str = "_None recorded._") -> str:
    cleaned = [_text(item) for item in items if _text(item)]
    if not cleaned:
        return empty
    return "\n".join(f"- {item}" for item in cleaned)


def build_scaffold_markdown(candidate: dict[str, Any], run_date: str) -> str:
    """Render the full DRAFT evidence-pack Markdown for one candidate."""
    case_file = build_case_file(candidate, run_date)
    yaml_block = yaml.safe_dump(
        case_file,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=4096,
    ).rstrip("\n")

    index = candidate.get("index")
    title = _text(candidate.get("title")) or "Untitled candidate"
    decision = _text(candidate.get("decision")) or "unknown"
    gate = case_file["m3_gate"]
    question = case_file["question"] or "_Not recorded._"
    resolution_source = case_file["resolution_source"] or "_Not recorded._"
    deadline = case_file["deadline_or_window"] or "_Not recorded._"
    gate_reason = case_file["gate_reason"] or "_Not recorded._"
    evidence_now = _text(candidate.get("evidence_available_now")) or "_Not recorded._"

    ranked_lines: list[str] = []
    for ref in candidate.get("m2_ranked_refs") or []:
        parts: list[str] = []
        rank_id = _text(ref.get("rank_id"))
        if rank_id:
            parts.append(f"`{rank_id}`")
        display_title = _text(ref.get("display_title"))
        if display_title:
            parts.append(display_title)
        source_ids = ", ".join(_text(s) for s in ref.get("source_ids") or [] if _text(s))
        parts.append(f"sources: {source_ids or '—'}")
        ranked_lines.append("- " + " — ".join(parts))
    ranked_block = "\n".join(ranked_lines) if ranked_lines else "_No linked M2 ranked rows._"

    checklist_block = "\n".join(f"- [ ] {line}" for line in _checklist(candidate))
    missing_block = _bullets(case_file["missing_evidence"])

    return f"""# Evidence Pack (DRAFT) — {title}

> **Auto-generated scaffold — not authoritative.** Produced by
> `scripts/scaffold_m3_case_file.py` from
> `runs/{run_date}/m2_sampling_decisions.json` (candidate #{index}, decision
> `{decision}`). The gate is held at `{gate}` and is never set to
> `ready_for_m3` automatically. No probability is assigned and
> `forecasts/forecast_log.jsonl` is untouched. A human or LLM must complete the
> checklist below and paste real source excerpts before this becomes a real
> evidence pack. Delete this file to discard the draft.

## M3 Case File

```yaml
{yaml_block}
```

## Candidate Question

{question}

## Resolution Path

- Resolution source: {resolution_source}
- Deadline / window: {deadline}

## Evidence Available Now

{evidence_now}

### Linked M2 Ranked Rows

{ranked_block}

## Missing Evidence

{missing_block}

## Scaffold Checklist (fill before `ready_for_m3`)

{checklist_block}

## M3 Gate

- Gate: `{gate}`
- Probability: not assigned.
- Forecast log: unchanged.
- Why not M3 yet: {gate_reason}
"""


def slug_for(candidate: dict[str, Any]) -> str:
    title = _text(candidate.get("title"))
    ascii_title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_title.lower()).strip("_")
    slug = slug[:60].strip("_") or "candidate"
    try:
        prefix = f"{int(candidate.get('index')):02d}"
    except (TypeError, ValueError):
        prefix = "00"
    return f"{prefix}_{slug}"


def scaffold_path(run_dir: Path, candidate: dict[str, Any]) -> Path:
    return run_dir / "evidence_packs" / f"{slug_for(candidate)}{DRAFT_SUFFIX}"


def write_scaffold(
    run_dir: Path,
    candidate: dict[str, Any],
    *,
    run_date: str,
) -> Path:
    """Write one ``*.draft.md`` scaffold, never clobbering an existing file."""
    path = scaffold_path(run_dir, candidate)
    # Defensive: this script must only ever create *.draft.md files.
    assert path.name.endswith(DRAFT_SUFFIX), path
    if path.exists():
        raise ScaffoldError(
            f"{path} already exists. Delete or rename it to regenerate the draft."
        )
    markdown = build_scaffold_markdown(candidate, run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    return path


def format_candidate_list(sampling: dict[str, Any], run_date: str) -> str:
    candidates: list[dict[str, Any]] = sampling.get("candidates") or []
    lines = [f"{run_date}: {len(candidates)} candidate(s) in m2_sampling_decisions.json", ""]
    for candidate in candidates:
        decision = _text(candidate.get("decision")) or "unknown"
        rank_ids = ", ".join(
            _text(ref.get("rank_id"))
            for ref in candidate.get("m2_ranked_refs") or []
            if _text(ref.get("rank_id"))
        ) or "-"
        lines.append(
            f"  [{candidate.get('index')}] {decision} -> {gate_for_decision(decision)} "
            f"| rank_ids: {rank_ids}"
        )
        lines.append(f"      {_text(candidate.get('title'))}")
    lines.append("")
    lines.append("Scaffold one with: --candidate-index N   (or)   --rank-id <id>")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scaffold one partial M3 evidence-pack DRAFT (.draft.md) from a "
            "sampled candidate in m2_sampling_decisions.json. Never assigns a "
            "probability or sets ready_for_m3."
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Run date (YYYY-MM-DD). The run directory must already exist.",
    )
    parser.add_argument(
        "--candidate-index",
        type=int,
        default=None,
        help="1-based candidate index from m2_sampling_decisions.json.",
    )
    parser.add_argument(
        "--rank-id",
        default=None,
        help="Select the candidate linked to this m2 ranked rank_id.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_candidates",
        help="List candidates and their gates, then exit.",
    )
    parser.add_argument(
        "--runs-dir",
        default=str(RUNS_DIR),
        help="Root directory for run artifacts.",
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.runs_dir) / args.date
    try:
        sampling = load_sampling_decisions(run_dir)
    except ScaffoldError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.list_candidates:
        print(format_candidate_list(sampling, args.date))
        return 0

    if (args.candidate_index is None) == (args.rank_id is None):
        print(
            "Select exactly one of --candidate-index or --rank-id (or use --list).",
            file=sys.stderr,
        )
        return 1

    try:
        candidate = select_candidate(
            sampling, index=args.candidate_index, rank_id=args.rank_id
        )
        path = write_scaffold(run_dir, candidate, run_date=args.date)
    except ScaffoldError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    gate = gate_for_decision(candidate.get("decision") or "")
    print(f"Wrote {path}")
    print(
        f"gate: {gate} (draft — complete the checklist, then re-run "
        "scripts/validate_m3_case_file.py)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
