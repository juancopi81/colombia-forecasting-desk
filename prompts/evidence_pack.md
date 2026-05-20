# Prompt: M3 Evidence Pack

You are running M3 for Colombia Forecasting Desk.

Input may include `metasource_brief.md`, `m2_review_packet.md`,
`m2_review_packet.json`, `m1_candidates.json`, `source_health.json`,
`legislative_reconciler.json`, `m2_ranked_questions.json`, and
`forecasts/forecast_log.jsonl`.

Use the rich M2 context to reason and challenge heuristic scores. Do not treat
the M2 ranking as authoritative. Do not browse unless the user explicitly asks.
Do not give investment, trading, betting, or execution advice.

## Goal

Produce one human-reviewable M3 evidence pack for a selected forecast candidate.
The evidence pack must start with an `M3 Case File` section. The case file is
the accountability contract for the LLM's selected forecast, not the input that
limits M2 exploration.

If the selected candidate is not ready for probability work, set
`m3_gate: research_more` or `m3_gate: reject` and explain what is missing. Do
not assign a probability unless `m3_gate: ready_for_m3`.

After writing the pack, validate it with:

```bash
./.venv/bin/python scripts/validate_m3_case_file.py runs/YYYY-MM-DD/evidence_packs/<slug>.md
```

## Required First Section

The first level-2 section must be exactly:

````markdown
## M3 Case File

```yaml
schema_version: m3_case_file.v1
question:
resolution_source:
resolution_criteria:
  - ...
deadline_or_window:
source_excerpts:
  - source_id:
    source_name:
    url:
    date:
    excerpt:
source_health_caveats:
  - ...
missing_evidence:
  - ...
duplicate_check:
  status: no_active_duplicate | possible_duplicate | duplicate | not_checked
  matched_forecast_ids:
    - ...
  notes:
m3_gate: ready_for_m3 | research_more | reject
gate_reason:
reasons_to_challenge:
  - ...
artifact_refs:
  - artifact:
    key:
    value:
```
````

Gate rules:

- `ready_for_m3`: use only when the question, resolution source, resolution
  criteria, deadline/window, source excerpts, and duplicate check are concrete.
- `research_more`: use when the candidate is promising but a source, deadline,
  current status, or resolution criterion is still missing.
- `reject`: use when the candidate is resolved, duplicate, too vague, unsafe,
  or lacks a plausible public resolution path.

## Evidence Pack Body

After the case file, include:

- Forecast question.
- Resolution criteria and deadline/check window.
- Relevant evidence items, each with source, date, summary, and direction
  (`supports`, `contradicts`, or `neutral`).
- Conflicting information and how to weigh it.
- Missing evidence that would significantly change the estimate.
- Prior forecast if one exists.
- Probability, reasoning, uncertainty, and counterarguments only when
  `m3_gate: ready_for_m3`.
- Draft post only when probability work is allowed and the user requested one.

Keep wording concrete and source-backed. The pack should be readable by both a
human reviewer and a later LLM pass.
