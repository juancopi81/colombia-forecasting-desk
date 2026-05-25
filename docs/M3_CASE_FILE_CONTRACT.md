# M3 Case File Contract

M3 turns a selected candidate into a human-reviewable forecast package. It is
not the exploratory M2 layer.

M2 should stay broad and content-rich: `metasource_brief.md`,
`m2_review_packet.md`, `cooccurrence_bundles.md`, `m1_candidates.json`,
`source_health.json`, and the forecast log are all available for reasoning and
heuristic challenge.

The M3 case file is the first section of an evidence pack. It records the
selected forecast in a structured way before probability or draft-post work.
Its purpose is to prove that the forecast is resolvable, source-backed, and
non-duplicative enough to continue.

## Required Evidence-Pack Section

Every M3 evidence pack should start with:

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

## Gate Semantics

- `ready_for_m3`: the question has a concrete resolution source, resolution
  criteria, deadline/window, source excerpts, and a clear active-forecast
  duplicate check.
- `research_more`: the candidate is promising, but a resolution source,
  deadline/window, current status, or important source excerpt is missing.
- `reject`: the candidate is resolved, duplicate, too vague, unsafe, or lacks a
  plausible public resolution path.

Probability and draft-post work should only happen after `ready_for_m3`.

## Validation

Validate evidence packs with:

```bash
./.venv/bin/python scripts/validate_m3_case_file.py runs/YYYY-MM-DD/evidence_packs/<slug>.md
```

The validator prints the detected gate and exits nonzero when the pack is
missing the first-section case file, has invalid YAML, or claims
`ready_for_m3` without the required resolution, source, deadline, excerpt, or
duplicate-check fields.

## Why This Exists

The case file should not narrow the LLM's M2 input. It is the LLM's accountable
M3 output before it estimates probability.

This keeps the intelligence layer broad while forcing the final selected
forecast to answer basic resolution questions:

- What exactly is being forecast?
- What official or durable source resolves it?
- By when?
- What evidence is actually parsed and cited?
- What is still missing?
- Is this already covered by the forecast log?
