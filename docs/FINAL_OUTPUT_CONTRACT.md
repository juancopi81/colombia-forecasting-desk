# Final Output Contract

This contract defines the first human-facing surface after M2 review. It keeps
forecastable questions, source-backed insights, and early investigation leads in
separate lanes so the workflow does not force every useful discovery into a
forecast.

The generated artifacts are:

```text
runs/YYYY-MM-DD/analyst_leads.json
runs/YYYY-MM-DD/analyst_leads.md
```

They are built from `m2_review_packet.json`, `indicator_tension_cards.json`,
`cooccurrence_bundles.json`, and selected structured lead builders such as
SECOP concentration and zona-franca land-use screens.
They do not replace `candidate_questions.md`, M3 evidence packs, or the forecast
log. They are a review surface for the human and the next LLM step.

## Output Types

### `forecast_question`

A source-backed question that appears ready for M3 evidence-pack work because it
has a concrete future resolution path.

Promotion rule: only `ready_for_m3` or `select_for_evidence_pack` M2 items with
source evidence can become `forecast_question` leads.

### `analyst_insight`

A descriptive, source-backed finding that matters but does not need to be forced
into a yes/no forecast question.

Examples:

- An official-data tension card showing tax collection growing below IPC.
- A procurement concentration pattern, once entity matching and contract totals
  are reliable enough.
- A SECOP screen showing repeated supplier/entity pairs, direct-contracting
  concentration, low-competition process clusters, or cancelled-process
  clusters, framed only as a review prompt.
- A MinCIT approved-zonas-francas registry change naming a company/zone,
  municipality, resolution, and official follow-up path.
- A regulatory or land-use document that changes the public map but has no
  natural probability question yet.

Promotion rule: an `analyst_insight` may cite deterministic screens or
source-backed patterns, but it must not receive a probability or forecast-log
treatment. Procurement screens must not be framed as fraud findings without
separate legal, audit, or investigative evidence.
Zona-franca land-use screens must not be framed as investment recommendations;
they are prompts to verify the legal act, local planning context, and public
impact.

### `investigation_lead`

A plausible lead that needs more research before it can become an insight or
forecast question.

Examples:

- A cross-impact hypothesis between a budget bill and TES pressure.
- A document-link-only signal that has not been parsed.
- A source-backed issue whose timing, mechanism, or resolution source is still
  underqualified.

Promotion rule: use `investigation_lead` when timing, resolution criteria,
source coverage, or causal mechanism is still underqualified.

## Required Fields

Every lead must include:

- `claim_or_question`: the question or descriptive claim under review.
- `evidence`: source-backed excerpts or deterministic official-data inputs.
- `caveats`: missing evidence, data caveats, heuristic caveats, or review flags.
- `next_check`: the next concrete review action.
- `disposition`: the current editorial lane.

Allowed dispositions in v0:

- `select_for_evidence_pack`
- `monitor_or_research`
- `research_more_before_m3`

## Workflow Position

The intended path is:

```text
m2_review_packet
  -> cooccurrence_bundles
  -> analyst_leads
  -> candidate_questions / selected M3 Case File
  -> evidence pack
  -> probability/draft
  -> human decision
  -> forecast log, only when explicitly selected
```

`analyst_leads.md` should make it easy to ask:

- Which items are true forecast-question candidates?
- Which findings are useful insights but not forecasts?
- Which leads should stay in research until evidence improves?

Insights and investigation leads are not forecast-log entries. They should not
receive probabilities until an M3 Case File says they are ready.
