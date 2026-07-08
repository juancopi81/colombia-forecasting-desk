# Run Artifacts

Each pipeline run writes a dated folder under `runs/YYYY-MM-DD/`. This page is
the artifact-by-artifact catalog, plus the validation tools that operate on run
folders. For what each section of the HTML review surface means, see
[`REVIEW_SURFACE.md`](REVIEW_SURFACE.md).

## Artifact catalog

- `raw_items.json` — every item fetched from each enabled metasource
- `cleaned_items.json` — items after HTML stripping, normalization, filtering, and dedupe
- `clusters.json` — clusters of related items, ranked by simple heuristics
- `indicator_watch.json` — curated latest-known indicator cards for durable economic, fiscal, energy, and activity signals
- `indicator_tension_cards.json` / `.md` — advisory cross-indicator screens that flag official-data tensions for M2 review without making conclusions
- `market_pricing_watch.json` / `.md` — experimental fail-closed ADR, ETF, and Brent/oil pricing context for M2 review
- `cooccurrence_bundles.json` / `.md` — neutral M2 context bundles that package related ingredients that co-occurred today without choosing a thesis
- `m3_preflight_opportunities.json` / `.md` — advisory scheduled-event prompts that flag near-term clean M3 opportunities without creating forecasts, probabilities, or evidence packs
- `legislative_reconciler.json` — one bill-status record per reconciled legislative identity, including M2 readiness and contradictions
- `m2_ranked_questions.json` — advisory M2 legislative triage with transparent scores, buckets, review queue, and heuristic-risk audit
- `m2_review_packet.json` — balanced, content-rich M2 review queue that attaches source excerpts, structured context, traceability, and advisory cross-impact hypotheses to M1/M2 candidates
- `m2_review_packet.md` — paste-ready M2 review packet that tells the reviewer to read excerpts before trusting heuristic scores or cross-impact prompts
- `analyst_leads.json` / `.md` — final output-surface v0 that separates forecast-question candidates from analyst insights and investigation leads
- `m2_sampling_decisions.json` / `.md` — post-editorial bridge artifacts generated after `candidate_questions.md`, recording sampled candidates, M2 decisions, missing M3 fields, duplicate status, and deterministic M2-ranker links
- `m1_candidates.json` — deterministic candidate/rejection/source-caveat database used as the M2 input contract
- `metasource_brief.md` — the human-readable daily brief
- `m2_handoff.md` — paste-ready M2 question-selection packet for manual AI testing
- `acceptance_report.json` — hard M1 quality checks and warning-level source/candidate caveats
- `source_failures.json` — per-source errors (run never crashes on a single source)
- `source_health.json` — per-source raw, dated, rankable, tag, content-mode, document-link, parsed-content, and failure counts
- `run_summary.json` — counts and timestamps for the run
- `run_trace.json` — diagnostic stage/source trace with durations, counts, metadata, and caught errors for debugging and AI-agent handoffs
- `run_manifest.json` — run provenance, artifact inventory, schema versions, git context, and enabled capabilities for fair historical comparison
- `review.html` — deterministic daily review surface rendered by `scripts/render_review.py` (gitignored; regenerate any time). A recent-runs `runs/review_index.html` is rendered alongside it.

## How the artifacts relate

For legislative sources, `legislative_reconciler.json` is the broad case-file
artifact, while `m2_ranked_questions.json` is only an advisory triage layer.
The reconciler also ingests the tracked manual override file
`colombia_forecasting_desk/data/resolved_status_overrides.json`. That file is
desk memory for already-reviewed hygiene contradictions, such as an archived
registry row followed only by Gaceta project-text publication. It does not
replace official evidence, and it is condition-gated so later ponencias,
agendas, debate results, transfers, corrections, archive reversals, or Diario
Oficial items still surface for review.
`m2_review_packet.json` / `.md` are the content-first M2 inputs: they package
source excerpts and structured context so low-ranked items can still be sampled
by a human or LLM when the evidence suggests possible heuristic blind spots.
They reserve room for legislative records, Indicator Watch seeds, event leads,
explicitly advisory cross-impact hypotheses, and Indicator Tension Cards so
structured bills do not crowd out macro/fiscal/market signals.
`indicator_tension_cards.json` / `.md` are deterministic review prompts, not
probability inputs. They currently look for TES-policy spread pressure, high
ex-post real policy rates, real tax-revenue squeeze, high TES auction cutoff
rates, and construction-cost pressure versus headline IPC.
`market_pricing_watch.json` / `.md` is experimental, fail-closed market context
for EC, CIB, COLO, and Brent spot. It is not investment advice, a ranking
signal, or a probability input; endpoint failures and stale closes are surfaced
as source-health caveats so silence is not mistaken for no market movement.
`cooccurrence_bundles.json` / `.md` group related active ingredients such as
fiscal/TES pressure, monetary/credit transmission, construction/housing costs,
energy/tariff/subsidy context, and Colombia market-pricing context. They are
neutral routing aids for M2: the agent must review cross-bundle links and
unbundled items instead of treating the bundles as the only possible stories.
`m3_preflight_opportunities.json` / `.md` flags near-term scheduled official
events with clean resolution sources, such as a BanRep board decision named in
official minutes. It asks whether to scaffold M3; it does not create a
forecast, assign probability, update `forecast_log.jsonl`, or mark a lead
`ready_for_m3`.
`analyst_leads.json` / `.md` apply the
[`Final Output Contract`](FINAL_OUTPUT_CONTRACT.md): `forecast_question`
for evidenced M3-ready questions, `analyst_insight` for source-backed findings
that do not need to become forecasts, and `investigation_lead` for plausible
but underqualified leads. This keeps useful civic/economic insights visible
without adding them to the forecast log or assigning probabilities too early.
SECOP procurement concentration screens can now contribute conservative
`analyst_insight` leads when recent official rows show repeated supplier/entity
pairs, direct-contracting concentration, low-competition process clusters, or
cancelled-process clusters. These are review prompts, not fraud findings.
MinCIT zona-franca registry diffs can also contribute land-use/economic
development insights when the approved-zones registry adds or changes a named
zone. These are not investment recommendations; they are prompts to verify the
resolution text and local implications.
`run_trace.json` is diagnostic only; it helps explain how a run executed, but it
does not feed candidate ranking, acceptance gates, or M2 question selection.

## Validating and comparing run folders

For refactors that should preserve behavior, compare regenerated run folders
with the stable artifact parity guard:

```bash
uv run python scripts/check_artifact_parity.py runs/YYYY-MM-DD runs/YYYY-MM-DD-candidate
```

For M3 evidence packs, validate the required `## M3 Case File` section before
probability or draft-post work:

```bash
uv run python scripts/validate_m3_case_file.py runs/YYYY-MM-DD/evidence_packs/<slug>.md
```
