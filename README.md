# Colombia Forecasting Desk

Experimental agent-assisted forecasting project focused on Colombian political, economic, regulatory, and institutional events. See [`PROJECT_SPEC.md`](PROJECT_SPEC.md) for the full vision and milestones.

Current editorial bias: prefer public-interest forecast hooks over merely clean
indicator continuation. Strong M2/M3 candidates usually involve a pending
decision, cost/input pressure, a contradiction between credible sources, or a
named entity with a clear institutional path, for example a zona franca
decision, material-cost increase, regulatory proposal, bill, court decision, or
official-data tension.

## Quickstart

Requires Python 3.12 and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest -q
uv run python scripts/scan_metasources.py
uv run python scripts/scan_metasources.py --source-report
```

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

The pipeline produces a dated run folder under `runs/YYYY-MM-DD/` containing:

- `raw_items.json` — every item fetched from each enabled metasource
- `cleaned_items.json` — items after HTML stripping, normalization, filtering, and dedupe
- `clusters.json` — clusters of related items, ranked by simple heuristics
- `indicator_watch.json` — curated latest-known indicator cards for durable economic, fiscal, energy, and activity signals
- `indicator_tension_cards.json` / `.md` — advisory cross-indicator screens that flag official-data tensions for M2 review without making conclusions
- `legislative_reconciler.json` — one bill-status record per reconciled legislative identity, including M2 readiness and contradictions
- `m2_ranked_questions.json` — advisory M2 legislative triage with transparent scores, buckets, review queue, and heuristic-risk audit
- `m2_review_packet.json` — balanced, content-rich M2 review queue that attaches source excerpts, structured context, traceability, and advisory cross-impact hypotheses to M1/M2 candidates
- `m2_review_packet.md` — paste-ready M2 review packet that tells the reviewer to read excerpts before trusting heuristic scores or cross-impact prompts
- `analyst_leads.json` / `.md` — final output-surface v0 that separates forecast-question candidates from analyst insights and investigation leads
- `m1_candidates.json` — deterministic candidate/rejection/source-caveat database used as the M2 input contract
- `metasource_brief.md` — the human-readable daily brief
- `m2_handoff.md` — paste-ready M2 question-selection packet for manual AI testing
- `acceptance_report.json` — hard M1 quality checks and warning-level source/candidate caveats
- `source_failures.json` — per-source errors (run never crashes on a single source)
- `source_health.json` — per-source raw, dated, rankable, tag, content-mode, document-link, parsed-content, and failure counts
- `run_summary.json` — counts and timestamps for the run
- `run_trace.json` — diagnostic stage/source trace with durations, counts, metadata, and caught errors for debugging and AI-agent handoffs
- `run_manifest.json` — run provenance, artifact inventory, schema versions, git context, and enabled capabilities for fair historical comparison

For legislative sources, `legislative_reconciler.json` is the broad case-file
artifact, while `m2_ranked_questions.json` is only an advisory triage layer.
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
`analyst_leads.json` / `.md` apply the
[`Final Output Contract`](docs/FINAL_OUTPUT_CONTRACT.md): `forecast_question`
for evidenced M3-ready questions, `analyst_insight` for source-backed findings
that do not need to become forecasts, and `investigation_lead` for plausible
but underqualified leads. This keeps useful civic/economic insights visible
without adding them to the forecast log or assigning probabilities too early.
SECOP procurement concentration screens can now contribute conservative
`analyst_insight` leads when recent official rows show repeated supplier/entity
pairs, direct-contracting concentration, low-competition process clusters, or
cancelled-process clusters. These are review prompts, not fraud findings.
`run_trace.json` is diagnostic only; it helps explain how a run executed, but it
does not feed candidate ranking, acceptance gates, or M2 question selection.

### Optional flags

```bash
uv run python scripts/scan_metasources.py --date 2026-04-27 --config config/metasources.yaml --source-report
uv run python scripts/scan_metasources.py --date 2026-04-27 --strict
```

`--strict` exits nonzero when M1 hard gates fail. It checks both candidate
quality and operational coverage: malformed candidates, link-only evidence
promoted as forecastable, too few raw/cleaned/rankable items in a full run, too
many source failures, too few observed Indicator Watch cards, or excessive
high-impact source failures.

## Project layout

```
colombia_forecasting_desk/   # core package (config, cleaner, dedupe, cluster, ranker, brief, pipeline)
  fetchers.py                # compatibility import path for source fetching
  observability.py           # run_trace.json stage/source diagnostics
  source_fetching/           # source fetching and source-specific parser internals
config/metasources.yaml      # registry of public sources (enabled/disabled, fetch_method, priority, trust_role)
scripts/scan_metasources.py  # M1 entry point
scripts/check_artifact_parity.py # stable generated-artifact comparison guard
scripts/validate_m3_case_file.py # M3 evidence-pack readiness contract guard
prompts/                     # placeholder prompts (used in later milestones)
runs/YYYY-MM-DD/             # generated run artifacts (gitignored content)
forecasts/                   # forecast log (used in later milestones)
tests/                       # pytest suite
```

`colombia_forecasting_desk.fetchers` remains the supported import path used by
the pipeline, tests, and workflow snippets. Its implementation is staged under
`colombia_forecasting_desk/source_fetching/` so future source-family parser work
can be moved behind clearer boundaries without changing the daily command. The
current split keeps shared helpers in `common.py`, dispatcher functions in
`core.py`, and source-family logic in modules such as `dane.py`, `imprenta.py`,
`minhacienda.py`, `mincit.py`, `registries.py`, `rss.py`, and `socrata.py`.
Fetcher parser tests mirror that boundary: generic dispatcher/facade coverage
stays in `tests/test_fetchers.py`, while source-family parser cases live in
`tests/test_fetchers_*.py` files.
Indicator Watch's static catalog lives in
`colombia_forecasting_desk/data/indicator_catalog.json`; golden fixtures under
`tests/fixtures/indicator_watch/` pin the card order, component defaults, and
selected runtime summaries before catalog edits.

## Status

Currently at **M2.5 — final output surface v0**, building on the
M1.20 legislative registry pipeline, M1.21 MinCIT zonas-francas parser, M1.22
official legal-resolution bridge, and M1.23 GDP/ISE Indicator Watch coverage. The
official Senado Sección de Leyes and Cámara Proyectos de Ley registries now
provide primary structured bill identity/status records; Senado agenda PDFs and
Gacetas remain fallback/follow-up evidence. The MinCIT zonas francas source
parses the official approved-zones PDF into named registry rows with NIT,
location, declaratory/prórroga resolutions, and legal follow-up sources, while
promoting only future new/changed snapshot rows as current decision signals.
Diario Oficial PDFs, SUIN/Gestor legal rows, and MinCIT rows now share
normalized legal-act identities so official resolution matches can be attached
only when the act number/year and MinCIT or zone-name context agree. DIAN
regulatory-project coverage is source-specific instead of broad site
navigation, but still marked as parser feasibility rather than rankable
evidence. DANE PIB and ISE official pages are now first-class Indicator Watch
cards, including PIB sector drivers and current-release official document
links, so GDP/ISE releases can become M2-ready activity seeds instead of only
appearing as indirect context. Legislative records now also get an advisory M2
ranking with explicit score reasons, review buckets, and heuristic-risk audit
flags. M2.4 keeps that content-first review packet but balances the queue across
legislative records, indicator seeds, event leads, and conservative
cross-impact hypotheses and deterministic Indicator Tension Cards. Those
hypotheses and cards are review prompts only, not causal claims or probability
inputs, so humans and LLMs can challenge brittle rules instead of inheriting
them silently. See
[`docs/M1_METASOURCE_PIPELINE.md`](docs/M1_METASOURCE_PIPELINE.md) for the
detailed plan, the
[`Legislative Reconciler Contract`](docs/LEGISLATIVE_RECONCILER_CONTRACT.md)
for the legislative identity/status contract, and
[`PROJECT_SPEC.md`](PROJECT_SPEC.md) for upcoming milestones (M2 question
discovery, M3 evidence packs, M4 public X experiment).
