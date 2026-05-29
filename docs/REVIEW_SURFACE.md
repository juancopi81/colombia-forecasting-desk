# Review Surface

The review surface is a human-friendly HTML read of what a run produced. It
exists to make monitor/no-post days legible: on most days there is no M3-ready
forecast question, but the run still records analyst insights, investigation
leads, source caveats, tension cards, market context, and co-occurrence
bundles. The surface gathers those into one page so "nothing posted" does not
read as "nothing happened".

It is rendered by `scripts/render_review.py` (logic in
`colombia_forecasting_desk/review_html.py`).

```text
runs/YYYY-MM-DD/review.html   # daily TLDR for one run
runs/review_index.html        # recent-runs trends across the last --window runs
```

## How to render

```bash
uv run python scripts/render_review.py                 # latest run + index
uv run python scripts/render_review.py --date 2026-05-29
uv run python scripts/render_review.py --window 21
uv run python scripts/render_review.py --daily-only    # or --index-only
```

## Design constraints

- **Deterministic, regenerated, never hand-edited.** The HTML is built in Python
  from the structured JSON/Markdown artifacts a run already wrote. There is no
  LLM in the loop and no network access. Every timestamp shown comes from the
  artifacts (`finished_at` / `generated_at`), never wall-clock time, so the same
  artifacts always produce byte-identical HTML. Do not edit the HTML by hand;
  re-render instead.
- **No new dependency.** HTML is assembled with `html.escape`, the same way
  `brief.py` assembles Markdown.
- **Generated, so gitignored.** `runs/*/review.html` is covered by the existing
  `runs/*/` rule; `runs/review_index.html` has its own `.gitignore` line.
- **Excluded from the parity guard.** `check_artifact_parity.py` does not compare
  these files. They are derived purely from artifacts it already compares, and a
  CSS tweak must not trip a behavior-parity check.

## Guardrails the surface must preserve

The renderer only *reads and arranges* artifacts. It must never promote, score,
or reinterpret them. In particular:

- It does **not** loosen the M3 gate. The post/monitor status is derived
  strictly: a run is "review for possible forecast" only when an artifact already
  carries an M3-ready signal — a `forecast_question` lead in `analyst_leads.json`
  or a non-empty `ready_for_m3` bucket in `m2_ranked_questions.json`. Otherwise it
  is "monitoring — no new forecast", framed as intentional, not as an error.
- It does **not** turn tension cards, market-pricing rows, or co-occurrence
  bundles into probability inputs. Each is shown with its own advisory/context
  label and its source caveats.
- It does **not** add insights or investigation leads to the forecast log, and it
  assigns no probabilities. It mirrors the
  [`Final Output Contract`](FINAL_OUTPUT_CONTRACT.md) lanes.
- The recorded human decision (if any) is read from `human_decisions.md` for
  display only; the page's primary status is always the artifact-derived one.

## Daily view (`review.html`)

| Section | Source artifact(s) | Notes |
| --- | --- | --- |
| Decision banner | `analyst_leads.json`, `m2_ranked_questions.json`, `human_decisions.md` | Derived post/monitor status; surfaces the recorded human decision when present. |
| Why no M3 today | `analyst_leads.json`, `m2_ranked_questions.json` | The gating facts (forecast-question count, M2 buckets, review-queue size). |
| At a glance | `run_summary.json`, `run_manifest.json`, `analyst_leads.json` | Counts grid. |
| Top analyst insights | `analyst_leads.json` (`analyst_insight`) | Source-backed findings; not forecasts. |
| Top investigation leads | `analyst_leads.json` (`investigation_lead`) | Underqualified leads needing more research. |
| Monitor queue (derived) | `analyst_leads.json`, `m2_ranked_questions.json` (`review_queue`) | "What to sample next", not a promotion. |
| Source-health caveats | `source_health.json`, `acceptance_report.json` | Only genuine visibility gaps: fetch failures, `needs_parser` sources, and document-links-without-parsed-content. A working source with parsed content but no rankable candidate is healthy and is **not** flagged. |
| Indicator tension cards | `indicator_tension_cards.json` | Advisory screens only. |
| Market-pricing context | `market_pricing_watch.json` | Experimental, fail-closed context only. |
| Co-occurrence bundles | `cooccurrence_bundles.json` | Neutral routing aids; not a thesis. |
| Source artifacts | files present in the run folder | Links back to the JSON/Markdown, plus hand-written human notes. |

## Recent-runs index (`review_index.html`)

| Section | Derivation | Notes |
| --- | --- | --- |
| Forecast-question drought | trailing consecutive runs with no M3-ready signal | The headline pattern; expected, by design. |
| Counts over time | per-run summary rows, newest first | FQ column of zeros = the drought, visualized. |
| Recurring analyst insights | `analyst_leads.json` insight titles across the window | Frequency `days/total`. |
| Repeated tension cards | `indicator_tension_cards.json` titles across the window | Persistent ≠ resolvable. |
| Source reliability issues | `source_health.json` caveats across the window | Sources whose silence is repeatedly unreliable. |
| Active monitor queue | latest run's investigation leads + M2 review queue | Derived. |
| Per-run reviews | one link per run | Jump to each daily `review.html`. |

Older runs may lack newer artifacts (e.g. `cooccurrence_bundles.json` or
`market_pricing_watch.json`). Missing artifacts load as empty and render as zero
counts rather than crashing, so the index stays fair across dates — the same
principle as `run_manifest.json`.
