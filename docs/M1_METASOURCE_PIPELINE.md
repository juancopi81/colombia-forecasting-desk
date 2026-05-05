# M1 — Metasource Pipeline

## Goal

Build the first deterministic pipeline that reads public metasources and produces a useful daily brief.

The daily brief should help the next agentic step answer:

- What happened today?
- What seems important?
- What may be forecastable?
- What is probably noise?
- What evidence or sources should be checked next?

This milestone does not make forecasts, generate X posts, or publish anything.

## Scope

M1 starts from the existing metasource registry:

```text
config/metasources.yaml
```

The pipeline should:

```text
metasources.yaml
→ fetch raw items
→ clean and normalize items
→ filter low-quality/irrelevant items
→ deduplicate obvious duplicates
→ group related items into simple clusters
→ rank clusters
→ build an indicator watch for durable latest-known stats
→ write a daily metasource brief
```

## Non-Goals

Do not build:

- autonomous publishing
- forecast probability estimation
- full evidence packs
- a dashboard
- a database
- scheduled jobs
- X/social automation
- complex clustering or embeddings unless clearly needed

Keep this milestone local-first and file-based.

## Expected Output

Each run should create a dated run folder:

```text
runs/YYYY-MM-DD/
```

Minimum outputs:

```text
runs/YYYY-MM-DD/raw_items.json
runs/YYYY-MM-DD/cleaned_items.json
runs/YYYY-MM-DD/clusters.json
runs/YYYY-MM-DD/indicator_watch.json
runs/YYYY-MM-DD/metasource_brief.md
runs/YYYY-MM-DD/source_health.json
```

Optional output:

```text
runs/YYYY-MM-DD/ranked_signals.json
```

The main human-readable artifact is:

```text
runs/YYYY-MM-DD/metasource_brief.md
```

## Raw Item Shape

Each fetched item should roughly contain:

```json
{
  "id": "",
  "source_id": "",
  "source_name": "",
  "source_type": "",
  "url": "",
  "title": "",
  "published_at": "",
  "fetched_at": "",
  "raw_text": "",
  "metadata": {}
}
```

The schema can evolve, but every item should preserve source, URL, title, timestamp, and raw content when available.

## Cleaned Item Shape

Each cleaned item should roughly contain:

```json
{
  "id": "",
  "source_id": "",
  "source_name": "",
  "source_type": "",
  "url": "",
  "title": "",
  "published_at": "",
  "fetched_at": "",
  "clean_text": "",
  "summary": "",
  "detected_entities": [],
  "detected_topics": [],
  "country_relevance": "",
  "signal_type": "",
  "quality_notes": ""
}
```

For M1, some fields can be simple heuristics or placeholders. The priority is to preserve useful structure.

## Signal Types

Use a small initial set of signal types:

```text
official_update
new_data
poll
legislative_movement
court_or_regulatory_movement
political_statement
market_move
media_narrative
social_attention
economic_indicator
public_order
rumor
contradiction
correction
calendar_event
unknown
```

Do not over-optimize classification in the first version.

## Cluster Shape

Each cluster should roughly contain:

```json
{
  "cluster_id": "",
  "title": "",
  "summary": "",
  "items": [],
  "source_count": 0,
  "source_types": [],
  "latest_published_at": "",
  "signal_types": [],
  "why_it_matters": "",
  "possible_questions": [],
  "missing_evidence": [],
  "recommended_next_sources": [],
  "confidence": ""
}
```

For M1, clustering can start simple:

- similar titles
- shared keywords
- shared entities
- same broad topic
- manual/simple heuristics before embeddings

## Ranking

Rank clusters using simple heuristic scores.

Suggested dimensions:

```text
colombia_relevance
public_interest
freshness
source_reliability
source_diversity
forecastability
information_gain
urgency
novelty
noise_risk
legal_reputational_risk
```

The score does not need to be perfect. It only needs to help prioritize what the next agent should inspect.

M1.1 adds two safeguards so a single high-volume media feed does not dominate
the brief:

- sources may define `max_items` in `config/metasources.yaml`
- the top-ranked clusters are lightly diversified by source when alternatives exist

The target source mix before M2 is rankable output from multiple distinct
sources, including at least two official sources and at least one
legal/regulatory or agenda source.

M1.2 adds onboarding tooling (status field, sandbox runner, fixture-based
parser tests) and surfaces parser problems through the source-health table
instead of letting them fail silently. See
[ADDING_METASOURCE.md](ADDING_METASOURCE.md) for the workflow.

M1.5 adds Socrata API fetchers for public procurement datasets, making SECOP
signals available through structured datos.gov.co endpoints where the source
data is fresh enough to rank.

M1.6 adds the first document-content parser. The DANE ICOCED source now follows
the latest XLSX annex and extracts headline total, residential, and
non-residential index/variation metrics instead of only surfacing the annex
link. This keeps M1 honest about the difference between link-level coverage and
parsed evidence.

M1.7 adds an Indicator Watch alongside event clusters. Some high-value public
signals are not daily events; they are latest-known state variables that should
remain visible after the normal freshness window. The watch starts with twelve
must-track cards:

- IPC / inflation
- TRM / USD-COP
- policy rate + IBR
- labor market
- retail sales
- manufacturing
- construction bundle
- SECOP public procurement pulse
- energy demand / reservoirs / spot price
- external trade
- oil and gas production
- fiscal / tax pulse

Cards can be `observed` when M1 already has structured data, or
`pending_source` when the indicator is registered but still needs an easy API,
HTML table, or lightweight parser. This is deliberately not M2 question
generation; it is a durable evidence surface for humans and later agents.

## Indicator Watch

Each run writes:

```text
runs/YYYY-MM-DD/indicator_watch.json
```

Each card contains:

```json
{
  "indicator_id": "",
  "name": "",
  "category": "",
  "status": "observed | pending_source",
  "frequency": "",
  "period": "",
  "release_date": "",
  "headline": "",
  "values": {},
  "why_it_matters": "",
  "correlations": [],
  "next_step": ""
}
```

Current observed cards:

- `construction_bundle` from the parsed DANE ICOCED XLSX annex
- `secop_procurement` from existing Socrata-backed SECOP cleaned items

The remaining cards are intentionally visible as parser/source backlog so M1
can prioritize high-value official data before automating M2 question writing.

## Source Health

Each run writes:

```text
runs/YYYY-MM-DD/source_health.json
```

The smoke/report command is:

```bash
uv run python scripts/scan_metasources.py --source-report
```

The report shows, per source:

```text
source_id | content | raw | dated | rankable | doc_links | parsed | failures
```

`content` distinguishes HTML/API records from document links and parsed
document content. Link-only sources can still be useful as calendar signals,
but M1.6 treats parsed document content as the stronger evidence contract for
sources whose useful data lives inside PDFs or spreadsheets.

## Daily Brief Structure

Generate:

```text
runs/YYYY-MM-DD/metasource_brief.md
```

Recommended structure:

```markdown
# Metasource Brief — YYYY-MM-DD

## Run Summary

- Run date:
- Sources checked:
- Sources failed:
- Raw items collected:
- Cleaned items retained:
- Clusters created:

## Top Signals

### 1. [Cluster Title]

Priority:
Confidence:
Source types:
Latest update:

Summary:

Why it may matter:

Possible forecastable questions:

Missing evidence:

Recommended next sources:

Links:

---

## Indicator Watch

### IPC / inflation

Status:
Category:
Frequency:
Period:
Latest release:
Source:

Headline:

Values:

Why it matters:

Useful correlations:

M1 next step:

## Emerging Questions

- ...

## Topics to Monitor

- ...

## Noisy / Low-Confidence Items

- ...

## Source Failures

- ...

## Suggested Next Step

- ...
```

## Implementation Plan

### Step 1 — Read metasource registry

- [x] Load `config/metasources.yaml`.
- [x] Validate required fields.
- [x] Ignore disabled sources.
- [x] Skip paywalled, blocked, or manual-only sources for now.
- [x] Print a clear summary of enabled sources.

### Step 2 — Fetch raw items

- [x] Support RSS sources.
- [x] Support basic public HTML sources.
- [x] Store all fetched items in `raw_items.json`.
- [x] Log source failures without crashing the full run.
- [x] Preserve URLs and timestamps.

### Step 3 — Clean items

- [x] Extract title.
- [x] Extract publication date when available.
- [x] Extract main text when feasible.
- [x] Normalize whitespace.
- [x] Mark low-quality parses.
- [x] Store results in `cleaned_items.json`.

### Step 4 — Filter and deduplicate

- [x] Remove clearly irrelevant items.
- [x] Remove empty or unusable items.
- [x] Remove obvious duplicates by URL/title.
- [x] Keep borderline items if they may indicate public interest.
- [x] Prefer official or primary sources when duplicates exist.

### Step 5 — Classify and cluster

- [x] Assign basic signal type.
- [x] Detect broad topic.
- [x] Group related items into simple clusters.
- [x] Add source diversity count.
- [x] Add latest update timestamp.
- [x] Save `clusters.json`.
- [x] Save `indicator_watch.json`.

### Step 6 — Rank clusters

- [x] Score clusters with simple heuristics.
- [x] Penalize noisy or low-confidence clusters.
- [x] Promote fresh, Colombia-relevant, multi-source clusters.
- [x] Sort clusters by priority.

### Step 7 — Generate daily brief

- [x] Create `runs/YYYY-MM-DD/metasource_brief.md`.
- [x] Include run summary.
- [x] Include top ranked clusters.
- [x] Include possible forecastable questions.
- [x] Include missing evidence.
- [x] Include recommended next sources.
- [x] Include source failures.

## Acceptance Criteria

M1 is complete when:

- [x] A single command can run the metasource pipeline.
- [x] The command creates a dated folder under `runs/`.
- [x] `raw_items.json` is generated.
- [x] `cleaned_items.json` is generated.
- [x] `clusters.json` is generated.
- [x] `indicator_watch.json` is generated.
- [x] `metasource_brief.md` is generated.
- [x] Source failures are logged but do not crash the full run.
- [x] The daily brief is useful enough for an LLM or human to decide what to inspect next.

## Suggested First Command

The exact command can change, but aim for something like:

```bash
uv run python scripts/scan_metasources.py
```

or:

```bash
uv run python -m colombia_forecasting_desk.scan_metasources
```

Prefer the simpler option first.

## Notes

Keep this milestone intentionally simple.

The goal is not perfect news intelligence. The goal is to create the first repeatable artifact that turns scattered public sources into a structured daily brief.

Future milestones can improve classification, clustering, LLM-assisted summaries, source scoring, embeddings, scheduling, and publication workflows.
