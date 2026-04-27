# Colombia Forecasting Desk — Project Specification

## Purpose

Colombia Forecasting Desk is an experimental agent-assisted forecasting project focused on Colombian political, economic, regulatory, and institutional events.

The goal is to turn messy public information into explicit, source-backed probability estimates.

The first public surface is an X account. The first internal product is a lightweight research pipeline that helps identify what may be worth forecasting today.

This project should stay flexible. The initial goal is not to build a full app, dashboard, or monetized product. The initial goal is to test whether source-backed probabilistic analysis about Colombia is useful, interesting, and repeatable.

## Core Thesis

People already consume Colombian news, polls, official announcements, political narratives, and market signals, but most analysis remains qualitative.

This project explores whether an agent-assisted workflow can produce a better output:

- What changed?
- Why does it matter?
- What question does it raise?
- Can that question be forecasted?
- What evidence supports each side?
- What probability should we assign?
- What would change the estimate?

## Product Boundary

The project publishes probabilistic analysis, not personalized advice.

It should not be positioned as:

- investment advice
- betting advice
- trading signals
- automated execution
- a Polymarket bot

Prediction markets may be used as public signals of interest or market-implied probabilities, but the project should not tell users what to buy, sell, trade, or bet on.

## Initial Workflow

The project has three layers:

```text
1. Metasource pipeline
   Collects and organizes public signals.

2. Forecasting agent workflow
   Reads the organized signals, proposes questions, researches evidence, and drafts forecasts.

3. Human editorial review
   Approves, edits, rejects, or publishes outputs manually.
```

The system should avoid letting an LLM freely browse and publish. The preferred pattern is:

```text
public sources
→ cleaned items
→ clustered signals
→ daily brief
→ agent reasoning
→ evidence pack
→ forecast draft
→ human review
→ X post
→ forecast log
```

## Starting Stack

Use a simple local-first stack.

- Python
- uv
- Markdown
- YAML
- JSON / JSONL
- SQLite or DuckDB later, only if useful
- LLM usage can start manually and become API-based later

The project should avoid premature infrastructure. Build scripts and artifacts first. Add databases, dashboards, APIs, or scheduled jobs only when they solve real friction.

## Suggested Project Structure

```text
colombia-forecasting-desk/
  PROJECT_SPEC.md
  README.md

  config/
    metasources.yaml
    agent_policy.yaml

  prompts/
    daily_scout.md
    question_selection.md
    evidence_pack.md
    forecast_draft.md
    x_post.md

  scripts/
    scan_metasources.py
    clean_items.py
    cluster_signals.py
    build_daily_brief.py
    build_evidence_pack.py
    draft_forecast.py

  data/
    raw/
    cleaned/

  runs/
    YYYY-MM-DD/
      raw_items.json
      cleaned_items.json
      clusters.json
      metasource_brief.md
      candidate_questions.md
      evidence_packs/
      forecast_drafts/
      human_decisions.md

  forecasts/
    forecast_log.jsonl
    resolved/
```

This structure is provisional. It can change as the workflow becomes clearer.

## Key Concepts

### Metasource

A metasource helps detect what may be important today.

Examples:

- news pages
- RSS feeds
- official update pages
- polling pages
- public market pages
- government calendars
- curated X lists
- public datasets

Metasources answer:

```text
What should we look into?
```

### Source

A source provides evidence for or against a specific forecast.

Examples:

- DANE publication
- BanRep statement
- Congreso agenda
- Corte Constitucional decision
- Registraduría release
- poll methodology PDF
- primary interview
- official calendar

Sources answer:

```text
What evidence supports or contradicts the forecast?
```

### Daily Brief

A daily brief is the main output of the metasource pipeline.

It should summarize:

- sources checked
- source failures
- important signals
- topic clusters
- possible forecastable questions
- missing evidence
- suggested next sources
- noisy or low-confidence items

The brief is not a forecast. It is input for the next step.

### Evidence Pack

An evidence pack is the working document for one forecast question.

It should contain:

- forecast question
- resolution criteria
- deadline
- relevant evidence
- source summaries
- conflicting information
- missing evidence
- prior forecast, if any

The evidence pack should be readable by both a human and an LLM.

### Forecast

A forecast is a timestamped probability estimate.

Each forecast should eventually track:

- question
- probability
- timestamp
- confidence
- evidence used
- post draft
- final published post, if any
- resolution criteria
- final outcome
- notes after resolution

## Source Selection Principles

Initial sources should be selected using these criteria:

- accessible without paywall or login
- relevant to Colombia
- reliable for their intended role
- useful for detecting important questions
- technically feasible to fetch or inspect
- likely to surface forecastable events
- not overwhelmingly noisy

A source does not need to be perfect. It needs to have a clear role.

Useful source roles include:

- official signal
- media signal
- polling signal
- market signal
- agenda signal
- narrative signal
- resolution source

## Development Principles

Keep the system lightweight.

Prefer:

- simple scripts over services
- files over databases
- manual review over full automation
- narrow source sets over broad noisy crawling
- repeatable artifacts over hidden agent behavior
- clear logs over complex orchestration

Avoid:

- autonomous publishing at the beginning
- scraping paywalled content
- overfitting to one news cycle
- pretending weak social signals are strong evidence
- building a dashboard before the workflow is useful
- making the project dependent on any single platform

## Milestones

### M0 — Project Setup

- [x] Initialize Python project with `uv`.
- [x] Add this `PROJECT_SPEC.md`.
- [ ] Create initial folder structure.
- [ ] Create initial `config/metasources.yaml`.
- [ ] Create initial prompt files as placeholders.
- [ ] Create forecast log placeholder.

### M1 — Metasource Pipeline

Goal: produce a useful daily brief from public metasources.

- [ ] Define 5–10 initial metasources.
- [ ] Fetch raw items from accessible sources.
- [ ] Clean and normalize items.
- [ ] Filter irrelevant or low-quality items.
- [ ] Deduplicate obvious duplicates.
- [ ] Cluster related signals.
- [ ] Generate `runs/YYYY-MM-DD/metasource_brief.md`.

### M2 — Question Discovery

Goal: identify potentially forecastable questions from the daily brief.

- [ ] Read daily brief.
- [ ] Generate candidate questions.
- [ ] Score questions by interest, forecastability, evidence availability, freshness, and risk.
- [ ] Select a small number of questions for deeper research.
- [ ] Save candidate and selected questions.

### M3 — Evidence Pack and Forecast Draft

Goal: produce one human-reviewable forecast draft.

- [ ] Build an evidence pack for one selected question.
- [ ] Ask the LLM for probability, reasoning, uncertainty, and counterarguments.
- [ ] Generate a draft X post.
- [ ] Save the draft and supporting evidence.
- [ ] Record human decision: publish, edit, monitor, discard, or research more.

### M4 — Public X Experiment

Goal: test whether people care.

- [ ] Create or choose X account.
- [ ] Publish a simple intro post.
- [ ] Publish 3–5 source-backed forecast posts or updates.
- [ ] Track replies, reposts, likes, bookmarks, and inbound suggestions.
- [ ] Note which topics and formats get the strongest response.

### M5 — Review and Pivot Decision

Goal: decide what this project wants to become next.

- [ ] Review workflow friction.
- [ ] Review source quality.
- [ ] Review public response.
- [ ] Review legal/reputational comfort.
- [ ] Decide whether to continue, narrow, automate, pivot, or pause.

Possible next directions:

- X-only forecasting account
- paid newsletter
- Telegram alert product
- internal research assistant
- B2B political/regulatory intelligence product
- broader LATAM forecasting desk
- discontinued experiment

## Success Criteria

The early project is successful if it can repeatedly answer:

- What happened today?
- What seems important?
- What is noise?
- What could become a forecastable question?
- What evidence is missing?
- What sources should be checked next?
- What probability should be assigned after research?
- Is the resulting public post useful enough to publish?

The first version does not need to be complete. It only needs to produce useful artifacts that make the next decision easier.

## North Star

Build a lightweight agent-assisted research desk that watches Colombia, identifies important forecastable questions, gathers evidence, estimates probabilities, and creates transparent public analysis with a track record.
