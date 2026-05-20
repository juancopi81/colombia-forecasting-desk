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
→ write a diagnostic run trace and manifest
```

The public fetcher import path is still `colombia_forecasting_desk.fetchers`.
The implementation is staged under `colombia_forecasting_desk/source_fetching/`
so source-specific parsers can be split by source family while preserving the
daily workflow command and existing import snippets. `core.py` owns the fetch
dispatchers, `common.py` owns shared HTTP/date/anchor helpers, and parser-heavy
logic is split into source-family modules such as `dane.py`, `imprenta.py`,
`minhacienda.py`, `mincit.py`, `registries.py`, `rss.py`, and `socrata.py`.
`observability.py` owns the lightweight `run_trace.json` event schema.

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
runs/YYYY-MM-DD/m1_candidates.json
runs/YYYY-MM-DD/metasource_brief.md
runs/YYYY-MM-DD/m2_handoff.md
runs/YYYY-MM-DD/acceptance_report.json
runs/YYYY-MM-DD/source_failures.json
runs/YYYY-MM-DD/source_health.json
runs/YYYY-MM-DD/run_summary.json
runs/YYYY-MM-DD/run_trace.json
runs/YYYY-MM-DD/run_manifest.json
```

Optional output:

```text
runs/YYYY-MM-DD/ranked_signals.json
```

The main human-readable artifact is:

```text
runs/YYYY-MM-DD/metasource_brief.md
```

The main paste-ready artifact for manual M2 question selection is:

```text
runs/YYYY-MM-DD/m2_handoff.md
```

The primary structured M1-to-M2 contract is:

```text
runs/YYYY-MM-DD/m1_candidates.json
```

The diagnostic run trace for humans and AI agents is:

```text
runs/YYYY-MM-DD/run_trace.json
```

`run_trace.json` records stage/source durations, counts, metadata, and caught
source errors. It is observability only; it does not change ranking, acceptance,
or M2 selection logic.

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
remain visible after the normal freshness window. The watch starts with fourteen
must-track cards:

- IPC / inflation
- TRM / USD-COP
- policy rate + IBR
- labor market
- GDP / PIB growth
- ISE / monthly activity
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

M1.8 starts filling the watch through easy structured sources before adding
more PDF/XLSX parsers. It adds the official datos.gov.co TRM dataset as a live
structured fetch and expands the SECOP pulse from a simple count into day,
process-type, and top-entity aggregations. IBR/policy rate remains
`pending_source` because datos.gov.co currently exposes IBR as a link resource
into BanRep's statistics portal rather than a simple table/API.

M1.9 adds DANE activity indicators without starting another document-parser
round. The watch now reads the current-result HTML summaries for IPC inflation,
GEIH labor market, EMC retail sales, and EMMET manufacturing. These pages expose
the headline values directly in stable text, so M1 can surface the state
variables now while leaving category/city/subsector XLSX annex parsing for a
later deepening pass.

M1.10 hardens the watch before broadening it. Each card now carries a
`freshness_status` (`current`, `stale`, `pending`, `failed`, or `unknown`), so
old but valuable state variables remain visible instead of being silently
dropped. Bundle cards can also expose typed `components`. The construction
bundle now merges ICOCED costs with DANE headline HTML for cement production
and shipments, construction licenses, and housing finance; deeper XLSX annexes
remain optional follow-up work.

M1.11 adds the first non-DANE bundle expansion. The `energy_system` card now
uses XM's public API for electricity demand, useful reservoir volume, and
weighted national spot price. The `oil_gas_production` card now uses ANH's
official datos.gov.co Socrata mirrors for consolidated crude and gas
production, selecting the latest complete-looking period when the newest month
is only partially loaded. This gives the watch a daily operating-stress view
and a monthly hydrocarbon fiscal/external-account view without starting PDF or
spreadsheet parsing.

M1.12 completed first-pass Indicator Watch wiring for the original twelve
cards. Policy rate + IBR now comes from BanRep's SUAMECA JSON series endpoint.
External trade now reads DANE's current export/import headline HTML.
Fiscal/tax pulse now parses DIAN's official monthly tax-collection XLSX inside
its published ZIP using the standard library. This does not mean every card is
fully deepened; it means the original watch no longer had placeholder cards and
could be evaluated for which signals deserved second-pass detail.

M1.13 makes the brief more analyst-facing before starting M2. The brief now
opens with deterministic `Analyst Attention` bullets, renders the Indicator
Watch before latest event clusters, adds per-card alert lines, and turns Source
Health into an action queue. The ranker also reduces the advantage of
single-source local incident news that is merely fresh, while preserving
strategic terms such as fiscal, electoral, government, BanRep, DANE, Farc, and
ELN. Legal/listing clusters are kept internally consistent by summarizing the
same member that supplies the title, and obvious UI artifacts such as
`ui-button` are stripped during cleaning.

M1.14 prepares the manual M2 handoff. Each run now writes `m2_handoff.md`, a
shorter paste-ready artifact with the M2 task contract, indicator-led seed
questions, forecastable event signals, rejected/noisy signal notes, source
coverage caveats, and the required M2 output schema. The normal brief also
renders `M2 Seed Questions`, `Forecastable Signals`, and `Rejected / Noisy Top
Signals`. The ranker now uses a shared forecastability heuristic that rewards
official decisions, legal/regulatory/electoral signals, data releases,
resolution terms, and multi-source corroboration while penalizing
low-forecastability local, curiosity, and single-source media items. DANE
press-release PDFs get a standard-library best-effort text excerpt when
readable, Imprenta/Gaceta rows preserve document titles when the static table
exposes them, and future calendar items are retained inside the planning
window instead of being dropped as future-dated news.

M1.15 makes the M2 handoff deterministic. Each run now writes
`m1_candidates.json`, a structured candidate/rejection/source-caveat database
with stable candidate ids, deterministic question seeds, evidence links,
resolution-source hints, deadline windows, entity/topic tags, and explicit
missing evidence. The Markdown handoff remains the human-readable/pasteable
view, but it is backed by the candidate database. Each run also writes
`acceptance_report.json`; `scripts/scan_metasources.py --strict` exits nonzero
when error-level checks find malformed candidates, link-only evidence promoted
as forecastable, an otherwise nonempty run with zero candidates, or a full
multi-source run that is operationally too thin to trust. Operational hard gates
cover raw/cleaned/rankable item floors, source failure share, observed Indicator
Watch coverage, and excessive high-impact source failures. Cleaning now adds
deterministic entity/topic tags, clusters aggregate those tags, and source
health reports tag coverage and an acceptance status. MinCIT PDF attachment URLs
now enter the same fail-closed PDF enrichment path used by DANE press documents.

M1.16 starts the document-intelligence track with Senado agenda PDFs. The
fetcher now has a no-new-dependency PDF text-operator extractor for official
PDFs that expose readable text fragments, plus a Senado-specific enrichment
step that replaces generic agenda PDF links with parsed legislative agenda
entries. Those entries carry `content_extraction: senado_agenda_pdf`,
`agenda_source_url`, `agenda_window_start`, optional `scheduled_date`,
`agenda_action_type`, `project_label`, `project_records`, `document_title`,
`project_identity_status`, and follow-up source hints for Gacetas/Congreso, so
M2 can reason over named public-interest bill/action candidates instead of a
generic weekly PDF title. M1.17 hardens promotion: Senado agenda entries without
a clean project number and usable bill title are visible as research leads but
are rejected as rankable forecast candidates. The
parser remains fail-closed: if text extraction or entry extraction fails, the
item keeps `content_extraction_error` and should not be treated as parsed
document evidence.

M1.18 extends this document-intelligence track to Gacetas del Congreso. The
Imprenta table rows still start as official document links, but the fetcher now
captures each row's PrimeFaces download button and posts the JSF form for the
first recent Gaceta PDFs. A row is promoted to parsed document content only if
the downloaded PDF yields `content_extraction: gaceta_pdf_text` plus a project
label, document title, or body snippet. Rows that do not yield usable PDF text
stay link-level with `content_extraction_error`.

M1.19 adds the deterministic decision-record bridge. Raw metadata now survives
cleaning and clustering, and `decision_records.link_legislative_followups`
attaches parsed Gaceta follow-up matches to clean Senado agenda records when
project number, year, and chamber agree. M1 candidates then expose those
concrete follow-up sources instead of only generic search hints.

M1.20 pivots the legislative source hierarchy from PDF-first parsing to official
registry-first parsing. `senado_leyes_registry` posts Senado's public project
search endpoint and follows detail fragments for clean bill number, title,
status, commission, filing date, Gaceta/publication links, and filed-text links.
`camara_proyectos_ley_registry` uses the Cámara Proyectos de Ley AJAX table and
detail pages for Cámara/Senado project numbers, status, authors, commission,
legislature, object text, and publication PDFs. Agenda PDFs and Gacetas remain
useful as schedule/follow-up evidence, but they are no longer the first place
the pipeline tries to recover bill identity.

M1.21 applies the same registry-first principle to MinCIT zonas francas, but
with an important forecasting guardrail. The `mincit_zonas_francas` source now
expands the official `Zonas Francas aprobadas` PDF into structured approved-zone
rows with `registry: mincit_zonas_francas_aprobadas`, `nit`,
`zona_franca_name`, `zone_class`, `user_type`, `department`, `municipality`,
`declaratory_resolution`, `extension_resolution`, `ciiu`, `snapshot_date`, and
legal follow-up sources for MinCIT press, Diario Oficial, SUIN, and Gestor
Normativo. These registry rows are historical snapshot evidence. They become
current M1 decision signals only when `registry_changes` can compare them with
a prior structured run and detect a new row or changed resolution field.

M1.22 adds the official legal-resolution bridge. `legal_identity` normalizes
acts such as `Resolución 2118 de 2025`, `Ley 1474 de 2011`, and
`Decreto 123 de 2026` into stable kind/number/year keys. Diario Oficial now
posts the Imprenta JSF download button for recent editions, follows the embedded
PDF viewer, and reads the official PDF with `pdfplumber`. Readable editions get
`content_extraction: diario_oficial_pdf_text`; when published legal-act headings
are present, M1 emits one row per act with a semantic `#act-...` URL fragment.
Referenced legal acts remain in `referenced_legal_act_records` so citations do
not become false publication rows. Readable no-act editions are treated as
parsed but non-rankable, so source health no longer confuses "we read the PDF
and found no legal acts" with "we only saw a document link." SUIN and
Gestor-style legal rows are annotated with the same metadata when their titles
expose act identities.
`decision_records.link_official_legal_records` then attaches official
resolution matches to MinCIT approved-zone rows only when the act key matches
and the official legal source also contains MinCIT or named zone context. A
same-number resolution from an unrelated entity remains unlinked.

Reaching the Diario Oficial PDF viewer is not treated as parsed evidence by
itself. If the downloaded PDF yields no extractable text, the item keeps
`content_extraction_error` metadata and the source-health gate remains
`document_unparsed` / `document_links_only` rather than green. Parsed Diario
rows are still resolution evidence first; generic edition titles such as
`Diario Oficial 53.491 — Ordinaria` should not become standalone forecast
candidates unless a later parser emits a specific unresolved decision hook.
Likewise, act-level Diario rows such as `Diario Oficial 53.491 — Decreto 502 de
2026` are final-publication evidence and should be linked to existing leads
rather than promoted as unresolved forecasts on their own.

M1.23 closes the DANE GDP/ISE coverage gap exposed by the May 15, 2026
PIB/ISE release. The Indicator Watch now reads DANE's official PIB technical
page and ISE page directly, so headline GDP growth and monthly ISE activity are
structured cards rather than social-media-only context. The PIB card also
captures DANE's top sector drivers and both cards retain current-release
official document/annex links for M2/M3 follow-up. A strong ISE reading fires
an `activity_acceleration` M2 seed, making the follow-up question explicit
instead of relying on indirect retail/manufacturing clues.

M1.24 adds official sovereign-debt/TES coverage. `minhacienda_tes_reports`
uses the MinHacienda Investor Relations Colombia (`irc.gov.co`) COP, UVR, and
TCO auction-result document-library pages because the main MinHacienda
`Informes TES 2026` page is Radware-blocked in shell fetches. It marks
`content_extraction: minhacienda_tes_auction_pdf` only when the parser recovers
the auction date, TES type/currency, total issued, demand, bid-to-cover,
maturity rows, coupon rates when present, cutoff rates, per-maturity demand,
approved amounts, maximum cutoff rate, long-maturity cutoff rate, and source
PDF link. The parser uses `pdfplumber` for portable layout-preserving PDF text
because the no-dependency extractor drops or mangles the numeric table. IRC
returns 403 to simple shell fetches, so this source uses a Playwright browser
path; if the browser path is blocked or a PDF table cannot be parsed, the item
stays link-level with `content_extraction_error`.

M1.24 also deepens the fiscal card with official BanRep TES curve observations
from SUAMECA. The parent group page is
`informacionSerie/220002/tasas_interes_cero_cupon_tes`, but the group id is not
a data series. The wired official child series are exactly `15272` (TES pesos
1y), `15273` (TES pesos 5y), and `15274` (TES pesos 10y) through the existing
`consultaInformacionSerieXTipoDato` endpoint. Do not add non-official or
guessed TES series ids. A parsed TES auction with max cutoff rate at or above
14.0% fires a `tes_funding_cost` M2 seed resolved against the next official
MinHacienda / IRC auction-result PDF for the same auction type.

M1.25 replaces the DIAN regulatory-project scout link parser with DIAN's
official SharePoint list API for `Proyectos de normas`. The source now emits one
dated project row per draft norm, including project PDF URL, description, norm
type, issue date, comment-window start/end, mailbox, observations link, and annex
link when DIAN exposes those fields. That makes DIAN regulatory proposals
project-level M1 evidence instead of undated parser-feasibility links.

M1.26 adds a BanRep Junta browser fallback. `banrep_junta_comunicados` still
tries direct HTTP first, but if BanRep returns a Radware Bot Manager page the
fetcher opens the official Junta page with Playwright, passes the rendered HTML
through the existing dated-anchor parser, and uses the same minutas body parser
on recent minutas detail pages. This keeps normal runs fast while preventing
bot-block pages from turning BanRep policy/minutas coverage into a source
failure.

M1.27 adds a MinHacienda decree-project browser parser for
`minhacienda_proyectos_decreto`. The source still tries direct HTTP first, but
if the 2026 page returns Radware the fetcher renders the official page with
Playwright and emits one row per draft-decree project. A row is treated as
parsed content only when the parser has the draft title, publication date,
description/comment-window text, project PDF URL, and comment form URL. If any
required field is missing, the row remains link-level with
`content_extraction_error` so source health cannot look fully content-ready.

M1.28 adds a second fallback for this source. The access order is:

1. direct official HTML from `www.minhacienda.gov.co`;
2. official HTML rendered with Playwright;
3. Jina Reader markdown for the same official URL, only when Radware blocks the
   first two paths.

Rows from the reader fallback keep `official_source_url`, `reader_proxy_url`,
and `source_access=jina_reader_proxy` metadata so M2/M3 can see that the
evidence came from the official page through a reader proxy, not from direct
official transport. The parser still fails closed on incomplete rows.

M1.29 adds a guarded browser-backed parser for `registraduria_noticias`. The
source now uses Registraduría's official 2026 news archive (`/-2026-.html`),
tries direct official HTML first, and falls back to a normal Playwright render
when Cloudflare blocks shell fetches. The parser emits one row per news card with
`content_extraction=registraduria_news_card`, comunicado number, title, date,
excerpt, and article URL; the browser path also enriches the first few rows
with official article detail text when available using
`content_extraction=registraduria_news_article_html`. Live validation should
still be treated fail-closed: if a fresh browser session remains
Cloudflare-challenged, the source is a source-health failure, not evidence of no
Registraduría activity.

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
  "status": "observed | pending_source | failed",
  "frequency": "",
  "period": "",
  "release_date": "",
  "headline": "",
  "values": {},
  "freshness_status": "current | stale | pending | failed | unknown",
  "components": [
    {
      "component_id": "",
      "name": "",
      "status": "observed | pending_source | failed",
      "period": "",
      "release_date": "",
      "headline": "",
      "values": {},
      "freshness_status": "",
      "next_step": ""
    }
  ],
  "why_it_matters": "",
  "correlations": [],
  "next_step": ""
}
```

Current observed cards:

- `trm_usd_cop`: official datos.gov.co TRM rows with daily, seven-day, and
  thirty-day moves.
- `ipc_inflation`: DANE IPC headline monthly, year-to-date, annual, and largest
  division movements from the current technical page.

## M2 Review Packet Balance

`m2_review_packet.json` and `.md` are the content-first M2 review surface. The
packet is deliberately balanced: legislative records are capped, Indicator Watch
seeds get reserved space, event leads remain visible, and a few cross-impact
hypotheses can be added when existing metadata suggests that a legal decision
and an indicator should be reviewed together.

Cross-impact items are advisory only. They are not causal evidence, do not set a
probability, and should be used only to decide whether an LLM or human reviewer
should drill back into `indicator_watch.json`, `m1_candidates.json`,
`legislative_reconciler.json`, `raw_items.json`, or `cleaned_items.json`.
- `labor_market`: DANE GEIH national unemployment, participation, occupation,
  and prior-year comparisons from the current labor page.
- `gdp_growth`: DANE PIB quarterly real GDP annual growth, seasonally adjusted
  quarter-over-quarter growth, top sector drivers, and current-release
  document/annex links from the official technical page.
- `ise_activity`: DANE ISE monthly original-series index and annual growth
  from the official ISE page, plus adjusted annual growth when DANE exposes it
  in the current-result HTML and current-release document/annex links.
- `retail_sales`: DANE EMC headline real retail sales, employment, and ex-fuel
  annual changes from the current commerce page.
- `manufacturing`: DANE EMMET headline real production, real sales, and
  employment annual changes from the current manufacturing page.
- `construction_bundle`: DANE ICOCED XLSX headline total, residential, and
  non-residential cost metrics plus DANE headline HTML components for cement,
  construction licenses, and housing finance.
- `secop_procurement`: existing Socrata procurement adapters aggregated by day,
  source, process type, and top entity.
- `energy_system`: XM public API components for SIN electricity demand, useful
  reservoir volume, and weighted national spot price.
- `policy_rate_ibr`: BanRep SUAMECA latest policy rate and IBR overnight
  nominal series, including the IBR-policy spread.
- `external_trade`: DANE / DIAN headline exports and imports, including sector
  shares and same-period goods balance when available.
- `oil_gas_production`: ANH / datos.gov.co consolidated crude and fiscalized
  gas production aggregates, including top departments by volume.
- `fiscal_tax_pulse`: DIAN monthly gross tax collection by broad bucket from
  the official XLSX ZIP, MinHacienda TES auction facts from official report
  PDFs, and BanRep TES pesos 1y/5y/10y zero-coupon observations from verified
  SUAMECA child series.

All fourteen cards now have a first-pass source. The next hardening candidates
are source health thresholds, regression fixtures from live structured
endpoints, and explicit alert rules for stale critical components. The next
deepening candidates are SECOP sector fields, external-trade product/country
annexes, DIAN/Minhacienda deficit and debt-stock components, BanRep IBR term
structure, TES UVR curve coverage once official child series are verified, and
energy thermal/non-regulated/scarcity-price details.

M1.13 adds deterministic alert rendering for known high-value conditions:

- `material_move`: a large short-window market move, currently used for TRM.
- `liquidity_spread`: a large IBR-policy spread.
- `mixed_period_components`: bundle components should not be combined because
  they refer to different periods, currently used for external trade.
- `real_terms_warning`: nominal tax collection growth is below annual IPC.
- `activity_acceleration`: monthly ISE annual growth is strong enough to merit
  a next-release follow-up question.
- `cross_indicator_tension`: activity indicators disagree in a way worth
  inspection, for example strong retail sales with negative manufacturing
  sales.
- `observation_lag`: a monthly card's latest observed period is more than four
  months behind the run date.
- `stale_observation`: the card is observed but no longer fresh by its expected
  release cadence.

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

For document-intelligence sources, `parsed` should be backed by an emitted
`RawItem.metadata.content_extraction` value and tests that demonstrate both the
positive and guarded cases. For example, Senado agenda entries use
`senado_agenda_pdf`, but only entries with clean project identity metadata should
promote to M2 candidates. Gacetas rows use `gaceta_pdf_text` after a successful
official PDF download and project/title extraction; project rows use `#project`
URL fragments, title-only rows use `#title` fragments and remain parsed research
leads, while rows without a usable title remain document-link coverage debt.
The dedupe layer preserves these semantic document fragments for Imprenta row
types so multiple acts or bill items from the same PDF edition do not collapse
back into one edition-level row.

The parser metadata contract is intentionally narrow. A parser may mark an item
as parsed only by setting a stable `metadata.content_extraction` parser id, or
by setting `metadata.parsed_content` for structured records that already have a
clear source-specific shape. If a parser reaches a document or source-specific
record but cannot recover usable content, it must leave `content_extraction`
unset and write `metadata.content_extraction_error` instead. Source health,
candidate gates, Indicator Watch seeds, and M2 review packets all rely on that
fail-closed distinction.

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

## Analyst Attention

- ...

## Indicator Watch

### IPC / inflation

Status:
Freshness:
Category:
Frequency:
Period:
Latest release:
Source:

Headline:

Alerts:

Values:

Components:

Why it matters:

Useful correlations:

M1 next step:

## M2 Seed Questions

Indicator-driven question seeds with trigger, likely resolution source,
deadline/window hint, and missing evidence.

## Forecastable Signals

Event clusters that passed the deterministic forecastability filter.

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

## Emerging Questions

- ...

## Topics to Monitor

- ...

## Rejected / Noisy Top Signals

- ...

## Source Health Actions

- ...

## Source Health

| Source | Onboarding | Status | Acceptance | Content | Raw | Dated | Rankable | Tagged | Untagged | Doc links | Parsed | Failures |

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
- [x] Create `runs/YYYY-MM-DD/m2_handoff.md`.
- [x] Create `runs/YYYY-MM-DD/m1_candidates.json`.
- [x] Create `runs/YYYY-MM-DD/acceptance_report.json`.
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
- [x] `m2_handoff.md` is generated.
- [x] `m1_candidates.json` is generated.
- [x] `acceptance_report.json` is generated.
- [x] `run_trace.json` is generated.
- [x] `run_manifest.json` is generated.
- [x] Source failures are logged but do not crash the full run.
- [x] The daily brief is useful enough for an LLM or human to decide what to inspect next.
- [x] The M2 handoff is useful enough to paste into an AI for candidate question selection.
- [x] `--strict` can enforce hard M1 quality gates before M2.

## Suggested First Command

The exact command can change, but aim for something like:

```bash
uv run python scripts/scan_metasources.py
```

For behavior-preserving refactors, compare regenerated run folders with:

```bash
uv run python scripts/check_artifact_parity.py runs/YYYY-MM-DD runs/YYYY-MM-DD-candidate
```

Prefer the script entry point first; it is the path used by the daily workflow
skill and the CLI documentation.

## Notes

Keep this milestone intentionally simple.

The goal is not perfect news intelligence. The goal is to create the first repeatable artifact that turns scattered public sources into a structured daily brief.

Future milestones can improve classification, clustering, LLM-assisted summaries, source scoring, embeddings, scheduling, and publication workflows.
