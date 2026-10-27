# Make the desk useful — merged implementation plan

*Written 2026-10-27. Status: proposed; nothing implemented yet.*
*Revision 1 (2026-10-27) adds six corrections from Codex's review; see the [Revision log](#revision-log).*
*Audience: AI coding sessions (Claude Code or Codex) and Juan.*

## How to use this file

- Each workstream (A–G) is written so it can be handed to one AI session as its brief.
- **Workstream 0 comes first.** It's a short step that freezes the shared schemas
  and plain-language rules, so parallel sessions build against the same shapes.
- **Then A, B, C, D, E and F can run at the same time.** They own different files
  (see [Who owns which files](#who-owns-which-files)). **G runs last**, because it
  wires everything into the daily routine.
- Every session works on its own branch and commits there. It does not push or open
  a PR unless Juan asks.
- **Step 0** is a five-minute change for this Friday's BanRep decision. It does not
  wait for anything else.

## Why

Two reviews on 2026-10-27 reached the same conclusion:

- Across 130 daily runs the desk collected and verified a lot.
- Its last selected forecast was June 29, and 48 of the 56 daily analyses since
  August 11 abstained.
- The daily report is a process log (codes, counts, validator results) that Juan
  can't use.
- Key monetary and jobs feeds have been failing for weeks.
- SECOP is fetched like a 30-row news ticker, so it can never show who keeps
  winning contracts.

The fix is mostly on the output side:

- say what changed in plain words
- make routine calls on scheduled events
- remember stories and entities over time
- summarize the week

Collection and sourcing safeguards stay as they are.

Sources: `reviews/2026-10-27-project-review.html` (Codex review) and the Claude
Code review session of 2026-10-27 (summarized in this file).

## What Juan gets when this is done

1. **Daily brief (≤250 words).** It is the automation's final message and the top
   of `review.html`. It answers:
   - Does anything need you?
   - What changed (at most 3 plain items)?
   - What's coming up, with our call?
   - Which forecasts are open?
   - Any data problems (only if they matter)?

   A quiet day is one line: "Nothing needs you today."
2. **Routine forecasts on scheduled events.** Every BanRep rate decision, and every
   DANE release on inflation, activity and unemployment, gets a probability 1–7
   days ahead. Each one resolves automatically from official data. Skipping a call
   needs a written reason, and skips are counted.
3. **Running stories.** About eight case files, each with:
   - a plain description
   - the current view
   - what changed
   - the next trigger

   Bill numbers always come with a plain description.
4. **Entity memory and a personal watchlist.** Juan lists companies, public
   entities, places and topics in `config/watchlist.yaml`. The desk keeps their
   history and flags "patterns worth a look". History covers SECOP contracts over
   24 months, sanctions, zonas francas, decrees and bills.
5. **Friday weekly brief (≤450 words).** One page:
   - the big picture
   - the top 3 stories
   - our calls and scoreboard
   - next week's calendar
   - patterns worth a look
   - one question for Juan
6. **A usefulness check after 10 runs.** Did any brief change what Juan believes,
   plans or checks?

## Ground rules (every workstream)

- **Additive and reversible.**
  - Prefer new files.
  - Behavior that changes daily output stays opt-in until G wires it.
  - Re-rendering a historical run must stay byte-identical; check with
    `scripts/check_artifact_parity.py`.
- **Ledgers are protected.**
  - Never delete or reorder rows in `forecasts/forecast_log.jsonl` or
    `forecasts/shadow_forecast_log.jsonl`.
  - Prediction fields never change after creation: question, probability,
    baseline, criteria, deadlines, `created_at`, rationale and evidence.
  - Only the resolver scripts may update an existing row, and only its resolution
    fields: status, outcome, resolution value and source, `resolved_at`, scores,
    and the `needs_review` flag (workstream B). This matches how
    `resolve_shadow_forecast` already works: it rewrites the ledger atomically
    with the resolved row.
  - Missing official records never resolve a forecast as NO.
- **Deterministic where possible.**
  - Collection, matching, dates, arithmetic, validation and rendering stay pure
    Python, with no LLM and no network in renderers or validators.
  - The LLM writes only narrative text, probabilities and judgments, into JSON
    that a validator checks.
- **Plain language is enforced, not hoped for.** Reader-facing artifacts are
  validated for word caps, required sections and banned internal codes (list in
  workstream A).
- **Sources.**
  - Use official and open sources only.
  - Never disable TLS verification.
  - Prefer official alternative endpoints when a page blocks automated access.
  - Do not add CAPTCHA solving, stealth plugins or fingerprint spoofing.
- **People and accusations.**
  - Entity outputs aggregate natural persons (cédula) by default instead of
    listing their names.
  - A pattern is a prompt for review, never a claim of wrongdoing.
  - Say plainly that direct contracting is legal for many contract types.
- **No advice.** Outputs may say what an outcome usually means for the peso,
  interest rates or bond yields. They never tell Juan to buy, sell or bet.
- **Tests.**
  - New tests use fixtures, with no network.
  - `./.venv/bin/python -m pytest -q` must pass before committing.
  - Don't edit existing golden fixtures unless your workstream owns them.
- **Tooling.** Use `./.venv/bin/python`. `httpx` and `PyYAML` are already
  dependencies. Add no new runtime dependency without a strong reason.

## Step 0 — Friday's BanRep call (today, no code)

BanRep's board decides on **Friday 2026-10-30**; the policy rate is now 12.25%. The
existing shadow lane already allows one forecast per run if it has:

- an exact question
- an official resolver
- a deadline
- YES/NO criteria
- a named baseline

Steps:

1. Add one dated instruction to the Codex automation prompt
   (`~/.codex/automations/colombia-forecasting-weekday-review/automation.toml`):

   > On the 2026-10-28 or 2026-10-29 run, record a shadow forecast for: "Will
   > Banco de la República set its policy rate above 12.25% at the October 30,
   > 2026 decision?" Use the analyst-survey median as the baseline if available;
   > otherwise "same direction as the last decision", or an explicitly
   > uninformative 50/50. If you still abstain, give the reason in one sentence at
   > the top of the report.

2. Remove the instruction after Friday.

Before forecasting, the run confirms the current rate from BanRep's official
communiqué for the September 30 decision, because the policy-rate feed is down.

Step 0 depends on neither that feed nor the preflight artifact. Today's preflight
has no BanRep slot, because the BanRep detector skips silently when no current
policy rate is observed. **Don't let workstreams B or C hold up Friday's call.**

This needs Juan's approval, because the file is outside the repo.

## Shared interfaces

These shapes let the workstreams build in parallel against fixtures. Workstream 0
freezes them as validators plus example fixtures before A–F start. Field names are
binding; extra optional fields are allowed. Values below are illustrative.

`runs/<date>/reader_brief.json` — A defines and validates it; the LLM writes it once G wires it in.

```json
{
  "schema_version": "reader_brief.v1",
  "run_date": "2026-10-27",
  "needs_you": {"required": false, "action": null, "by": null},
  "headline": "Friday is the big day: interest rates and unemployment.",
  "changes": [
    {"title": "Tobacco and vape tax bill moves toward a final Senate vote",
     "what_happened": "...", "why_it_matters": "...", "who_is_affected": "...",
     "confidence": "confirmed", "source_url": "https://...", "case_id": "tobacco-nicotine-tax"}
  ],
  "coming_up": [
    {"date": "2026-10-30", "event": "BanRep interest-rate decision",
     "our_call": {"probability": 0.5, "outcome": "rate set above 12.25%", "forecast_id": "..."},
     "no_call_reason": null}
  ],
  "open_forecasts": [
    {"forecast_id": "...", "plain_question": "...", "probability": 0.55, "resolves_by": "2026-10-30"}
  ],
  "data_problems": ["BanRep policy-rate feed down since Sep 11"]
}
```

`runs/<date>/family_forecast_slots.json` — B, deterministic.

```json
{"schema_version": "family_forecast_slots.v1", "run_date": "2026-10-27",
 "slots": [{"slot_id": "banrep_rate_decision:2026-10-30", "family_id": "banrep_rate_decision",
   "question": "...", "resolution_source": "https://...", "resolution_criteria": "YES: ... NO: ...",
   "resolution_deadline": "2026-10-30", "resolution_check_window_end": "2026-11-02",
   "baseline": {"label": "...", "probability": 0.5, "rationale": "..."},
   "event_date": "2026-10-30", "days_until_event": 3, "enabled": true}]}
```

`runs/<date>/family_forecasts.json` — B validates it; the LLM fills it once G wires it in.

```json
{"schema_version": "family_forecasts.v1", "run_date": "2026-10-27",
 "entries": [{"slot_id": "banrep_rate_decision:2026-10-30", "decision": "forecast",
   "probability": 0.5, "rationale_for": ["..."], "rationale_against": ["..."],
   "falsifier": "...", "skip_reason": null,
   "evidence_refs": [{"artifact": "indicator_watch.json", "locator": "indicator_id=ipc_inflation"}]}]}
```

`forecasts/scoreboard.json` and `.md` — B, deterministic. They cover every public,
shadow and family forecast:

- open forecasts
- resolved forecasts with outcomes
- right-side rate
- mean Brier score against the baseline (descriptive only, always with n)
- per family: events, calls, skips and missed events

`cases/<case_id>.yaml` — D; schema in workstream D.

`runs/<date>/case_updates.json` — D, deterministic. It holds candidate evidence for
each case. The LLM writes the update text into the case file.

`runs/<date>/watchlist_hits.json` and `.md` — E, deterministic.

```json
{"schema_version": "watchlist_hits.v1", "run_date": "2026-10-27",
 "hits": [{"watch_id": "...", "kind": "company", "source_id": "secop_ii_contratos",
   "title": "...", "url": "https://...", "matched_on": "nit", "summary": "..."}],
 "patterns": [{"pattern_id": "...", "watch_id": "...", "kind": "supplier_share",
   "plain_text": "...", "numbers": {"share": 0.31, "contracts": 120, "window_months": 24},
   "evidence_urls": ["https://..."], "caveat": "Direct contracting is legal for many contract types; this is a prompt for review."}]}
```

`kind` values: `company | public_entity | place | topic | bill | decree`.
Pattern kinds: `supplier_share | repeat_direct_award | large_addition | sanctioned_supplier`.

Weekly files, all owned by F:

- `weekly/<YYYY-Www>/weekly_facts.json` (deterministic)
- `weekly_brief.json` (LLM-written, validated)
- `weekly_brief.md`
- `weekly.html`

## Workstream 0 — Shared foundations (first, short)

**Goal:** freeze the shapes and rules that several workstreams share, so parallel
sessions don't drift apart.

**Owns:**

- new `colombia_forecasting_desk/plain_language.py`, holding the reader-text rules
  listed in workstream A, step 1
- new `colombia_forecasting_desk/schemas/`, with one small validator per shared
  artifact:
  - `reader_brief.v1`
  - `family_forecast_slots.v1`
  - `family_forecasts.v1`
  - `scoreboard.v1`
  - `case_file.v1`
  - `watchlist_hits.v1`
  - `weekly_facts.v1`
  - `weekly_brief.v1`
- new `tests/fixtures/shared_interfaces/`, with one valid and one invalid example
  per schema
- new `tests/test_shared_interfaces.py`

**Build:** validators only. No renderers and no pipeline changes. Encode the
fields from [Shared interfaces](#shared-interfaces) and the case-file schema in
workstream D.

**Done when:**

- Each schema accepts its valid fixture and rejects its invalid one.
- The full suite passes.
- This work is merged to `main` before A–F branch off.

From then on, A–F import these modules instead of defining their own copies.

## Workstream A — Daily reader brief and a readable review page

**Goal:** a validated plain-language brief of at most 250 words. It leads the daily
review and becomes the automation's report.

**Owns:**

- new `colombia_forecasting_desk/reader_brief.py`, importing its schema and
  `plain_language.py` from Workstream 0
- new `scripts/finalize_reader_brief.py`
- new `prompts/reader_brief.md`
- new `docs/READER_BRIEF_CONTRACT.md`
- new `tests/test_reader_brief.py`
- edits to `colombia_forecasting_desk/review_html.py` and `tests/test_review_html.py`

**Build:**

1. The reader-text rules below are implemented once, in `plain_language.py`, by
   Workstream 0. A, D and F import them.
   - **Banned internal tokens.** Case-insensitive, matched at word boundaries:
     `M1`, `M2`, `M3`, `insight_only`, `shadow_track`, `monitor_no_new_m3`,
     `research_more`, `promote_to_m3`, `packet`, `lane`, `bundle`, `preflight`,
     `tension card`, `disposition`.
   - **Codes need words.** A bill or decree code (`PL ?\d+`, `PAL ?\d+`,
     `Decreto ?\d+`, `Resolución ?\d+`) must sit in a field that also has at least
     8 other words describing it.
   - **No squashed text.** Reject any token of 30 or more characters without
     spaces, except URLs.
   - **Word counting.** Count words in rendered output, not in the JSON.
2. Schema `reader_brief.v1`, with a validator that enforces:
   - rendered brief ≤250 words
   - `changes` ≤3, `coming_up` ≤5, `data_problems` ≤2
   - every `coming_up` item has either `our_call` or a one-sentence `no_call_reason`
   - every `source_url` uses https
3. A Markdown renderer writes `runs/<date>/reader_brief.md`. This exact text is
   what the automation sends Juan.
4. `scripts/finalize_reader_brief.py --date YYYY-MM-DD` validates the brief,
   renders the Markdown and re-renders `review.html`. It never touches the ledgers.
5. Change `review.html` only when `reader_brief.json` exists, so historical runs
   render byte-identical:
   - The brief becomes the first section.
   - The operational sections move into collapsed `<details>` blocks below the
     brief: sampling lanes, bundles, tension cards, source health, preflight and
     market rows.
   - Fix the horizontal overflow: the Oct 27 page rendered about 2,938 px wide in
     a 1,280 px window. Use `overflow-wrap: anywhere` on text, put tables inside
     `overflow-x: auto` containers, and cap widths.
6. `prompts/reader_brief.md` tells the LLM how to write the brief from:
   - `agent_analysis.json`
   - family slots and family forecasts
   - case updates
   - watchlist hits
   - the scoreboard

   Its style target is the daily example in the appendix. Rules:
   - plain names first, codes in parentheses
   - say who is affected
   - no process narration

**Done when:**

- Validator tests cover passing and failing cases.
- A golden render of the appendix example exists.
- `review_html` tests show unchanged output without a brief, and brief-first
  output with one.
- A hand-written brief for `runs/2026-10-27` renders with no horizontal scroll at
  1,280 px.
- The full suite passes.

## Workstream B — Forecast families, automatic resolution, scoreboard

**Goal:** routine, cheap, scored forecasts on scheduled official events. Making the
call becomes the default instead of a daily debate.

**Owns:**

- new files:
  - `config/forecast_families.yaml`
  - `colombia_forecasting_desk/forecast_families.py`
  - `scripts/plan_family_forecasts.py`
  - `scripts/append_family_forecasts.py`
  - `scripts/resolve_family_forecasts.py`
  - `scripts/build_scoreboard.py`
  - `docs/FORECAST_FAMILIES.md`
  - tests
- edits to:
  - `colombia_forecasting_desk/shadow_forecasts.py`
  - `scripts/resolve_shadow_forecast.py`
  - `scripts/validate_shadow_forecasts.py`
  - `config/agent_policy.yaml`
- append-only writes to `forecasts/`

**Families (v1):**

| family_id | Event clock | Question template | Official resolver | Baseline |
|---|---|---|---|---|
| `banrep_rate_decision` | BanRep events in `m3_preflight_opportunities.json` (from `banrep_junta_calendar`) | Will BanRep set its policy rate above {current}% at the {date} decision? | BanRep board communiqué (`banrep_rss`), cross-checked with the `policy_rate_ibr` card | Rule (a), else (b), else (c) |
| `ipc_annual_up` | DANE calendar IPC releases | Will DANE report annual inflation for {month} above {previous}%? | DANE IPC (`ipc_inflation` card) | Rule (b), else (c) |
| `ise_annual_up` | DANE calendar ISE releases | Will DANE report annual ISE growth for {month} above {previous}%? | DANE ISE (`ise_activity` card) | Rule (b), else (c) |
| `unemployment_down_yoy` | DANE calendar GEIH releases | Will the national unemployment rate for {month} be below {same month last year}%? | DANE GEIH (`labor_market` card) | Rule (b), else (c). **Disabled until C makes `labor_market` observable.** |

**Baseline probability rules.** Every baseline must be a probability produced by a
documented rule.

- **(a) Survey share.** Use the share of surveyed analysts expecting the YES
  outcome, but only when the published survey reports the distribution of answers.
  A median or mean rate is a point estimate, never a probability.
- **(b) Empirical frequency.** Use how often the YES outcome happened in the
  official historical series over a stated window. For IPC, that is how often
  annual inflation rose from one month to the next. For BanRep, it is how often a
  decision moved in the same direction as the decision before it.
- **(c) Fallback.** Otherwise, use an explicitly labeled, uninformative 50/50.

Each slot records the rule used, its inputs and the window.

**BanRep dependency.** The planner can create a BanRep slot only when the current
policy rate is available. Today the BanRep preflight detector skips silently
without it. The rate must come either from C's repaired `policy_rate_ibr` card or
from a verified official fallback (the latest board communiqué) that records its
provenance. When neither is available, the planner says so explicitly instead of
skipping silently.

**Build:**

1. Write `config/forecast_families.yaml` from the table, with templates, resolver
   ids, baseline rules, a forecast window of 1–7 days before the event, and an
   `enabled` flag per family.
2. **Planner** (deterministic): reads `m3_preflight_opportunities.json` and
   `indicator_watch.json` and writes `runs/<date>/family_forecast_slots.json`.
   - Each slot is fully specified: question, criteria, resolver URL, deadline,
     check window and baseline.
   - There is one slot per family event, made idempotent across days by `slot_id`.
3. **Append:** validates `runs/<date>/family_forecasts.json`.
   - Each `forecast` entry goes into `forecasts/shadow_forecast_log.jsonl` as a
     normal shadow row, plus `forecast_family` and `slot_id`. The current
     validator already allows extra fields.
   - Each `skip` goes into `forecasts/family_skips.jsonl`, with its reason.
   - Re-running is idempotent.
4. **Policy** (`config/agent_policy.yaml`):
   - Keep `max_forecasts_per_run: 1` for discretionary shadows.
   - Add `family_forecasts: {required_for_due_slots: true, skip_requires_reason: true}`.
   - Document the rule in `docs/FORECAST_FAMILIES.md`.
5. **Resolver** (deterministic): for open family rows past their deadline, it reads
   the official value from run artifacts (the `indicator_watch.json` card values
   and their official URLs).
   - **Match precisely.** Match on all of these before resolving:
     - the exact event (date and release)
     - the reference period (the month)
     - the series variant (original or seasonally adjusted; headline or core)
   - **Score on the first official release.** Store that value; later revisions
     never change a score.
   - **Only pre-event forecasts count.** The append step rejects any forecast
     whose `created_at` is not before the event's scheduled release time.
   - It resolves through the same code path as `scripts/resolve_shadow_forecast.py`:
     YES or NO, the resolution value, the official source and the artifact path.
   - If the official value hasn't been captured yet, the row stays open.
6. **Review flag, never automatic voiding.**
   - When `resolution_check_window_end` + 21 days pass without a captured official
     value, the resolver sets `needs_review: true` on the row. This is a
     resolution field.
   - It never voids or resolves on its own: a broken feed doesn't make a forecast
     invalid.
   - A human-approved command can then resolve the row from the official source.
     Or it can void the row with a written reason:
     `resolve_shadow_forecast.py --void --reason "..."`.
   - Void rows are unscored and listed separately.
   - The scoreboard and both briefs list forecasts that are waiting for official
     records, so cases like PL012 don't sit unnoticed.
7. **Scoreboard** (deterministic): writes `forecasts/scoreboard.json` and `.md`
   with the contents listed under Shared interfaces. Public and internal forecasts
   stay distinct.

**Done when:**

- Tests cover:
  - the planner: fixture preflight plus indicator cards produce the expected slots
  - append idempotency
  - skip logging
  - the resolver: YES, NO and leave-open cases
  - the void rules
  - the scoreboard math
- Existing shadow tests still pass.
- Running the planner on `runs/2026-10-27` yields:
  - a GEIH slot marked disabled
  - an explicit "BanRep slot unavailable: no current policy rate" notice
- A fixture with a verified current rate yields a BanRep slot for 2026-10-30.
- Tests cover the baseline rules, the precise matching and first-release scoring,
  rejection of forecasts created after an event, and the `needs_review` flag.

## Workstream C — Repair the monetary and jobs feeds

**Goal:** the three inputs that matter most for calendar forecasts work again.

**Owns:** for **these three indicator cards only**, the source-fetching modules,
their `config/metasources.yaml` entries, the `indicator_watch.py` code paths, and
their fixtures and tests:

| Card or source | Problem |
|---|---|
| `policy_rate_ibr` | Failing on every run since 2026-09-11 (TLS error) |
| `banrep_eme_expectations` | Failed on 52 of 55 runs since it was added on 2026-08-12; last success 2026-08-24 |
| `labor_market` | GEIH parse failures through October |

**Build:** for each problem, reproduce it, find the root cause, fix it and add a
regression fixture.

- **TLS:** fix the trust chain (current `certifi`, a missing intermediate
  certificate, or an official alternate host). Never use `verify=False`.
- **Expectations survey:** if the page blocks automated access, use an official
  alternative: the published XLSX or PDF, BanRep's statistics or series endpoints,
  or a datos.gov.co mirror. Record provenance.
- **GEIH:** update the parser to DANE's current page and annex layout.
- **If a feed can't be fixed:** leave it failing with a precise reason and
  document a fallback in `docs/`. B's baselines degrade gracefully.

**Done when:**

- Each card is `observed` on a live strict scan
  (`./.venv/bin/python scripts/scan_metasources.py --date <today> --strict --source-report`),
  or has a documented, specific failure reason.
- The fixed parsers have fixtures and tests.
- Indicator golden fixtures changed only for these cards.
- The full suite passes.

## Workstream D — Running stories (case files)

**Goal:** stories carry forward instead of restarting from each day's snapshot, and
every story has a plain description.

**Owns:**

- new `cases/` folder (tracked in git)
- new `colombia_forecasting_desk/cases.py`
- new `scripts/update_cases.py`
- new `scripts/validate_cases.py`
- new `scripts/snapshot_cases.py`
- new `docs/CASE_FILE_CONTRACT.md`
- tests

**Case file (`case_file.v1`, YAML):**

```yaml
schema_version: case_file.v1
case_id: tobacco-nicotine-tax
plain_title: Tobacco and vape tax bill
plain_summary: >-          # ≤60 words; any code explained in words
  ...
why_it_matters: ...
who_is_affected: ...
identifiers:
  bills: ["PL 243/2026 Senado", "PL 136/2025 Cámara", "PL 290/2025 Cámara"]
  decrees: []
  indicator_ids: []
  nits: []
  keywords: ["tabaco", "nicotina", "vapeador"]
current_view: {date: 2026-10-27, text: "...", confidence: likely}   # text ≤80 words
next_trigger: {date: null, event: "The Senate schedules the final vote"}
forecast_ids: []
status: active             # active | parked | closed
park_after_days: 21
updates:
  - {date: 2026-10-27, change: "...", source_url: "https://..."}
```

**Build:**

1. Schema and validator in `scripts/validate_cases.py`, using the
   `plain_language.py` rules from A.
2. `scripts/update_cases.py --date YYYY-MM-DD`, a deterministic matcher:
   - It searches the day's run artifacts by identifier and keyword:
     `legislative_reconciler.json`, `cleaned_items.json`, `indicator_watch.json`,
     `analyst_leads.json` and `m2_review_packet.json`.
   - It writes `runs/<date>/case_updates.json` (candidate evidence with refs). It
     never edits case files itself.
   - It suggests `parked` when nothing has matched for `park_after_days`.
3. Workflow: the LLM edits `cases/*.yaml` directly from the candidates, then
   `validate_cases.py` must pass. G wires this in.
4. `scripts/snapshot_cases.py --date YYYY-MM-DD` copies the validated case files
   into `runs/<date>/cases_snapshot/`. That gives every day a frozen record, so
   later knowledge can't leak into an earlier week's brief.
4. Seed eight cases from existing run artifacts, with plain summaries and source
   URLs:
   1. Interest rates and inflation (BanRep, IPC)
   2. The economy is slowing (ISE, manufacturing, retail, trade)
   3. The 2026 spending cut and the 2027 budget by decree (Decreto 1585/2026, Decreto 1599/2026)
   4. Tobacco and vape tax bill (PL 243/2026 Senado, from PL 136/2025 and PL 290/2025 Cámara)
   5. Repeal of the ultra-processed food tax (PL 012/2026 Cámara)
   6. Secrecy for security-force purchases (PL 433/2026 Cámara with PL 296/2026 Senado)
   7. Gas and electricity supply (gas-rationing resolution, spot prices, reservoirs)
   8. Pension funds' overseas-investment rules (Decreto 1490/2026)

**Done when:**

- Tests cover the schema, the matcher and the park logic.
- All eight seeds validate.
- `update_cases.py --date 2026-10-27` produces sensible candidates for at least
  the tobacco and food-tax cases.

## Workstream E — Entity memory and personal watchlist (SECOP first, not SECOP only)

**Goal:** remember companies, public entities and places over time and across
sources, and flag patterns worth a look. A search box can't do this part.

**Owns:**

- new `config/watchlist.yaml`
- new `colombia_forecasting_desk/entities.py`
- new `colombia_forecasting_desk/entity_history.py`
- new `scripts/build_entity_history.py`
- new `scripts/watchlist_hits.py`
- new `docs/ENTITY_MEMORY.md`
- tests
- a cache in `data/entity_history/`; add an ignore line to `.gitignore`

This replaces the earlier "SECOP supplier-history prototype" proposal.

**Build:**

1. **`config/watchlist.yaml`.** Each entry has:
   - `watch_id`
   - `kind`: `company | public_entity | place | topic | bill | decree`
   - identifiers: NIT, exact names, DIVIPOLA code, keywords, bill numbers
   - a `why` note

   Ship it with commented examples and two active demo entries: one departmental
   government and one large company with public contracts. Juan replaces them.
   Private individuals are not a watch kind.
2. **Normalization (`entities.py`):**
   - NIT cleanup: strip dots and dashes, separate the check digit.
   - Names: case, accents, legal suffixes such as S.A.S.
   - Places: municipality and department codes from DANE's official DIVIPOLA list.
3. **History (`entity_history.py`).** Use datos.gov.co Socrata aggregate queries
   (`$select`/`$where`/`$group`/`$order`) and reuse the HTTP helper in
   `source_fetching/socrata.py`. **Do not change the daily scan.** Datasets:

   | Dataset | Socrata id | Note |
   |---|---|---|
   | SECOP II contratos | `jbjy-vk9h` | |
   | SECOP II adiciones | `cb9c-h8sn` | |
   | SECOP I procesos | `f789-7hwg` | |
   | multas y sanciones | `4n4q-k399` | Query directly; leave it disabled in the daily scan |

   The default window is 24 months. Cache results with their fetch time.

   A live check on 2026-10-27 confirmed this works: one grouped query counted
   SECOP II contracts signed in Jan–Sep 2026 by method. About 690K of roughly
   860K were direct awards.
4. **Pattern screens** (deterministic; thresholds in config):
   - a supplier's share of an entity's contracts or value above a threshold
   - repeated direct awards to the same company
   - additions that are large relative to the original value
   - sanctioned suppliers that keep contracting

   Report medians as well as sums. SECOP has large value outliers, so flag them.
   Natural persons are aggregated by default.
5. **`scripts/watchlist_hits.py --date YYYY-MM-DD`** matches the day's artifacts to
   watchlist entries by NIT, name, DIVIPOLA code or place name, and keyword:
   - SECOP rows
   - MinCIT zonas francas rows
   - Diario Oficial and Gaceta items
   - MinHacienda decrees
   - legislative records

   It writes `runs/<date>/watchlist_hits.json` and `.md`, including the patterns
   from step 4.
6. **Zonas francas.** Include MinCIT's approved-zones rows in place matching. The
   existing zona-franca screen has never surfaced an analyst lead; find out why
   and document it.

**Done when:**

- Offline tests use synthetic fixtures with no real personal names.
- A live demo for the two demo entries writes readable reports to
  `reviews/entity-<watch_id>-<date>.md`. Do not commit them.
- `watchlist_hits.py --date 2026-10-27` runs.
- The full suite passes.

## Workstream F — Friday weekly brief

**Goal:** one page Juan can read in about three minutes every Friday.

**Owns:**

- new `colombia_forecasting_desk/weekly_brief.py`
- new `scripts/build_weekly_facts.py`
- new `scripts/finalize_weekly_brief.py`
- new `prompts/weekly_brief.md`
- new `docs/WEEKLY_BRIEF_CONTRACT.md`
- tests
- the `weekly/` output folder; add `weekly/*/` to `.gitignore`

**Build:**

1. **`scripts/build_weekly_facts.py --week 2026-W43`** (deterministic). It
   collects:
   - the week's indicator changes, from each run's `indicator_watch.json`
   - forecasts made, resolved and skipped (ledgers and `family_skips.jsonl`)
   - case updates (`cases/`)
   - watchlist hits and patterns
   - the next 14 days of events (preflight plus family slots)
   - data problems that affect decisions

   If an input is missing, its section is left out; that is not an error.

   No hindsight: use only artifacts dated on or before the week's last run, and
   read cases from that run's `cases_snapshot/`, never from the live `cases/`
   folder.
2. **`weekly_brief.v1`**, LLM-written JSON with a validator. Sections:
   - *Big picture*: at most 2 sentences
   - *Top stories*: at most 3, each with what happened, why it matters and who is affected
   - *Our calls*: the scoreboard line plus this week's calls
   - *Next week*: date, event, and our call or the reason there isn't one
   - *Patterns worth a look*: at most 3, each with its caveat
   - *Data problems*: at most 2 lines
   - *One question for you*

   Cap it at 450 words, with the same `plain_language.py` rules as A.
3. **Renderers:** `weekly_brief.md` and a single responsive `weekly.html`. It
   needs light and dark modes, no horizontal scroll, and must read well on a
   phone.
4. **Usefulness log.** Each weekly brief ends with one question. When Juan answers
   (in chat or in a file), the next run appends the answer to
   `reviews/usefulness_log.md` with the date. G's checklist reviews it after 10 runs.

**Done when:**

- Validator and renderer tests pass.
- A demo brief for 2026-W43 (Oct 19–23), built from the existing runs, matches
  the style of the weekly example in the appendix. Case files didn't exist that
  week, so the demo uses run artifacts only.
- The full suite passes.

## Workstream G — Wire it into the daily routine (last)

**Goal:** the automation produces the new outputs every weekday, and the weekly
brief on Fridays.

**Owns:**

- outside the repo (Juan approves each change):
  - `~/.codex/automations/colombia-forecasting-weekday-review/automation.toml`
  - its `memory.md`
  - `~/.codex/skills/colombia-forecasting-workflow/SKILL.md`
- in the repo:
  - `prompts/agent_analysis.md`
  - `README.md`
  - `PROJECT_SPEC.md`
  - `docs/RUN_ARTIFACTS.md`
  - `docs/REVIEW_SURFACE.md`
  - merging any vendored copies of `plain_language.py`

**Build:**

1. **New daily order.** Today's existing steps stay, and these follow them:
   1. Plan the family slots.
   2. The LLM fills `family_forecasts.json`.
   3. Append the forecasts.
   4. Resolve due family rows.
   5. Refresh the scoreboard (`build_scoreboard.py`), so the brief sees today's
      forecasts and resolutions.
   6. Update cases (the LLM writes the case text), run `validate_cases.py`, then
      `snapshot_cases.py`.
   7. Compute watchlist hits.
   8. The LLM writes `reader_brief.json`.
   9. Run `finalize_reader_brief.py`.

   The automation's final message is `reader_brief.md`, verbatim.
2. **Fridays:** also build the weekly facts, have the LLM write
   `weekly_brief.json`, finalize it, and send the weekly brief as the final
   message.
3. **Prompt cleanup.**
   - Remove: the daily pilot retrospective, count-heavy reporting, and
     "experiment progress out of 10".
   - Keep: the live-network rule, the evidence safeguards and the protected-ledger
     rules.
4. **Skill cleanup.**
   - Replace "Do not over-promote dry indicator continuations" with the
     forecast-family rule.
   - Keep the single bounded follow-up, but point it at the case with the most
     consequential missing evidence.
5. **Memory cleanup.** Move the current `memory.md` (388 KB) to
   `memory_archive_2026-10-27.md`. Start a rolling summary of at most 5 KB, with
   entries of at most 10 lines and normal spacing.
6. **Docs.** Update:
   - README status
   - PROJECT_SPEC milestones: tick the M2/M3 items these workstreams complete
   - `docs/RUN_ARTIFACTS.md` for every new artifact

**Done when:**

- One full live weekday run produces all the new artifacts, validators pass, and
  the brief reads in under a minute.
- A Friday run produces the weekly brief.
- Earlier historical runs still render byte-identical.

## Who owns which files

| Area | Owner |
|---|---|
| `plain_language.py`, `schemas/`, shared-interface fixtures and tests | 0 |
| `review_html.py`, reader-brief files | A |
| `shadow_forecasts.py`, shadow scripts, `agent_policy.yaml`, `forecasts/*` (append-only), family files | B |
| fetchers, parsers and fixtures for the policy-rate, expectations and labor feeds | C |
| `cases/`, case code | D |
| `watchlist.yaml`, entity code, `data/entity_history/` | E |
| `weekly/`, weekly-brief code | F |
| `~/.codex/*`, `prompts/agent_analysis.md`, README, PROJECT_SPEC, `docs/RUN_ARTIFACTS.md`, `docs/REVIEW_SURFACE.md` | G |
| `.gitignore` | E and F each append one line (trivial merge) |
| `colombia_forecasting_desk/pipeline.py` | nobody, except C if strictly needed for the three feeds |

## Merge order

1. Merge Workstream 0 first. A–F branch off after it.
2. Merge A, B, C, D, E and F in any order as each finishes.
3. Run the full suite on `main` after each merge.
4. Then merge G.
5. Then do one live weekday run and one Friday run.

## Success check (after 10 weekday runs)

- Juan reads the daily brief in under a minute and the weekly brief in about three.
- Every scheduled family event got a call or a written skip, with zero missed events.
- At least one brief changed what Juan believes, plans or checks. Topics that never
  do get narrowed or dropped.
- The policy-rate, expectations and labor feeds are observed, or have documented
  fallbacks.

## Later (not in this plan)

- **Public X experiment (M4).** Each week, decide whether to publish the best call
  or explainer, drafted with `prompts/x_post.md`. Write
  `forecasts/forecast_log.jsonl` only after Juan approves.
- **Slim the old daily editorial artifacts** (`candidate_questions.md`,
  `m2_sampling_decisions.*`, `daily_comparison.md`) once the brief proves itself.
  Note that `review_html.py` parses some of them.
- **More forecast families** (TRM, retail, exports) and place timelines.

## Revision log

**Revision 1 (2026-10-27): six corrections from Codex's review.**

1. **BanRep dependency.** Today's preflight has no BanRep slot, because the
   detector needs a current policy rate and that feed is down.
   - B now uses either C's repaired feed or a verified official fallback.
   - The planner states explicitly when the slot is unavailable.
   - Step 0 confirms the rate from the official communiqué and isn't blocked.
2. **Baselines are probabilities.** A survey median is a rate estimate, not a
   probability. B now has explicit baseline rules (a)–(c).
3. **Precise resolution.** The resolver matches exact event, period and series,
   and scores on the first official release. Forecasts created after an event are
   rejected.
4. **No automatic voiding.** Missing records raise a `needs_review` flag. Voiding
   needs a human-approved command with a written reason.
5. **Ledger rule clarified.** Prediction fields are immutable. Only the resolver
   scripts may update resolution fields. This matches how the current resolver
   works.
6. **Shared foundations first.** Workstream 0 freezes the schemas and
   plain-language rules before parallel work. Daily case snapshots keep hindsight
   out of weekly briefs. G refreshes the scoreboard explicitly.

## Appendix — target examples

### Daily brief (October 27, rewritten)

> **Nothing needs you today.**
> - **Friday is the big day:** BanRep decides interest rates (now 12.25%) and DANE
>   publishes September unemployment. The desk has no call on either yet; it should
>   have one by Thursday.
> - **Tobacco and vape tax bill (PL 243 Senado)** has a favorable report heading into
>   its final Senate vote. It proposes a tax of COP 5,100 per pack of 20, rising each
>   year with inflation plus 4 points, with part of the money earmarked for health and
>   sports. No vote date yet.
> - **Open forecast:** the bill to repeal the ultra-processed food tax (PL 012) passing
>   its House committee vote on Monday. We said 55%; the official result isn't
>   published yet.
> - **Data problem:** the BanRep policy-rate feed has been broken since Sep 11.

### Weekly brief (week of October 19–23, rewritten from existing runs)

> **The week in Colombia — Oct 19–23**
>
> **Big picture:** the economy shrank slightly in August while inflation stays above
> 6%. That squeeze is the backdrop for next Friday's interest-rate decision.
>
> **1. The economy stalled in August.** DANE's monthly activity index was 0.04% below
> a year earlier and fell 3.1% from July after seasonal adjustment. Factories are the
> weak spot (sales −3.5%, 29 of 39 industries down); shops are not (retail +7.3%).
> Imports jumped 31.7%; whether demand is shifting to foreign goods is not yet clear.
>
> **2. Budget: a cut now, and next year's budget by decree.** Decreto 1585 trims
> COP 13.8 trillion from 2026 spending; it's a budget cut on paper, and contracts
> already signed are protected. Decreto 1599 sets the 2027 budget at COP 634.95
> trillion. The decree says it was issued because Congress didn't reconcile the House
> and Senate versions by the deadline.
>
> **3. Worth watching: secrecy for security purchases.** A public-order bill
> (PL 433 Cámara / 296 Senado) would exempt some reserved security-force purchases from
> the usual rules for publishing contract documents. Three lawmakers filed a report
> asking to shelve it. The joint committee was scheduled to debate it Oct 21; no
> official result yet.
>
> **Next week:** Fri Oct 30 — BanRep rate decision (12.25% now) and September
> unemployment. Our call on the rate: by Thursday.
>
> **Scoreboard:** 9 resolved forecasts, 6 on the right side. Open: none.
>
> **Data problems:** the policy-rate feed (down since Sep 11) and the analyst-survey
> feed (last worked Aug 24).
>
> **One question for you:** did anything here change what you think or plan to do?
