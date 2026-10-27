# Daily and Friday reader workflow

Run for today's date in `America/Bogota`, using `./.venv/bin/python`. All
reader-facing writing is in English; preserve official names and identifiers.
The dated artifacts and validators are the interface between deterministic
collection and the model's narrative and probability judgments.

## Collect and review

Request live-network execution before the strict scan. A denied or unavailable
network permission ends the run as `blocked_network_not_decision_grade`; do not
interpret sandbox DNS failures or all-zero artifacts as source evidence.

```bash
./.venv/bin/python scripts/scan_metasources.py --date YYYY-MM-DD --strict --source-report
./.venv/bin/python scripts/capture_family_evidence.py --date YYYY-MM-DD
```

The second command is also network-dependent. It collects the official GEIH
historical comparator for an upcoming jobs release and exact dated bulletins
for due open DANE family calls. Exit 2 means the capture report has actionable
source notices; inspect it and keep affected outcomes unresolved. Never invent
a first-release marker or substitute a current revised value. See
[Forecast families](FORECAST_FAMILIES.md) for targeted capture options.

Compare with the previous decision-grade run. Read source excerpts, source
health, official indicators, all triggered tensions, market context, bundles,
legislative identities and heuristic audit flags. Read the actual case material
and challenge deterministic rankings. A calendar event or proposal is not an
outcome. Treat failed, stale, link-only and unparsed sources as coverage gaps.
Market rows and deterministic screens are context, not probability formulas.

Keep the current `candidate_questions.md`, `human_decisions.md`,
`daily_comparison.md` and sampling bridge. Complete one bounded official-source
follow-up on the case with the most consequential missing evidence: at most
three official sources or about ten minutes. It may legitimately end with an
unknown result. Author `agent_analysis.json` using
[its contract](AGENT_ANALYSIS_CONTRACT.md) and `prompts/agent_analysis.md`.
Review every triggered tension and at least one cross-bundle or unbundled
relationship. Distinguish the public editorial decision from internal calls.

```bash
./.venv/bin/python scripts/write_m2_sampling_decisions.py --date YYYY-MM-DD
./.venv/bin/python scripts/finalize_agent_analysis.py --date YYYY-MM-DD
./.venv/bin/python scripts/plan_family_forecasts.py --date YYYY-MM-DD
```

## Make and score the scheduled calls

Author `family_forecasts.json` from the preserved slots, current evidence and
counterarguments. Every enabled due event needs one call or written skip.
Already-recorded events use their existing immutable call. No quota forces a
discretionary shadow; at most one discretionary shadow can come from the
separate analysis. Family calls are a separate required internal workflow.

Use the actual model and reasoning information available in the session. Never
substitute a configured model for an unknown actual runtime. The appender uses
the actual clock and rejects backdated or post-event calls. Baselines must be
published YES shares or documented relevant empirical frequencies; a median or
mean is not a probability. Otherwise label the 50/50 comparator uninformative.

```bash
./.venv/bin/python scripts/append_family_forecasts.py --date YYYY-MM-DD --model ACTUAL_MODEL --reasoning-effort ACTUAL_EFFORT
./.venv/bin/python scripts/resolve_family_forecasts.py --date YYYY-MM-DD
./.venv/bin/python scripts/build_scoreboard.py --date YYYY-MM-DD
./.venv/bin/python scripts/summarize_shadow_experiment.py
```

Check due discretionary shadows too. Only their declared official records may
establish YES or NO through `resolve_shadow_forecast.py`. Source silence is not
NO. The family resolver flags missing records after the check window plus 21
days; it never voids automatically. A specific human-approved void needs a
written reason. Do not change existing questions, probabilities, cutoffs,
baselines, rationales or evidence. Public and internal scores remain separate,
with paired baseline sample sizes and no calibration claim from small samples.

## Carry the stories and watchlist, then write the brief

```bash
./.venv/bin/python scripts/update_cases.py --date YYYY-MM-DD
```

Read candidate evidence and edit `cases/*.yaml` only when it warrants a changed
view. Keep prior dated updates and sources; a repeated source is not a new
development. Identify proposals, adopted text and certified outcomes separately.
Then validate and freeze today's state:

```bash
./.venv/bin/python scripts/validate_cases.py
./.venv/bin/python scripts/snapshot_cases.py --date YYYY-MM-DD
```

History collection stays separate from the daily scanner. When a configured
organization's cache is absent, more than seven days old, or the watchlist has
changed, refresh only the affected entries with network execution:
`build_entity_history.py --date YYYY-MM-DD --watch-id WATCH_ID`. Preserve
unknown/truncated coverage and natural-person aggregation. The shipped entries
are labeled demos, not Juan's stated interests. See [Entity memory](ENTITY_MEMORY.md).

Refresh the affected caches before matching, so today's output uses their new
state. Then run the offline matcher:

```bash
./.venv/bin/python scripts/watchlist_hits.py --date YYYY-MM-DD
```

Write `reader_brief.json` using `prompts/reader_brief.md` and the current analysis,
family calls, scoreboard, case updates and watchlist hits. Explain at most three
changes, affected people and upcoming calls in plain words. Include waiting
official records and material data problems. Keep the complete rendered brief
at or below 250 words. A genuinely quiet day is one line.

```bash
./.venv/bin/python scripts/finalize_reader_brief.py --date YYYY-MM-DD
./.venv/bin/python scripts/check_usefulness.py --date YYYY-MM-DD
```

The final weekday message is `reader_brief.md` verbatim. Counts, validation logs
and the old pilot retrospective stay in supporting artifacts. Avoid appending
another process report after the reader brief.

## Fridays and usefulness

On Fridays, capture the current timezone-aware timestamp and build that ISO
week's facts with that timestamp as `--cutoff-at`:

```bash
./.venv/bin/python scripts/build_weekly_facts.py --week YYYY-Www --cutoff-at ISO_TIMESTAMP
# Author weekly/<week>/weekly_brief.json from these frozen facts and the weekly prompt.
./.venv/bin/python scripts/finalize_weekly_brief.py --week YYYY-Www
```

The Friday final message is `weekly_brief.md` verbatim, at most 450 words. Use
dated case snapshots and archived first-release evidence, never live cases or
later resolutions to rewrite a past week. A retrospective demo is labeled with
its historical cutoff; it is not a new forecast. Missing inputs remain explicit
coverage gaps. Source corrections and superseded legal wording must survive.

Log only explicit user feedback using the finalizer's `--feedback-file` option;
the [weekly contract](WEEKLY_BRIEF_CONTRACT.md) defines its fields and source
reference. Do not infer the user's answer or assume access to another chat.
`check_usefulness.py` counts only successfully finalized weekday reader briefs
on strict-pass runs, using a context-validation receipt matching the JSON and
Markdown hashes. Drafts, failed finalizations and edited outputs do not count.
At ten runs, review the briefs and event coverage against explicit answers
in `reviews/usefulness_log.md`. If feedback is absent, ask the weekly question
and mark usefulness unknown. Record the qualitative review in
`reviews/usefulness_review.md`; narrow topics only when the evidence or Juan's
feedback supports it. This is a new usefulness check, not a restart of the old
shadow pilot or a claim of forecasting calibration.

Keep the automation memory under 5 KB, with dated entries at most ten lines.
Refer to repo artifacts rather than copying counts and compressed run logs.
The October 2026 archive preserves the earlier history.

Public M3 evidence packs still need the existing exact-source/cutoff/readiness
contract and human publication review. A Court communication cannot establish
an operative deadline without the complete written ruling. Family calls never
authorize public posting, investment advice or public-ledger migration.
