# Scheduled internal forecasts

The four v1 families are BanRep policy-rate increases, headline annual IPC
increases, original-series total annual ISE growth increases, and national
original-series unemployment below the same reference month a year earlier.
They are internal forecasts in the protected shadow ledger. Public posting and
the public forecast ledger keep their existing human-review gate.

## Daily commands

After the strict scan passes and before writing the reader brief:

```sh
./.venv/bin/python scripts/capture_family_evidence.py --date YYYY-MM-DD
./.venv/bin/python scripts/plan_family_forecasts.py --date YYYY-MM-DD
# Author runs/YYYY-MM-DD/family_forecasts.json against the planned slots.
./.venv/bin/python scripts/append_family_forecasts.py --date YYYY-MM-DD --model ACTUAL_MODEL --reasoning-effort ACTUAL_EFFORT
./.venv/bin/python scripts/resolve_family_forecasts.py --date YYYY-MM-DD
./.venv/bin/python scripts/build_scoreboard.py --date YYYY-MM-DD
```

The planner, resolver and scoreboard do no network or LLM work. All command-line
tools accept explicit roots or ledgers for offline fixtures and isolated demos.
The appender has no creation-timestamp override: it records the real clock and
rejects historical dates, post-event calls and calls outside the 1–7-day window.
Every enabled due slot needs either a forecast or a written skip. Missing or
failed acceptance blocks all appends. Forecast evidence files and locators must
exist in the preserved run. Probabilities, questions, thresholds, baselines,
cutoffs and rationale never change after creation.

Reruns are idempotent. A changed prediction for an existing slot is rejected.
Slot identity is `family_id:event_date`, and compatible older discretionary or
public forecasts can cover a family event. A 50/50 prediction has a Brier score
but has no directional success or failure. Family calls do not consume the
single discretionary shadow allowance. Same-day skip records are immutable;
a later prospective call can supersede an earlier skip without deleting it.

## Clocks, comparators and baselines

The planner reads preflight opportunities and the official calendar records in
`raw_items.json`. BanRep's raw calendar remains usable when preflight omitted
its slot because the rate feed failed. If no current official rate exists, it
emits `BanRep slot unavailable: no current policy rate` instead of disappearing
silently. Source gaps and absent calendar captures never establish a quiet day.

If an official calendar supplies a timezone-aware time, that is the cutoff.
A date-only calendar uses the conservative start of that date in Bogotá and
records that this is a conservative cutoff, not an official publication time.
IPC and GEIH calendar releases normally refer to the preceding month; ISE to
two months earlier. An explicit calendar reference period takes precedence.
IPC and ISE thresholds require the exact previous reference month. The GEIH
threshold requires the exact target month one year earlier. For example, the
August 2025 comparator on an August 2026 card cannot supply September 2025.
GEIH stays disabled with a written reason until its feed and comparator work.

BanRep can use a separately verified official fallback:

```json
{
  "policy_rate_pct": 12.25,
  "decision_date": "2026-09-30",
  "source_url": "https://banrep.gov.co/es/noticias/junta-directiva/junta-directiva-septiembre-2026",
  "verified": true,
  "verified_at": "2026-10-27T16:00:00Z"
}
```

Pass that file with `--policy-rate-fallback PATH`. It must identify BanRep,
precede the run, have been verified within seven days, and refer to a decision
no more than 62 days old. The caller must verify it is still the latest decision.
The fallback is copied into the slot's provenance; a bare number is insufficient.

Baseline order is published survey YES share, a relevant official historical
YES frequency, then explicitly uninformative 50/50. Every baseline records its
rule, inputs and window. The EME median or mean is never converted to a
probability. A survey distribution needs explicit rates/counts, a known
publication date, an official URL and the exact event horizon. Current October
2026 EME has only point estimates, so it supplies no survey probability.

Optional histories live in an indicator card's `values.history`, with exact
`period` (monthly) or `date` (decision), the family value key, and `published_at`
per observation. `values.history_source_url` identifies the official source.
Only comparable pre-forecast observations are used. Monthly gaps are not
treated as adjacent months. For BanRep, hike-after-hike frequencies apply to
the YES event only when the latest decision was a hike; persistence of a cut
does not estimate a hike probability. Missing history degrades to 50/50.

## Resolution and delayed records

The resolver matches event date, reference period, exact value key and series
variant. It checks official institutional URLs and requires an observed card
whose official release date matches the planned event. Same-day archived DANE
cards pin the initial release. A later capture is not enough: it needs an
explicit `values.first_release: true` and a verified official document URL in
`values.first_release_source_url`; revised or conflicting records stay open.
The stored evidence includes source URL, artifact, locator, original value,
series/period, publication timestamp and SHA-256 hash. A future publication
timestamp or a pre-event resolution clock is rejected. Later revisions cannot
overwrite a recorded outcome or score.

For BanRep the primary record is the exact dated board communiqué, not later
minutes or a daily IBR value. The policy-rate card is cross-checked when it
contains a contemporaneous observation; disagreement leaves the row open.
Strictly greater and strictly less comparisons handle ties as NO, solely when
the official value is known. Missing, delayed or incomparable records remain
unresolved.

After the record-check window plus 21 days, open family and discretionary rows
receive `needs_review: true`. This never creates an outcome or automatically
voids a forecast. Both the scoreboard and briefs can display the waiting queue.
An explicitly approved invalidation is available only after that interval:

```sh
./.venv/bin/python scripts/resolve_shadow_forecast.py --forecast-id ID --void --reason 'Juan approved: the declared series was withdrawn' --resolved-at ISO_TIMESTAMP --human-approved
```

The approval must concern that particular forecast. Implementation approval is
not approval to void existing rows. Voids retain predictions, reasons and dates,
and remain visibly unscored. Repeated finalization of the original analysis
does not resurrect or duplicate a voided forecast. Only resolver scripts may
change resolution fields on an existing row; all writes are locked and atomic.

## Scoreboard and weekly cutoffs

`forecasts/scoreboard.json` and `.md` separate public and internal forecasts;
internal results additionally separate discretionary and family calls. They
show open/resolved/void records, waiting and review queues, directional counts,
and mean Brier scores with sample sizes. Baseline comparisons use paired rows
only. Scores are descriptive; a small sample does not establish calibration.

`build_scoreboard(repo_root, as_of=DATE)` is also available to weekly builders.
It excludes predictions made after that Bogotá date and treats outcomes or
voids recorded later as still open. `write_scoreboard` additionally stores a
frozen daily copy at `runs/DATE/scoreboard.json`. Future predictions or outcomes
cannot enter an earlier week's summary.

Family event/call/skip/miss counts cover captured calendar slots, not an assumed
complete official calendar. Unavailable calendar artifacts are unknown;
zero detected misses does not prove complete coverage. Disabled events are
explained as disabled skips, and a call takes precedence over prior skips.

## Python entry points

- `plan_family_forecasts(run_dir, config_path=..., policy_fallback=None)` returns
  the validated slots artifact.
- `append_family_forecasts(run_dir, ledger_path, skips_path, model=...,
  reasoning_effort=...)` validates and writes internal calls/skips only.
- `resolve_family_forecasts(runs_root, ledger_path, as_of=...)` returns resolved,
  waiting and newly flagged forecast IDs.
- `build_scoreboard(repo_root, as_of=...)` returns a hindsight-safe artifact;
  `write_scoreboard` persists JSON, Markdown and the daily frozen copy.

These producers import the frozen schemas from Workstream 0. They add no runtime
dependency and leave ordinary daily rendering opt-in until integration.

## Production evidence intake

`capture_family_evidence.py` is an explicit network command. Put it immediately
after the live strict scan, before planning calls and resolving earlier calls.
It needs the same live-network execution authorization as that scan. By default
it discovers the GEIH annex when a labor release is due in 1–7 days, and exact
dated technical bulletins for due open DANE family rows. It records actionable
notices in `family_evidence_capture.json`; exit status 2 means some requested
evidence was unavailable or rejected. Keep that slot disabled or forecast open
and show the material gap. The command itself never records a forecast/outcome.

### GEIH historical comparators

```sh
./.venv/bin/python scripts/capture_family_evidence.py --date YYYY-MM-DD --history
# Explicit fallback when the captured labor card has no annex link:
./.venv/bin/python scripts/capture_family_evidence.py --date YYYY-MM-DD --history-url OFFICIAL_ANNEX_URL
```

The default discovers `anex-GEIH-<month><year>.xlsx` in the labor card's official
document links. It resolves the workbook relationship for the exact `Total
nacional` worksheet and reads only the first monthly section's TD row, with its
merged year headers and Spanish month columns. It never substitutes city or
quarterly tables. Preserved bytes, SHA-256 and exact cell locators back every
derived observation in `family_reference_history.json`. Raw values remain
available, while comparators use DANE's published one-decimal percentage,
consistently with the first-release bulletin. The history is explicitly current
revised/backcast history with a methodology caveat, not an initial-release
resolver. The planner rehashes and reparses it and uses only evidence already
captured by the run's Bogotá date.

In the August 2026 annex, September 2025 is `Total nacional!KL17`; raw
8.173482473735278 becomes the explicitly declared 8.2% comparator. That is an
observation from the source, not a number hardcoded into the planner. The
history also supplies the empirical annual-decline baseline where comparable
months exist.

### Next-morning first-release capture

The normal morning schedule can resolve a release on the next run using a dated
original technical bulletin, never a latest revised annex. Intake discovers
`bol-IPC`, `bol-ISE` or `bol-GEIH` for the exact target month in the captured
official document links. When IPC's card lacks links, it follows the captured
official operation page and preserves that discovery page. A targeted follow-up
is also executable:

```sh
./.venv/bin/python scripts/capture_family_evidence.py --date YYYY-MM-DD --slot-id FAMILY:EVENT_DATE --release-url OFFICIAL_BULLETIN_URL
```

Intake matches the filename's operation/period, the bulletin's own publication
date and the predeclared event, then extracts the exact headline annual IPC,
original-series annual total ISE, or national monthly GEIH statement. It rejects
wrong dates/periods/series/geographies, a correction of this bulletin, and HTTP
or PDF modification metadata later than the release date. Generic notes about
revisions to earlier periods and provisional-data methodology are allowed.

Original bytes and `family_release_evidence.json` preserve the URL, SHA-256,
publication date, capture time, value and text fingerprint. The offline resolver
rehashes and reparses this stored PDF before recording an outcome, so next-day
resolution works even if the current indicator card has changed. Missing,
ambiguous, modified or unreadable original bulletins remain open with a specific
follow-up notice. A changed current-card value is never treated as a first
release merely because it was the first value this workflow captured.

Live read-only parser checks on October 27 recovered September IPC 6.29% from
the October 7 bulletin, August ISE −0.04% from the October 19 bulletin, and August
national GEIH 9.4% from the September 30 bulletin. No historical forecast was
created or resolved by those parser checks.

`family_evidence.capture_geih_history` and `capture_first_release` perform the
explicit network intake. `reference_history_for_run` and
`verified_release_for_row` recheck preserved bytes without network access.
