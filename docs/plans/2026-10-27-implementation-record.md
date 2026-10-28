# Usefulness plan implementation — October 27, 2026

Revision 1 was committed first as `daf538a`. Workstream 0 was merged before
A–F branched off. Each implementation has its own local branch; G integrates
the finished work. No push or pull request was created.

| Workstream | Branch | Implementation commits | Branch validation |
| --- | --- | --- | --- |
| 0: shared foundations | `codex/desk-shared-foundations` | `fb1536a` | 748 tests |
| A: daily reader | `codex/desk-a-reader` | `bb3953a` | 766 tests |
| B: event families and intake | `codex/desk-b-families` | `a50ec57`, `a398d84` | 817 tests |
| C: feed repairs | `codex/desk-c-feeds` | `d88c20b` | 756 tests; live strict scan passed |
| D: running stories | `codex/desk-d-cases` | `8a6c846` | 756 tests |
| E: entity history | `codex/desk-e-entities` | `7c6dd3c` | 787 tests; two live demos |
| F: Friday brief | `codex/desk-f-weekly` | `3a61b58` | 791 tests; historical W43 replay |
| G: workflow integration | `codex/desk-g-integration` | this change | **887 tests passed** |

The differing branch counts reflect independent branches. The final integrated
suite passed 887 tests and `git diff --check` passed. No new production dependency
was added.

## Step 0 and automation

BanRep's September 30 official communiqué confirmed 12.25%. The existing
weekday automation received its temporary October 28–29 priority before the
feed repair. Its final prompt is maintained in `prompts/weekday_automation.md`.
The saved prompt was read back; schedule, status, model, effort, execution
environment, project and cwd were preserved. The dated priority expires after
October 29 and instructs the automation to remove it after expiry.

The local forecasting skill now routes to `docs/DAILY_READER_WORKFLOW.md` and
passed Skill Creator's validator. The old 388,083-byte automation memory was
preserved as
`~/.codex/automations/colombia-forecasting-weekday-review/memory_archive_2026-10-27.md`.
The replacement is 2,143 bytes; the previous skill was also archived beside it.

## Live integrated run

The fresh live C collection at `/private/tmp/cfd-c-live-scan/2026-10-27` passed
strict acceptance with zero errors and two warnings. Its collection code is the
same code in the final integration. The rest of the new workflow was executed
against these actual October 27 artifacts in the main workspace.

Before refreshing the run, the earlier same-day artifacts and both ledgers were
copied to `tmp/pre_usefulness_20261027_8f5o7v6y/`; the receipt is
`tmp/usefulness_integration_receipt.json`. The public ledger is byte-identical
to that backup. All original shadow-ledger bytes are preserved as an unchanged
prefix; exactly two unique prospective family rows were appended using the
actual creation clock:

- `family_banrep_rate_decision_2026-10-30`: 65% that the policy rate is above
  12.25%. The baseline is explicitly uninformative 50/50, not the survey median.
- `family_unemployment_down_yoy_2026-10-30`: 35% that September national
  unemployment is below 8.2%. The official annex supplies the September 2025
  cell and published one-decimal comparator, plus a documented historical
  frequency baseline with a revised/backcast-history caveat.

The original food-tax forecast remains open at 55%, with its exclusive October
27 event cutoff unchanged; October 30 is only its record-check window. There
are six resolved public forecasts and three resolved/three open internal rows.
No October 30 outcome was fabricated and no historical prediction was created.
Repeating the appender recorded zero new rows. Shadow validation reports zero
issues. Public posting remains a separate human decision.

The final daily brief has 145 visible words. Both upcoming calls and all three
open rows appear in `runs/2026-10-27/review.html`. Chrome verified the actual
page at 1280 and 390 pixels with all details collapsed and expanded: no document
overflow. Three historical no-brief renders remained byte-identical for the same
inputs after the implementation.

## Weekly, evidence and history checks

The 321-word W43 demo is under `weekly/2026-W43/`. It uses five historical runs
and eleven verified original captures, retains the corrected ISE minus sign,
and distinguishes revised security-purchase wording from the earlier filed
text. It does not import October 27's jobs calendar or live case files. Replaying
the weekly pipeline after the two new calls produced byte-identical Markdown.
Desktop/phone and light/dark layout checks passed.

The new capture command retrieved 308 official GEIH monthly observations with
no notices, enabling both real October 30 slots. A separate read-only live
capture of the dated October 19 ISE bulletin recovered -0.04%; the stored bytes
were hash-checked and reparsed successfully. This created no forecast or outcome.

The two labeled entity demos completed with no failed queries. SECOP I/II remain
separate; duplicate rows, missing signatures, 500-group bounds, ambiguous buyer
IDs and possible NIT alias overlap stay explicit. Ambiguous groups are withheld
from pattern screening rather than summed. Natural persons and unknown types
are aggregated on the server. The zona-franca deduplication loss is documented;
watchlist matching reads raw rows without changing the daily scanner.

## Independent operator check and remaining observation

An independent operator check exercised all documented CLI help paths, copied
daily finalization, the historical Friday replay and the actual rendered page.
Its findings were fixed and covered by regression tests:

- Successful finalization now emits a receipt with input/output hashes. Failed
  drafts and edited reader outputs do not count toward usefulness.
- Review flags carry an actual recording timestamp. Intraday weekly cutoffs
  retain flags already known and exclude later flags, including when a result
  is resolved after the historical cutoff.
- Cache refresh precedes watchlist matching in the procedure and skill.

Implementation and live weekday validation are complete. The first scheduled
Friday execution is still prospective, on October 30. The new usefulness check
is **1/10**, separate from the completed old shadow pilot. Usefulness and any
topic narrowing require Juan's explicit feedback; no such answer has been
invented. Source publication dates or first-release evidence that remain
unavailable stay unknown and cannot produce an automatic negative outcome.

## October 28 follow-ups

Completed on the single local branch `codex/desk-followups`, without a push:

- Public entities match their configured names/NITs, without geographic code
  or place-name aliases. Earlier reported watch ID/source/URL combinations and
  unchanged historical patterns are suppressed; later runs cannot affect a
  historical report, and same-day reruns remain stable.
- Entity reports and the daily watchlist use English money units and dataset
  names, and must pass the shared plain-language validator. A public buyer's
  direct-award screen names up to five organizational suppliers with distinct
  contract counts and shares of all that buyer's direct awards. Unknown
  denominators, ambiguous aliases and source limits remain explicit.
- New unemployment rows condition the comparator on the exact latest annual
  rise when it reaches 0.5 percentage points. The preserved official history
  reproduces 13 next-month declines in 72 such pairs (18.1%), compared with
  177/296 overall (59.8%). Existing rows retain their original baselines.
- The two family IDs are linked in the rates and activity/jobs cases. Both
  brief renderers show a call only once across Coming up and Open forecasts.
  `tmp/` is ignored. All five completed `/private/tmp/cfd-desk-*` worktrees were
  removed after verifying their commits were merged; useful untracked and
  ignored artifacts were archived with hashes under the local follow-up tmp
  directory before removal.

Validation: **906 tests passed**, including new regression coverage for the
above behavior. A pre-existing mocked workbook fixture now fixes its ZIP entry
timestamps so repeated source downloads have identical bytes; the production
same-day source freeze remains unchanged. `git diff --check` passed.

The actual October 28 strict scan passed with zero errors and one untagged-item
warning. Live history queries completed without failures, and the named
Antioquia screen used 2,360 direct awards as its denominator. The updated daily
matcher emitted zero new hits for the demo entries. All reports passed plain
language checks. The October 27 and 28 HTML/Markdown briefs show each recorded
call once; today's brief is 89 words. The usefulness count is now **2/10**.
The first scheduled Friday run remains October 30.

Both forecast ledgers were byte-for-byte unchanged after the follow-up and
today's workflow. The existing food-tax outcome remains unverified after one
fresh official resolver check; no forecast, outcome or user feedback was
invented.
