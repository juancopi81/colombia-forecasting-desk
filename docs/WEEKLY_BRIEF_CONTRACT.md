# Friday weekly brief

The weekly pipeline separates deterministic evidence collection from narrative
judgment. It makes no network requests and never changes forecast ledgers.

```sh
./.venv/bin/python scripts/build_weekly_facts.py --week 2026-W43
# author weekly/2026-W43/weekly_brief.json from the frozen facts
./.venv/bin/python scripts/finalize_weekly_brief.py --week 2026-W43
```

The builder accepts --runs-dir, --weekly-dir, --repo-root, --as-of and --cutoff-at.
The pure entry point is build_weekly_facts(runs_dir, week, repo_root=...,
as_of=..., cutoff_at=...). An ISO week means Monday through Friday. By default
the cutoff is the end of the last available run's date in America/Bogota; an
explicit timezone-aware cutoff_at reproduces an intraday boundary. No wall-clock
time or filesystem modification time enters the output.

Facts retain distinct indicator observations, repeated-observation date ranges,
same-period correction/revision warnings, the strongest daily analysis signals,
and full official_followup.md text. Original metadata/source references, hashes
and inspected page numbers stay attached so a full-text budget finding cannot
disappear behind the raw scanner's cap. References use exact absolute artifact
paths. A matching URL alone is not proof of the surrounding interpretation.

Cases come only from the cutoff run's cases_snapshot and are checked against
its hash manifest and case schema. Future-dated views and updates are rejected.
The live cases directory is never read. Missing historical cases or watchlist
snapshots are explicit coverage gaps. Repeating watchlist hits, patterns, signals
and calendar events are deduplicated without interpreting their significance.
The next fourteen days include captured official calendar rows even when a
failed indicator suppressed the automatic preflight.

The same-run scoreboard.json is the preferred forecast snapshot when available.
Otherwise the builder projects both protected ledgers to the cutoff: later
forecasts are excluded; a resolution recorded later is shown as still open,
without its outcome or score. Timestamp-less resolutions are not trusted.
Public/internal scores remain separate. Model and baseline Brier scores use
paired rows only for comparison; neutral 50/50 calls are separate from the
directional right-side rate. Skips require an eligible timestamp. This is
descriptive history, not evidence of calibration from a small sample.

weekly_brief.v1 is LLM-authored JSON. Its shared validator plus the weekly module
enforce two sentences for the big picture, three top stories, three patterns,
two material data-problem lines and 450 words in the complete rendered Markdown.
Each story links to an HTTPS source already in the dated facts. Upcoming events
are limited to fourteen days and need a recorded call or one-sentence skip.
The finalizer validates against weekly_facts.json before writing weekly_brief.md
and a responsive, escaped weekly.html with light and dark modes. Authored HTML
is rejected. It does not touch the daily review or generate a forecast.

When there are open forecasts, list them in the optional open_forecasts field
using the daily brief's forecast_id, plain_question, probability and resolves_by
shape. Finalization checks each against the dated record and rejects omissions,
so forecasts waiting for official records remain visible.

Usefulness answers are explicit inputs. Optionally supply --feedback-file with
week, date, answer, source and source_ref; source must be user_message or
user_file. --usefulness-log changes the destination (default
reviews/usefulness_log.md). The append is idempotent and preserves the answer as
a quote. Nothing reads chat history or invents a response when Juan is silent.

Historical demo outputs are ignored under weekly/*/. The W43 demo uses October
19–23 artifacts only and explicitly acknowledges that case/watchlist snapshots
did not yet exist. It creates no retrospective forecast or score.
