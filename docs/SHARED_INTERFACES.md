# Shared reader and forecasting interfaces

Revision 1 of the usefulness plan is committed in
`docs/plans/2026-10-27-make-the-desk-useful.md`.

All new producers import their validator from
`colombia_forecasting_desk.schemas.<artifact>`. `validate(payload)` returns a
list of readable errors; `schemas.require_valid(payload, schema_version)` raises
`ValueError` on failure. These functions do no I/O. Optional extra fields are
allowed. The paired valid and invalid JSON examples under
`tests/fixtures/shared_interfaces/` are the shared producer/consumer fixtures.

- Dates are ISO strings. YAML loaders normalize date objects before validation.
- Probabilities are finite numbers in [0, 1]; booleans are rejected.
- Reader changes use title, what_happened, why_it_matters, who_is_affected,
  confidence and source_url. Confidence is confirmed, likely or uncertain.
- Upcoming events use date, event, our_call and no_call_reason. Exactly one of
  our_call and no_call_reason is populated. A call has forecast_id, probability
  and outcome.
- Scoreboards use as_of, public, internal and families. Each public/internal
  group contains open, resolved, void and needs_review row lists, n_scored,
  right_side_n, right_side_rate, mean_brier, baseline_n, model_brier_paired,
  baseline_brier and improvement. Empty scores are null. Families record
  family_id, events, calls, skips and missed_events. Further breakdowns are
  optional; public and internal results always remain distinct.
- Weekly facts use week, as_of, run_dates and coverage_gaps. The optional
  indicator_changes, cases, watchlist_hits, events and data_problems fields are
  lists; forecasts is an object. A missing historical source is a coverage gap.
- Weekly briefs use week, as_of, big_picture, top_stories, our_calls, next_week,
  patterns, data_problems and question. our_calls is reader text. Patterns have
  plain_text and caveat.

Reader renderers enforce word caps on the complete rendered text: 250 words for
the daily brief and 450 for the weekly brief. The shared plain-language checks
apply to reader text, not source IDs, identifiers, URLs or evidence locators.

Family appenders additionally validate the actual slot, evidence references,
pre-event cutoff, target period/series and uniqueness. Resolvers additionally
verify the exact first official release before recording an outcome. Schema
validation alone does not establish evidence or authorize a ledger mutation.
