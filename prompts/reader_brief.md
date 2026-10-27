# Write the daily reader brief

Author `runs/<date>/reader_brief.json` using the frozen `reader_brief.v1`
contract and `tests/fixtures/shared_interfaces/reader_brief.valid.json`. Finalize
it with `scripts/finalize_reader_brief.py --date YYYY-MM-DD`.

Read the validated daily analysis and its evidence first. Then read the family
slots and forecasts, case updates and dated case snapshots, watchlist hits and
the refreshed scoreboard. These are inputs to your judgment, not conclusions.

Write in English, using plain names before official codes. Say what happened,
why it matters and who is affected. Include at most three changed items, five
upcoming events and two material data problems. The complete rendered Markdown,
including headings, source labels and forecasts, must contain at most 250 words.
Do not narrate the workflow or use internal codes. Use plain text in all fields;
the renderer supplies formatting and source links.

Lead with whether Juan needs to do something and the date, if applicable. An
otherwise quiet day is exactly "Nothing needs you today." A quiet day still
includes relevant upcoming events, open forecasts and material coverage gaps.

For every change, cite an HTTPS source URL already captured in the run. Verify
the actual supporting excerpt before summarizing it. Describe a document as a
proposal, report, schedule or decision according to what the evidence establishes.
Do not promote a vote schedule into an approval or a truncated excerpt into an
unread finding. Use confirmed, likely or uncertain confidence accurately.

Every upcoming event has either our_call or a one-sentence no_call_reason. A call
must use the exact recorded forecast ID and probability. Never invent a call,
backdate one or change one while writing a brief. List open forecasts waiting for
official records, including rows flagged for review. Use their recorded
resolution_check_window_end as resolves_by, falling back to resolution_deadline.
Mention missing official records explicitly; silence never means NO. Public and
internal forecasts remain distinct in the scoreboard, and no brief authoring or
rendering step changes either ledger.

Use the format in the plan's October 27 example as the style target. Avoid long
bullet lists, raw bill numbers, process counts and unsupported causal claims.
