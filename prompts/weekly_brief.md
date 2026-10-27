# Write the Friday weekly brief

Read the week's frozen `weekly_facts.json`, its coverage_gaps, strongest daily
signals and complete official-follow-up text. Follow the exact artifact references
when needed, including metadata, verified originals and reviewed page numbers.
Important discoveries may be beyond the raw scanner's text cap. Do not drop a
full-source finding merely because it never appeared as a scanner item.

Use only evidence captured by the facts' cutoff_at. Read case views from the
cutoff run's cases_snapshot, never from live cases. Earlier weeks may lack these
snapshots or watchlist outputs; acknowledge those gaps. Preserve explicit parser
corrections and distinguish them from a new official release or official revision.
Repetition is not a new story. A missing calendar entry is not cancellation.

Author `weekly_brief.json` using the frozen weekly_brief.v1 schema and shared
fixture. Use English and plain text. Write a big_picture of at most two sentences,
at most three top_stories, our_calls, next_week, at most three patterns, at most
two data_problems and one question for Juan. Each story needs what happened, why
it matters, who is affected and an HTTPS source_url already in the dated facts.
Use plain names before bill or decree codes. The complete rendered brief must
contain at most 450 words, including headings and source labels.

Check the underlying evidence rather than copying a sample narrative. Keep a
bill's proposed text, an opposition report, a schedule and an enacted instrument
distinct. A reduced appropriation is not proof of realized cash savings. The
executive's stated reason for a decree is its account, not an independent legal
finding. Missing official results remain unknown; never infer NO from silence.

Keep public and internal forecast records distinct. State counts and sample sizes
with any score. Only forecasts and resolutions recorded by the cutoff count.
Do not create or backdate calls while preparing a historical example. Every
next_week event must be established in facts.events; include its optional
family_id to identify the exact event. Supply either a recorded call with the
same ID/probability or a one-sentence no_call_reason. The forecast date and target
event must agree. A meeting time is not automatically an announcement time.
Mention open forecasts waiting for official records and review flags.
List each open record in optional open_forecasts using the same forecast_id,
plain_question, probability and resolves_by fields as the daily brief. The
finalizer checks these against the dated ledger projection.

Patterns are review prompts with caveats, not accusations. Explain that direct
contracting is lawful in many circumstances. Do not give buy, sell or betting
instructions. End with a question about whether anything changed what Juan
believes, plans or checks.

Finalize with `scripts/finalize_weekly_brief.py --week YYYY-Www`. This writes the
Markdown and HTML; it does not fetch, forecast, publish or change ledgers.

Usefulness feedback is recorded only when Juan explicitly supplies an answer.
The finalizer can accept --feedback-file pointing to JSON with week, date,
answer, source (user_message or user_file), and source_ref identifying the supplied
message/file. Preserve his wording. There is no automatic access to chat history;
never invent an answer or infer usefulness from silence.
