# Daily reader brief

The LLM authors `runs/YYYY-MM-DD/reader_brief.json` under the frozen
`reader_brief.v1` schema. `reader_brief.py` imports the shared schema and
plain-language rules, validates the complete rendered brief, and renders Markdown
and escaped HTML without network access or LLM calls.

Run:

```sh
./.venv/bin/python scripts/finalize_reader_brief.py --date YYYY-MM-DD
```

`--runs-dir PATH` supports isolated fixture/demo runs. The finalizer validates all
inputs before writing `reader_brief.md` and `review.html`. It never changes a
ledger or runs index. Its Markdown is the automation's final message verbatim.

The schema defines needs_you, headline, changes, coming_up, open_forecasts and
data_problems. Changes are capped at three, upcoming events at five, and material
data problems at two. A call requires a recorded forecast ID and its exact
probability; otherwise the upcoming item has a one-sentence no_call_reason.
The complete rendered Markdown, including labels and headings, is capped at
250 visible words. A day without any changed item, event, open forecast, material
data problem or required action renders one line: "Nothing needs you today."

Source URLs must use HTTPS and be present in another JSON artifact in that run;
the brief cannot establish its own provenance. The finalizer checks forecast IDs,
probabilities, open statuses and official record-check dates against both ledgers.
It rejects a malformed ledger, a future forecast, missing disclosure on an
incomplete run, or omission of forecasts flagged for review. These checks support
source review; a matching URL alone does not prove the narrative is correct.

The HTML opts in only for a valid brief whose run_date matches the run folder.
It shows the reader brief first and puts the detailed analysis, forecast evidence,
indicators, coverage and other operational material in collapsed details blocks.
Only this path receives responsive overflow fixes. Missing or invalid briefs use
the existing renderer byte for byte. Narrative text and URLs are escaped; authored
HTML tags are rejected rather than interpreted.

The pure API is `validate_reader_brief(brief, run_date=..., forecast_rows=...,
source_urls=...)`, returning a list of errors; `render_reader_brief(brief)` and
`render_reader_brief_html(brief)` raise ValueError when validation fails. Context
arguments are optional for fixture-only rendering; finalization always supplies
them.
