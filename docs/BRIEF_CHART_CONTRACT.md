# Charts in reader briefs

New daily and weekly briefs use `reader_brief.v2` and `weekly_brief.v2`.
Every brief must assess a visual, with at most one chart attached to one story:

```json
"chart_decision": {
  "reason": "The historical comparison makes the sharp slowdown clear.",
  "chart": {
    "data_id": "2026-11-20:gdp-annual-original",
    "story_index": 0,
    "why_it_matters": "Annual growth slowed sharply; this warrants attention to demand and jobs."
  }
}
```

`story_index` is zero-based in `changes` or `top_stories`. When a chart does not
materially help, set `chart` to `null` and write a specific `reason`. This reason
is audit material, not reader text. Quiet days need no chart. Selection remains
model judgment: explain the previous value, new value, historical scale and
public consequence. Do not infer importance from unverified social popularity.
Do not force a visual from a numeric threshold or a quota.

## Source preparation

Charts use one captured official source, one source vintage, one series and one
unit. Do not mix original/adjusted series, annual/quarterly growth, estimates
and observed outcomes, or first releases and later revisions. Read the relevant
source headers and notes before choosing cells. Mechanical checks cannot judge
the substantive meaning of a selected series or the chart's importance.

Use the one bounded editorial follow-up to obtain a needed official structured
source; this feature adds no network collection or extra research allowance.
Do not transcribe PDF or screenshot values into a purported raw JSON source.
If validated structured data is unavailable, retain the narrative and record a
written chart skip. Missing observations are never replaced with zero.

Keep the original JSON or XLSX under the current run's `sources/`. A single
source receipt must identify `source_url`, run-relative `artifact`, `sha256`,
`content_type`, `status_code: 200`, `tls_verified: true`, and an aware
`captured_at` or `fetched_at`. Optional `published_at` must also be aware.
The source URL must belong to an official Colombian government or XM host.

Author `runs/<date>/brief_chart_request.json`:

```json
{
  "schema_version": "brief_chart_request.v1",
  "id": "gdp-annual-original",
  "title": "GDP growth slowed sharply",
  "source_receipt": "sources/gdp_annex_receipt.json",
  "frequency": "quarterly",
  "series_locator": "GDP!A8",
  "unit_locator": "GDP!A2",
  "points": [
    {"period_locators": ["GDP!G3", "GDP!G4"], "value_locator": "GDP!G8"},
    {"period_locators": ["GDP!G3", "GDP!H4"], "value_locator": "GDP!H8"}
  ]
}
```

The cell addresses above illustrate the shape; inspect the actual workbook.
The series locator must select its real row/column label. The unit locator must
contain the source's unit text. XLSX period locators select either one complete
period or separate year and quarter headers (including a merged year cell).
Numeric years and Roman quarters are accepted. Values must align with the
series row/column and a period header. Formula cells are withheld because their
cached results can be absent or stale. Percentage-formatted Excel cells are
converted from fractions to percentage points using the workbook's format.

JSON uses JSON pointers such as `/series`, `/unit`, and
`/observations/0/value`, with `/observations/0/period` for its period. Unit and
series labels must share a parent above the observation records; each value
and period must share a record. Each point is extracted by code, never authored.
Supported frequencies are annual (`YYYY`), quarterly (`YYYY-QN`), monthly
(`YYYY-MM`) and daily (`YYYY-MM-DD`). Select 2–24 consecutive observations;
gaps, duplicate periods, future observations and incomparable windows fail.

```sh
./.venv/bin/python scripts/prepare_brief_chart.py --date YYYY-MM-DD \
  --request runs/YYYY-MM-DD/brief_chart_request.json
```

This offline command writes immutable `brief_chart_data.json` with the source
hash, receipt hash, extraction request, timestamps, labels and extracted values.
It runs only for today's dated run. A same-request rerun is idempotent.

## Finalization and history

The daily finalizer rechecks the original bytes, receipt, locators, labels,
units, periods and values. All validation finishes before rendering. It writes
`brief_chart.svg` and `brief_chart.png`, inserts the PNG's absolute local path
in the Markdown beneath the selected story, and embeds accessible SVG in HTML.
It uses the existing Playwright dependency, installed Chromium or local Chrome,
with a temporary profile and all page network requests blocked. If needed, run
`playwright install chromium` during environment setup, outside the daily scan.
Rendering failure blocks finalization; it never silently claims chart delivery.

The chart is a straight line on a zero-inclusive scale. Recent values are
directly labeled; original values and periods are available to screen readers.
The image has a fixed, high-contrast palette and remains readable when reduced
to a narrow brief. The generated chart is not an AI-generated illustration.

The title/alt text and caption count toward the existing 250/450-word narrative
limits; plot ticks and numeric data labels are not narrative. The skip reason
is hidden. The daily validation receipt additionally hashes the prepared data,
SVG and PNG, and usefulness checks reject changed or missing chart files.

Before Friday facts are frozen, prepare any selected dataset in the current
dated run. The facts builder validates available prepared data and copies it
into `weekly_facts.json.chart_data` only when both source capture and extraction
precede the exact cutoff. Invalid or later data becomes a coverage gap. The
weekly brief selects a `data_id` from those frozen facts; its finalizer never
reads later source files or live cases. Keep revisions and source vintage clear.
Its v2 finalization receipt hashes the frozen facts, authored brief, rendered
Markdown/HTML and any SVG/PNG images.

Normal finalizer CLIs require v2. `--allow-legacy` is only for rebuilding an
existing historical v1 brief, including `--feedback-file` maintenance. Legacy
library rendering remains byte-compatible; do not migrate old briefs, append
charts to old receipts, change existing calls, or rewrite frozen weekly facts.

The automation returns the finalized Markdown verbatim, including its chart
image. It does not replace that image with a link or merely describe it. The
Spanish X draft remains text-only for manual review and posting.
