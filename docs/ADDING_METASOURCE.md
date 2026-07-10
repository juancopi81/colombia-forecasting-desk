# Adding a metasource

This guide describes the M1.2 onboarding workflow: how to add a new source to
`config/metasources.yaml`, iterate on parsing, and decide when it's ready for
the daily pipeline.

## 1. Add the YAML entry

Append a new entry to `config/metasources.yaml`. Required fields:

```yaml
- id: my_new_source # snake_case, unique
  name: My New Source # human-readable
  url: https://example.gov.co/feed
  type: news # news | official_updates | calendar | legal | polling | dataset
  country_relevance: high # high | medium | low
  access_status: rss_public # rss_public | html_public | api_public | paywalled | blocked | manual_only
  fetch_method: rss # rss | html | api (api is reserved for future use)
  priority: medium # high | medium | low
  update_frequency: daily # daily | weekly | event_driven | monthly
  trust_role: media_signal # official_signal | media_signal | polling_signal | resolution_source | agenda_signal | civic_signal | legal_signal
  parsing_difficulty: easy # easy | medium | hard
  enabled: false # leave disabled until a sandbox run looks healthy
```

Optional:

| Field               | Purpose                                                             |
| ------------------- | ------------------------------------------------------------------- |
| `notes`             | One- or two-sentence context. Mention quirks, redirects, bot walls. |
| `max_items`         | Cap raw items per run (useful for noisy media feeds).               |
| `verify_ssl`        | Set `false` only when a public source has a broken cert chain.      |
| `onboarding_status` | See section 3.                                                      |

## 2. Test it as a sandbox source

The single-source runner fetches, cleans, and ranks one source without touching
the dated daily run folders. Artifacts are written to
`runs/sandbox/<source_id>/`.

```bash
uv run python scripts/scan_metasources.py --source my_new_source
```

You'll see:

```
Wrote runs/sandbox/my_new_source
  raw_items=12 cleaned_items=11 clusters=3 failures=0

Sandbox: my_new_source
  raw=12 dated=11 cleaned=11 rankable=11 failures=0
  content=html_or_api doc_links=0 parsed=0
  first 5 cleaned items:
    - [2026-04-27T11:00:00Z] Title 1...
```

What to look at:

- **`raw=0`** with no failure → the parser found nothing (likely a JS-rendered
  page or a navigation hub). Capture the HTML and inspect.
- **`raw>0` but `rankable=0`** → items are cleaned but lack publication dates,
  or are flagged low-quality. Open `cleaned_items.json` and check the
  `quality_notes` field.
- **`content=pdf_links_only`, `spreadsheet_links_only`, or
  `document_links_only`** → the source found dated/rankable links, but the
  pipeline has not read the document body yet. Treat this as link-level
  coverage and add a PDF/XLSX parser before relying on the source for evidence.
- **`doc_links>0` and `parsed=0`** → the source has downstream documents but no
  document-content parser is active for those items.
- **`parsed>0`** → at least some raw items came from a parser that read document
  content and set `metadata.content_extraction` or `metadata.parsed_content`.
- **`failures>0`** → the fetcher raised. Check `source_failures.json` for
  the message and `run_trace.json` for the failing `fetch_source` event.
  Common cases: bot-block (Radware/Cloudflare), 403, timeout.
- **Bot-block detected** → the fetcher will now raise a `BotBlockError`
  rather than return 0 silent items. The default headers
  (`colombia_forecasting_desk.fetchers.DEFAULT_HEADERS`) include a Chrome-like
  User-Agent and Spanish Accept-Language; that gets past most static bot walls
  but not interactive challenges.

## 3. Pick the right onboarding_status

`onboarding_status` is optional. Absence means **working**. Annotate any
non-working or special-case source so the brief surfaces it cleanly.

| Status              | Meaning                                               | Skipped by fetcher?                          |
| ------------------- | ----------------------------------------------------- | -------------------------------------------- |
| `working` (default) | Source is producing rankable items.                   | No                                           |
| `needs_parser`      | Connects but parser doesn't extract usable items.     | No (still runs so source_health surfaces it) |
| `blocked`           | Permanently inaccessible (404, paywall, IP block).    | Yes                                          |
| `manual_only`       | Source must be reviewed by hand each run.             | Yes                                          |
| `disabled_future`   | Reserved for an upcoming fetcher (e.g., Socrata API). | Pair with `enabled: false`.                  |

Fetch behavior matrix:

| `enabled` | `onboarding_status`                 | Behavior                                      |
| --------- | ----------------------------------- | --------------------------------------------- |
| `false`   | any                                 | Always skip                                   |
| `true`    | absent / `working` / `needs_parser` | Run                                           |
| `true`    | `blocked` / `manual_only`           | Skip (defensive)                              |
| `true`    | `disabled_future`                   | Run (but normally pair with `enabled: false`) |

## 4. Capture an HTML fixture

Once a source is producing reasonable output, lock in current behavior with
a fixture-based test so future site redesigns don't regress silently.

```bash
uv run python - <<'PY'
import httpx, pathlib
from colombia_forecasting_desk.fetchers import DEFAULT_HEADERS

url = "https://example.gov.co/feed"
sid = "my_new_source"
date = "2026-04-29"

with httpx.Client(timeout=20.0, follow_redirects=True, headers=DEFAULT_HEADERS) as c:
    r = c.get(url)
out = pathlib.Path(f"tests/fixtures/{sid}/{date}.html")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_bytes(r.content)
print(r.status_code, len(r.content))
PY
```

Then add a test in `tests/test_fixture_parsers.py` that loads the fixture
and asserts the minimum count of dated items, plus any source-specific
invariants (a known title substring, a URL prefix, etc.).

## 5. Decide when to enable

Ready to flip `enabled: true` (and remove or keep `onboarding_status`) when:

- Sandbox run produces **at least 3 dated, rankable items**.
- Source health has an honest `content` mode. If it is link-only, document that
  limitation in `notes` and decide whether link-level coverage is enough for
  the next milestone.
- A fixture-based test pins current behavior.
- The source-health row shows `status: ok`.
- `notes` describes any quirks worth knowing for the next contributor.

For sources that connect but never produce rankable content (Corte
Constitucional SPA, Registraduría Cloudflare gate when a fresh browser session
is still challenged), keep
`onboarding_status: needs_parser` and document the underlying issue in
`notes`. The brief's source-health table will surface them every run, so
the gap stays visible without crashing the pipeline.

## 6. Source-specific parsers

Most sources work with the generic dated-anchor extractor. Write a
source-specific parser only when the page has structure the generic one
misses (a real comunicados table, a date column, an embedded calendar).
The supported import path remains `colombia_forecasting_desk.fetchers`; the
implementation now lives under `colombia_forecasting_desk/source_fetching/` so
source-family parser work can be separated without changing pipeline callers.
Current examples are split by family under
`colombia_forecasting_desk/source_fetching/`:

- `_extract_dane_comunicados` — table with date column.
- `_extract_corte_comunicados` — anchor-only listing filtered by keyword.
- `_enrich_senado_agenda_pdfs` — follows official agenda PDFs and emits
  bill-level, dated agenda entries with parsed-content metadata. Senado entries
  are M2-ready only when they have a clean project number and bill title; loose
  title-only extracts should remain research leads.
- `_extract_camara_agenda_pdf_links` / `_enrich_camara_agenda_pdfs` — discovers
  Cámara's EmbedPress agenda PDF link, downloads the official PDF, and emits
  bill-level agenda entries with `content_extraction: camara_agenda_pdf` when a
  project number is recoverable. PDFs without usable project entries stay
  link-level with `content_extraction_error`.
- `_enrich_gaceta_pdfs` — posts the official Imprenta/Gacetas JSF download
  button, extracts PDF text, and emits bill-item rows with `#project` / `#title`
  URL fragments plus parsed Gaceta project/title metadata. Committee acts that
  mention multiple unrelated bills emit one row per distinct project section;
  linked Cámara/Senado identities from the same heading remain one row. If the
  PDF exposes a clean title but no clean project number/year/chamber, keep it as
  a parsed research lead rather than a rankable candidate.
- `_enrich_diario_oficial_pdfs` — posts the official Imprenta/Diario JSF
  download button, extracts PDF text with `pdfplumber`, and emits one row per
  published legal-act heading such as `Resolución 2118 de 2025`. Referenced
  legal acts stay in metadata as references, not separate rows. Readable PDFs
  with no legal-act identities should still be marked as parsed content, but
  kept non-rankable.
- `_fetch_senado_leyes_registry` and `_fetch_camara_proyectos_ley_registry` —
  use the official public registry endpoints/pages as the primary legislative
  bill-identity/status layer, emitting parsed project number, chamber, title,
  status, date, and follow-up publication metadata before falling back to
  agenda/Gaceta document parsing.
- `_enrich_mincit_zonas_francas` — follows MinCIT's official approved-zones
  PDF and emits one structured registry row per approved zona franca. Rows are
  historical snapshot evidence; `registry_changes` promotes only new or changed
  rows across structured snapshots into fresh M1 decision signals.
- `_fetch_dian_regulatory_projects_api` — reads DIAN's official SharePoint
  `Proyectos de normas` list API and emits one dated regulatory-project row per
  draft norm, including project PDF URL, description, comment-window dates, and
  observations/annex links when available.
- `_enrich_minhacienda_tes_reports` — follows MinHacienda / IRC official COP,
  UVR, and TCO auction-result PDFs and emits parsed TES auction facts only when
  the report exposes the auction date, TES type/currency, total issued, demand,
  bid-to-cover, maturity rows, cutoff rates, per-maturity demand, and approved
  amounts. Numeric/table parsing uses `pdfplumber` when installed and fails
  closed to link-level evidence if the PDF cannot be read. The source uses a
  Playwright browser path because IRC returns 403 to simple shell fetches.
- `_fetch_minhacienda_decree_projects_with_fallbacks` — source-specific access
  chain for `minhacienda_proyectos_decreto`: direct official HTML first,
  Playwright-rendered official HTML second, then Jina Reader markdown as an
  explicit `source_access=jina_reader_proxy` fallback when Radware blocks local
  access. It emits draft-decree project rows as parsed content only when title,
  date, description/comment-window text, project PDF URL, and comment form URL
  are present; otherwise rows stay link-level with `content_extraction_error`.
- `_enrich_banrep_minutas_html` — keeps BanRep Junta/minutas under the existing
  `banrep_junta_comunicados` source, follows recent minutas detail pages, and
  adds parsed monetary-policy body metadata only when the official HTML exposes
  useful decision, vote, reasoning, or attachment context.
- `_fetch_banrep_junta_with_browser` — source-specific Playwright fallback for
  `banrep_junta_comunicados`; use only after direct HTTP returns a bot-block
  page, then feed the rendered listing and minutas detail HTML into the same
  BanRep parsers.
- `_fetch_registraduria_noticias` — uses Registraduría's official 2026 news
  archive, tries direct official HTML first, then falls back to a normal
  Playwright render when Cloudflare blocks shell fetches. It emits one row per
  `li.newsmodule` news card with comunicado number, title, date, excerpt, and
  article URL, and enriches the first few rows with article detail text when
  available. Keep source health fail-closed if fresh browser sessions are still
  challenged.
- `legal_identity.parse_legal_act_records` plus
  `decision_records.link_official_legal_records` — reusable bridge for Diario
  Oficial, SUIN, Gestor Normativo, and MinCIT rows. Use this for official
  resolution matching, but keep it conservative: a shared resolution number/year
  is not enough unless the source also contains MinCIT or named-entity context.

Wire a new extractor in `fetch_html` by source id, and prefer it to fall back
to `_extract_dated_anchors` when it returns nothing. If the parser grows beyond
a small source-specific function, move that source family into a dedicated
module under `colombia_forecasting_desk/source_fetching/` and keep
`colombia_forecasting_desk.fetchers` compatibility intact.

## 7. RawItem parser metadata contract

Source-family parsers communicate parse quality through `RawItem.metadata`.
Keep this contract stable because `source_health.json`, candidate gates,
Indicator Watch, and M2 review packets all use it to distinguish parsed
evidence from link-level coverage:

| Metadata key                 | Contract                                                                 |
| ---------------------------- | ------------------------------------------------------------------------ |
| `content_extraction`         | Stable parser id set only after the parser extracts usable record text, title, table data, or structured fields. |
| `parsed_content`             | Optional boolean for structured records that should count as parsed content; prefer pairing source-family parsers with a named `content_extraction`. |
| `content_extraction_error`   | Fail-closed reason when a parser attempts a document/source-specific read but cannot extract usable content. Do not set this alongside `content_extraction`. |
| `source_access`              | Optional access path such as `browser_official_html` or `jina_reader_proxy` when direct official HTTP was not enough. |
| `source_pdf_url` / URLs      | Preserve the official attachment or record URL so later evidence review can trace the parsed row back to the original document. |

Parser tests should cover both sides of this contract for every touched parser
path: a successful parsed row with the expected `content_extraction` value, and
a guarded failure or incomplete row that remains link-level with
`content_extraction_error`.

If the new source becomes part of Indicator Watch, keep the static card/catalog
text in `colombia_forecasting_desk/data/indicator_catalog.json` and update the
golden fixtures under `tests/fixtures/indicator_watch/` only when the resulting
card order, component defaults, or selected runtime summaries are intentionally
changed.

## 8. Document parsers

Some sources are only useful after following a PDF, spreadsheet, or attachment
link. The pipeline now reports this explicitly in `source_health.json`:

| Content mode                    | Meaning                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------ |
| `html_or_api`                   | Raw items look like HTML/API records, not downstream documents.                |
| `pdf_links_only`                | Every raw item points at a PDF, and no PDF text was parsed.                    |
| `spreadsheet_links_only`        | Every raw item points at a spreadsheet, and no spreadsheet content was parsed. |
| `document_links_only`           | Raw items point at a mix of PDF/spreadsheet/office document links.             |
| `mixed_document_and_html_links` | The source emits both page/API items and document links.                       |
| `parsed_content`                | Raw items include parsed document content metadata.                            |
| `mixed_with_parsed_content`     | Some raw items were document-parsed and some were link-level.                  |
| `no_items` / `failed`           | No raw items were available to classify.                                       |

For a document parser to count as parsed content, follow the RawItem metadata
contract above. Prefer adding one document parser at a time and keeping the
original attachment URL in `RawItem.url`.

For document-heavy sources, the proof loop is:

1. Add a small source-specific parser or enrichment function under
   `colombia_forecasting_desk/source_fetching/`.
2. Emit named raw items, not just a generic document title, when the document
   contains multiple actionable records.
3. Set `metadata.content_extraction` only after the parser extracts usable
   text/title/record content; leave `content_extraction_error` when it cannot.
4. Add a parser unit test with a realistic HTML/PDF/spreadsheet fixture shape.
5. Add a candidate-contract test when the parsed item should become M2-ready.
6. Run a live strict scan or single-source probe and inspect `raw_items.json`,
   `source_health.json`, `run_trace.json`, and `m1_candidates.json`.

For M1.15, link-only document sources can remain enabled for source-health
visibility, but they must not be promoted into `m1_candidates.json` as
forecastable candidates unless the item has a document title, parsed body text,
or another deterministic evidence excerpt. `acceptance_report.json` and
`--strict` catch this as an error when a candidate depends on a link-only
source.

For JSF/download-button sources like Gacetas, store the button name as metadata
but do not treat that as parsed evidence. Only mark `content_extraction` after
the download succeeds and the document body yields a project label, bill title,
or other usable text.

When a parsed source can corroborate another parsed source, keep the match
deterministic. For legislative records, `link_legislative_followups` only links
clean Senado agenda entries to parsed Gaceta rows when project number, year, and
chamber agree. Before expanding this into broader bill-status reconciliation,
use the dedicated
[`Legislative Reconciler Contract`](LEGISLATIVE_RECONCILER_CONTRACT.md) as the
target output shape and promotion gate.
