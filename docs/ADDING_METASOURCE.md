# Adding a metasource

This guide describes the M1.2 onboarding workflow: how to add a new source to
`config/metasources.yaml`, iterate on parsing, and decide when it's ready for
the daily pipeline.

## 1. Add the YAML entry

Append a new entry to `config/metasources.yaml`. Required fields:

```yaml
- id: my_new_source                # snake_case, unique
  name: My New Source              # human-readable
  url: https://example.gov.co/feed
  type: news                       # news | official_updates | calendar | legal | polling | dataset
  country_relevance: high          # high | medium | low
  access_status: rss_public        # rss_public | html_public | api_public | paywalled | blocked | manual_only
  fetch_method: rss                # rss | html | api (api is reserved for future use)
  priority: medium                 # high | medium | low
  update_frequency: daily          # daily | weekly | event_driven | monthly
  trust_role: media_signal         # official_signal | media_signal | polling_signal | resolution_source | agenda_signal | civic_signal | legal_signal
  parsing_difficulty: easy         # easy | medium | hard
  enabled: false                   # leave disabled until a sandbox run looks healthy
```

Optional:

| Field | Purpose |
| --- | --- |
| `notes` | One- or two-sentence context. Mention quirks, redirects, bot walls. |
| `max_items` | Cap raw items per run (useful for noisy media feeds). |
| `verify_ssl` | Set `false` only when a public source has a broken cert chain. |
| `onboarding_status` | See section 3. |

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
  first 5 cleaned items:
    - [2026-04-27T11:00:00Z] Title 1...
```

What to look at:

- **`raw=0`** with no failure → the parser found nothing (likely a JS-rendered
  page or a navigation hub). Capture the HTML and inspect.
- **`raw>0` but `rankable=0`** → items are cleaned but lack publication dates,
  or are flagged low-quality. Open `cleaned_items.json` and check the
  `quality_notes` field.
- **`failures>0`** → the fetcher raised. Check `source_failures.json` for
  the message. Common cases: bot-block (Radware/Cloudflare), 403, timeout.
- **Bot-block detected** → the fetcher will now raise a `BotBlockError`
  rather than return 0 silent items. The default headers
  (`fetchers.DEFAULT_HEADERS`) include a Chrome-like User-Agent and Spanish
  Accept-Language; that gets past most static bot walls but not interactive
  challenges.

## 3. Pick the right onboarding_status

`onboarding_status` is optional. Absence means **working**. Annotate any
non-working or special-case source so the brief surfaces it cleanly.

| Status | Meaning | Skipped by fetcher? |
| --- | --- | --- |
| `working` (default) | Source is producing rankable items. | No |
| `needs_parser` | Connects but parser doesn't extract usable items. | No (still runs so source_health surfaces it) |
| `blocked` | Permanently inaccessible (404, paywall, IP block). | Yes |
| `manual_only` | Source must be reviewed by hand each run. | Yes |
| `disabled_future` | Reserved for an upcoming fetcher (e.g., Socrata API). | Pair with `enabled: false`. |

Fetch behavior matrix:

| `enabled` | `onboarding_status` | Behavior |
| --- | --- | --- |
| `false` | any | Always skip |
| `true` | absent / `working` / `needs_parser` | Run |
| `true` | `blocked` / `manual_only` | Skip (defensive) |
| `true` | `disabled_future` | Run (but normally pair with `enabled: false`) |

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
- A fixture-based test pins current behavior.
- The source-health row shows `status: ok`.
- `notes` describes any quirks worth knowing for the next contributor.

For sources that connect but never produce rankable content (Cámara agenda
hub, Corte Constitucional SPA, Registraduría Cloudflare gate), keep
`onboarding_status: needs_parser` and document the underlying issue in
`notes`. The brief's source-health table will surface them every run, so
the gap stays visible without crashing the pipeline.

## 6. Source-specific parsers

Most sources work with the generic dated-anchor extractor. Write a
source-specific parser only when the page has structure the generic one
misses (a real comunicados table, a date column, an embedded calendar).
Examples in `colombia_forecasting_desk/fetchers.py`:

- `_extract_dane_comunicados` — table with date column.
- `_extract_corte_comunicados` — anchor-only listing filtered by keyword.

Wire a new extractor in `fetch_html` by source id, and prefer it to fall back
to `_extract_dated_anchors` when it returns nothing.
