# Optional Croma research

Croma is available through the project-scoped MCP configuration in
`.codex/config.toml`. It supplements the agent's bounded official-source
follow-up; it is not part of the deterministic scanner or a required daily
check. Authentication is separate from configuration: tools are usable only
after Croma sign-in succeeds and the current Codex session loads them.

## When to use it

Use a targeted lookup when a missing document could change an interpretation
or establish an outcome under the existing forecast's resolution rules:

- Imprenta Nacional: Gaceta documents, minutes, reports and approved texts;
  Diario Oficial issue discovery. Match bill number, year and chamber.
- Corte Constitucional: complete sentencias/autos behind a comunicado.
- DIAN: published doctrine and related legal texts.
- Función Pública: a named law, decree or other legal act.

Start with one specific search and retrieve the relevant document. Stay within
the existing limit of three official sources or about ten minutes, counting
the underlying institutions separately even when Croma serves them. Read
further pages or text chunks only when necessary; report any remaining
truncation. Do not add a parallel research quota or expand the watchlist.

The configuration exposes only ten document search/read tools for these four
sources. It does not expose account connections, monitors, feedback submission,
personal-record lookups or other Croma tools.

## Evidence handling

Croma is a third-party retrieval service, not the official issuer. Save the
query, returned evidence and retrieval time in a new directory under the
current run's `sources/croma-HHMMSS/`. Preserve the tool name, document identity,
`official_url`, publication/event dates, `as_of` or `checked_at` when returned,
cache/staleness notices and pagination/text completeness. Never save tokens or
authorization headers. Cite the saved artifact with an exact locator in the
existing analysis; no new analysis fields are required.

Inspect the relevant full text and verify the official document before making
a consequential claim. A search synopsis is not the operative court order or
a certified vote. Full-text search can return documents that merely mention
the requested identifier; check the returned ID rather than assuming the first
result is the requested ruling. Dataset freshness is distinct from document publication and
the event date; retrieval today does not establish pre-cutoff availability.
If the official document cannot be verified, record that limitation and retain
the existing evidence gate.

An empty search, `found: false`, stale data, partial text, an authentication
failure or exhausted credits never establishes a forecast's NO outcome.
Existing declared resolvers, cutoffs and immutable forecasts still govern.
For PL 012/2026 Cámara, use Croma to locate possible records, then check bill
identity, committee, session date and the declared official resolver. A
later-published record may describe an earlier event; it cannot move the event
cutoff or retroactively support a forecast. Continue the required minutes
capture in the daily workflow.

## Connection and verification

From this trusted project directory, run:

```bash
codex mcp get croma
codex mcp login croma
```

Complete Croma's browser sign-in yourself. Keep credentials out of chat and
the repository; Codex manages the OAuth credentials. Reload the Codex session
if its tools have not appeared. Configuration alone is not a successful query.
After connecting, test one bounded document search and retrieval, checking
the saved text against the linked official record.

Verified on November 9, 2026: Codex reported OAuth authentication and exposed
exactly the ten configured tools. A targeted Gaceta search for
`ultraprocesados`, Cámara, bill 12/year 2026 returned zero matches with
`as_of=2026-11-08T12:21:30.034Z`; this did not resolve the food-tax forecast.
A separate court search and retrieval returned C-536/23 with 54,417 characters,
`has_more=false` and `as_of=2026-11-09T10:51:37.278Z`. Its identity, subject and
operative decision were checked against the
[official ruling](https://www.corteconstitucional.gov.co/relatoria/2023/C-536-23.htm)
in the in-app browser. This verifies connection and those retrieval paths,
not complete coverage or every tool. Existing sessions may need reopening to
load newly configured tools.

If Croma is unavailable or returns an authentication, quota or source error,
use existing official-source routes and record the gap only when it affects
the analysis. Do not retry indefinitely or purchase an upgrade automatically.

Setup references, checked November 9, 2026:

- [Croma MCP and authentication](https://docs.usecroma.com/mcp-server)
- [Croma public endpoint catalog](https://api.croma.run/catalog)
- [Dataset freshness](https://docs.usecroma.com/datasets)
- [Codex MCP project configuration](https://developers.openai.com/codex/mcp)
