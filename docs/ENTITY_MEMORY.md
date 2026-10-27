# Entity memory and personal watchlist

`config/watchlist.yaml` is the user-editable entry point. It supports companies,
public entities, places, topics, bills and decrees. The two active entries are
explicit demonstrations: the Department of Antioquia and Colombia
Telecomunicaciones. They are not inferred personal interests. Other kinds have
commented examples. Private individuals are not a supported watch kind.

## Commands

Fetch organization history independently of the daily scan:

```bash
./.venv/bin/python scripts/build_entity_history.py --date 2026-10-27
./.venv/bin/python scripts/build_entity_history.py --date 2026-10-27 --watch-id demo-antioquia
```

This writes local caches under `data/entity_history/` and readable reports under
`reviews/entity-<watch_id>-<date>.md`. Caches are ignored by Git. Reports are
uncommitted review artifacts, not publication. `--offline` renders existing
caches without network requests; it does not refresh their dates. `--cache-dir`,
`--reviews-dir` and `--watchlist` select explicit destinations.

The daily matcher is entirely offline:

```bash
./.venv/bin/python scripts/watchlist_hits.py --date 2026-10-27
```

It writes `watchlist_hits.json` and `.md` in that day's run directory and imports
the frozen shared validator. `--runs-dir`, `--cache-dir`, `--watchlist` and
`--metasources` support isolated runs. A later-fetched cache is excluded from an
earlier run. Older caches keep their actual as-of and fetched dates, with an
explicit coverage note; the matcher never invents a refresh.

## Identity and matching

NIT normalization removes punctuation and separates an explicitly supplied
check digit, validating it with the standard modulus-11 check. A bare long ID is
ambiguous and is not automatically shortened. A configured, verified company
base allows its exact base-plus-check-digit alias to match. The verified demo
company appears as both `830122566` and `8301225661` in SECOP II, so both forms
are included in its known-identity queries.

Before screening grouped counterparties, possible base/check-digit aliases are
withheld when their distinct-contract overlap is unknown. Their counts are not
summed, and they cannot produce two shares or duplicate pattern IDs for one
potential organization. This is a coverage gap, not proof that either ID is a
different legal entity.

Names are compared with accent, punctuation and common legal-suffix
normalization. Exact NIT, bill/chamber/year, decree/year and DIVIPOLA matches
take precedence. Name and keyword matches are explicitly labeled candidates.
Names alone do not merge entities or establish wrongdoing.

Places use the official DANE-supplied
[DIVIPOLA dataset](https://www.datos.gov.co/resource/gdxc-w37w.json). Its codes,
publisher attribution, actual update vintage and retrieval time are cached;
the matcher retains that provenance. Without the catalog it discloses the gap.
Municipality-name matches remain candidates, particularly for ambiguous names.

The matcher reads raw and cleaned official items, legislative records,
indicator cards, analyst leads and the review packet. This covers SECOP,
MinCIT zones, Diario Oficial, Gacetas, MinHacienda decrees and bills without
changing collection. Media/polling sources are excluded by their configured
trust role. Repeated artifact copies of the same source URL are deduplicated,
while distinct official document fragments remain separate records.

## History and quantitative screens

The default window is 24 calendar months of recorded contract signatures.
SECOP I and II are reported separately and never added together as though their
records had been mutually deduplicated. Query URLs, parameters, retrieval time,
row counts and failure/truncation status are retained for checking every number.

- [SECOP II contracts](https://www.datos.gov.co/resource/jbjy-vk9h.json): buyer
  `nit_entidad` is numeric; supplier `documento_proveedor` is text. Queries use
  the correct native types. Dates are `fecha_de_firma`.
- [SECOP I](https://www.datos.gov.co/resource/f789-7hwg.json): buyer and supplier
  identifiers are text. Dates are `fecha_de_firma_del_contrato`. Records uploaded
  in the window without a signature date are counted separately, excluded from
  signed-contract totals, and cannot establish contract timing.
- [SECOP II additions](https://www.datos.gov.co/resource/cb9c-h8sn.json): this
  dataset has contract IDs and event descriptions, but no monetary amounts.
  Addition counts are linked only to captured contract IDs. The configured ID
  bound and incomplete linkage are disclosed. Amounts are never guessed from
  descriptions.
- [Sanctions](https://www.datos.gov.co/resource/4n4q-k399.json): the dataset has no
  contractor document-type field. Public-entity queries remain counts without
  contractor identities. Identity-level post-sanction matching runs only for a
  configured known company NIT, using a recorded firm date and later contract
  signatures. Sanction scope and validity still require review.

Screens cover supplier shares of distinct contracts or reported value, repeated
direct awards, SECOP I monetary additions relative to original recorded value,
and later contracts for a configured supplier with a recorded sanction. They
are review prompts. Direct contracting is legal for many contract types.

Each contract summary includes reported-row sums, distinct ID counts, maximum
values and the exact population median of reported values. The median uses the
nonnull population count and an ordered one/two-row query at its midpoint;
there is no sample median masquerading as a population estimate. Duplicate
source rows are disclosed, and value-share screening is suppressed when row
counts and distinct contract counts differ. Large values are flagged using a
configurable threshold.

Natural-person/unknown document types are aggregated on the server, without
grouping by their names or IDs. Only organizational NIT groups enter supplier
history. Contract text is replaced with a generic description in daily hits so
an incidental private name is not copied into watchlist output. Tests use
synthetic organization data and no real personal names.

## Why the zona-franca screen yielded no leads

The October 27 audit found approved-zone rows in 118 historical runs (13,302
raw rows), across three snapshot dates. Five raw change events exist on June 1
and August 20, but none of their IDs survives into `cleaned_items.json`.

`dedupe._canonical_item_url` strips document fragments unless the record has
one of its explicit semantic row types. MinCIT approved rows and their appended
change events do not have one of those exceptions, so the shared PDF URL keeps
the first row and displaces the remaining `#zf-*` rows/change events. The
zona-franca lead builder requires a cleaned change event; it cannot receive
these discarded IDs. Stable snapshots also produce no new change event by
design.

This workstream documents that collection-to-lead loss without changing the
daily scan. The watchlist instead reads the raw MinCIT registry rows directly,
preserving their fragment identities and matching their NITs/places. A registry
match is historical context, not evidence of a new land-use decision. A separate
future collection change would need to preserve row/change identity explicitly.

## Validation

Focused tests cover identifier ambiguity/check digits, exact legal and place
matching, candidate labels, future-cache exclusion, contract-text privacy,
population medians, native numeric/text query types, source failures,
truncation, duplicate rows, and withholding aliases with unknown overlap.
All **13 focused tests** and the complete **787-test suite** passed on this
workstream branch. `git diff --check` passed.

The October 27 live demos completed with no failed queries. In the 24-month
signature window, Antioquia had 2,696 SECOP II contract IDs and 74 SECOP I rows
covering 38 distinct IDs. Eight SECOP I uploads lacked a signature date. The
company demo had 26 SECOP II contract IDs and 32 linked addition events. Exact
population medians were obtained. These are dated snapshots, not fixed current
statistics.

Coverage is deliberately incomplete where reported: Antioquia's supplier groups
and addition-link contract IDs reach their 500-record bound; ambiguous and
possible base/check-digit alias groups are withheld. The company also has
ambiguous buyer groups withheld. No query failure or omitted group is counted
as evidence of absence. The offline October 27 matcher produced 24 candidate or
identifier matches, one review prompt and eleven coverage notes on copied run
artifacts. The original daily artifacts and scan were unchanged.
