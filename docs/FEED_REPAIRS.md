# Monetary and labor feed repairs — October 27, 2026

These changes affect only `policy_rate_ibr`, `labor_market` and
`banrep_eme_expectations`. The live verification runs in an isolated temporary
run folder, preserving the original daily artifacts.

## Policy rate and IBR

SUAMECA's leaf certificate was replaced on September 11, 2026. The server does
not send its GeoTrust EV RSA CA G2 intermediate, so both the Python default
bundle and a default system SSL context fail with “unable to get local issuer
certificate.” The official alternate host `totoro.banrep.gov.co` does not expose
the same REST route (404).

`banrep_tls.py` supplies the public intermediate from
[DigiCert's issuer distribution](https://cacerts.digicert.com/GeoTrustEVRSACAG2.crt)
to an otherwise normal SSL context. The certificate is signed by DigiCert Global
Root G2; its signature was independently checked against the existing default
CA bundle. It expires July 2, 2030. Certificate and hostname verification remain
required, and partial-chain trust is disabled. This context is used only for
policy/IBR and the EME portal. The experimental TES feed is unchanged.

The official series API also contains nested timestamp/value history. The
policy/IBR parser now reads those points, so an as-of date cannot accidentally
inherit the current header's value. On October 27 the verified API returned a
12.25% policy rate and an 11.412% nominal overnight IBR. Both original official
series URLs remain in the card's values.

## Labor market

The current DANE page starts “Para el mes de…” and uses direct prior-year
comparisons instead of only the old “disminución/aumento de…” wording. The
parser now accepts both layouts and limits national comparisons to their own
paragraph. In particular, it cannot mistake the occupation-rate comparison for
an unemployment comparison or borrow a national value from the city paragraph.

The page also contains RELAB documents published later than the GEIH release.
GEIH's release date and document list are now read from its own reference-month
rows. The live August 2026 observation is national unemployment 9.4%, versus
8.6% in August 2025, released September 30. The card explicitly records
`national_unemployment_previous_year_period: 2025-08` and
`series_variant: national_total_original`. That comparator does not establish
September 2025 unemployment and must not be used for a September forecast.

## Analyst survey

The old website archive stops at June 2026, and its detail page redirects HTTP
requests to a Radware block. The configured source now uses the official
[SUAMECA survey menu](https://suameca.banrep.gov.co/estadisticas-economicas-back/rest/estadisticaEconomicaRestService/consultaMenuXopcion?opcion=ENCUESTAS)
to discover the exact current dated XLSX, then parses the published aggregate
statistics. It does not guess filenames or solve browser challenges. The
October workbook changed the headline inflation section label; both labels are
now accepted by the existing label-based parser.

The current workbook has fieldwork October 8–13 and an October 31 policy-rate
median of 12.5%. The portal's document date is 2024 migration metadata, not this
survey's publication date. Therefore `published_at` and metadata `release_date`
remain null; `publication_date_known` is false. Freshness is conservatively
measured from fieldwork end and labeled `freshness_basis: fieldwork_end`.
HTTP file modification time is retained separately, never called publication.

The survey statistics are point estimates. They contain no probability
distribution usable for the YES/NO forecast baseline; the median must never be
converted into a probability. The existing empirical or 50/50 fallback applies.

## Verification

Offline regression inputs and exact source locations are under
`tests/fixtures/feed_repairs_20261027/`. They include the downloaded aggregate
October EME workbook, policy/IBR API rows, and the relevant GEIH rows plus a
newer RELAB row to guard the date-mixing bug. Tests make no network calls.

The complete offline suite passed **756 tests** (100 focused feed/parser tests
also passed). `git diff --check` passed.

A fresh strict scan on October 27 passed acceptance with zero errors and two
warnings. The policy/IBR and labor cards were both `observed` and current. EME
produced one parsed current-survey item with zero source failures. It remains
`no_rankable` because its exact publication date is unknown; this is usable
consensus context, not an automatically promoted forecast lead. The unrelated
SECOP II process and SUIN source failures remain visible. The isolated evidence
folder is `/private/tmp/cfd-c-live-scan/2026-10-27`; the original October 27 run
was not overwritten.
