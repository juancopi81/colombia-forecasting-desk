# Bounded Cámara minutes capture

The Comisión Tercera `Actas y otros` page renders an empty HTML table and loads
its rows through WordPress AJAX. Reading the HTML shell alone misses the index.
`capture_camara_minutes.py` fetches fresh HTML, reads `window.AP_CFG`, and uses
that page's endpoint, commission and nonce for the read-only
`get_actas_y_otros_page` query. The nonce is never hardcoded or copied into the
report; it remains in the preserved original HTML.

Request live-network execution before running this command. Use a new output
directory for every capture, outside historical run folders during debugging:

```bash
./.venv/bin/python scripts/capture_camara_minutes.py \
  --date-from 2026-10-20 --date-to 2026-10-30 \
  --output-dir /tmp/camara-minutes-20261030-recent \
  --max-pages 2 --max-documents 1
```

For a normal daily selected follow-up, place the new capture under the current
run's sources directory, for example
`--output-dir runs/YYYY-MM-DD/sources/camara-minutes-HHMMSS`. Use today's run,
never a historical run. Keep `/tmp` for debugging and standalone probes.
The companion `receipts.json` follows the existing weekly evidence contract,
so the normal weekly builder discovers source URLs, verifies stored hashes and
retains TLS status when collecting that run's official follow-up provenance.
Its completion and individual retrieval timestamps exclude later captures from
earlier publication cutoffs. Receipts use absolute artifact paths within the
capture directory; keep that directory in place for subsequent verification.

Date bounds are inclusive `fecha_iso` index dates. Every returned row is checked
locally against those bounds. Rows outside the range, invalid dates/identities,
duplicate IDs and inconsistent pages are rejected with a notice; their raw
response remains available. `index_date` preserves the exact source date, but
`date_verified_in_document: false` makes clear that the date has not been
verified against the actual session/document. A later upload or Gaceta date is
not substituted for it.

The fixed page size is 10. Defaults allow two filtered pages and one matching
PDF; hard caps are five pages and three PDFs, including failed download attempts.
`--max-documents 0` captures only the index. The command does not search for a
bill, follow multimedia/annex/Gaceta portal links, or crawl the full archive.
All discovered request URLs and each redirect destination must be HTTPS on
`camara.gov.co` or `www.camara.gov.co`; the AJAX path is also validated. TLS
certificate verification stays enabled. Responses have size caps (3 MiB for
HTML/JSON and 25 MiB for documents), a 30-second HTTP timeout and at most three
redirects per request. A size-limited body is explicitly marked partial.

`capture.json` contains the requested filters, page totals, accepted records,
document status, notices and retrieval receipts. Each received body, including
error responses and redirects, is preserved beside it with a SHA-256 hash, UTC
retrieval timestamp, URL, HTTP status and TLS-verification policy. Stored bytes
are the HTTP content-decoded response body, before HTML/JSON/PDF parsing.
Original JSON retains secondary metadata/links. The output directory must not
already exist; captures cannot overwrite earlier evidence.
`receipts.json` exposes the same receipt timestamps, hashes and body-completeness
flags with the standard `url`, `artifact` and `tls_verified` fields;
`capture.json` remains the detailed index/document report. Neither receipt file
marks any document pages as reviewed.

Interpret `index_status` narrowly:

- `complete`: every row reported by this bounded date-range query was captured
  and passed identity/date/pagination checks. This describes the API query,
  not complete coverage of all real sessions or publications.
- `truncated`: the page cap was reached, a later page failed, totals changed,
  or some rows were malformed, duplicated or outside the requested range.
- `unavailable`: no validated response page was obtained. HTTP, nonce, non-JSON,
  config and network errors are coverage gaps.

An empty successful filtered query triggers exactly one unfiltered diagnostic
page. Its total and observed date range, including `diagnostic.date_max`, are
reported separately; those rows are never matching evidence or PDF download
targets. The latest date on that single page is only an observed diagnostic,
not proof of the archive's true latest record. A stale or empty index never
certifies absence, a vote result or NO.

Downloaded PDF bodies are `downloaded_unread`, with `text_extraction:
not_attempted`. Scanned and text PDFs alike need separate editorial review;
successful retrieval does not establish a bill identity, vote, quorum or legal
outcome. No bill status, forecast, case, ledger or resolution is changed.
Document failures remain separate from index-query completeness.

Exit 0 means a complete query with no capture notices. Exit 2 means inspect
the report (including empty results, partial coverage or failed documents).
Exit 1 means invalid arguments or a local filesystem failure. Sandbox network
failure must be rerun with live-network permission before drawing any source
health conclusion.

When the daily selected official follow-up concerns Cámara commission minutes,
capture the narrow relevant session-date range once, review the report and
matching source documents, and record unresolved coverage honestly. Preserve
the normal three-source/about-ten-minute editorial bound; do not expand to a
full historical crawl to fill a missing vote record.
