# Global Law trial — food-tax case — 2 November 2026

**Decision: keep Global Law out of automated evidence intake.** The Colombia
collection can help locate provisions and identify the law that changed them,
but this trial found two material defects in a directly relevant example:
missing statutory tables and an in-force label that misses the tax's deferred
start. Manual discovery remains a possible use, with independent official-text
and date verification. No demonstrated research-time saving is claimed.

The trial used the desk's existing `food-tax-repeal` case. Its question was
whether the collection could reliably supply the existing tax's text and
history. This was not an investigation or resolution of the pending Cámara
committee vote. The existing forecast, case view and probability were not
changed.

**The 10:30 a.m. Bogotá automation can proceed normally.** Its saved configuration
was ACTIVE for weekday runs at 10:30. This trial made no change to its prompt,
schedule, sources, cases, ledgers, scores or daily artifacts. The downloaded
repository lives under `/private/tmp/colombia-law-trial-20261102-0933`; the
evidence bundle is under `reviews/global-law-trial-2026-11-02/`. Nothing was
installed as an MCP server or added as a dependency. The final preservation
check is in [validation.json](global-law-trial-2026-11-02/validation.json).

The upstream revision was pinned to
[`366b1d6de06965a4ac927792a09522d7ec85eac6`](https://github.com/lexifina/colombia-law/tree/366b1d6de06965a4ac927792a09522d7ec85eac6),
which was also the Colombia submodule revision returned by Global Law during
the trial. See [upstream metadata](global-law-trial-2026-11-02/upstream.json).
The official comparison used two pages fetched successfully on 2 November at
09:36 Bogotá time: SENA's consolidated Estatuto Tributario and DIAN's copy of
Ley 2277 de 2022. Original response bodies, hashes, timestamps and encoding
information are preserved in the [capture receipts](global-law-trial-2026-11-02/official-receipts.json).

| Check | Result | Observed evidence |
| --- | --- | --- |
| Retrieve the relevant current article | Pass with exact file ID | `art-0513-06` returns Article 513-6. |
| Look up the conventional article number | Usability failure | `513-6` returns an absent-on-date message for the same date and article. |
| Preserve complete operative text | Fail | Article 513-6 loses the nutrient and product-classification tables, including exclusions. |
| Preserve a simpler prose article | Pass in this sample | Article 513-9's entire rate sentence matches the official text after whitespace normalization. |
| Distinguish pre-enactment absence | Pass | The exact article is absent on 12 December 2022. |
| Handle delayed commencement | Fail | On 31 October 2023, the tool labels the article in force from 13 December 2022; the official tax commencement is 1 November 2023. |
| Identify the amending law | Pass | The history names Ley 2277 de 2022 and links an official source, agreeing with the official addition note. |

**Missing content matters for this case.** The pinned
[Article 513-6 Markdown](https://github.com/lexifina/colombia-law/blob/366b1d6de06965a4ac927792a09522d7ec85eac6/estatuto-tributario/art-0513-06.md)
introduces two tables but does not include them. The
[official SENA-hosted article](https://normograma.sena.edu.co/compilacion/docs/estatuto_tributario_pr021.htm)
contains both. For example, the product code `04.04.90.00.00` and the exclusion
for bread and wafers appear in the official table and are absent from the
repository file. Some nutrient thresholds also appear in prose later in the
article and survive extraction; that does not restore the omitted tables.
Using the Markdown alone could therefore misstate the tax's scope.

**The time lookup needs a separate commencement check.** Seven invocations of
the upstream native Colombia CLI were run, with full arguments, output, exit
codes and timings preserved in [cli-results.json](global-law-trial-2026-11-02/cli-results.json)
and its linked evidence files. The before-start query was:

```sh
python tools/en_vigor_el.py --law 2023-10-31 estatuto-tributario art-0513-06
```

It exited successfully and began:

```text
# in force from 2022-12-13, last changed by Ley 2277 de 2022 (a2753724e4)
```

There was no delayed-application caveat in the captured output. Article 96 of
[Ley 2277 de 2022](https://normograma.dian.gov.co/dian/compilacion/docs/ley_2277_2022.htm)
specifically sets 1 November 2023 as the start of the ultra-processed-food tax.
The text had been enacted earlier; a tool presenting it as in force needs to
preserve this distinction. The same 2022 in-force label appears on the actual
commencement date and in the current query. This is a failure for our intended
date-sensitive use, without implying that every date in the collection is
wrong.

The native scripts were inspected before execution and use Python's standard
library and local Git reads. No build, scraper or package installer from the
upstream repository was executed. To reproduce the CLI checks, obtain the
pinned revision with its full main-branch history, make that revision the local
`main`, and use the commands in the saved invocation records. A shallow clone
is insufficient for the historical checks. The future branch and Global Law's
MCP wrapper were not exercised.

**Usefulness and next decision.** Exact-file lookup and source attribution are
convenient, but official pages were still necessary to recover content and
correct the commencement interpretation. This one-case trial supports optional
manual navigation only. It does not justify a new daily source, automatic
forecast resolution, a backtest input, or a corpus-wide accuracy claim. No
change to the imminent automation is needed.

Reconsider automated use after the sampled tables survive extraction, the
deferred start is represented correctly, and article-number lookup distinguishes
an unrecognized identifier from a provision's historical absence. Any later
trial should pin its revision again because upstream history is rebuilt.

The [assessment](global-law-trial-2026-11-02/assessment.json) records the seven
checks and their evidence references. The [evidence manifest](global-law-trial-2026-11-02/evidence-manifest.json)
lists saved file sizes and SHA-256 hashes. These captures are observations made
today; upstream legal dates are not proof of what was available to the desk at
an earlier forecast cutoff.
