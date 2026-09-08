# Spending Execution Audit

## Purpose

`spending_execution_audit.json` and `.md` are phase-one shadow artifacts. They
test whether official public-spending execution data adds reliable,
non-duplicative context to the forecasting desk before it is allowed into M2 or
M3.

The audit is diagnostic only. It does not create candidates, change ranking,
feed the agent-analysis prompt, alter strict acceptance, assign a probability,
or support a claim of fraud or misconduct.

## Sources

### SECOP II Plan de pagos

- Official dataset: [SECOP II - Plan de pagos](https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-II-Plan-de-pagos/uymx-8p3j)
- Publisher: Agencia Nacional de Contratacion Publica - Colombia Compra
  Eficiente
- Intended update frequency in the dataset metadata: daily
- Role in the audit: latest paid-day execution snapshot as of the run date

The collector first resolves the latest `Pagado` row whose real payment date is
not after the run date. It then requests only that exact day, with a 5,000-row
cap plus one sentinel row. If the sentinel is returned, the day is marked
`truncated` and monetary totals and entity rankings are withheld.

The request selects only contract/payment keys, payment and receipt dates,
payment value, entity fields, and supplier name for an in-memory distinct count.
Supplier names and raw payment rows are not persisted. Supplier and supervisor
document fields are never requested. Nonpositive values are counted as quality
signals and excluded from monetary totals. Duplicate composite payment keys,
invalid rows, or stale latest dates fail closed.

### CUIPO territorial execution

- Official dataset: [OVCF - CUIPO - Ejecucion de Gastos](https://www.datos.gov.co/widgets/4f7r-epif)
- Publisher: Contraloria General de la Republica, Observatorio de Vigilancia y
  Control Fiscal
- Role in the audit: latest reported territorial commitments, obligations, and
  payments for `Departamentos` and `Municipios`

The collector asks Socrata to aggregate the latest period server-side. It does
not download the underlying accounting rows. On later daily runs it checks the
official `rowsUpdatedAt` metadata value and reuses the previous CUIPO aggregate
when that value has not changed. This avoids repeating a large quarterly query
while still detecting dataset revisions.

CUIPO is reported administrative data. Its row counts are accounting records,
not unique entities, projects, or transactions, and a previously observed
period may be revised.

## Fail-closed behavior

The two sources fail independently. An HTTP, schema, parsing, completeness, or
freshness problem is recorded inside the audit artifact and does not crash the
normal daily pipeline. It also does not change M1 strict acceptance: a spending
audit failure is a shadow coverage gap, not evidence of quiet public spending.

SECOP totals are emitted only for a complete, non-stale latest day with valid,
unique composite payment keys. CUIPO ratios are emitted only when both expected
territorial scopes contain parseable nonnegative aggregates.

## Trial and promotion gate

Collect the artifact alongside at least 10 decision-grade weekday runs. Review:

- source availability, latency, truncation, and revision frequency;
- whether latest-day SECOP snapshots are complete often enough to be useful;
- whether CUIPO changes add information beyond existing fiscal indicators;
- whether a proposed signal can be stated neutrally and reproduced from the
  bounded artifact; and
- whether the signal creates forecastable questions rather than generic anomaly
  hunting.

Promotion requires an explicit editorial and engineering decision. It must add
the chosen fields to the relevant M2/agent contract, define falsifiers and
source-health treatment, and add regression tests. Nothing in the shadow audit
automatically promotes a row or modifies either forecast ledger.

## Daily automation decision

No scheduler or automation-prompt change is required during phase one. The
existing weekday automation already invokes `scripts/scan_metasources.py`, and
that command now writes the audit artifacts. Keeping the prompt unchanged is
intentional: the daily model must not treat these sources as forecast evidence
until the trial passes the promotion gate.
