# Legislative Reconciler Contract

This document defines the expected output for the next legislative hardening
milestone. It is intentionally written before implementation so code, tests,
and agent work all aim at the same target.

## Plain-English Goal

The system already sees legislative signals from several places: Senado,
Cámara, Gacetas del Congreso, Diario Oficial, and sometimes news. The problem is
that the same bill can appear with slightly different names, numbers, chamber
labels, or status wording.

The reconciler should answer a simple question:

> Are these records talking about the same bill, and is there still an
> unresolved public decision worth forecasting?

In non-technical terms, it works like a desk researcher who checks whether a
bill mentioned in one place is the same bill mentioned somewhere else, then
summarizes where that bill stands today.

## Basic Terms

- **Bill / proyecto de ley:** a proposal being discussed by Congress. It is not
  a law yet.
- **Project number:** the tracking number for a bill in one chamber, for example
  `560 de 2025 Cámara`.
- **Chamber:** either Cámara or Senado. A bill may have one number in Cámara and
  another in Senado as it moves.
- **Gaceta:** the official Congressional publication where bill texts,
  ponencias, reports, and other legislative documents are published.
- **Registry:** the official Senado or Cámara tracking page for a bill.
- **Latest movement:** the newest official action we can identify, such as a
  published ponencia, agenda listing, approval, archive, or final publication.
- **Contradiction:** two credible official sources appear to disagree about an
  important fact, such as whether the bill is active or archived.

## What The Reconciler Should Produce

The reconciler should emit one record per bill identity. It should not emit one
record per article, PDF, or source row.

Each output record should follow this shape:

```json
{
  "schema_version": "legislative_reconciler.v1",
  "canonical_bill_id": "bill:2025:camara:560",
  "display_title": "Proyecto de Ley 560 de 2025 Cámara - subsidio al transporte de GLP para San Andrés, Providencia y Santa Catalina",
  "title_normalized": "subsidio al transporte de glp para san andres providencia y santa catalina",
  "origin_project": {
    "chamber": "camara",
    "number": "560",
    "year": "2025"
  },
  "linked_projects": [
    {
      "chamber": "camara",
      "number": "560",
      "year": "2025",
      "source_id": "camara_proyectos_ley_registry",
      "url": "https://..."
    }
  ],
  "status": {
    "stage": "active",
    "label": "En trámite",
    "as_of": "2026-05-18T00:00:00Z",
    "source_id": "camara_proyectos_ley_registry",
    "url": "https://..."
  },
  "latest_movement": {
    "date": "2026-05-17T00:00:00Z",
    "action_type": "ponencia_publicada",
    "label": "Ponencia publicada en Gaceta del Congreso",
    "source_id": "gacetas_congreso",
    "source_name": "Gacetas del Congreso",
    "url": "https://...",
    "gaceta_number": "485"
  },
  "source_evidence": [
    {
      "source_id": "camara_proyectos_ley_registry",
      "role": "identity_status",
      "date": "2026-05-18T00:00:00Z",
      "url": "https://...",
      "summary": "Registry row with project number, title, chamber, and status."
    },
    {
      "source_id": "gacetas_congreso",
      "role": "movement",
      "date": "2026-05-17T00:00:00Z",
      "url": "https://...",
      "summary": "Parsed Gaceta item with project number and ponencia title."
    }
  ],
  "contradiction": {
    "has_contradiction": false,
    "severity": "none",
    "fields": [],
    "summary": ""
  },
  "decision_state": "unresolved",
  "m2_readiness": {
    "state": "ready",
    "reason": "Clean project number, title, active status, latest official movement, and plausible official resolution source are available.",
    "missing": []
  }
}
```

Some records may also include `resolved_status_override` when a prior manual
reconciliation has resolved a narrow official-record contradiction. This is not
new source evidence; it is desk memory that says "we already checked this
specific artifact pattern."

## Required Fields

- `schema_version`: fixed string for this contract version.
- `canonical_bill_id`: stable internal ID. Prefer exact chamber, number, and
  year: `bill:<year>:<chamber>:<number>`.
- `display_title`: human-readable label for analysts and M2.
- `title_normalized`: lowercase, accent-folded title used for comparison, not
  for display.
- `origin_project`: the cleanest known project identity from the originating
  chamber.
- `linked_projects`: all clean Cámara/Senado project identities we can attach
  with evidence.
- `status`: latest known official status from a registry or official source.
- `latest_movement`: newest official action that explains why the bill matters
  now.
- `source_evidence`: compact evidence trail for the identity, status, and
  movement.
- `contradiction`: whether official sources disagree in a way a human must
  inspect.
- `decision_state`: one of `unresolved`, `resolved`, `archived`, or `unknown`.
- `m2_readiness`: whether this bill can become a forecast question candidate.
- `resolved_status_override` (optional): manual reconciliation metadata when a
  tracked override suppresses a previously reviewed hygiene contradiction.

## M2 Readiness States

Use exactly one of these values for `m2_readiness.state`:

- `ready`: the bill has clean identity, clean title, an unresolved decision, a
  latest official movement, and a likely official source for resolution.
- `research_lead`: the item is interesting, but identity or status is not clean
  enough to forecast yet.
- `blocked`: the source evidence is contradictory or missing enough that M2
  should not rank it until a person investigates.
- `resolved`: the decision already happened, so it is evidence or history, not a
  new forecast candidate.

## Promotion Rules

The reconciler may promote a bill to `ready` only when all of these are true:

- At least one clean project number, year, and chamber exists.
- A meaningful bill title exists.
- The latest status is not clearly final, archived, withdrawn, sanctioned, or
  already published as law.
- At least one official source gives the current status.
- At least one official source gives the latest movement or follow-up action.
- A future resolution source is plausible, such as the same registry, Gacetas,
  Diario Oficial, or another official page.
- There is no unresolved material contradiction.

If a match depends only on similar title text, it can create `research_lead`,
but it must not create `ready`.

## Contradiction Rules

Set `contradiction.has_contradiction` to `true` when official sources disagree
about a fact that changes the forecast question. Examples:

- One source says the bill is active while another says it is archived.
- A Gaceta appears to describe a later action than the registry status admits.
- Cámara and Senado records appear to have the same title but incompatible
  numbers, years, or legislative periods.
- A source describes a final act, while another still presents the bill as
  pending.

Contradictions are useful. They should not be hidden. A contradiction can become
a public-interest research question, but it should block M2 ranking until the
evidence is reconciled or explicitly framed as the question.

Manual reconciliation overrides live in
`colombia_forecasting_desk/data/resolved_status_overrides.json`. Use them
sparingly, only after a human-reviewed note explains why a recurring source
pattern is not a live procedural movement. Overrides are condition-gated: for
example, an archived bill plus a later Gaceta project-text publication can be
marked resolved, while a later ponencia, agenda, debate result, transfer,
correction, archive reversal, or Diario Oficial item should still surface for
review. By default, an override only applies to a detected contradiction. A
human-verified official status may set `require_contradiction: false` when the
run has only `unknown` status evidence; it must still constrain the current
status stage and allowed movement types, and should include `status_override`
provenance for the verified official record.

## Non-Goals

The reconciler should not:

- Guess bill identity from title similarity alone.
- Treat media articles as official status.
- Promote already-final Diario Oficial legal acts as unresolved forecast
  candidates.
- Hide uncertainty behind a green status.
- Replace human editorial judgment about whether a question is worth publishing.

## Golden Test Scenarios

Implementation should include fixture-backed tests for these cases:

1. **Clean Cámara registry plus Gaceta movement**
   - Same number, year, and chamber.
   - Output is `ready` if status is active and latest movement is official.

2. **Clean Senado registry plus Gaceta movement**
   - Same number, year, and chamber.
   - Output links both sources and keeps the registry as status authority.

3. **Agenda or PDF row without a clean number**
   - Output is `research_lead`, not `ready`.

4. **Title-only fuzzy match**
   - Output may include a possible match note, but `m2_readiness.state` is
     `research_lead` or `blocked`.

5. **Registry/Gaceta contradiction**
   - Output sets `has_contradiction=true` and `m2_readiness.state=blocked`.
   - If a matching manual override applies, output clears the contradiction,
     records `resolved_status_override`, and sets readiness to `resolved`.
   - If the later movement is substantive, such as a ponencia, the override
     does not apply and the contradiction remains blocked.

6. **Already-final act**
   - Output sets `decision_state=resolved` and `m2_readiness.state=resolved`.

7. **Cross-chamber identity**
   - If a Cámara bill later has a Senado number, preserve both in
     `linked_projects` and explain the match basis in `source_evidence`.

## Implementation Boundary

This contract is the prerequisite. The next code milestone should implement the
record builder, add the golden tests above, then rerun the daily workflow to see
whether legislative candidates become cleaner and less noisy.
