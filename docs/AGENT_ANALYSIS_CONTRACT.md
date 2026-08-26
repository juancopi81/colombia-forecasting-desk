# Agent Analysis Contract

`agent_analysis.json` is the accountable output of the daily LLM intelligence
pass. It sits after deterministic M1/M2 artifacts and before any M3 case-file or
forecast-log decision.

The artifacts are:

```text
runs/YYYY-MM-DD/agent_analysis.json  # authored by the reviewing LLM
runs/YYYY-MM-DD/agent_analysis.md    # rendered deterministically from JSON
```

This pass forces analysis, not publication. It may recommend an M3 case or
record one internal shadow forecast. It must never publish, update
`forecasts/forecast_log.jsonl`, or treat a deterministic screen as a conclusion.

## Required JSON Shape

```json
{
  "schema_version": "agent_analysis.v1",
  "run_date": "2026-08-11",
  "model": "gpt-5.6-sol",
  "reasoning_effort": "high",
  "strongest_changed_signal": {
    "signal": "What materially changed or now matters most.",
    "evidence_refs": [
      {
        "artifact": "indicator_watch.json",
        "locator": "indicator_id=ipc_inflation",
        "note": "Why this reference supports the analysis."
      }
    ],
    "interpretation": "The best current interpretation.",
    "alternative_explanations": ["A serious competing explanation."],
    "falsifiers": ["Evidence that would weaken or reverse the interpretation."]
  },
  "tension_card_reviews": [
    {
      "card_id": "real_policy_rate",
      "assessment": "What the triggered contrast does and does not imply.",
      "forecast_implication": "Whether it creates an M3, shadow, insight, or no-action path.",
      "disposition": "shadow_candidate"
    }
  ],
  "relationships": [
    {
      "type": "cross_bundle",
      "relationship": "A relationship the model noticed across inputs.",
      "evidence_refs": [
        {
          "artifact": "cooccurrence_bundles.json",
          "locator": "bundle_id=monetary_credit_transmission"
        }
      ],
      "interpretation": "Why the relationship could matter.",
      "caveats": ["Why the relationship may be spurious or incomplete."]
    }
  ],
  "official_source_follow_up": {
    "candidate": "The one candidate researched after initial review.",
    "bounded_scope": "The explicit research limit used.",
    "sources_checked": [
      {
        "source_id": "banrep_junta_calendar",
        "url": "https://www.banrep.gov.co/",
        "result": "What the official source established or failed to establish."
      }
    ],
    "result": "The resulting decision-grade conclusion."
  },
  "public_interest_candidate": {
    "candidate": "The best public-interest question or claim considered.",
    "disposition": "research_more",
    "rationale": "Why it belongs in this lane.",
    "evidence_refs": [
      {
        "artifact": "m2_review_packet.json",
        "locator": "packet_item_id=example"
      }
    ],
    "missing_evidence": ["The exact evidence still needed."]
  },
  "shadow_forecast": null,
  "overall_disposition": "insight_only",
  "overall_rationale": "Why this is the correct final lane for the run."
}
```

## Allowed Dispositions

Overall disposition:

- `promote_to_m3`: the public-interest candidate should enter the existing M3
  case-file workflow. Its candidate disposition must also be `promote_to_m3`.
- `shadow_track`: record one complete internal forecast for calibration. This
  is not approval to publish or add it to the selected forecast log.
- `insight_only`: preserve a source-backed interpretation without forcing a
  probability.
- `abstain`: record why neither an insight nor a clean forecast is defensible.

Tension-card review disposition:

- `promote_to_m3`
- `shadow_candidate`
- `insight_only`
- `no_action`

Public-interest candidate disposition:

- `promote_to_m3`
- `research_more`
- `insight_only`
- `reject`
- `none`

When the run directory is available, validation compares
`tension_card_reviews[].card_id` with every card in
`indicator_tension_cards.json`. Every triggered card must be reviewed exactly
once, and reviews may not invent untriggered card IDs.

## Relationship Requirement

At least one relationship is mandatory. Its type is either:

- `cross_bundle`: connects signals already organized in different bundles.
- `unbundled`: records a relationship outside the predefined bundle vocabulary.

This requirement makes bundles an aid rather than the boundary of model
reasoning. Each relationship needs evidence, interpretation, and a caveat.

## Bounded Official-Source Follow-Up

The model must research one candidate after its initial artifact review. Record
the candidate, the explicit search limit, every official source checked, and
the result. A missing or blocked result is valid when stated accurately; source
silence is not negative evidence.

The authoring prompt currently recommends no more than three official sources
or ten minutes. That is an execution budget, not a validation rule.

For Corte Constitucional cases, distinguish an official communication from the
complete written sentencia/auto. A communication may support an
`insight_only` public-interest candidate when it reports consequential
implementation or correction timing. It cannot support an exact compliance
deadline, `promote_to_m3`, or a Court-implementation shadow forecast until the
written ruling and operative order are cited.

## Shadow Forecast Contract

`shadow_forecast` must be `null` unless `overall_disposition` is
`shadow_track`. A shadow forecast requires:

- `id`
- `visibility: internal`
- exact binary `question`
- `probability` from 0 to 1
- `prediction_date` and `resolution_date` in `YYYY-MM-DD` format
- `official_resolver.source_name` and `official_resolver.source_url`
- explicit `resolution_criteria.yes` and `resolution_criteria.no`
- `baseline.name`, `baseline.probability`, and `baseline.rationale`
- artifact-backed `evidence`
- non-empty `rationale_for` and `rationale_against`
- a concrete `falsifier`

The prediction date must equal the analysis run date, and the resolver date may
not precede it. Shadow forecasts remain outside `forecasts/forecast_log.jsonl`;
only a separate validated shadow-ledger step may persist or resolve them.

The baseline name must describe how its probability was obtained. Prefer an
empirical frequency, official consensus, or computed persistence rate. When no
informative comparator exists, use `uninformative_50_50` with probability 0.50
and an explicit rationale. A threshold matching the latest observation does not
by itself justify calling a 0.50 comparator a persistence baseline.

Resolution persists the model Brier score, baseline Brier score, and baseline
minus model improvement. Positive improvement means the model scored better.
These are descriptive experiment outputs, not evidence of calibration from a
small sample.

## Validation And Rendering

Validate one or more authored JSON files:

```bash
./.venv/bin/python scripts/validate_agent_analysis.py \
  runs/YYYY-MM-DD/agent_analysis.json
```

Render only after validation succeeds:

```bash
./.venv/bin/python scripts/render_agent_analysis.py --date YYYY-MM-DD
```

The render command writes `runs/YYYY-MM-DD/agent_analysis.md`. It exits nonzero
and writes nothing when the JSON or tension-card coverage is invalid.

## Workflow Boundary

```text
deterministic M1/M2 artifacts
  -> LLM intelligence pass
  -> agent_analysis.json
  -> validation
  -> agent_analysis.md
  -> human review
  -> optional existing M3 workflow
```

The validator and renderer do not call an LLM or use the network. The normal
daily handoff uses one idempotent command:

```bash
./.venv/bin/python scripts/finalize_agent_analysis.py --date YYYY-MM-DD
```

It validates and renders the analysis, optionally appends one internal row to
`forecasts/shadow_forecast_log.jsonl`, refreshes the deterministic 10-run
summary, and rerenders the HTML. It never writes the public forecast log.
