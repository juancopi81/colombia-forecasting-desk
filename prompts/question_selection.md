# Prompt: Question Selection

You are running M2 question selection for Colombia Forecasting Desk.

Input may include `m2_handoff.md`, `m2_review_packet.md`,
`market_pricing_watch.md`, `cooccurrence_bundles.md`,
`indicator_tension_cards.md`, and `analyst_leads.md`. Use only the provided
evidence unless you explicitly label a gap as missing evidence. Do not browse,
estimate probabilities, draft posts, or give investment, trading, betting, or
execution advice.

## Goal

Turn the M1 handoff into a ranked list of forecastable questions worth deeper
research.

Prefer questions that are:

- important for Colombian politics, economics, regulation, institutions, energy,
  fiscal conditions, elections, or public order
- precise enough to resolve as yes/no or with a numeric threshold
- unresolved at the handoff timestamp
- tied to a primary resolution source
- answerable with public, non-paywalled evidence
- useful even before full automation exists

Prefer public-interest hooks over merely clean indicator continuation. A
question is more valuable when it has at least one of these shapes:

- `decision_pending`: a permit, decree, reform, bill, court case, regulatory
  approval, land-use decision, zona franca decision, or administrative act may
  resolve by a date.
- `cost_pressure`: materials, construction costs, cement, energy, fuel, TRM,
  imports, logistics, or other input costs may rise enough to matter.
- `contradiction`: two credible sources, indicators, or official narratives
  point in different directions and a later source can clarify the gap.
- `named_entity_path`: a company, municipality, project, bill, agency, court, or
  regulator is tied to a clear next institutional step.
- `public_consequence`: the outcome matters for households, firms, public
  finances, elections, public services, or institutional credibility without
  becoming personalized advice.

Treat Co-Occurrence Bundles as neutral context packaging. They can help you
notice related ingredients, but they are not thesis labels, hard filters,
forecast questions, or probability inputs. Always inspect cross-bundle links,
alternative explanations, and important unbundled items before deciding what to
select.

Treat Market Pricing Watch as experimental context only. ADR, ETF, and Brent
rows can help you ask whether market pricing aligns with official evidence,
contradicts it, or is unrelated, but they are not advice, conclusions, ranking
signals, or probability inputs. Endpoint failures and stale closes are
source-health caveats, not evidence that nothing moved.

Examples: "Will this project/land/company become a zona franca?", "Will
construction or material costs increase again?", "Two official or credible
sources appear to conflict; which one resolves, and what explains the gap?",
"Will a named bill/decree/regulatory proposal advance before a deadline?"

Reject questions that are:

- vague, subjective, or framed as "will this matter?"
- already resolved by the handoff evidence
- mostly human-interest, curiosity, or isolated local crime items
- dependent on private, paywalled, or login-required evidence
- missing a plausible resolution source or deadline/window
- a legislative agenda item without a clean project number, bill title, and
  follow-up source for resolution
- actually investment, trading, betting, or execution recommendations

## Scoring

Score each candidate from 1 to 5:

- `interest_score`: importance to Colombia-focused forecasting.
- `forecastability_score`: clarity of event, threshold, and resolution.
- `evidence_score`: enough public evidence exists for an evidence pack.
- `freshness_score`: timely and unresolved.
- `risk_score`: legal, reputational, safety, or misuse risk; 5 is highest risk.

Use the scores to select the top 1-3 questions for evidence-pack research.
When two candidates have similar scores, prefer the one with a stronger
public-interest hook and clearer "why should a reader care?" story. Avoid
selecting another routine next-data-point forecast when a decision, cost
pressure, contradiction, or named institutional path is available.
High risk does not automatically reject a question, but explain the risk and
only select it if the public-interest value is strong.

## Required Output

Return exactly these sections:

```markdown
## Selected Questions

### 1. [question]

- decision: select_for_evidence_pack
- why_now:
- interest_score:
- forecastability_score:
- evidence_score:
- freshness_score:
- risk_score:
- resolution_source:
- deadline_or_window:
- starting_evidence:
- missing_evidence:
- rejection_risks:

## Other Candidate Questions

### [question]

- decision: reject | maybe_later
- reason:
- scores: interest=?, forecastability=?, evidence=?, freshness=?, risk=?
- what_would_make_it_better:

## Source Caveats

- ...

## Evidence-Pack Queue

1. [question] — [first sources to inspect]
```

Keep wording concrete. If a promising question needs one missing fact before it
can be selected, put it under `maybe_later` and name that fact.
