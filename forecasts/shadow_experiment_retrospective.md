# Shadow Forecast Experiment Retrospective

**Experiment status:** `completed`
**Run window:** `2026-08-12` through `2026-08-25`
**Decision-grade runs:** `10 / 10`

## Decision

Retain the daily intelligence pass and the protected shadow-forecast lane.
The experiment improved review discipline, abstention, official-source
follow-up, and artifact quality control. It did not produce enough independent
forecast outcomes to establish calibration or general predictive skill.

Continue collecting shadow forecasts opportunistically. Do not extend the
10-run counter, create a forecast quota, lower the public M3 gate, or promote a
shadow forecast automatically. Review the process again after 10 additional
resolved shadows with broader variation in source family, domain, and horizon.
Calibration claims require a substantially larger sample; 100 resolved
forecasts would still be only a first serious checkpoint.

## Quantitative Record

- Bounded official follow-ups completed: `10 / 10`
- Triggered tension cards reviewed: `18 / 18`
- Final dispositions: `8 insight_only`, `2 shadow_track`
- Shadow forecasts created: `2`
- Shadow forecasts resolved: `2 YES`, `0 NO`
- Open or overdue shadows: `0`
- Mean model Brier score: `0.1604`
- Mean paired baseline Brier score: `0.2339`
- Mean baseline-minus-model improvement: `+0.0735`

The Brier comparison is descriptive only. Both forecasts were short-horizon
DANE releases with similar threshold structures and both resolved YES. They do
not test legislative, regulatory, market, longer-horizon, tail-risk, or NO
outcomes.

## Qualitative Record

| Run | Disposition | What the bounded review established |
| --- | --- | --- |
| 2026-08-12 | `shadow_track` | DANE July core IPC had a clean next-day resolver and supported a 58% internal forecast. |
| 2026-08-13 | `insight_only` | The official TES auction supported funding-cost context, but not a PGN or fiscal conclusion. |
| 2026-08-14 | `insight_only` | DANE resolved the core-IPC shadow YES at 5.96%; mixed activity and employment evidence remained an insight. |
| 2026-08-17 | `shadow_track` | DANE June ISE had a clean next-day resolver and an empirical baseline, supporting a 62% internal forecast. |
| 2026-08-18 | `insight_only` | DANE resolved the ISE shadow YES at 3.51%; no substitute source was used after transient 502s. |
| 2026-08-19 | `insight_only` | The review caught an invalid FOB-export versus CIF-import subtraction and restored the official comparable FOB balance. |
| 2026-08-20 | `insight_only` | A clean GEIH release clock existed, but no non-arbitrary region, metric, threshold, or baseline justified a forecast. |
| 2026-08-21 | `insight_only` | PL 211 had a consequential fiscal mechanism but no procedural clock; the review also exposed a bill-identity parsing problem. |
| 2026-08-24 | `insight_only` | PL 178 had a concrete FOVIS housing mechanism but no committee transfer, rapporteur, ponencia, agenda, or vote. |
| 2026-08-25 | `insight_only` | The TES curve steepened at longer maturities, while PGN 2027 still lacked a gate-clearing joint-commission event. |

## What Worked

1. **Abstention remained normal.** Eight runs preserved useful analysis without
   forcing a probability.
2. **Official follow-up changed decisions.** It resolved two shadows, rejected
   an arbitrary GEIH forecast, and kept legislative cases at `research_more`
   when their clocks were missing.
3. **The LLM challenged deterministic artifacts.** The external-trade
   valuation and Gaceta bill-identity findings led to narrow code fixes rather
   than being repeated as analysis.
4. **Public and internal lanes stayed separate.** No shadow changed the public
   M3 gate, public forecast ledger, or posting decision.
5. **The analysis remained falsifiable.** Every run recorded competing
   explanations, falsifiers, tension-card reviews, and one bounded follow-up.

## What Remains Unknown

- Whether the probabilities are calibrated across topics or horizons.
- Whether the lane adds value for institutional and legislative cases rather
  than only clean scheduled macro releases.
- Whether model judgment beats strong empirical or consensus baselines over a
  larger, less selected sample.
- Whether the intelligence pass increases the eventual yield of public-interest
  M3 cases. None of these ten runs produced a new public M3 forecast.

## Operating Decision

The daily workflow remains unchanged:

```text
decision-grade M1/M2 artifacts
  -> accountable agent analysis
  -> bounded official follow-up
  -> insight_only, abstain, shadow_track, or promote_to_m3
  -> human review
```

For the next phase:

- keep at most one shadow forecast per run;
- select only questions with a declared official resolver, exact YES/NO
  criteria, deadline, and named baseline;
- seek broader source-family, domain, and horizon coverage without manufacturing
  forecasts to satisfy a mix;
- retain paired baseline scoring and explicit source-backed resolution;
- keep public M3 selection independent from shadow-experiment needs;
- perform the next qualitative review after 10 additional resolved shadows.

The next product milestone remains the first strong, non-duplicate
public-interest M3 case. The shadow lane supports that work but is not a
substitute for it.
