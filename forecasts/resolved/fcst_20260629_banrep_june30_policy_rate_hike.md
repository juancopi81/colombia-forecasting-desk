# Forecast Resolution - BanRep June 30 Policy Rate Hike

## Forecast

- Forecast ID: `fcst_20260629_banrep_june30_policy_rate_hike`
- Created at: 2026-06-29T16:11:57Z
- Question: Will Banco de la Republica raise its policy rate above 11.25% at the June 30, 2026 board decision?
- Probability: 70% YES
- Confidence: low-medium

## Resolution

- Resolved at: 2026-07-01T15:39:39Z
- Outcome: YES
- Resolution source: Banco de la Republica June 30, 2026 Junta Directiva communique, cross-checked against the official SUAMECA policy-rate series captured in the 2026-07-01 run artifacts.
- Resolution value: BanRep reported that the board increased the policy rate by 75 bps to 12.00%, above the forecast threshold of 11.25%.
- Related values: The 2026-07-01 indicator watch recorded `policy_rate_pct=12.00`, `policy_rate_date=2026-07-01`, `ibr_overnight_nominal_pct=10.538`, and `ibr_policy_spread_pp=-1.462`.

## Scoring

- Forecasted probability for YES: 0.70
- Outcome encoded as YES=1, NO=0: 1
- Brier score: 0.0900

## Notes

The binary forecast resolved correctly. The modal call in the draft was +50 bps, while the actual move was +75 bps, so the direction/threshold call was right but the preferred magnitude undershot the decision.

This should be treated as a forecast-track-record resolution, not as financial, borrowing, or trading advice.

## Sources

- BanRep June 30, 2026 communique: https://www.banrep.gov.co/es/noticias/junta-directiva-junio-2026
- BanRep SUAMECA policy-rate series: https://suameca.banrep.gov.co/estadisticas-economicas/informacionSerie/59/tasas_interes_politica_monetaria/
- Original evidence pack: `runs/2026-06-29/evidence_packs/banrep_june30_policy_rate_hike.md`
- Original forecast draft: `runs/2026-06-29/forecast_drafts/banrep_june30_policy_rate_hike.md`
- Resolution run: `runs/2026-07-01/indicator_watch.json`
