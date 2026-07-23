# Forecast Resolution - USD/COP TRM Persistence To 2026-05-21

## Forecast

- Forecast ID: `fcst_20260514_trm_persistence_20260521`
- Created at: 2026-05-14T15:57:43Z
- Question: Will the official USD/COP TRM in force on 2026-05-21 be at or above 3780.57 COP/USD?
- Probability: 54% YES
- Confidence: low

## Resolution

- Resolved at: 2026-07-23T16:04:55Z
- Outcome: NO
- Resolution source: Superintendencia Financiera official TRM data published through datos.gov.co and captured in the 2026-05-21 run.
- Resolution value: The official TRM in force on 2026-05-21 was 3730.49 COP/USD, below the 3780.57 threshold.

## Scoring

- Forecasted probability for YES: 0.54
- Outcome encoded as YES=1, NO=0: 0
- Brier score: 0.2916

## Notes

The forecast resolved incorrectly. The evidence pack identified the main failure mode: the threshold was only 0.38% below the starting TRM, so a modest peso appreciation was enough to produce a NO outcome.

This remains an internal calibration result, not trading or investment advice.

## Sources

- Official TRM dataset: https://www.datos.gov.co/Econom-a-y-Finanzas/Tasa-de-Cambio-Representativa-del-Mercado-TRM/32sa-8pi3
- Original evidence pack: `runs/2026-05-14/evidence_packs/trm_persistence_20260521.md`
- Resolution run: `runs/2026-05-21/indicator_watch.json`
