# Forecast Resolution - DANE ICOCED Construction Costs Above IPC

## Forecast

- Forecast ID: `fcst_20260513_icoced_costs_above_ipc`
- Created at: 2026-05-13T20:56:12Z
- Question: Will the next DANE ICOCED release after 2026-05-13 show annual total building-construction cost inflation above the latest DANE annual IPC rate available when the ICOCED release is published?
- Probability: 60% YES
- Confidence: low-medium

## Resolution

- Resolved at: 2026-06-05T15:48:23Z
- Outcome: YES
- Resolution source: DANE ICOCED April 2026 annex and DANE IPC April 2026 current technical page, both captured in the 2026-06-05 run artifacts.
- Resolution value: April 2026 annual ICOCED total variation was 6.45%, above the latest annual IPC rate available for the ICOCED publication context, 5.68%.
- Related values: ICOCED monthly variation 0.30%, year-to-date 6.78%, total index 135.84.

## Scoring

- Forecasted probability for YES: 0.60
- Outcome encoded as YES=1, NO=0: 1
- Brier score: 0.1600

## Notes

The forecast resolved correctly. The spread widened from the March evidence-pack baseline of +0.65 pp to +0.77 pp in April, so construction-cost inflation remained above headline IPC despite a modest April monthly ICOCED increase.

This should stay an internal calibration result unless there is a broader public-interest update to explain. It is not a housing-price, land, construction-company, or investment recommendation.

## Sources

- DANE ICOCED April 2026 annex: https://www.dane.gov.co/files/operaciones/ICOCED/anex-ICOCED-abr2026.xlsx
- DANE ICOCED landing page: https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-costos-de-la-construccion-de-edificaciones-icoced
- DANE IPC technical page: https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-al-consumidor-ipc/ipc-informacion-tecnica
- Original evidence pack: `runs/2026-05-13/evidence_packs/icoced_costs_above_ipc.md`
- Resolution run: `runs/2026-06-05/indicator_watch.json`
- Resolution tension card: `runs/2026-06-05/indicator_tension_cards.json`
