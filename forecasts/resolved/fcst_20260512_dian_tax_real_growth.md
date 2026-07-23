# Forecast Resolution - DIAN Tax Revenue Growth Below IPC

## Forecast

- Forecast ID: `fcst_20260512_dian_tax_real_growth`
- Created at: 2026-05-12T16:28:29Z
- Question: Will the next DIAN monthly tax-collection update after 2026-05-12 show nominal gross tax revenue growth below the latest DANE annual IPC rate available when the update is published?
- Probability: 56% YES
- Confidence: low

## Resolution

- Resolved at: 2026-07-23T16:04:55Z
- Outcome: YES
- Resolution source: DIAN April 2026 and April 2025 monthly tax-collection reports, compared with the DANE May 2026 IPC observation.
- Resolution value: Gross tax revenue rose from COP 22,383,244 million in April 2025 to COP 22,824,791 million in April 2026, an increase of 1.97% year over year. That was below the latest annual IPC rate available in the publication context, 5.84% for May 2026.

## Scoring

- Forecasted probability for YES: 0.56
- Outcome encoded as YES=1, NO=0: 1
- Brier score: 0.1936

## Notes

The forecast resolved correctly, but the resolution was recorded late. The original 2026-06-15 date was a resolution-check window, while the forecast criteria explicitly selected the next DIAN update after March 2026. The later availability of the April report therefore delayed the check without changing the binary resolver.

This is an internal fiscal-data calibration result. A single monthly comparison should not be presented as evidence of a fiscal crisis or as tax, investment, or political advice.

## Sources

- DIAN April 2026 monthly report: https://www.dian.gov.co/impuestos/InformeMensualRecaudo/04-Informe-mensual-recaudo-abril-2026.pdf
- DIAN April 2025 monthly report: https://www.dian.gov.co/impuestos/InformeMensualRecaudo/4-Informe-Mensual-Recaudo-abril-2025.pdf
- DANE IPC technical page: https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-al-consumidor-ipc/ipc-informacion-tecnica
- Original evidence pack: `runs/2026-05-12/evidence_packs/dian_tax_real_growth.md`
- IPC observation: `runs/2026-06-15/indicator_watch.json`
