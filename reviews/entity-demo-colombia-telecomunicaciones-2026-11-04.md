# Colombia Telecomunicaciones (demo)

Demo organization. Window: 2024-11-04 through 2026-11-04; fetched 2026-11-04 at 15:41 +0000.

Each SECOP dataset is reported separately. Money totals refer to reported rows; displayed amounts are rounded.

## SECOP II contracts

27 distinct contracts, 27 rows. Reported value sum: COP 15.51 billion; middle reported contract value: COP 62.81 million; largest: COP 12.8 billion.
[Official aggregate](https://www.datos.gov.co/resource/jbjy-vk9h.json?%24select=count%28%2A%29+as+records%2Ccount%28distinct+id_contrato%29+as+contracts%2Ccount%28valor_del_contrato%29+as+valued_records%2Csum%28valor_del_contrato%29+as+value_cop%2Cmax%28valor_del_contrato%29+as+max_value_cop&%24limit=500&%24where=fecha_de_firma+%3E%3D+%272024-11-04T00%3A00%3A00%27+and+fecha_de_firma+%3C+%272026-11-05T00%3A00%3A00%27+and+%28documento_proveedor+in+%28%27830122566%27%2C%278301225661%27%2C%27830122566-1%27%2C%27830.122.566-1%27%29%29+and+upper%28tipodocproveedor%29+in+%28%27NIT%27%2C%27N.I.T.%27%29)

## SECOP I contracts

0 distinct contracts, 0 rows. Reported value sum: unknown; middle reported contract value: unknown; largest: unknown.
[Official aggregate](https://www.datos.gov.co/resource/f789-7hwg.json?%24select=count%28%2A%29+as+records%2Ccount%28distinct+uid%29+as+contracts%2Ccount%28cuantia_contrato%29+as+valued_records%2Csum%28cuantia_contrato%29+as+value_cop%2Cmax%28cuantia_contrato%29+as+max_value_cop&%24limit=500&%24where=fecha_de_firma_del_contrato+%3E%3D+%272024-11-04T00%3A00%3A00%27+and+fecha_de_firma_del_contrato+%3C+%272026-11-05T00%3A00%3A00%27+and+%28identificacion_del_contratista+in+%28%27830122566%27%2C%278301225661%27%2C%27830122566-1%27%2C%27830.122.566-1%27%29%29+and+upper%28tipo_identifi_del_contratista%29+in+%28%27NIT%27%2C%27N.I.T.%27%29)

## SECOP II contract additions

32 addition events linked to 27 captured contract IDs. Complete linkage: yes. This dataset contains no monetary amounts.

## Recorded sanctions

Recorded sanctions in the publication window: 0. Missing records do not establish a clean history.

## Coverage and caveats

- Direct contracting is legal for many contract types; this is a prompt for review, not a claim of wrongdoing.
- SECOP I and II are separate reported datasets; their totals are not combined or presumed mutually deduplicated.
- Only recorded contract signature dates define the window; upload dates are not substituted for signatures.
- Current open-data snapshots may revise historical records; this is not an archived historical-vintage estimate.
- SECOP II contracts buyer groups: ambiguous organization ID omitted; no check-digit split was guessed
- SECOP II contracts direct-award groups: ambiguous organization ID omitted; no check-digit split was guessed
