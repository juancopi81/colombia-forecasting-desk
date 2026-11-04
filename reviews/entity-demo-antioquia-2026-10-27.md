# Department of Antioquia (demo)

Demo organization. Window: 2024-10-27 through 2026-10-27; fetched 2026-10-27T18:09:20.744565+00:00.

Each SECOP dataset is reported separately. Values are COP; sums refer to reported rows.

## secop ii

2696 distinct contracts, 2696 rows. Reported value sum: 7474181187446.37; population median of reported rows: 202613023.0; maximum: 712569236855.
[Official aggregate](https://www.datos.gov.co/resource/jbjy-vk9h.json?%24select=count%28%2A%29+as+records%2Ccount%28distinct+id_contrato%29+as+contracts%2Ccount%28valor_del_contrato%29+as+valued_records%2Csum%28valor_del_contrato%29+as+value_cop%2Cmax%28valor_del_contrato%29+as+max_value_cop&%24limit=500&%24where=fecha_de_firma+%3E%3D+%272024-10-27T00%3A00%3A00%27+and+fecha_de_firma+%3C+%272026-10-28T00%3A00%3A00%27+and+%28nit_entidad+in+%28890900286%2C8909002860%29%29)
Cédulas and unknown document types are aggregated without names or identifiers.

## secop i

38 distinct contracts, 74 rows. Reported value sum: 486684342917; population median of reported rows: 1190962502.0; maximum: 153980946052.
[Official aggregate](https://www.datos.gov.co/resource/f789-7hwg.json?%24select=count%28%2A%29+as+records%2Ccount%28distinct+uid%29+as+contracts%2Ccount%28cuantia_contrato%29+as+valued_records%2Csum%28cuantia_contrato%29+as+value_cop%2Cmax%28cuantia_contrato%29+as+max_value_cop&%24limit=500&%24where=fecha_de_firma_del_contrato+%3E%3D+%272024-10-27T00%3A00%3A00%27+and+fecha_de_firma_del_contrato+%3C+%272026-10-28T00%3A00%3A00%27+and+%28nit_de_la_entidad+in+%28%27890900286%27%2C%278909002860%27%2C%27890900286-0%27%2C%27890.900.286-0%27%29%29)
Cédulas and unknown document types are aggregated without names or identifiers.

## secop ii additions

660 addition events linked to 500 captured contract IDs. Complete linkage: no. This dataset contains no monetary amounts.

## sanctions

Recorded sanctions in the publication window: 0. Missing records do not establish a clean history.

## Patterns worth a look

- 80 recorded organizational ID groups have at least 10 direct awards in jbjy-vk9h during the window. Direct contracting is legal for many contract types; this is a prompt for review, not a claim of wrongdoing. Verify supplier and buyer aliases before combining ID groups.
  [Official query 1](https://www.datos.gov.co/resource/jbjy-vk9h.json?%24select=documento_proveedor+as+organization_id%2Ccount%28distinct+id_contrato%29+as+contracts&%24limit=500&%24where=%28fecha_de_firma+%3E%3D+%272024-10-27T00%3A00%3A00%27+and+fecha_de_firma+%3C+%272026-10-28T00%3A00%3A00%27+and+%28nit_entidad+in+%28890900286%2C8909002860%29%29%29+and+upper%28tipodocproveedor%29+in+%28%27NIT%27%2C%27N.I.T.%27%29+and+upper%28modalidad_de_contratacion%29+like+%27%25DIRECTA%25%27&%24group=documento_proveedor&%24order=contracts+DESC)

## Coverage and caveats

- Direct contracting is legal for many contract types; this is a prompt for review, not a claim of wrongdoing.
- SECOP I and II are separate reported datasets; their totals are not combined or presumed mutually deduplicated.
- Only recorded contract signature dates define the window; upload dates are not substituted for signatures.
- Current open-data snapshots may revise historical records; this is not an archived historical-vintage estimate.
- jbjy-vk9h organization suppliers: group limit 500 reached; unreturned groups are unknown
- jbjy-vk9h supplier groups: ambiguous organization ID omitted; no check-digit split was guessed
- jbjy-vk9h supplier groups: potential base/check-digit aliases for organization 890983740 withheld from screening; distinct-contract overlap is unknown
- jbjy-vk9h direct-award groups: ambiguous organization ID omitted; no check-digit split was guessed
- jbjy-vk9h direct-award groups: potential base/check-digit aliases for organization 890983740 withheld from screening; distinct-contract overlap is unknown
- f789-7hwg totals: row count differs from distinct contract count; value sums are reported-row sums, not deduplicated contract values
- SECOP I: 8 records uploaded in the window have no signature date; they are excluded from dated totals and cannot establish contract timing
- SECOP II linked contract IDs: group limit 500 reached; unreturned groups are unknown
- Sanctions lack contractor document types; organization-level post-sanction matching is limited to configured known-company NITs.
