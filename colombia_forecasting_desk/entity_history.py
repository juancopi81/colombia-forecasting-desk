"""Bounded official procurement aggregates, retained separately from the daily scan."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path

import httpx

from .entities import nit_variants, normalize_nit
from .plain_language import describe_datasets, format_cop, validate_plain_text
from .source_fetching.socrata import _http_get

DATASETS = {
    "secop_ii": {"id": "jbjy-vk9h", "date": "fecha_de_firma", "buyer": "nit_entidad", "buyer_numeric": True, "supplier": "documento_proveedor", "supplier_name": "proveedor_adjudicado", "doctype": "tipodocproveedor", "value": "valor_del_contrato", "key": "id_contrato"},
    "secop_i": {"id": "f789-7hwg", "date": "fecha_de_firma_del_contrato", "buyer": "nit_de_la_entidad", "supplier": "identificacion_del_contratista", "supplier_name": "nom_razon_social_contratista", "doctype": "tipo_identifi_del_contratista", "value": "cuantia_contrato", "key": "uid"},
}
CAVEAT = "Direct contracting is legal for many contract types; this is a prompt for review, not a claim of wrongdoing."


def _literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _in(field: str, values: list[str], *, numeric: bool = False) -> str:
    literals = [value for value in values if value.isdigit()] if numeric else [_literal(value) for value in values]
    return field + " in (" + ",".join(literals) + ")"


def _number(value, *, default=None):
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            return default
        return int(number) if number == number.to_integral() else float(number)
    except (InvalidOperation, TypeError, ValueError):
        return default


def _window_start(as_of: date, months: int) -> date:
    month_index = as_of.year * 12 + as_of.month - 1 - months
    year, month = divmod(month_index, 12)
    return date(year, month + 1, min(as_of.day, monthrange(year, month + 1)[1]))


class QueryLog:
    def __init__(self, client: httpx.Client):
        self.client = client
        self.queries: list[dict] = []
        self.gaps: list[str] = []

    def get(self, dataset: str, label: str, *, select: str, where: str = "", group: str | None = None, order: str | None = None, limit: int = 500, offset: int | None = None) -> list[dict] | None:
        url = f"https://www.datos.gov.co/resource/{dataset}.json"
        params = {"$select": select, "$limit": str(limit)}
        if where:
            params["$where"] = where
        if group:
            params["$group"] = group
        if order:
            params["$order"] = order
        if offset is not None:
            params["$offset"] = str(offset)
        record = {"label": label, "dataset": dataset, "url": str(httpx.Request("GET", url, params=params).url), "params": params}
        self.queries.append(record)
        try:
            response = _http_get(self.client, url, params=params)
            rows = response.json()
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise ValueError("expected an array of aggregate rows")
        except (httpx.HTTPError, ValueError) as exc:
            record.update(status="failed", error_type=type(exc).__name__)
            if isinstance(exc, httpx.HTTPStatusError):
                record["status_code"] = exc.response.status_code
                try:
                    diagnostic = exc.response.json()
                except ValueError:
                    diagnostic = {}
                if isinstance(diagnostic, dict):
                    record["source_error_code"] = diagnostic.get("errorCode")
            self.gaps.append(f"{label}: official query failed ({type(exc).__name__}); result is unknown")
            return None
        record.update(status="observed", rows=len(rows), truncated=bool(group and len(rows) >= limit))
        if record["truncated"]:
            self.gaps.append(f"{label}: group limit {limit} reached; unreturned groups are unknown")
        return rows

    def url(self, label: str) -> str:
        return next(query["url"] for query in reversed(self.queries) if query["label"] == label)


def _summary(log: QueryLog, dataset: dict, where: str, label: str, outlier: int) -> dict:
    value, identity = dataset["value"], dataset["key"]
    rows = log.get(dataset["id"], label, select=f"count(*) as records,count(distinct {identity}) as contracts,count({value}) as valued_records,sum({value}) as value_cop,max({value}) as max_value_cop", where=where)
    if rows is None or len(rows) != 1:
        if rows is not None:
            log.gaps.append(f"{label}: expected one aggregate row; totals are unknown")
        return {"status": "unknown"}
    row = rows[0]
    result = {key: _number(row.get(key)) for key in ("records", "contracts", "valued_records", "value_cop", "max_value_cop")}
    if any(result[key] is None for key in ("records", "contracts", "valued_records")):
        log.gaps.append(f"{label}: official counts are incomplete; totals are unknown")
        return {"status": "unknown"}
    result.update(status="observed", evidence_url=log.url(label), median_value_cop=None)
    result["median_basis"] = "reported_rows"
    count = result["valued_records"]
    if isinstance(count, int) and count > 0:
        take = 2 if count % 2 == 0 else 1
        median_rows = log.get(dataset["id"], label + " median", select=value, where=f"({where}) and {value} is not null", order=value, limit=take, offset=(count - 1) // 2)
        if median_rows is not None and len(median_rows) == take:
            numbers = [_number(row.get(value)) for row in median_rows]
            if all(number is not None for number in numbers):
                result["median_value_cop"] = sum(numbers) / take
        if result["median_value_cop"] is None:
            log.gaps.append(f"{label}: population median unavailable; no sample median substituted")
    result["value_outlier"] = result["max_value_cop"] is not None and result["max_value_cop"] > outlier
    if result["records"] != result["contracts"]:
        log.gaps.append(f"{label}: row count differs from distinct contract count; value sums are reported-row sums, not deduplicated contract values")
    return result


def _pattern(entry: dict, kind: str, text: str, numbers: dict, urls: list[str], *, suffix: str = "", caveat: str = CAVEAT) -> dict:
    identity = hashlib.sha256((entry["watch_id"] + kind + suffix).encode()).hexdigest()[:16]
    return {"pattern_id": identity, "watch_id": entry["watch_id"], "kind": kind, "plain_text": describe_datasets(text),
            "numbers": numbers, "evidence_urls": list(dict.fromkeys(urls)), "caveat": caveat, "demo": bool(entry.get("demo"))}


def _unambiguous_groups(rows: list[dict], field: str, log: QueryLog, label: str) -> list[dict]:
    """Do not add distinct counts across aliases whose contract overlap is unknown."""
    known = {number for row in rows if (number := normalize_nit(str(row.get(field, ""))))}
    buckets: dict[str, list[dict]] = {}
    for row in rows:
        raw = str(row.get(field, ""))
        number = normalize_nit(raw)
        # A bare base+DV is not automatically an identity. When its candidate
        # base also appears, withhold BOTH groups pending identity/ID-level
        # reconciliation rather than splitting one supplier into two shares.
        if number is None and raw.isdigit() and raw[:-1] in known:
            number = normalize_nit(raw, expected_base=raw[:-1])
        if number is None:
            log.gaps.append(f"{label}: ambiguous organization ID omitted; no check-digit split was guessed")
            continue
        buckets.setdefault(number, []).append(row)
    result = []
    for number, group in buckets.items():
        if len(group) > 1:
            log.gaps.append(f"{label}: potential base/check-digit aliases for organization {number} withheld from screening; distinct-contract overlap is unknown")
        else:
            result.append({**group[0], "canonical_nit": number})
    return result


def _contract_history(log: QueryLog, entry: dict, dataset: dict, base_where: str, settings: dict) -> dict:
    label = dataset["id"]
    thresholds = settings.get("thresholds", {})
    limit = int(settings.get("group_limit", 500))
    summary = _summary(log, dataset, base_where, label + " totals", int(thresholds.get("value_outlier_cop", 10**12)))
    org = f"upper({dataset['doctype']}) in ('NIT','N.I.T.')"
    groups: list[dict] = []
    patterns = []
    if summary.get("status") != "observed":
        return {"summary": summary, "organizations": groups, "patterns": patterns}
    if entry["kind"] == "public_entity":
        field = dataset["supplier"]
        rows = log.get(dataset["id"], label + " organization suppliers", select=f"{field} as organization_nit,count(distinct {dataset['key']}) as contracts,sum({dataset['value']}) as value_cop", where=f"({base_where}) and {org}", group=field, order="contracts DESC", limit=limit)
        for row in _unambiguous_groups(rows or [], "organization_nit", log, label + " supplier groups"):
            number = row["canonical_nit"]
            group = {"organization_nit": number, "contracts": _number(row.get("contracts")), "value_cop": _number(row.get("value_cop"))}
            groups.append(group)
            if group["contracts"] is not None and summary.get("contracts"):
                share = group["contracts"] / summary["contracts"]
                value_share = group["value_cop"] / summary["value_cop"] if group["value_cop"] is not None and summary.get("value_cop") and summary["records"] == summary["contracts"] else None
                if 0 <= share <= 1 and max(share, value_share or 0) >= float(thresholds.get("supplier_share", .25)):
                    value_text = f" and {value_share:.1%} of reported value" if value_share is not None else ""
                    patterns.append(_pattern(entry, "supplier_share", f"The supplier recorded under NIT {number} accounts for {share:.1%} of distinct contracts{value_text} recorded for {entry['label']} in {label}.", {**group, "share": share, "value_share": value_share, "denominator_contracts": summary["contracts"]}, [log.url(label + " organization suppliers"), summary["evidence_url"]], suffix=label+number))
        # Unknown types and cédulas stay aggregated on the server; neither their
        # names nor identifying numbers enter local memory or cache files.
        other = log.get(dataset["id"], label + " persons or unknown types aggregate", select=f"count(*) as records,sum({dataset['value']}) as value_cop", where=f"({base_where}) and ({dataset['doctype']} is null or upper({dataset['doctype']}) not in ('NIT','N.I.T.'))")
        natural = {key: _number((other or [{}])[0].get(key)) for key in ("records", "value_cop")} if other is not None else {"status": "unknown"}
        direct_group = field
        direct_where = f"({base_where}) and upper(modalidad_de_contratacion) like '%DIRECTA%'"
    else:
        field = dataset["buyer"]
        rows = log.get(dataset["id"], label + " buyers", select=f"{field} as buyer_id,count(distinct {dataset['key']}) as contracts,sum({dataset['value']}) as value_cop", where=base_where, group=field, order="contracts DESC", limit=limit)
        groups = [{"buyer_id": str(row.get("buyer_id", "")), "canonical_nit": row["canonical_nit"], "contracts": _number(row.get("contracts")), "value_cop": _number(row.get("value_cop"))} for row in _unambiguous_groups(rows or [], "buyer_id", log, label + " buyer groups")]
        for group in groups[:int(settings.get("buyer_limit", 5))]:
            buyer = group["buyer_id"]
            # The denominator is this exact recorded buyer ID and the same
            # signature-date window, not the supplier's total across buyers.
            date_where = base_where.split(" and (", 1)[0]
            buyer_literal = str(int(Decimal(buyer))) if dataset.get("buyer_numeric") else _literal(buyer)
            denominator = _summary(log, dataset, f"{date_where} and {dataset['buyer']} = {buyer_literal}", label + " buyer denominator " + buyer, int(thresholds.get("value_outlier_cop", 10**12)))
            if denominator.get("contracts") and group.get("contracts") is not None:
                share = group["contracts"] / denominator["contracts"]
                value_share = group["value_cop"] / denominator["value_cop"] if group["value_cop"] is not None and denominator.get("value_cop") and denominator["records"] == denominator["contracts"] else None
                if 0 <= share <= 1 and max(share, value_share or 0) >= float(thresholds.get("supplier_share", .25)):
                    value_text = f" and {value_share:.1%} of reported value" if value_share is not None else ""
                    patterns.append(_pattern(entry, "supplier_share", f"{entry['label']} accounts for {share:.1%} of one buyer's distinct contracts{value_text} recorded in {label}.", {**group, "share": share, "value_share": value_share, "denominator_contracts": denominator["contracts"]}, [log.url(label + " buyers"), denominator["evidence_url"]], suffix=label+buyer))
        natural = None
        direct_group = field
        direct_where = f"({base_where}) and upper(modalidad_de_contratacion) like '%DIRECTA%'"
    result = {"summary": summary, "organizations": groups, "patterns": patterns}
    if entry["kind"] == "public_entity":
        # The denominator includes ALL of this buyer's direct awards. Names and
        # IDs are fetched only for the organizational-document subset.
        totals = log.get(dataset["id"], label + " direct-award denominator", select=f"count(distinct {dataset['key']}) as contracts", where=direct_where)
        denominator = _number((totals or [{}])[0].get("contracts")) if totals is not None else None
        direct = log.get(dataset["id"], label + " repeat direct awards", select=f"{direct_group} as organization_id,min({dataset['supplier_name']}) as organization_name,count(distinct {dataset['key']}) as contracts", where=f"({direct_where}) and {org}", group=direct_group, order="contracts DESC,organization_id", limit=limit)
        named = []
        if denominator is None:
            log.gaps.append(f"{label}: buyer's total direct awards unavailable; supplier shares are unknown")
        for row in _unambiguous_groups(direct or [], "organization_id", log, label + " direct-award groups"):
            contracts = _number(row.get("contracts"))
            name = " ".join(str(row.get("organization_name") or "").split())
            if not name:
                log.gaps.append(f"{label}: an organizational supplier has no reported name; omitted from the named direct-award screen")
                continue
            if not isinstance(contracts, int) or denominator is None:
                continue
            if contracts <= 0 or denominator < contracts:
                log.gaps.append(f"{label}: inconsistent direct-award counts; an organizational supplier share was withheld")
                continue
            named.append({"organization_nit": row["canonical_nit"], "organization_name": name, "contracts": contracts, "share": contracts / denominator, "denominator_contracts": denominator})
        top = sorted(named, key=lambda row: (-row["contracts"], row["organization_name"], row["organization_nit"]))[:5]
        result["direct_awards"] = {"contracts": denominator, "top_organizations": top, "status": "observed" if denominator is not None and direct is not None else "unknown"}
        for supplier in top:
            patterns.append(_pattern(entry, "repeat_direct_award", f"{supplier['organization_name']} received {supplier['contracts']:,} direct-award contracts, {supplier['share']:.1%} of {entry['label']}'s {denominator:,} direct awards recorded in {label} during the window.", supplier, [log.url(label + " repeat direct awards"), log.url(label + " direct-award denominator")], suffix=label+supplier["organization_nit"], caveat=CAVEAT + " Shares include all the buyer's direct awards; only organizational suppliers are named. Names are as reported in the official records."))
    else:
        direct = log.get(dataset["id"], label + " repeat direct awards", select=f"{direct_group} as organization_id,count(distinct {dataset['key']}) as contracts", where=direct_where, group=direct_group, order="contracts DESC", limit=limit)
        direct_count = sum(1 for row in _unambiguous_groups(direct or [], "organization_id", log, label + " direct-award groups") if (_number(row.get("contracts"), default=0) >= int(thresholds.get("repeat_direct_award", 10))))
        if direct_count:
            patterns.append(_pattern(entry, "repeat_direct_award", f"{direct_count} public buyers each recorded at least {thresholds.get('repeat_direct_award', 10)} direct awards to {entry['label']} in {label} during the window.", {"buyers": direct_count, "minimum_awards": int(thresholds.get("repeat_direct_award", 10))}, [log.url(label + " repeat direct awards")], suffix=label))
    if natural is not None:
        result["persons_or_unknown_types_aggregate"] = natural
    return result


def build_entity_history(entry: dict, settings: dict, *, as_of: date, client: httpx.Client, fetched_at: str | None = None) -> dict:
    if entry["kind"] not in {"company", "public_entity"} or not entry["identifiers"].get("nit"):
        raise ValueError("procurement history requires a known organization NIT")
    months = int(settings.get("window_months", 24))
    if not 1 <= months <= 120:
        raise ValueError("window_months must be between one and 120")
    start, end = _window_start(as_of, months), as_of + timedelta(days=1)
    variants = nit_variants(str(entry["identifiers"]["nit"]))
    log = QueryLog(client)
    result = {"schema_version": "entity_history.v1", "watch_id": entry["watch_id"], "kind": entry["kind"], "label": entry["label"], "demo": bool(entry.get("demo")),
              "as_of": as_of.isoformat(), "fetched_at": fetched_at or datetime.now(timezone.utc).isoformat(), "window_months": months,
              "window_start": start.isoformat(), "window_end_exclusive": end.isoformat(), "datasets": {}, "patterns": [], "coverage_gaps": log.gaps, "queries": log.queries,
              "caveats": [CAVEAT, "SECOP I and II are separate reported datasets; their totals are not combined or presumed mutually deduplicated.", "Only recorded contract signature dates define the window; upload dates are not substituted for signatures.", "Current open-data snapshots may revise historical records; this is not an archived historical-vintage estimate."]}
    wheres = {}
    for key, dataset in DATASETS.items():
        date_where = f"{dataset['date']} >= '{start}T00:00:00' and {dataset['date']} < '{end}T00:00:00'"
        field = dataset["buyer"] if entry["kind"] == "public_entity" else dataset["supplier"]
        where = date_where + " and (" + _in(field, variants, numeric=bool(entry["kind"] == "public_entity" and dataset.get("buyer_numeric"))) + ")"
        if entry["kind"] == "company":
            where += f" and upper({dataset['doctype']}) in ('NIT','N.I.T.')"
        wheres[key] = where
        history = _contract_history(log, entry, dataset, where, settings)
        result["datasets"][key] = history
        result["patterns"].extend(history.pop("patterns"))
        if key == "secop_i":
            undated_where = _in(field, variants) + f" and {dataset['date']} is null and fecha_de_cargue_en_el_secop >= '{start}T00:00:00' and fecha_de_cargue_en_el_secop < '{end}T00:00:00'"
            if entry["kind"] == "company":
                undated_where += f" and upper({dataset['doctype']}) in ('NIT','N.I.T.')"
            undated = log.get(dataset["id"], "SECOP I missing signature dates", select="count(*) as records", where=undated_where)
            count = _number((undated or [{}])[0].get("records")) if undated is not None else None
            history["undated_signature_records_loaded_in_window"] = count
            if count:
                log.gaps.append(f"SECOP I: {count} records uploaded in the window have no signature date; they are excluded from dated totals and cannot establish contract timing")
    # SECOP II additions do not have monetary amounts or supplier/buyer IDs.
    # Link only IDs actually captured; disclose the bound instead of treating a
    # sample as complete entity history.
    second = DATASETS["secop_ii"]
    id_limit = int(settings.get("contract_id_limit", 500))
    ids = log.get(second["id"], "SECOP II linked contract IDs", select="id_contrato", where=wheres["secop_ii"], group="id_contrato", order="id_contrato", limit=id_limit)
    captured = [str(row["id_contrato"]) for row in ids or [] if row.get("id_contrato")]
    if captured:
        additions = log.get("cb9c-h8sn", "SECOP II addition events", select="count(*) as events", where=_in("id_contrato", captured) + f" and fecharegistro >= '{start}T00:00:00' and fecharegistro < '{end}T00:00:00'")
        result["datasets"]["secop_ii_additions"] = {"events": _number((additions or [{}])[0].get("events")) if additions is not None else None, "captured_contract_ids": len(captured), "complete_contract_linkage": ids is not None and len(ids) < id_limit, "monetary_amounts_available": False}
    else:
        result["datasets"]["secop_ii_additions"] = {"events": None, "captured_contract_ids": 0, "complete_contract_linkage": ids == [], "monetary_amounts_available": False}
    first = DATASETS["secop_i"]
    ratio = float(settings.get("thresholds", {}).get("large_addition_ratio", .5))
    addition_where = f"({wheres['secop_i']}) and upper({first['doctype']}) in ('NIT','N.I.T.') and cuantia_contrato > 0 and valor_total_de_adiciones / cuantia_contrato >= {ratio}"
    large = log.get(first["id"], "SECOP I large monetary additions", select="count(*) as records,max(valor_total_de_adiciones / cuantia_contrato) as max_ratio,sum(valor_total_de_adiciones) as additions_value_cop", where=addition_where)
    if large and _number(large[0].get("records"), default=0) > 0:
        numbers = {key: _number(large[0].get(key)) for key in ("records", "max_ratio", "additions_value_cop")}
        result["patterns"].append(_pattern(entry, "large_addition", f"{numbers['records']} organizational SECOP I records show monetary additions of at least {ratio:.0%} of the original recorded value.", numbers, [log.url("SECOP I large monetary additions")]))
    # Sanctions expose no document type; only a configured known company NIT
    # permits an identity-level post-sanction screen. Public-entity results are
    # server-side counts with no contractor names or identifying numbers.
    sanction_field = "documento_contratista" if entry["kind"] == "company" else "nit_entidad"
    sanctions = log.get("4n4q-k399", "sanctions aggregate", select="count(*) as records,min(fecha_de_firmeza) as earliest_firm_at,max(fecha_de_firmeza) as latest_firm_at", where=_in(sanction_field, variants) + f" and fecha_de_publicacion >= '{start}T00:00:00' and fecha_de_publicacion < '{end}T00:00:00'")
    result["datasets"]["sanctions"] = sanctions[0] if sanctions else {"status": "unknown" if sanctions is None else "observed", "records": 0}
    firm = (sanctions or [{}])[0].get("earliest_firm_at")
    if entry["kind"] == "company" and firm and str(firm)[:10] <= as_of.isoformat():
        for key, dataset in DATASETS.items():
            rows = log.get(dataset["id"], key + " post-sanction contracts", select=f"count(distinct {dataset['key']}) as contracts", where=f"({wheres[key]}) and {dataset['date']} > {_literal(str(firm))}")
            count = _number((rows or [{}])[0].get("contracts"), default=0)
            if count:
                result["patterns"].append(_pattern(entry, "sanctioned_supplier", f"{count} {key} contracts for the configured company have signature dates after a recorded sanction's firm date.", {"contracts": count, "earliest_firm_at": firm}, [log.url("sanctions aggregate"), log.url(key + " post-sanction contracts")], suffix=key, caveat="Verify sanction scope, applicability and validity at each award; a later contract does not establish wrongdoing."))
    elif entry["kind"] == "public_entity":
        log.gaps.append("Sanctions lack contractor document types; organization-level post-sanction matching is limited to configured known-company NITs.")
    result["coverage_gaps"] = list(dict.fromkeys(log.gaps))
    return result


def fetch_divipola(client: httpx.Client, *, fetched_at: str | None = None) -> dict:
    metadata = _http_get(client, "https://www.datos.gov.co/api/views/gdxc-w37w.json").json()
    response = _http_get(client, "https://www.datos.gov.co/resource/gdxc-w37w.json", params={"$select": "cod_dpto,dpto,cod_mpio,nom_mpio", "$limit": "3000", "$order": "cod_mpio"})
    rows = response.json()
    if not isinstance(rows, list) or len(rows) >= 3000 or not all(isinstance(row, dict) and len(str(row.get("cod_mpio", ""))) == 5 for row in rows):
        raise ValueError("DANE DIVIPOLA catalog is invalid or truncated")
    updated = metadata.get("rowsUpdatedAt")
    return {"source_url": str(response.url), "attribution": metadata.get("attribution"), "rows_updated_at": updated, "vintage": datetime.fromtimestamp(updated, timezone.utc).date().isoformat() if isinstance(updated, (int, float)) else None, "fetched_at": fetched_at or datetime.now(timezone.utc).isoformat(), "rows": rows}


def render_entity_history(history: dict) -> str:
    show = lambda value: "unknown" if value is None else str(value)
    fetched = datetime.fromisoformat(history["fetched_at"].replace("Z", "+00:00")).strftime("%Y-%m-%d at %H:%M %z")
    lines = [f"# {history['label']}", "", f"{'Demo organization. ' if history.get('demo') else ''}Window: {history['window_start']} through {history['as_of']}; fetched {fetched}.", "", "Each SECOP dataset is reported separately. Money totals refer to reported rows; displayed amounts are rounded."]
    labels = {"secop_ii": "SECOP II contracts", "secop_i": "SECOP I contracts", "secop_ii_additions": "SECOP II contract additions", "sanctions": "Recorded sanctions"}
    for key, dataset in history["datasets"].items():
        lines += ["", f"## {labels.get(key, key.replace('_', ' '))}", ""]
        if "summary" in dataset:
            row = dataset["summary"]
            if row.get("status") == "observed":
                lines.append(f"{row.get('contracts'):,} distinct contracts, {row.get('records'):,} rows. Reported value sum: {format_cop(row.get('value_cop'))}; middle reported contract value: {format_cop(row.get('median_value_cop'))}; largest: {format_cop(row.get('max_value_cop'))}.")
                if row.get("value_outlier"):
                    lines.append("A large value outlier is present; check the source before interpreting the sum.")
                lines.append(f"[Official aggregate]({row['evidence_url']})")
                if dataset.get("persons_or_unknown_types_aggregate") is not None:
                    lines.append("Cédulas and unknown document types are aggregated without names or identifiers.")
            else:
                lines.append("Official query unavailable; totals and absence of patterns are unknown.")
        elif key == "secop_ii_additions":
            lines.append(f"{show(dataset.get('events'))} addition events linked to {dataset.get('captured_contract_ids')} captured contract IDs. Complete linkage: {'yes' if dataset.get('complete_contract_linkage') else 'no'}. This dataset contains no monetary amounts.")
        elif key == "sanctions":
            lines.append(f"Recorded sanctions in the publication window: {dataset.get('records', 'unknown')}. Missing records do not establish a clean history.")
    if history["patterns"]:
        lines += ["", "## Patterns worth a look", ""]
        for pattern in history["patterns"]:
            lines.append(f"- {pattern['plain_text']} {pattern['caveat']}")
            lines.append("  " + " · ".join(f"[Official query {n+1}]({url})" for n, url in enumerate(pattern["evidence_urls"])))
    notes = history["caveats"] + history["coverage_gaps"]
    lines += ["", "## Coverage and caveats", ""] + [f"- {text}" for text in notes[:16]]
    if len(notes) > 16:
        lines.append(f"- {len(notes) - 16} additional coverage notes are retained in the local history cache.")
    rendered = describe_datasets("\n".join(lines) + "\n")
    issues = validate_plain_text(rendered)
    if issues:
        raise ValueError("; ".join(issues))
    return rendered
