from __future__ import annotations

import copy
import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any

import httpx

SCHEMA_VERSION = "spending_execution_audit.v1"
SECOP_PAYMENTS_URL = "https://www.datos.gov.co/resource/uymx-8p3j.json"
SECOP_METADATA_URL = "https://www.datos.gov.co/api/views/uymx-8p3j"
CUIPO_URL = "https://www.datos.gov.co/resource/4f7r-epif.json"
CUIPO_METADATA_URL = "https://www.datos.gov.co/api/views/4f7r-epif"
SPENDING_EXECUTION_TIMEOUT = httpx.Timeout(30.0, connect=5.0)
SECOP_ROW_LIMIT = 5_000
SECOP_MAX_STALENESS_DAYS = 7
CUIPO_SCOPES = ("Departamentos", "Municipios")
USER_AGENT = "colombia-forecasting-desk/1.0"
_RUN_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def fetch_spending_execution_audit(
    *,
    now: datetime | None = None,
    client: httpx.Client | None = None,
    previous_audit: dict[str, Any] | None = None,
    secop_row_limit: int = SECOP_ROW_LIMIT,
) -> dict[str, Any]:
    """Fetch bounded public-spending diagnostics without promoting them to M2/M3."""
    if secop_row_limit < 1:
        raise ValueError("secop_row_limit must be positive")

    current = now or datetime.now(timezone.utc)
    close_client = client is None
    active_client = client or httpx.Client(
        timeout=SPENDING_EXECUTION_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    try:
        secop = _fetch_secop_payments(
            active_client,
            now=current,
            row_limit=secop_row_limit,
        )
        cuipo = _fetch_cuipo_execution(
            active_client,
            now=current,
            previous_audit=previous_audit,
        )
    finally:
        if close_client:
            active_client.close()

    sources = {
        "secop_ii_plan_pagos": secop,
        "cuipo_territorial_execution": cuipo,
    }
    statuses = [str(source.get("status") or "failed") for source in sources.values()]
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": current.date().isoformat(),
        "generated_at": _iso_datetime(current),
        "mode": "shadow_audit",
        "policy": {
            "decision_use": "diagnostic_only",
            "m2_m3_eligible": False,
            "automatic_promotion": False,
            "notes": [
                "This artifact does not create forecast questions or probabilities.",
                "Payment or execution patterns are not findings of fraud or misconduct.",
                "Incomplete or quality-warning SECOP days withhold monetary totals.",
                "Both sources are official administrative data but retain reporting and revision risk.",
            ],
        },
        "summary": {
            "source_count": len(sources),
            "observed_count": sum(
                status in {"observed", "reused"} for status in statuses
            ),
            "failed_count": sum(status == "failed" for status in statuses),
            "no_data_count": sum(status == "no_data" for status in statuses),
            "stale_count": sum(status == "stale" for status in statuses),
            "truncated_count": sum(status == "truncated" for status in statuses),
            "quality_warning_count": sum(
                status == "quality_warning" for status in statuses
            ),
            "m2_m3_eligible": False,
        },
        "sources": sources,
    }


def load_previous_spending_execution_audit(
    runs_root: str | Path,
    run_date: str,
) -> dict[str, Any] | None:
    """Load the latest earlier audit so unchanged CUIPO snapshots can be reused."""
    root = Path(runs_root)
    if not root.exists():
        return None
    candidates = sorted(
        (
            child
            for child in root.iterdir()
            if child.is_dir() and _RUN_DIR_RE.match(child.name) and child.name < run_date
        ),
        key=lambda path: path.name,
        reverse=True,
    )
    for run_dir in candidates:
        path = run_dir / "spending_execution_audit.json"
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and payload.get("schema_version") == SCHEMA_VERSION:
            return payload
    return None


def render_spending_execution_audit(audit: dict[str, Any]) -> str:
    run_date = str(audit.get("run_date") or "unknown date")
    summary = audit.get("summary") or {}
    sources = audit.get("sources") or {}
    lines = [
        f"# Spending Execution Audit - {run_date}",
        "",
        (
            "Shadow diagnostic only. This artifact is not eligible for M2/M3, does "
            "not assign probabilities, and does not identify fraud or misconduct."
        ),
        "",
        "Summary:",
        "",
        f"- Sources checked: {summary.get('source_count', 0)}",
        f"- Observed or reused: {summary.get('observed_count', 0)}",
        f"- Failed: {summary.get('failed_count', 0)}",
        f"- No data: {summary.get('no_data_count', 0)}",
        f"- Stale: {summary.get('stale_count', 0)}",
        f"- Truncated: {summary.get('truncated_count', 0)}",
        f"- Quality warnings: {summary.get('quality_warning_count', 0)}",
        "",
    ]
    _render_secop(lines, sources.get("secop_ii_plan_pagos") or {})
    _render_cuipo(lines, sources.get("cuipo_territorial_execution") or {})
    lines.extend(
        [
            "Policy reminders:",
            "",
            "- Do not feed this artifact into M2/M3 during the shadow trial.",
            "- Do not interpret administrative-data anomalies as wrongdoing.",
            "- Recheck official source semantics before any later promotion.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _fetch_secop_payments(
    client: httpx.Client,
    *,
    now: datetime,
    row_limit: int,
) -> dict[str, Any]:
    base = {
        "source_name": "SECOP II - Plan de pagos",
        "source_url": SECOP_PAYMENTS_URL,
        "dataset_metadata_url": SECOP_METADATA_URL,
        "checked_at": _iso_datetime(now),
        "row_cap": row_limit,
        "usable_for_analysis": False,
        "caveats": [
            "SECOP payment records are administrative platform data and may be revised.",
            "Zero or negative payment values are excluded from monetary aggregates.",
            "Supplier and supervisor document identifiers are neither requested nor persisted.",
        ],
    }
    try:
        latest_rows = _get_json_list(
            client,
            SECOP_PAYMENTS_URL,
            params={
                "$select": "id_del_contrato,id_de_pago,fecha_real_de_pago",
                "$where": (
                    "estado='Pagado' AND fecha_real_de_pago IS NOT NULL AND "
                    f"fecha_real_de_pago <= '{now.date().isoformat()}T23:59:59.999'"
                ),
                "$order": "fecha_real_de_pago DESC",
                "$limit": "1",
            },
        )
        if not latest_rows:
            return {**base, "status": "no_data", "error": "No paid rows as of run date."}

        latest_date = _parse_socrata_date(latest_rows[0].get("fecha_real_de_pago"))
        if latest_date is None or latest_date > now.date():
            raise ValueError("SECOP latest payment date is missing or outside as-of date")

        rows = _get_json_list(
            client,
            SECOP_PAYMENTS_URL,
            params={
                "$select": (
                    "id_del_contrato,id_de_pago,fecha_de_recepcion,"
                    "fecha_real_de_pago,valor_a_pagar,codigo_entidad,"
                    "nombre_entidad,nombre_proveedor"
                ),
                "$where": (
                    "estado='Pagado' AND fecha_real_de_pago IS NOT NULL AND "
                    f"fecha_real_de_pago >= '{latest_date.isoformat()}T00:00:00.000' "
                    f"AND fecha_real_de_pago <= '{latest_date.isoformat()}T23:59:59.999'"
                ),
                "$order": "id_del_contrato,id_de_pago",
                "$limit": str(row_limit + 1),
            },
        )
        return _summarize_secop_rows(
            base,
            rows,
            latest_date=latest_date,
            as_of=now.date(),
            row_limit=row_limit,
        )
    except Exception as exc:  # pragma: no cover - final fail-closed guard.
        return _failed_source(base, exc)


def _summarize_secop_rows(
    base: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    latest_date: date,
    as_of: date,
    row_limit: int,
) -> dict[str, Any]:
    truncated = len(rows) > row_limit
    inspected_rows = rows[:row_limit]
    seen_keys: set[tuple[str, str]] = set()
    duplicate_keys = 0
    invalid_rows = 0
    nonpositive_rows = 0
    missing_entity_rows = 0
    missing_supplier_rows = 0
    negative_delay_rows = 0
    delays: list[int] = []
    positive_rows: list[tuple[dict[str, Any], Decimal]] = []

    for row in inspected_rows:
        contract_id = _clean_text(row.get("id_del_contrato"))
        payment_id = _clean_text(row.get("id_de_pago"))
        payment_date = _parse_socrata_date(row.get("fecha_real_de_pago"))
        value = _parse_decimal(row.get("valor_a_pagar"))
        if not contract_id or not payment_id or payment_date != latest_date or value is None:
            invalid_rows += 1
            continue
        key = (contract_id, payment_id)
        if key in seen_keys:
            duplicate_keys += 1
        seen_keys.add(key)

        entity = _clean_text(row.get("nombre_entidad"))
        supplier = _clean_text(row.get("nombre_proveedor"))
        if not entity:
            missing_entity_rows += 1
        if not supplier:
            missing_supplier_rows += 1
        if value <= 0:
            nonpositive_rows += 1
        else:
            positive_rows.append((row, value))

        receipt_date = _parse_socrata_date(row.get("fecha_de_recepcion"))
        if receipt_date is not None:
            delay = (payment_date - receipt_date).days
            if delay < 0:
                negative_delay_rows += 1
            else:
                delays.append(delay)

    stale = (as_of - latest_date).days > SECOP_MAX_STALENESS_DAYS
    quality_warning = invalid_rows > 0 or duplicate_keys > 0 or not positive_rows
    usable = not truncated and not stale and not quality_warning
    if truncated:
        status = "truncated"
    elif stale:
        status = "stale"
    elif quality_warning:
        status = "quality_warning"
    else:
        status = "observed"

    result: dict[str, Any] = {
        **base,
        "status": status,
        "latest_payment_date": latest_date.isoformat(),
        "returned_rows": len(rows),
        "inspected_rows": len(inspected_rows),
        "complete_day": not truncated,
        "usable_for_analysis": usable,
        "quality": {
            "positive_value_rows": len(positive_rows),
            "nonpositive_value_rows": nonpositive_rows,
            "duplicate_payment_keys": duplicate_keys,
            "invalid_rows": invalid_rows,
            "missing_entity_rows": missing_entity_rows,
            "missing_supplier_rows": missing_supplier_rows,
            "valid_delay_rows": len(delays),
            "negative_delay_rows": negative_delay_rows,
        },
    }
    if truncated:
        result["caveats"] = [
            *base["caveats"],
            "The latest payment day exceeded the row cap; totals and rankings are withheld.",
        ]
    elif stale:
        result["caveats"] = [
            *base["caveats"],
            f"Latest payment date is more than {SECOP_MAX_STALENESS_DAYS} days old.",
        ]
    elif quality_warning:
        result["caveats"] = [
            *base["caveats"],
            (
                "Row validation, duplicate-key, or positive-value coverage failed; "
                "totals and rankings are withheld."
            ),
        ]
    else:
        result["metrics"] = _secop_metrics(positive_rows, delays)
        result["top_entities"] = _top_entities(positive_rows)
    return result


def _secop_metrics(
    positive_rows: list[tuple[dict[str, Any], Decimal]],
    delays: list[int],
) -> dict[str, Any]:
    entities = {
        _clean_text(row.get("nombre_entidad")).casefold()
        for row, _ in positive_rows
        if _clean_text(row.get("nombre_entidad"))
    }
    suppliers = {
        _clean_text(row.get("nombre_proveedor")).casefold()
        for row, _ in positive_rows
        if _clean_text(row.get("nombre_proveedor"))
    }
    return {
        "positive_paid_rows": len(positive_rows),
        "total_positive_paid_cop": _decimal_output(
            sum((value for _, value in positive_rows), Decimal("0"))
        ),
        "entity_count": len(entities),
        "supplier_count": len(suppliers),
        "median_receipt_to_payment_days": _median_output(delays),
    }


def _top_entities(
    positive_rows: list[tuple[dict[str, Any], Decimal]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"payment_count": 0, "positive_paid_cop": Decimal("0")}
    )
    for row, value in positive_rows:
        name = _clean_text(row.get("nombre_entidad"))
        if not name:
            continue
        grouped[name]["payment_count"] += 1
        grouped[name]["positive_paid_cop"] += value
    ranked = sorted(
        grouped.items(),
        key=lambda item: (-item[1]["positive_paid_cop"], item[0].casefold()),
    )[:10]
    return [
        {
            "name": name,
            "payment_count": values["payment_count"],
            "positive_paid_cop": _decimal_output(values["positive_paid_cop"]),
        }
        for name, values in ranked
    ]


def _fetch_cuipo_execution(
    client: httpx.Client,
    *,
    now: datetime,
    previous_audit: dict[str, Any] | None,
) -> dict[str, Any]:
    base = {
        "source_name": "OVCF - CUIPO - Ejecucion de Gastos",
        "source_url": CUIPO_URL,
        "dataset_metadata_url": CUIPO_METADATA_URL,
        "checked_at": _iso_datetime(now),
        "usable_for_analysis": False,
        "caveats": [
            "CUIPO is a quarterly administrative snapshot reported by territorial entities.",
            "Dataset revisions can change a previously observed reporting period.",
            "Row counts are accounting records, not unique entities, projects, or transactions.",
        ],
    }
    try:
        metadata = _get_json_object(client, CUIPO_METADATA_URL)
        updated_epoch = _parse_int(metadata.get("rowsUpdatedAt"))
        reused = _reuse_previous_cuipo(
            base,
            previous_audit=previous_audit,
            updated_epoch=updated_epoch,
        )
        if reused is not None:
            return reused

        latest_rows = _get_json_list(
            client,
            CUIPO_URL,
            params={
                "$select": "periodo",
                "$where": f"periodo <= '{now.date().strftime('%Y%m%d')}'",
                "$order": "periodo DESC",
                "$limit": "1",
            },
        )
        if not latest_rows:
            return {**base, "status": "no_data", "error": "No CUIPO period as of run date."}
        period = _clean_text(latest_rows[0].get("periodo"))
        if not re.fullmatch(r"\d{8}", period):
            raise ValueError("CUIPO latest period is not YYYYMMDD")

        aggregate_rows = _get_json_list(
            client,
            CUIPO_URL,
            params={
                "$select": (
                    "ambito_nombre,count(*) as rows,sum(compromisos) as compromisos,"
                    "sum(obligaciones) as obligaciones,sum(pagos) as pagos"
                ),
                "$where": (
                    f"periodo='{period}' AND ambito_nombre in"
                    "('Departamentos','Municipios')"
                ),
                "$group": "ambito_nombre",
                "$order": "ambito_nombre",
            },
        )
        scopes = _parse_cuipo_scopes(aggregate_rows)
        return {
            **base,
            "status": "observed",
            "latest_period": period,
            "dataset_rows_updated_at_epoch": updated_epoch,
            "dataset_rows_updated_at": _epoch_iso(updated_epoch),
            "scopes": scopes,
            "usable_for_analysis": True,
        }
    except Exception as exc:  # pragma: no cover - final fail-closed guard.
        return _failed_source(base, exc)


def _reuse_previous_cuipo(
    base: dict[str, Any],
    *,
    previous_audit: dict[str, Any] | None,
    updated_epoch: int | None,
) -> dict[str, Any] | None:
    if updated_epoch is None or not previous_audit:
        return None
    previous_source = (
        (previous_audit.get("sources") or {}).get("cuipo_territorial_execution") or {}
    )
    previous_scopes = previous_source.get("scopes") or []
    previous_scope_names = {
        scope.get("scope") for scope in previous_scopes if isinstance(scope, dict)
    }
    if (
        previous_source.get("status") not in {"observed", "reused"}
        or previous_source.get("dataset_rows_updated_at_epoch") != updated_epoch
        or previous_scope_names != set(CUIPO_SCOPES)
    ):
        return None
    reused = copy.deepcopy(previous_source)
    reused.update(base)
    reused.update(
        {
            "status": "reused",
            "dataset_rows_updated_at_epoch": updated_epoch,
            "dataset_rows_updated_at": _epoch_iso(updated_epoch),
            "reused_from_run_date": previous_audit.get("run_date"),
            "usable_for_analysis": True,
        }
    )
    return reused


def _parse_cuipo_scopes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_scope = {_clean_text(row.get("ambito_nombre")): row for row in rows}
    if set(by_scope) != set(CUIPO_SCOPES):
        raise ValueError("CUIPO aggregate did not return both territorial scopes")
    scopes: list[dict[str, Any]] = []
    for scope in CUIPO_SCOPES:
        row = by_scope[scope]
        row_count = _parse_int(row.get("rows"))
        commitments = _parse_decimal(row.get("compromisos"))
        obligations = _parse_decimal(row.get("obligaciones"))
        payments = _parse_decimal(row.get("pagos"))
        if (
            row_count is None
            or row_count < 0
            or commitments is None
            or obligations is None
            or payments is None
            or min(commitments, obligations, payments) < 0
        ):
            raise ValueError(f"CUIPO aggregate has invalid values for {scope}")
        scopes.append(
            {
                "scope": scope,
                "rows": row_count,
                "compromisos_cop": _decimal_output(commitments),
                "obligaciones_cop": _decimal_output(obligations),
                "pagos_cop": _decimal_output(payments),
                "payment_to_commitment_ratio": _ratio(payments, commitments),
                "payment_to_obligation_ratio": _ratio(payments, obligations),
            }
        )
    return scopes


def _render_secop(lines: list[str], source: dict[str, Any]) -> None:
    lines.extend(
        [
            "## SECOP II Plan de pagos",
            "",
            f"- Status: {source.get('status', 'missing')}",
            f"- Latest payment date: {source.get('latest_payment_date', 'not available')}",
            f"- Returned rows: {source.get('returned_rows', 0)}",
            f"- Complete latest day: {bool(source.get('complete_day'))}",
            f"- Usable within shadow audit: {bool(source.get('usable_for_analysis'))}",
        ]
    )
    metrics = source.get("metrics") or {}
    if metrics:
        lines.extend(
            [
                f"- Positive paid rows: {metrics.get('positive_paid_rows', 0)}",
                "- Total positive paid value: "
                f"COP {_format_number(metrics.get('total_positive_paid_cop'))}",
                f"- Entities: {metrics.get('entity_count', 0)}",
                f"- Suppliers: {metrics.get('supplier_count', 0)}",
                "- Median receipt-to-payment delay: "
                f"{metrics.get('median_receipt_to_payment_days', 'not available')} days",
            ]
        )
    else:
        lines.append("- Monetary totals and entity ranking: withheld")
    _render_caveats(lines, source)


def _render_cuipo(lines: list[str], source: dict[str, Any]) -> None:
    lines.extend(
        [
            "## CUIPO territorial execution",
            "",
            f"- Status: {source.get('status', 'missing')}",
            f"- Latest period: {source.get('latest_period', 'not available')}",
            f"- Usable within shadow audit: {bool(source.get('usable_for_analysis'))}",
        ]
    )
    if source.get("reused_from_run_date"):
        lines.append(f"- Reused from run: {source['reused_from_run_date']}")
    for scope in source.get("scopes") or []:
        lines.extend(
            [
                "",
                f"### {scope.get('scope', 'Unknown scope')}",
                "",
                f"- Accounting rows: {_format_number(scope.get('rows'))}",
                f"- Commitments: COP {_format_number(scope.get('compromisos_cop'))}",
                f"- Obligations: COP {_format_number(scope.get('obligaciones_cop'))}",
                f"- Payments: COP {_format_number(scope.get('pagos_cop'))}",
                "- Payment / commitment: "
                f"{_format_ratio(scope.get('payment_to_commitment_ratio'))}",
                "- Payment / obligation: "
                f"{_format_ratio(scope.get('payment_to_obligation_ratio'))}",
            ]
        )
    _render_caveats(lines, source)


def _render_caveats(lines: list[str], source: dict[str, Any]) -> None:
    error = source.get("error")
    if error:
        lines.extend(["", f"Error: {error}"])
    caveats = source.get("caveats") or []
    if caveats:
        lines.extend(["", "Caveats:", ""])
        lines.extend(f"- {caveat}" for caveat in caveats)
    lines.append("")


def _get_json_list(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    response = client.get(url, params=params)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
        raise ValueError("Official data endpoint returned an unexpected JSON shape")
    return payload


def _get_json_object(client: httpx.Client, url: str) -> dict[str, Any]:
    response = client.get(url)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Official metadata endpoint returned an unexpected JSON shape")
    return payload


def _failed_source(base: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return {
        **base,
        "status": "failed",
        "error": f"{exc.__class__.__name__}: {exc}",
        "usable_for_analysis": False,
    }


def _parse_socrata_date(value: Any) -> date | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _parse_int(value: Any) -> int | None:
    parsed = _parse_decimal(value)
    if parsed is None or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _decimal_output(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _median_output(values: list[int]) -> int | float | None:
    if not values:
        return None
    result = median(values)
    return int(result) if float(result).is_integer() else float(result)


def _ratio(numerator: Decimal, denominator: Decimal) -> float | None:
    if denominator == 0:
        return None
    return round(float(numerator / denominator), 4)


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _iso_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_number(value: Any) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return "not available"


def _format_ratio(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "not available"
    return f"{float(value):.1%}"
