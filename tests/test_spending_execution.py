from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx

from colombia_forecasting_desk.spending_execution import (
    CUIPO_METADATA_URL,
    CUIPO_URL,
    SECOP_PAYMENTS_URL,
    fetch_spending_execution_audit,
    load_previous_spending_execution_audit,
    render_spending_execution_audit,
)

NOW = datetime(2026, 9, 1, 23, 59, 59, tzinfo=timezone.utc)
UPDATED_EPOCH = 1_788_200_000


def _secop_row(
    payment_id: str,
    value: str,
    *,
    entity: str = "Entidad A",
    supplier: str = "PERSONA PROVEEDORA",
    receipt_date: str = "2026-08-29T00:00:00.000",
) -> dict[str, str]:
    return {
        "id_del_contrato": "CO1.PCCNTR.123",
        "id_de_pago": payment_id,
        "fecha_de_recepcion": receipt_date,
        "fecha_real_de_pago": "2026-08-30T00:00:00.000",
        "valor_a_pagar": value,
        "codigo_entidad": "123",
        "nombre_entidad": entity,
        "nombre_proveedor": supplier,
    }


def _cuipo_rows() -> list[dict[str, str]]:
    return [
        {
            "ambito_nombre": "Departamentos",
            "rows": "10",
            "compromisos": "1000",
            "obligaciones": "800",
            "pagos": "600",
        },
        {
            "ambito_nombre": "Municipios",
            "rows": "20",
            "compromisos": "2000",
            "obligaciones": "1600",
            "pagos": "1200",
        },
    ]


def _complete_handler(request: httpx.Request) -> httpx.Response:
    if str(request.url).startswith(CUIPO_METADATA_URL):
        return httpx.Response(200, json={"rowsUpdatedAt": UPDATED_EPOCH})
    if request.url.path.endswith("uymx-8p3j.json"):
        select = request.url.params.get("$select", "")
        if select == "id_del_contrato,id_de_pago,fecha_real_de_pago":
            return httpx.Response(
                200,
                json=[{"fecha_real_de_pago": "2026-08-30T00:00:00.000"}],
            )
        return httpx.Response(
            200,
            json=[
                _secop_row(
                    "1",
                    "100",
                    receipt_date="2026-08-28T00:00:00.000",
                ),
                _secop_row(
                    "2",
                    "300",
                    entity="Entidad B",
                    supplier="EMPRESA PROVEEDORA",
                ),
                _secop_row("3", "0", receipt_date="2026-08-30T00:00:00.000"),
            ],
        )
    if request.url.path.endswith("4f7r-epif.json"):
        if request.url.params.get("$select") == "periodo":
            return httpx.Response(200, json=[{"periodo": "20260601"}])
        return httpx.Response(200, json=_cuipo_rows())
    raise AssertionError(f"Unexpected request: {request.url}")


def test_fetch_builds_bounded_shadow_audit_without_personal_rows() -> None:
    with httpx.Client(transport=httpx.MockTransport(_complete_handler)) as client:
        audit = fetch_spending_execution_audit(now=NOW, client=client)

    assert audit["schema_version"] == "spending_execution_audit.v1"
    assert audit["mode"] == "shadow_audit"
    assert audit["policy"]["m2_m3_eligible"] is False
    assert audit["policy"]["automatic_promotion"] is False
    assert audit["summary"]["observed_count"] == 2

    secop = audit["sources"]["secop_ii_plan_pagos"]
    assert secop["status"] == "observed"
    assert secop["complete_day"] is True
    assert secop["quality"]["positive_value_rows"] == 2
    assert secop["quality"]["nonpositive_value_rows"] == 1
    assert secop["metrics"] == {
        "positive_paid_rows": 2,
        "total_positive_paid_cop": 400,
        "entity_count": 2,
        "supplier_count": 2,
        "median_receipt_to_payment_days": 1,
    }
    assert secop["top_entities"][0]["name"] == "Entidad B"

    serialized = json.dumps(audit, ensure_ascii=False)
    assert "PERSONA PROVEEDORA" not in serialized
    assert "EMPRESA PROVEEDORA" not in serialized
    assert "documento_proveedor" not in serialized
    assert "documento_supervisor" not in serialized

    cuipo = audit["sources"]["cuipo_territorial_execution"]
    assert cuipo["status"] == "observed"
    assert cuipo["latest_period"] == "20260601"
    assert cuipo["scopes"][0]["payment_to_commitment_ratio"] == 0.6
    assert cuipo["scopes"][0]["payment_to_obligation_ratio"] == 0.75


def test_secop_truncation_withholds_totals_and_rankings() -> None:
    with httpx.Client(transport=httpx.MockTransport(_complete_handler)) as client:
        audit = fetch_spending_execution_audit(
            now=NOW,
            client=client,
            secop_row_limit=2,
        )

    secop = audit["sources"]["secop_ii_plan_pagos"]
    assert secop["status"] == "truncated"
    assert secop["returned_rows"] == 3
    assert secop["inspected_rows"] == 2
    assert secop["complete_day"] is False
    assert secop["usable_for_analysis"] is False
    assert "metrics" not in secop
    assert "top_entities" not in secop
    assert audit["summary"]["truncated_count"] == 1


def test_secop_duplicate_keys_fail_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("uymx-8p3j.json"):
            select = request.url.params.get("$select", "")
            if select == "id_del_contrato,id_de_pago,fecha_real_de_pago":
                return httpx.Response(
                    200,
                    json=[{"fecha_real_de_pago": "2026-08-30T00:00:00.000"}],
                )
            return httpx.Response(
                200,
                json=[_secop_row("1", "100"), _secop_row("1", "100")],
            )
        return _complete_handler(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        audit = fetch_spending_execution_audit(now=NOW, client=client)

    secop = audit["sources"]["secop_ii_plan_pagos"]
    assert secop["status"] == "quality_warning"
    assert secop["quality"]["duplicate_payment_keys"] == 1
    assert secop["usable_for_analysis"] is False
    assert "metrics" not in secop
    assert "top_entities" not in secop
    assert audit["summary"]["quality_warning_count"] == 1


def test_source_failure_does_not_hide_other_official_source() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("uymx-8p3j.json"):
            return httpx.Response(503, text="unavailable")
        return _complete_handler(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        audit = fetch_spending_execution_audit(now=NOW, client=client)

    assert audit["sources"]["secop_ii_plan_pagos"]["status"] == "failed"
    assert audit["sources"]["cuipo_territorial_execution"]["status"] == "observed"
    assert audit["summary"]["failed_count"] == 1
    assert audit["summary"]["observed_count"] == 1


def test_cuipo_reuses_unchanged_previous_snapshot() -> None:
    previous = {
        "schema_version": "spending_execution_audit.v1",
        "run_date": "2026-08-31",
        "sources": {
            "cuipo_territorial_execution": {
                "status": "observed",
                "latest_period": "20260601",
                "dataset_rows_updated_at_epoch": UPDATED_EPOCH,
                "scopes": [
                    {
                        "scope": "Departamentos",
                        "rows": 10,
                        "compromisos_cop": 1000,
                        "obligaciones_cop": 800,
                        "pagos_cop": 600,
                        "payment_to_commitment_ratio": 0.6,
                        "payment_to_obligation_ratio": 0.75,
                    },
                    {
                        "scope": "Municipios",
                        "rows": 20,
                        "compromisos_cop": 2000,
                        "obligaciones_cop": 1600,
                        "pagos_cop": 1200,
                        "payment_to_commitment_ratio": 0.6,
                        "payment_to_obligation_ratio": 0.75,
                    },
                ],
            }
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("uymx-8p3j.json"):
            return httpx.Response(200, json=[])
        if str(request.url).startswith(CUIPO_METADATA_URL):
            return httpx.Response(200, json={"rowsUpdatedAt": UPDATED_EPOCH})
        if str(request.url).startswith(CUIPO_URL):
            raise AssertionError("CUIPO data endpoint should not be queried")
        raise AssertionError(f"Unexpected request: {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        audit = fetch_spending_execution_audit(
            now=NOW,
            client=client,
            previous_audit=previous,
        )

    cuipo = audit["sources"]["cuipo_territorial_execution"]
    assert cuipo["status"] == "reused"
    assert cuipo["reused_from_run_date"] == "2026-08-31"
    assert cuipo["usable_for_analysis"] is True


def test_load_previous_audit_skips_current_and_invalid_runs(tmp_path) -> None:
    older = tmp_path / "2026-08-30"
    older.mkdir()
    (older / "spending_execution_audit.json").write_text(
        json.dumps(
            {
                "schema_version": "spending_execution_audit.v1",
                "run_date": "2026-08-30",
            }
        ),
        encoding="utf-8",
    )
    invalid = tmp_path / "2026-08-31"
    invalid.mkdir()
    (invalid / "spending_execution_audit.json").write_text("[]", encoding="utf-8")
    current = tmp_path / "2026-09-01"
    current.mkdir()
    (current / "spending_execution_audit.json").write_text(
        json.dumps(
            {
                "schema_version": "spending_execution_audit.v1",
                "run_date": "2026-09-01",
            }
        ),
        encoding="utf-8",
    )

    loaded = load_previous_spending_execution_audit(tmp_path, "2026-09-01")

    assert loaded is not None
    assert loaded["run_date"] == "2026-08-30"


def test_markdown_keeps_shadow_boundary_visible() -> None:
    with httpx.Client(transport=httpx.MockTransport(_complete_handler)) as client:
        audit = fetch_spending_execution_audit(now=NOW, client=client)

    rendered = render_spending_execution_audit(audit)

    assert "Shadow diagnostic only" in rendered
    assert "not eligible for M2/M3" in rendered
    assert "does not identify fraud or misconduct" in rendered
