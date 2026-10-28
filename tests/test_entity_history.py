from __future__ import annotations

from datetime import date
import json

import httpx
import pytest

from colombia_forecasting_desk.entity_history import build_entity_history, render_entity_history
from colombia_forecasting_desk.plain_language import format_cop, validate_plain_text


def _entry(kind="public_entity"):
    return {"watch_id": "synthetic-organization", "kind": kind, "label": "Synthetic Organization", "identifiers": {"nit": "900123456"}, "demo": True}


def _transport(*, failure=False, truncate=False, duplicates=False, aliases=False, many_suppliers=False, missing_denominator=False):
    requests = []

    def handler(request):
        requests.append(request)
        p = request.url.params
        select = p.get("$select", "")
        if failure:
            return httpx.Response(400, json={"error": "Synthetic source failure"})
        if request.url.path.endswith("4n4q-k399.json"):
            return httpx.Response(200, json=[{"records": "1", "earliest_firm_at": "2026-04-01T00:00:00", "latest_firm_at": "2026-04-01T00:00:00"}])
        if request.url.path.endswith("cb9c-h8sn.json"):
            return httpx.Response(200, json=[{"events": "3"}])
        if "max(valor_total_de_adiciones" in select:
            return httpx.Response(200, json=[{"records": "1", "max_ratio": "0.75", "additions_value_cop": "750"}])
        if "count(*) as records,count(distinct" in select:
            return httpx.Response(200, json=[{"records": "21" if duplicates else "200" if many_suppliers else "20", "contracts": "200" if many_suppliers else "20", "valued_records": "20", "value_cop": "1000", "max_value_cop": "500"}])
        if select in {"valor_del_contrato", "cuantia_contrato"}:
            return httpx.Response(200, json=[{select: "40"}, {select: "60"}])
        if " as organization_nit" in select:
            if aliases:
                return httpx.Response(200, json=[{"organization_nit": nit, "contracts": "12", "value_cop": "500"} for nit in ("830122566", "8301225661")])
            return httpx.Response(200, json=[{"organization_nit": "900123456", "contracts": "10", "value_cop": "500"}])
        if " as buyer_id" in select:
            return httpx.Response(200, json=[{"buyer_id": "900000001", "contracts": "10", "value_cop": "500"}])
        if " as organization_id" in select:
            if aliases:
                return httpx.Response(200, json=[{"organization_id": nit, "contracts": "12"} for nit in ("830122566", "8301225661")])
            count = 7 if many_suppliers else 1
            return httpx.Response(200, json=[{"organization_id": str(900123456 + index), "organization_name": f"Synthetic Supplier {index}", "contracts": str(12 - index)} for index in range(count)])
        if select == "id_contrato":
            return httpx.Response(200, json=[{"id_contrato": "synthetic-contract-a"}, {"id_contrato": "synthetic-contract-b"}] if truncate else [{"id_contrato": "synthetic-contract-a"}])
        if select.startswith("count(distinct"):
            return httpx.Response(200, json=[{}] if missing_denominator else [{"contracts": "100" if many_suppliers else "20"}])
        if select.startswith("count(*) as records,sum("):
            return httpx.Response(200, json=[{"records": "2", "value_cop": "100"}])
        if select == "count(*) as records":
            return httpx.Response(200, json=[{"records": "1"}])
        raise AssertionError(f"Unexpected select {select}")

    return httpx.MockTransport(handler), requests


def test_history_uses_population_median_and_separate_datasets_without_person_id_queries():
    transport, requests = _transport()
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {}, as_of=date(2026, 10, 27), client=client, fetched_at="2026-10-27T17:00:00Z")
    assert result["datasets"]["secop_ii"]["summary"]["median_value_cop"] == 50
    assert result["datasets"]["secop_i"]["summary"]["median_value_cop"] == 50
    assert result["datasets"]["secop_ii_additions"]["monetary_amounts_available"] is False
    assert result["datasets"]["secop_ii_additions"]["events"] == 3
    assert {pattern["kind"] for pattern in result["patterns"]} >= {"supplier_share", "repeat_direct_award", "large_addition"}
    assert not any(pattern["kind"] == "sanctioned_supplier" for pattern in result["patterns"])
    assert any(request.url.params.get("$offset") == "9" and request.url.params.get("$limit") == "2" for request in requests)
    for request in requests:
        select = request.url.params.get("$select", "")
        if "proveedor_adjudicado" in select or "nom_razon_social_contratista" in select:
            assert "('NIT','N.I.T.')" in request.url.params["$where"]
        if " as organization_nit" in select:
            assert "('NIT','N.I.T.')" in request.url.params["$where"]
    assert result["window_start"] == "2024-10-27"
    assert result["datasets"]["secop_i"]["undated_signature_records_loaded_in_window"] == 1
    assert any("no signature date" in text for text in result["coverage_gaps"])
    first_where = next(request.url.params["$where"] for request in requests if request.url.path.endswith("jbjy-vk9h.json"))
    assert "nit_entidad in (900123456," in first_where
    assert "nit_entidad in ('900123456'" not in first_where


def test_known_company_checks_post_sanction_dates_without_accusations():
    transport, requests = _transport()
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry("company"), {}, as_of=date(2026, 10, 27), client=client)
    prompts = [pattern for pattern in result["patterns"] if pattern["kind"] == "sanctioned_supplier"]
    assert len(prompts) == 2
    assert all("does not establish wrongdoing" in pattern["caveat"] for pattern in prompts)
    assert any("fecha_de_firma > '2026-04-01T00:00:00'" in request.url.params.get("$where", "") for request in requests)
    assert all("median" not in pattern["kind"] for pattern in result["patterns"])


def test_query_failure_remains_unknown_and_does_not_create_clean_history():
    transport, _ = _transport(failure=True)
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {}, as_of=date(2026, 10, 27), client=client)
    assert result["datasets"]["secop_ii"]["summary"]["status"] == "unknown"
    assert result["patterns"] == []
    assert result["coverage_gaps"]
    assert all(query["status"] == "failed" for query in result["queries"])


def test_truncation_and_duplicate_contracts_are_explicit():
    transport, _ = _transport(truncate=True, duplicates=True)
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {"contract_id_limit": 2}, as_of=date(2026, 10, 27), client=client)
    assert result["datasets"]["secop_ii_additions"]["complete_contract_linkage"] is False
    assert any("group limit" in text for text in result["coverage_gaps"])
    assert any("distinct contract count" in text for text in result["coverage_gaps"])
    assert "SECOP I and II" in json.dumps(result["caveats"])
    assert "separately" in render_entity_history(result)


def test_base_and_check_digit_aliases_with_unknown_contract_overlap_are_withheld():
    transport, _ = _transport(aliases=True)
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {}, as_of=date(2026, 10, 27), client=client)
    assert not any(pattern["kind"] in {"supplier_share", "repeat_direct_award"} for pattern in result["patterns"])
    assert result["datasets"]["secop_ii"]["organizations"] == []
    assert any("830122566" in gap and "overlap is unknown" in gap for gap in result["coverage_gaps"])
    ids = [pattern["pattern_id"] for pattern in result["patterns"]]
    assert len(ids) == len(set(ids))


def test_direct_awards_name_top_five_organizations_and_use_all_buyer_awards():
    transport, requests = _transport(many_suppliers=True)
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {}, as_of=date(2026, 10, 27), client=client)
    for dataset in ("secop_i", "secop_ii"):
        screen = result["datasets"][dataset]["direct_awards"]
        assert screen["contracts"] == 100
        assert [row["contracts"] for row in screen["top_organizations"]] == [12, 11, 10, 9, 8]
        assert screen["top_organizations"][0]["share"] == .12
        assert all(row["organization_name"].startswith("Synthetic Supplier") for row in screen["top_organizations"])
    denominators = [request for request in requests if request.url.params.get("$select", "").startswith("count(distinct")]
    assert len(denominators) == 2
    assert all("DIRECTA" in request.url.params["$where"] and "NIT" not in request.url.params["$where"] for request in denominators)
    text = render_entity_history(result)
    assert "Synthetic Supplier 0 received 12 direct-award contracts, 12.0%" in text
    assert "Synthetic Supplier 5" not in text
    assert "ID groups" not in text
    assert all(not validate_plain_text(pattern["plain_text"]) for pattern in result["patterns"])


def test_unknown_direct_award_denominator_never_becomes_zero_or_a_share():
    transport, _ = _transport(missing_denominator=True)
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {}, as_of=date(2026, 10, 27), client=client)
    assert result["datasets"]["secop_ii"]["direct_awards"]["contracts"] is None
    assert not any(pattern["kind"] == "repeat_direct_award" for pattern in result["patterns"])
    assert any("supplier shares are unknown" in gap for gap in result["coverage_gaps"])


@pytest.mark.parametrize(("amount", "expected"), [(7_470_000_000_000, "COP 7.47 trillion"), (1_200_000_000, "COP 1.2 billion"), (2_500_000, "COP 2.5 million"), (5100, "COP 5,100"), (0, "COP 0"), (None, "unknown")])
def test_money_uses_plain_english_units(amount, expected):
    assert format_cop(amount) == expected


def test_entity_report_validates_visible_text_and_keeps_query_urls():
    transport, _ = _transport()
    with httpx.Client(transport=transport) as client:
        result = build_entity_history(_entry(), {}, as_of=date(2026, 10, 27), client=client)
    result["datasets"]["secop_ii"]["summary"]["value_cop"] = 7_470_000_000_000
    text = render_entity_history(result)
    assert "COP 7.47 trillion" in text
    assert not validate_plain_text(text)
    assert "recorded in SECOP II contracts" in text
    assert "jbjy-vk9h totals" not in text
    assert "https://www.datos.gov.co/resource/jbjy-vk9h.json" in text
    result["label"] = "M3 supplier packet"
    with pytest.raises(ValueError, match="internal token"):
        render_entity_history(result)
