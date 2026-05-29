from __future__ import annotations

from colombia_forecasting_desk.procurement_leads import (
    build_procurement_concentration_leads,
)


def _secop_raw(make_raw, item_id: str, fields: dict, entity: str = "Alcaldia de Cali"):
    return make_raw(
        id=item_id,
        source_id="secop_ii_contratos",
        source_name="SECOP II Contratos",
        source_type="dataset",
        url=f"https://www.datos.gov.co/resource/jbjy-vk9h.json?id={item_id}",
        title=f"SECOP II Contrato — {item_id}",
        published_at="2026-05-22T00:00:00Z",
        metadata={
            "entity": entity,
            "socrata_fields": fields,
        },
    )


def _secop_cleaned(make_cleaned, item_id: str, title: str = "SECOP II Contrato"):
    return make_cleaned(
        id=item_id,
        source_id="secop_ii_contratos",
        source_name="SECOP II Contratos",
        source_type="dataset",
        url=f"https://www.datos.gov.co/resource/jbjy-vk9h.json?id={item_id}",
        title=title,
        published_at="2026-05-22T00:00:00Z",
        clean_text=title,
        signal_type="new_data",
        trust_role="civic_signal",
    )


def test_procurement_leads_surface_repeated_supplier_entity_pair(
    make_raw,
    make_cleaned,
) -> None:
    raw = [
        _secop_raw(
            make_raw,
            "c1",
            {
                "proveedor_adjudicado": "Constructora Demo SAS",
                "documento_proveedor": "901111111",
                "valor_del_contrato": "120000000",
                "modalidad_de_contratacion": "Licitación pública",
                "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=1",
            },
        ),
        _secop_raw(
            make_raw,
            "c2",
            {
                "proveedor_adjudicado": "Constructora Demo SAS",
                "documento_proveedor": "901111111",
                "valor_del_contrato": "85000000",
                "modalidad_de_contratacion": "Selección abreviada",
                "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=2",
            },
        ),
    ]
    cleaned = [_secop_cleaned(make_cleaned, "c1"), _secop_cleaned(make_cleaned, "c2")]

    leads = build_procurement_concentration_leads(raw, cleaned)

    assert len(leads) == 1
    lead = leads[0]
    assert lead["lead_type"] == "analyst_insight"
    assert lead["review_context"]["pattern"] == "repeated_supplier_entity_pair"
    assert "Constructora Demo SAS" in lead["claim_or_question"]
    assert "not evidence of wrongdoing" in lead["caveats"][0]
    assert len(lead["evidence"]) == 2
    assert lead["evidence"][0]["content_kind"] == "structured_procurement"
    assert lead["source_refs"]["source_urls"][0].startswith("https://community.secop.gov.co")


def test_procurement_leads_normalize_legacy_url_object(
    make_raw,
    make_cleaned,
) -> None:
    raw = [
        _secop_raw(
            make_raw,
            "c1",
            {
                "proveedor_adjudicado": "Constructora Demo SAS",
                "urlproceso": {
                    "url": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=1"
                },
            },
        ),
        _secop_raw(
            make_raw,
            "c2",
            {
                "proveedor_adjudicado": "Constructora Demo SAS",
                "urlproceso": {
                    "url": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=2"
                },
            },
        ),
    ]
    cleaned = [_secop_cleaned(make_cleaned, "c1"), _secop_cleaned(make_cleaned, "c2")]

    leads = build_procurement_concentration_leads(raw, cleaned)

    assert len(leads) == 1
    urls = leads[0]["source_refs"]["source_urls"]
    assert urls == [
        "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=1",
        "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=2",
    ]
    assert all(evidence["url"].startswith("https://") for evidence in leads[0]["evidence"])


def test_procurement_leads_surface_direct_contracting_concentration(
    make_raw,
    make_cleaned,
) -> None:
    raw = [
        _secop_raw(
            make_raw,
            "d1",
            {"modalidad_de_contratacion": "Contratación directa"},
        ),
        _secop_raw(
            make_raw,
            "d2",
            {"modalidad_de_contratacion": "Contratación directa"},
        ),
        _secop_raw(
            make_raw,
            "d3",
            {"modalidad_de_contratacion": "Mínima cuantía"},
        ),
    ]
    cleaned = [
        _secop_cleaned(make_cleaned, "d1"),
        _secop_cleaned(make_cleaned, "d2"),
        _secop_cleaned(make_cleaned, "d3"),
    ]

    leads = build_procurement_concentration_leads(raw, cleaned)

    assert len(leads) == 1
    lead = leads[0]
    assert lead["review_context"]["pattern"] == "direct_contracting_concentration"
    assert "2 direct-contracting records out of 3" in lead["claim_or_question"]
    assert lead["disposition"] == "monitor_or_research"
    assert any("legal and routine" in caveat for caveat in lead["caveats"])


def test_procurement_leads_ignore_non_secop_and_low_signal_rows(
    make_raw,
    make_cleaned,
) -> None:
    raw = [
        make_raw(
            id="n1",
            source_id="eltiempo_colombia",
            metadata={"entity": "Alcaldia de Cali"},
        )
    ]
    cleaned = [
        make_cleaned(
            id="n1",
            source_id="eltiempo_colombia",
            quality_notes="",
        ),
        _secop_cleaned(make_cleaned, "s1"),
    ]

    assert build_procurement_concentration_leads(raw, cleaned) == []


def test_procurement_leads_do_not_count_open_processes_as_low_competition(
    make_raw,
    make_cleaned,
) -> None:
    raw = [
        make_raw(
            id="p1",
            source_id="secop_ii_procesos",
            source_name="SECOP II Procesos",
            source_type="dataset",
            metadata={
                "entity": "INSTITUCION EDUCATIVA DEMO",
                "socrata_fields": {
                    "estado_del_procedimiento": "Publicado",
                    "proveedores_unicos_con": "0",
                    "modalidad_de_contratacion": "Contratación régimen especial",
                },
            },
        ),
        make_raw(
            id="p2",
            source_id="secop_ii_procesos",
            source_name="SECOP II Procesos",
            source_type="dataset",
            metadata={
                "entity": "INSTITUCION EDUCATIVA DEMO",
                "socrata_fields": {
                    "estado_del_procedimiento": "Publicado",
                    "proveedores_unicos_con": "0",
                    "modalidad_de_contratacion": "Contratación régimen especial",
                },
            },
        ),
    ]
    cleaned = [
        make_cleaned(
            id="p1",
            source_id="secop_ii_procesos",
            source_name="SECOP II Procesos",
            source_type="dataset",
            published_at="2026-05-22T00:00:00Z",
        ),
        make_cleaned(
            id="p2",
            source_id="secop_ii_procesos",
            source_name="SECOP II Procesos",
            source_type="dataset",
            published_at="2026-05-22T00:00:00Z",
        ),
    ]

    assert build_procurement_concentration_leads(raw, cleaned) == []
