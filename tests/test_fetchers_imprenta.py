from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_extract_imprenta_table_includes_document_title_when_available(sample_source) -> None:
    source = replace(sample_source, id="gacetas_congreso", type="legal")
    html = """
    <table>
      <tr>
        <td>401</td>
        <td>Cámara de Representantes</td>
        <td>04/05/2026</td>
        <td>Informe de ponencia para primer debate reforma laboral</td>
        <td><button>ui-button</button></td>
      </tr>
    </table>
    """

    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
        source,
        "2026-05-06T00:00:00Z",
        edition_label="Gaceta del Congreso",
        query_param="gaceta",
    )

    assert len(items) == 1
    assert "reforma laboral" in items[0].title
    assert items[0].metadata["document_title"].startswith("Informe de ponencia")


def test_extract_imprenta_table_records_download_button(sample_source) -> None:
    source = replace(sample_source, id="gacetas_congreso", type="legal")
    html = """
    <table>
      <tr>
        <td>476</td>
        <td>Senado de la República</td>
        <td>14/05/2026</td>
        <td></td>
        <td><button name="formResumen:dataTableResumen:0:btnDescargarPdf">
          ui-button
        </button></td>
      </tr>
    </table>
    """

    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
        source,
        "2026-05-15T00:00:00Z",
        edition_label="Gaceta del Congreso",
        query_param="gaceta",
    )

    assert items[0].metadata["download_button_name"] == (
        "formResumen:dataTableResumen:0:btnDescargarPdf"
    )
    assert items[0].metadata["download_mechanism"] == "jsf_postback"


def test_parse_diario_oficial_pdf_text_extracts_legal_act_identities() -> None:
    parsed = _parse_diario_oficial_pdf_text(
        "Diario Oficial. Ministerio de Comercio, Industria y Turismo. "
        "Resolución No. 2118 del 26 de diciembre de 2025 por la cual se "
        "declara la Zona Franca Permanente Especial De Servicios Rionegro MRO."
    )

    assert parsed is not None
    assert parsed["legal_act_records"][0]["label"] == "Resolución 2118 de 2025"
    assert parsed["parse_status"] == "legal_act_identities_found"


def test_parse_diario_oficial_pdf_text_marks_readable_pdf_without_legal_acts() -> None:
    parsed = _parse_diario_oficial_pdf_text(
        "Diario Oficial 53.493. Imprenta Nacional de Colombia. "
        "Esta edicion contiene informacion institucional sobre servicios "
        "graficos y gestion documental, sin actos normativos publicados."
    )

    assert parsed is not None
    assert parsed["legal_act_records"] == []
    assert parsed["parse_status"] == "parsed_no_legal_act_identities"


def test_annotate_legal_identity_items_marks_gestor_normativo_anchor() -> None:
    item = RawItem(
        id="gestor-1",
        source_id="gestor_normativo_fp",
        source_name="Gestor Normativo",
        source_type="legal",
        url="https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=1",
        title="Resolución 110 de 2016",
        fetched_at="2026-05-15T00:00:00Z",
        raw_text="Resolución 110 de 2016 establece lineamientos.",
        metadata={"extraction": "anchor"},
    )

    annotated = _annotate_legal_identity_items([item])

    assert annotated[0].metadata["legal_act_records"][0]["label"] == (
        "Resolución 110 de 2016"
    )


def test_enrich_diario_oficial_pdfs_marks_pdf_as_parsed_legal_acts(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(sample_source, id="diario_oficial", type="legal")
    html = """
    <form id="frmConDiario" action="/diario/index.xhtml" method="post">
      <input type="hidden" name="frmConDiario" value="frmConDiario" />
      <input type="hidden" name="javax.faces.ViewState" value="view-state-2" />
      <table>
        <tr>
          <td>53.490</td>
          <td>Ordinaria</td>
          <td>14/05/2026</td>
          <td><button name="dtbDiariosOficiales:0:j_idt34">ui-button</button></td>
        </tr>
      </table>
    </form>
    """
    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        source,
        "2026-05-15T00:00:00Z",
        edition_label="Diario Oficial",
        query_param="edicion",
    )
    client = _FakeDiarioPdfClient()
    monkeypatch.setattr(
        imprenta_fetchers,
        "_extract_pdf_text_with_pdfplumber",
        lambda content, *, max_chars: (
            "Diario Oficial 53.490. Ministerio de Comercio, Industria y "
            "Turismo. RESOLUCIÓN NÚMERO 2118 DE 2026 por la cual se declara la "
            "Zona Franca Permanente Especial De Servicios Rionegro MRO."
        ),
    )

    enriched = _enrich_diario_oficial_pdfs(
        items,
        client,
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        max_items=1,
    )

    assert enriched[0].metadata["content_extraction"] == "diario_oficial_pdf_text"
    assert enriched[0].metadata["legal_act_records"][0]["label"] == (
        "Resolución 2118 de 2026"
    )
    assert enriched[0].metadata["document_row_type"] == "diario_legal_act"
    assert enriched[0].metadata["pdf_parse_status"] == "legal_act_identities_found"
    assert enriched[0].url.endswith("#act-resolucion-2118-de-2026")
    assert "Resolución 2118 de 2026" in enriched[0].title
    assert "Rionegro MRO" in enriched[0].raw_text
    assert client.posts[0][1]["javax.faces.ViewState"] == "view-state-2"
    assert client.posts[0][1]["frmConDiario"] == "frmConDiario"
    assert "dynamiccontent.properties.xhtml" in client.gets[0][0]
    assert enriched[0].metadata["pdf_embedded_url"].endswith("pfdrid=abc")


def test_enrich_diario_oficial_pdfs_marks_no_identity_pdf_as_parsed(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(sample_source, id="diario_oficial", type="legal")
    html = """
    <form id="frmConDiario" action="/diario/index.xhtml" method="post">
      <input type="hidden" name="frmConDiario" value="frmConDiario" />
      <input type="hidden" name="javax.faces.ViewState" value="view-state-2" />
      <table>
        <tr>
          <td>53.493</td>
          <td>Ordinaria</td>
          <td>17/05/2026</td>
          <td><button name="dtbDiariosOficiales:0:j_idt34">ui-button</button></td>
        </tr>
      </table>
    </form>
    """
    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        source,
        "2026-05-18T00:00:00Z",
        edition_label="Diario Oficial",
        query_param="edicion",
    )
    client = _FakeDiarioPdfClient()
    monkeypatch.setattr(
        imprenta_fetchers,
        "_extract_pdf_text_with_pdfplumber",
        lambda content, *, max_chars: (
            "Diario Oficial 53.493. Imprenta Nacional de Colombia publica "
            "informacion institucional y no registra actos normativos."
        ),
    )

    enriched = _enrich_diario_oficial_pdfs(
        items,
        client,
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        max_items=1,
    )

    assert enriched[0].metadata["content_extraction"] == "diario_oficial_pdf_text"
    assert enriched[0].metadata["legal_act_record_count"] == 0
    assert enriched[0].metadata["pdf_parse_status"] == (
        "parsed_no_legal_act_identities"
    )
    assert "PDF parsed; no legal-act identities found" in enriched[0].raw_text


def test_enrich_diario_oficial_pdfs_emits_one_row_per_published_act(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(sample_source, id="diario_oficial", type="legal")
    html = """
    <form id="frmConDiario" action="/diario/index.xhtml" method="post">
      <input type="hidden" name="frmConDiario" value="frmConDiario" />
      <input type="hidden" name="javax.faces.ViewState" value="view-state-2" />
      <table>
        <tr>
          <td>53.491</td>
          <td>Ordinaria</td>
          <td>15/05/2026</td>
          <td><button name="dtbDiariosOficiales:0:j_idt34">ui-button</button></td>
        </tr>
      </table>
    </form>
    """
    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        source,
        "2026-05-18T00:00:00Z",
        edition_label="Diario Oficial",
        query_param="edicion",
    )
    client = _FakeDiarioPdfClient()
    monkeypatch.setattr(
        imprenta_fetchers,
        "_extract_pdf_text_with_pdfplumber",
        lambda content, *, max_chars: (
            "DECRETO NÚMERO 0502 DE 2026 por el cual se designa un "
            "gobernador encargado. Decreto número 1083 de 2015 citado. "
            "RESOLUCIÓN NÚMERO 1002 DE 2026 por la cual se modifica un "
            "procedimiento administrativo."
        ),
    )

    enriched = _enrich_diario_oficial_pdfs(
        items,
        client,
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        max_items=1,
    )

    assert [item.metadata["legal_act_records"][0]["label"] for item in enriched] == [
        "Decreto 502 de 2026",
        "Resolución 1002 de 2026",
    ]
    assert all(item.metadata["document_row_type"] == "diario_legal_act" for item in enriched)
    assert all("#act-" in item.url for item in enriched)
    assert all(item.metadata["parent_edition_url"].endswith("?edicion=53.491") for item in enriched)


def test_parse_gaceta_pdf_text_extracts_project_identity(sample_source) -> None:
    item = RawItem(
        id="gaceta-476",
        source_id="gacetas_congreso",
        source_name="Gacetas del Congreso",
        source_type="legal",
        url="https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=476",
        title="Gaceta del Congreso 476 — Senado de la República",
        fetched_at="2026-05-15T00:00:00Z",
        published_at="2026-05-14T00:00:00Z",
        raw_text="476 | Senado de la República | 14/05/2026",
        metadata={"extraction": "imprenta_nacional_jsf_table"},
    )
    parsed = _parse_gaceta_pdf_text(
        item,
        (
            "Gaceta del Congreso 476. AL PROYECTO DE LEY NÚMERO 550 DE "
            "2026 CÁMARA Y SENADO por la cual se adiciona el Presupuesto "
            "General de la Nación de la vigencia fiscal de 2026. Página 1"
        ),
    )

    assert parsed is not None
    assert parsed["project_label"] == (
        "Proyecto de Ley 550 DE 2026 Cámara y Senado"
    )
    assert parsed["project_records"] == [
        {"number": "550", "year": "2026", "chamber": "Cámara/Senado"}
    ]
    assert parsed["document_title"].startswith("por la cual se adiciona")


def test_parse_gaceta_pdf_text_recovers_project_record_from_body_reference(
    sample_source,
) -> None:
    item = RawItem(
        id="gaceta-485",
        source_id="gacetas_congreso",
        source_name="Gacetas del Congreso",
        source_type="legal",
        url="https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=485",
        title="Gaceta del Congreso 485 — Senado de la República",
        fetched_at="2026-05-18T00:00:00Z",
        published_at="2026-05-15T00:00:00Z",
        raw_text="485 | Senado de la República | 15/05/2026",
        metadata={"extraction": "imprenta_nacional_jsf_table"},
    )

    parsed = _parse_gaceta_pdf_text(
        item,
        (
            "PROYECTO DELEY NÚMERO DE SENADODE CÁMARA por la cual se "
            "establece un subsidio de transporte del Gas Licuado de Petróleo "
            "(GLP) distribuido hacia el departamento Archipiélago de San "
            "Andrés, Providencia y Santa Catalina. Asunto: Informe de "
            "ponencia para primer debate del Proyecto de Ley No. de 2026 "
            "Senado, No. 560 de 2025 Cámara por la cual se establece un "
            "subsidio de transporte."
        ),
    )

    assert parsed is not None
    assert parsed["project_records"] == [
        {"number": "560", "year": "2025", "chamber": "Cámara"}
    ]
    assert parsed["project_label"] == "Proyecto de Ley 560 DE 2025 Cámara"
    assert parsed["identity_quality"] == "project_and_title"


def test_parse_gaceta_pdf_text_allows_title_only_research_lead(sample_source) -> None:
    item = RawItem(
        id="gaceta-484",
        source_id="gacetas_congreso",
        source_name="Gacetas del Congreso",
        source_type="legal",
        url="https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=484",
        title="Gaceta del Congreso 484 — Senado de la República",
        fetched_at="2026-05-18T00:00:00Z",
        published_at="2026-05-15T00:00:00Z",
        raw_text="484 | Senado de la República | 15/05/2026",
        metadata={"extraction": "imprenta_nacional_jsf_table"},
    )

    parsed = _parse_gaceta_pdf_text(
        item,
        (
            "PROYECTO DE LEY NÚMERO DE SENADO DE CÁMARA por el cual se "
            "expide el Estatuto Especial de Profesionalización para docentes "
            "y directivos docentes estatales."
        ),
    )

    assert parsed is not None
    assert parsed["project_records"] == []
    assert parsed["document_title"].startswith("por el cual se expide")
    assert parsed["identity_quality"] == "document_title_only"


def test_parse_gaceta_pdf_text_rejects_lossy_project_identity() -> None:
    item = RawItem(
        id="gaceta-476",
        source_id="gacetas_congreso",
        source_name="Gacetas del Congreso",
        source_type="legal",
        url="https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml?gaceta=476",
        title="Gaceta del Congreso 476 — Senado de la República",
        fetched_at="2026-05-15T00:00:00Z",
        published_at="2026-05-14T00:00:00Z",
        raw_text="476 | Senado de la República | 14/05/2026",
        metadata={"extraction": "imprenta_nacional_jsf_table"},
    )

    parsed = _parse_gaceta_pdf_text(
        item,
        (
            "AL PROYECTO DE LEY NÚMERO DE 2026 CÁMARA Y SENADO "
            "por la cual se adiciona el Presupuesto General de la Nación "
            "de la vigencia fiscal de"
        ),
    )

    assert parsed is None


def test_enrich_gaceta_pdfs_marks_pdf_as_parsed_followup(sample_source) -> None:
    source = replace(sample_source, id="gacetas_congreso", type="legal")
    html = """
    <form id="formResumen" action="/gacetas/index.xhtml" method="post">
      <input type="hidden" name="formResumen" value="formResumen" />
      <input type="hidden" name="javax.faces.ViewState" value="view-state-1" />
      <table>
        <tr>
          <td>476</td>
          <td>Senado de la República</td>
          <td>14/05/2026</td>
          <td></td>
          <td><button name="formResumen:dataTableResumen:0:btnDescargarPdf">
            ui-button
          </button></td>
        </tr>
      </table>
    </form>
    """
    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
        source,
        "2026-05-15T00:00:00Z",
        edition_label="Gaceta del Congreso",
        query_param="gaceta",
    )
    client = _FakeGacetaPdfClient()

    enriched = _enrich_gaceta_pdfs(
        items,
        client,
        html,
        "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
        max_items=1,
    )

    assert enriched[0].metadata["content_extraction"] == "gaceta_pdf_text"
    assert enriched[0].metadata["document_row_type"] == "gaceta_bill_item"
    assert enriched[0].metadata["project_label"] == (
        "Proyecto de Ley 550 DE 2026 Cámara y Senado"
    )
    assert enriched[0].metadata["matched_project_labels"] == [
        "Proyecto de Ley 550 DE 2026 Cámara y Senado"
    ]
    assert "Presupuesto General de la Nación" in enriched[0].raw_text
    assert "#project-proyecto-de-ley-550-de-2026-camara-y-senado" in enriched[0].url
    assert enriched[0].metadata["parent_edition_url"].endswith("?gaceta=476")
    assert client.posts[0][1]["javax.faces.ViewState"] == "view-state-1"
    assert (
        "formResumen:dataTableResumen:0:btnDescargarPdf" in client.posts[0][1]
    )
