from __future__ import annotations

import io
import time
import zipfile
from dataclasses import replace
from datetime import datetime, timezone

import httpx

from colombia_forecasting_desk.fetchers import (
    SOCRATA_ADAPTERS,
    SocrataAdapter,
    _enrich_dane_icoced_xlsx,
    _enrich_diario_oficial_pdfs,
    _enrich_gaceta_pdfs,
    _enrich_mincit_zonas_francas,
    _enrich_pdf_text,
    _enrich_senado_agenda_pdfs,
    _annotate_legal_identity_items,
    _extract_anchors,
    _extract_corte_comunicados,
    _extract_dane_comunicados,
    _extract_dian_regulatory_project_links,
    _extract_imprenta_jsf_table,
    _extract_mincit_zonas_francas_approved_rows_from_text,
    _extract_pdf_text,
    _extract_senado_agenda_entries_from_text,
    _cap_items,
    _parse_diario_oficial_pdf_text,
    _parse_gaceta_pdf_text,
    _parse_rss_entries,
    _parse_dane_icoced_xlsx,
    _parse_date_text_to_iso,
    _parse_socrata_date,
    _recover_rss_entries,
    _socrata_params,
    _socrata_row_to_item,
    _struct_time_to_iso,
    fetch_api,
    fetch_html,
)
from colombia_forecasting_desk.models import RawItem


def _xlsx_cell(ref: str, value: str | float | int) -> str:
    if isinstance(value, str):
        return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'
    return f'<c r="{ref}"><v>{value}</v></c>'


def _xlsx_row(row_num: int, values: dict[str, str | float | int]) -> str:
    cells = "".join(
        _xlsx_cell(f"{col}{row_num}", value) for col, value in values.items()
    )
    return f'<row r="{row_num}">{cells}</row>'


def _minimal_icoced_xlsx() -> bytes:
    workbook = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Anexo 1" sheetId="1" r:id="rId1"/>
    <sheet name="Anexo 2.1" sheetId="2" r:id="rId2"/>
    <sheet name="Anexo 2.2" sheetId="3" r:id="rId3"/>
  </sheets>
</workbook>"""
    rels = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/>
</Relationships>"""

    def sheet(row_values: dict[str, str | float | int]) -> str:
        rows = [
            _xlsx_row(1, {"A": 2026, "B": "Enero"}),
            _xlsx_row(2, row_values),
        ]
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"<sheetData>{''.join(rows)}</sheetData></worksheet>"
        )

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", rels)
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            sheet({"B": "Marzo", "C": 135.44, "D": 0.75, "E": 6.47, "F": 6.33}),
        )
        zf.writestr(
            "xl/worksheets/sheet2.xml",
            sheet({"B": "Marzo", "C": 134.81, "D": 0.77, "E": 6.43, "F": 6.26}),
        )
        zf.writestr(
            "xl/worksheets/sheet3.xml",
            sheet({"B": "Marzo", "C": 136.63, "D": 0.72, "E": 6.53, "F": 6.47}),
        )
    return out.getvalue()


def test_struct_time_to_iso_handles_none() -> None:
    assert _struct_time_to_iso(None) is None


def test_struct_time_to_iso_formats() -> None:
    st = time.strptime("2026-04-27T11:00:00", "%Y-%m-%dT%H:%M:%S")
    assert _struct_time_to_iso(st) == "2026-04-27T11:00:00Z"


def test_parse_date_text_to_iso_handles_spanish_dates() -> None:
    assert _parse_date_text_to_iso("Abril 09 de 2026 04:35 PM") == (
        "2026-04-09T00:00:00Z"
    )
    assert _parse_date_text_to_iso("27/04/2026") == "2026-04-27T00:00:00Z"
    assert _parse_date_text_to_iso("Agenda Legislativa del 20 al 24 de abril de 2026") == (
        "2026-04-20T00:00:00Z"
    )


def test_extract_anchors_filters_nav_and_short() -> None:
    html = """
    <html><body><main>
      <a href="/news/article-one">Junta del Banco mantiene tasa de interés en 9.5%</a>
      <a href="https://other.com/x">Junta del Banco mantiene tasa de interés en 9.5%</a>
      <a href="/menu">Menú</a>
      <a href="#">Inicio</a>
      <a href="/news/article-two">DANE publica cifras de inflación de marzo</a>
      <a href="/news/article-one?utm_source=foo">Junta del Banco mantiene tasa de interés en 9.5%</a>
    </main></body></html>
    """
    anchors = _extract_anchors(html, "https://example.com/")
    urls = [u for _, u in anchors]
    # canonical URL dedupe: only one article-one anchor (utm variant collapsed)
    assert sum(1 for u in urls if "article-one" in u) == 1
    assert any("article-two" in u for u in urls)
    # cross-domain dropped
    assert not any("other.com" in u for u in urls)
    # nav/short dropped
    assert not any(text.lower() in {"menú", "inicio"} for text, _ in anchors)


def test_fetch_senado_leyes_registry_parses_search_and_detail(sample_source) -> None:
    source = replace(
        sample_source,
        id="senado_leyes_registry",
        name="Senado — Sección de Leyes / Proyectos de Ley",
        type="legal",
        url="https://leyes.senado.gov.co/",
        fetch_method="html",
        trust_role="agenda_signal",
        max_items=1,
    )
    detail_html = """
    <table><tbody>
      <tr><td>Número Senado</td><td>001/25</td><td>Número Cámara</td><td></td></tr>
      <tr><td>Cuatrenio</td><td>2022-2026</td><td>Legislatura</td><td>2025-2026</td></tr>
      <tr><td>Comisión</td><td>SEPTIMA</td><td>Fecha de Presentación</td><td>20/07/2025</td></tr>
      <tr><td>Estado</td><td>PENDIENTE DISCUTIR PONENCIA PARA PRIMER DEBATE EN SENADO</td></tr>
    </tbody></table>
    <table><tr>
      <td class="celda-etiqueta">Primera Ponencia</td>
      <td class="celda-dato"><a href="https://svrpubindc.imprenta.gov.co/senado/">Gaceta 1502/2025</a></td>
    </tr></table>
    <button id="textoRadicadoBtn" data-link="p-ley/2025-2026/PL 001-25.pdf"></button>
    """
    payload = {
        "success": True,
        "data": [
            {
                "id": 9540,
                "numero_senado": "001/25",
                "numero_camara": "",
                "cuatrenio": "2022-2026",
                "titulo": "POR MEDIO DE LA CUAL SE ESTABLECEN LINEAMIENTOS EN SALUD",
                "autor": "H.S. LORENA RIOS CUELLAR.",
                "comision": "SEPTIMA",
                "estado": "PENDIENTE DISCUTIR PONENCIA PARA PRIMER DEBATE EN SENADO",
                "type": "pdly",
            }
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/":
            return httpx.Response(200, text="<html>ok</html>")
        if request.method == "POST" and request.url.path == "/api/search_pdly.php":
            return httpx.Response(200, json=payload)
        if request.method == "GET" and request.url.path == "/api/get_detalle_pdly.php":
            return httpx.Response(200, text=detail_html)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    item = items[0]
    assert item.published_at == "2025-07-20T00:00:00Z"
    assert item.metadata["content_extraction"] == "senado_leyes_registry"
    assert item.metadata["has_clean_project_identity"] is True
    assert item.metadata["project_label"] == "Proyecto de Ley 1 de 2025 Senado"
    assert item.metadata["project_records"] == [
        {"number": "1", "year": "2025", "chamber": "Senado"}
    ]
    assert item.metadata["publication_links"][0]["title"] == "Gaceta 1502/2025"
    assert item.metadata["text_radicado_url"].endswith("PL 001-25.pdf")
    assert "PENDIENTE DISCUTIR PONENCIA" in item.raw_text


def test_fetch_camara_proyectos_registry_parses_ajax_and_detail(sample_source) -> None:
    source = replace(
        sample_source,
        id="camara_proyectos_ley_registry",
        name="Cámara de Representantes — Proyectos de Ley",
        type="legal",
        url="https://www.camara.gov.co/proyectos-de-ley/",
        fetch_method="html",
        trust_role="agenda_signal",
        max_items=1,
    )
    home_html = """
    <script>window.PL_CFG = { AJAX_URL : "https://www.camara.gov.co/wp-admin/admin-ajax.php", PL_NONCE : "abc123" };</script>
    <select id="legislaturaField">
      <option value="13">2025-2026</option>
    </select>
    """
    payload = {
        "success": True,
        "data": {
            "items": [
                {
                    "nro_camara": "554/2026C",
                    "nro_senado": None,
                    "titulo": "POR LA CUAL SE MODIFICAN REGLAS DE PUBLICIDAD OFICIAL",
                    "proyecto": "GESTORAS SOCIALES",
                    "tipo": "Ley Ordinaria",
                    "estado": "Trámite en Comisión",
                    "origen": "Cámara",
                    "vigencia": "2025-2026",
                    "link_web": "gestoras-sociales",
                    "comisiones_pack": "1||Comisión Primera||https://example.com/comision",
                    "autores_pack": "95||Andrés Forero||representantes/andres-forero",
                    "otros_autores": "Y otros.",
                }
            ],
            "total": 1,
            "total_pages": 1,
        },
    }
    detail_html = """
    <script type="application/ld+json">{"datePublished":"2026-05-14T11:27:48-05:00"}</script>
    <div class="pl-nums-group">
      <div class="pl-nums-title">Fecha de Radicación</div>
      <div class="pl-kpi-card"><div class="pl-kpi-label">Cámara</div><div class="pl-kpi-value">12/5/2026</div></div>
    </div>
    <div class="pl-card"><div class="pl-title">Objeto del proyecto</div>
      <div class="pl-body">Prohibir el uso de recursos públicos en publicidad oficial.</div>
    </div>
    <div class="pl-card"><div class="pl-title">Publicación</div>
      <div class="pl-body"><a href="https://www.camara.gov.co/wp-content/uploads/proyecto.pdf">Ver Documento</a></div>
    </div>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/proyectos-de-ley/":
            return httpx.Response(200, text=home_html)
        if request.method == "POST" and request.url.path == "/wp-admin/admin-ajax.php":
            assert b"get_proyectos_ley_page" in request.content
            assert b"legislatura=13" in request.content
            return httpx.Response(200, json=payload)
        if request.method == "GET" and request.url.path == "/gestoras-sociales":
            return httpx.Response(200, text=detail_html)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    item = items[0]
    assert item.published_at == "2026-05-12T00:00:00Z"
    assert item.url == "https://www.camara.gov.co/gestoras-sociales"
    assert item.metadata["content_extraction"] == "camara_proyectos_ley_registry"
    assert item.metadata["project_label"] == "Proyecto de Ley 554 de 2026 Cámara"
    assert item.metadata["project_records"] == [
        {"number": "554", "year": "2026", "chamber": "Cámara"}
    ]
    assert item.metadata["publication_links"][0]["title"] == "Ver Documento"
    assert "Prohibir el uso de recursos públicos" in item.raw_text


def test_extract_anchors_caps_at_30() -> None:
    body = "".join(
        f'<a href="/n/{i}">Title number {i:03d} long enough to keep</a>'
        for i in range(60)
    )
    html = f"<html><body><main>{body}</main></body></html>"
    anchors = _extract_anchors(html, "https://example.com/")
    assert len(anchors) == 30


def test_extract_dane_comunicados_reads_dated_table(sample_source) -> None:
    source = replace(sample_source, id="dane_comunicados_prensa")
    html = """
    <table>
      <tr>
        <th>Documento</th><th>Fecha de publicación</th><th>Formato</th>
      </tr>
      <tr>
        <td>Boletín técnico mercado laboral nacional</td>
        <td>27/04/2026</td>
        <td><a href="/files/boletin.pdf">PDF</a></td>
      </tr>
    </table>
    """
    items = _extract_dane_comunicados(
        html,
        "https://www.dane.gov.co/index.php/sala-de-prensa",
        source,
        "2026-04-27T12:00:00Z",
    )
    assert len(items) == 1
    assert items[0].title == "Boletín técnico mercado laboral nacional"
    assert items[0].published_at == "2026-04-27T00:00:00Z"
    assert items[0].metadata["extraction"] == "dane_comunicados_table"


def test_parse_dane_icoced_xlsx_extracts_headline_metrics() -> None:
    parsed = _parse_dane_icoced_xlsx(
        _minimal_icoced_xlsx(),
        year=2026,
        month=3,
    )

    assert parsed is not None
    assert parsed["metrics"]["total"] == {
        "index": 135.44,
        "monthly_variation_pct": 0.75,
        "year_to_date_variation_pct": 6.47,
        "annual_variation_pct": 6.33,
    }
    assert parsed["metrics"]["residential"]["monthly_variation_pct"] == 0.77
    assert parsed["metrics"]["non_residential"]["monthly_variation_pct"] == 0.72
    assert "variación mensual de 0,75%" in parsed["headline"]
    assert "residenciales 0,77%" in parsed["headline"]


class _FakeBinaryResponse:
    status_code = 200

    def __init__(self, content: bytes, headers=None, url: str = "https://example.com"):
        self.content = content
        self.headers = headers or {}
        self.url = url

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="ignore")

    def raise_for_status(self) -> None:
        return None


class _FakeBinaryClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(_minimal_icoced_xlsx())


def _minimal_text_pdf(text: str) -> bytes:
    return (
        b"%PDF-1.4\n1 0 obj\n<<>>\nstream\nBT\n("
        + text.encode("latin-1")
        + b") Tj\nET\nendstream\nendobj\n%%EOF"
    )


def _minimal_operator_pdf(operator_stream: bytes) -> bytes:
    return (
        b"%PDF-1.4\n1 0 obj\n<<>>\nstream\n"
        + operator_stream
        + b"\nendstream\nendobj\n%%EOF"
    )


def test_enrich_dane_icoced_xlsx_marks_item_as_parsed_content() -> None:
    item = RawItem(
        id="icoced-1",
        source_id="dane_icoced",
        source_name="DANE ICOCED",
        source_type="economic_indicator",
        url="https://example.com/anex-ICOCED-mar2026.xlsx",
        title="DANE ICOCED — Anexo marzo 2026",
        fetched_at="2026-05-04T00:00:00Z",
        published_at="2026-04-30T00:00:00Z",
        raw_text="Link-level text",
        metadata={"period_year": 2026, "period_month": 3},
    )

    enriched = _enrich_dane_icoced_xlsx([item], _FakeBinaryClient())[0]

    assert enriched.metadata["content_extraction"] == "dane_icoced_xlsx"
    assert (
        enriched.metadata["headline_metrics"]["total"]["monthly_variation_pct"]
        == 0.75
    )
    assert "ICOCED total registró una variación mensual de 0,75%" in enriched.raw_text


def test_extract_pdf_text_reads_literal_text_stream() -> None:
    text = (
        "El DANE inicia la tercera entrega de resultados del Censo Economico "
        "Nacional Urbano con informacion suficiente para el analista."
    )
    assert "Censo Economico Nacional" in _extract_pdf_text(_minimal_text_pdf(text))


def test_extract_pdf_text_reads_split_tj_fragments() -> None:
    pdf = _minimal_operator_pdf(
        b"BT\n"
        b"[(El )4(Proyecto de Ley No. )-3(312 del 2025 Senado )"
        b"(sera discutido en primer debate por la comision.)] TJ\n"
        b"[(La agenda contiene informacion suficiente para el analista.)] TJ\n"
        b"ET"
    )

    text = _extract_pdf_text(pdf)

    assert "Proyecto de Ley No. 312 del 2025 Senado" in text
    assert "primer debate" in text


def test_extract_pdf_text_decodes_octal_escapes() -> None:
    text = _extract_pdf_text(
        _minimal_text_pdf(
            "Gaceta del Congreso. PROYECTO DE LEY N\\332MERO 550 DE 2026 "
            "C\\301MARA Y SENADO por la cual se adiciona el Presupuesto "
            "General de la Naci\\363n."
        )
    )

    assert "NÚMERO 550 DE 2026 CÁMARA Y SENADO" in text
    assert "Nación" in text


class _FakePdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        text = (
            "El DANE publica un comunicado tecnico con resultados economicos "
            "nacionales y suficiente texto para superar el umbral minimo."
        )
        return _FakeBinaryResponse(_minimal_text_pdf(text))


def test_enrich_pdf_text_marks_pdf_item_as_parsed_content() -> None:
    item = RawItem(
        id="dane-pdf-1",
        source_id="dane_comunicados_prensa",
        source_name="DANE",
        source_type="official_updates",
        url="https://www.dane.gov.co/files/prensa/comunicados/cp-demo.pdf",
        title="DANE publica comunicado tecnico",
        fetched_at="2026-05-06T00:00:00Z",
        published_at="2026-05-05T00:00:00Z",
        raw_text="DANE publica comunicado tecnico 05/05/2026 PDF Descargar",
        metadata={"extraction": "dane_comunicados_table"},
    )

    enriched = _enrich_pdf_text([item], _FakePdfClient(), max_items=1)[0]

    assert enriched.metadata["content_extraction"] == "pdf_text_best_effort"
    assert "PDF text excerpt" in enriched.raw_text
    assert "resultados economicos nacionales" in enriched.raw_text


def test_extract_senado_agenda_entries_from_pdf_text() -> None:
    item = RawItem(
        id="senado-agenda-1",
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        source_type="calendar",
        url=(
            "https://www.senado.gov.co/index.php/documentos/senado-prensa/"
            "agenda-legislativa-actual/9373-agenda-legislativa-del-11-al-15-"
            "de-mayo-de-2026/file"
        ),
        title="Agenda Legislativa del 11 al 15 de mayo de 2026 ( pdf, 944 KB )",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-05-11T00:00:00Z",
        raw_text="Agenda Legislativa del 11 al 15 de mayo de 2026",
        metadata={"extraction": "anchor"},
    )
    text = (
        "MARTES 12 de mayo TEMA: la presentacion en primer debate del "
        "Proyecto de Ley No. 312 del 2025 Senado 463 del 2025 Camara, "
        "\"POR MEDIO DE LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO\". "
        "Autores: Ministro de Hacienda. "
        "MIERCOLES 13 de mayo TEMA: ponencia del Proyecto de Ley No. "
        "550 de 2026 Camara, 369 de 2026 Senado."
    )

    entries = _extract_senado_agenda_entries_from_text(item, text)

    assert len(entries) == 2
    assert entries[0].published_at == "2026-05-12T00:00:00Z"
    assert "Proyecto de Ley 312 de 2025 Senado" in entries[0].title
    assert entries[0].metadata["content_extraction"] == "senado_agenda_pdf"
    assert entries[0].metadata["scheduled_date"] == "2026-05-12T00:00:00Z"
    assert entries[0].metadata["agenda_action_type"] == "primer debate"
    assert entries[0].metadata["project_records"] == [
        {
            "kind": "Ley",
            "number": "312",
            "year": "2025",
            "chamber": "Senado",
        },
        {
            "kind": "Ley",
            "number": "463",
            "year": "2025",
            "chamber": "Cámara",
        },
    ]
    assert entries[0].metadata["document_title"] == (
        "POR MEDIO DE LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO"
    )
    assert entries[0].metadata["project_identity_status"] == "clean_project_identity"
    assert entries[0].metadata["has_clean_project_identity"] is True
    assert entries[0].metadata["follow_up_sources"][0]["source_id"] == (
        "gacetas_congreso"
    )
    assert entries[0].url.endswith("#project-1")
    assert "official Senado agenda PDF" in entries[0].raw_text
    assert "Follow-up sources: Gacetas del Congreso" in entries[0].raw_text


def test_extract_senado_agenda_keeps_loose_title_as_research_lead() -> None:
    item = RawItem(
        id="senado-agenda-1",
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        source_type="calendar",
        url="https://www.senado.gov.co/index.php/documentos/agenda/file",
        title="Agenda Legislativa del 11 al 15 de mayo de 2026 ( pdf, 944 KB )",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-05-11T00:00:00Z",
        raw_text="Agenda Legislativa del 11 al 15 de mayo de 2026",
        metadata={"extraction": "anchor"},
    )
    text = (
        "LUNES 11 de mayo TEMA: ponencia: Proyecto de Ley Senado "
        "elacual se modificael articulo de laleydeyse Dictan "
        "otrasdisposiciones. Autores: Senadores."
    )

    entries = _extract_senado_agenda_entries_from_text(item, text)

    assert len(entries) == 1
    assert "el cual se modifica el articulo" in entries[0].title
    assert entries[0].metadata["project_label"] == "Proyecto de Ley Senado"
    assert entries[0].metadata["project_records"] == []
    assert entries[0].metadata["project_identity_status"] == "missing_project_number"
    assert entries[0].metadata["has_clean_project_identity"] is False
    assert entries[0].metadata["follow_up_sources"][0]["source_id"] == (
        "gacetas_congreso"
    )


class _FakeSenadoPdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        text = (
            "MARTES 12 de mayo TEMA: la presentacion en primer debate del "
            "Proyecto de Ley No. 312 del 2025 Senado 463 del 2025 Camara, "
            "POR MEDIO DE LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO. "
            "Autores: Ministro de Hacienda. La agenda contiene informacion "
            "suficiente para el analista."
        )
        return _FakeBinaryResponse(_minimal_text_pdf(text))


def test_enrich_senado_agenda_pdfs_replaces_pdf_link_with_entries() -> None:
    item = RawItem(
        id="senado-agenda-1",
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        source_type="calendar",
        url="https://www.senado.gov.co/index.php/documentos/agenda/file",
        title="Agenda Legislativa del 11 al 15 de mayo de 2026 ( pdf, 944 KB )",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-05-11T00:00:00Z",
        raw_text="Agenda Legislativa del 11 al 15 de mayo de 2026",
        metadata={"extraction": "anchor"},
    )

    enriched = _enrich_senado_agenda_pdfs(
        [item], _FakeSenadoPdfClient(), max_items=1
    )

    assert len(enriched) == 1
    assert enriched[0].metadata["extraction"] == "senado_agenda_pdf_entry"
    assert enriched[0].metadata["content_extraction"] == "senado_agenda_pdf"
    assert enriched[0].url.endswith("#project-1")
    assert "Agenda Legislativa" not in enriched[0].title


def test_enrich_pdf_text_handles_pdf_aspx_attachment_urls() -> None:
    item = RawItem(
        id="mincit-pdf-1",
        source_id="mincit_zonas_francas",
        source_name="MinCIT",
        source_type="regulatory",
        url="https://zf.mincit.gov.co/getattachment/estadisticas/zonas.pdf.aspx",
        title="Zonas Francas aprobadas",
        fetched_at="2026-05-06T00:00:00Z",
        published_at="2026-02-18T00:00:00Z",
        raw_text="Zonas Francas aprobadas Fecha de actualización: 18 de febrero de 2026",
        metadata={"extraction": "anchor"},
    )

    enriched = _enrich_pdf_text([item], _FakePdfClient(), max_items=1)[0]

    assert enriched.metadata["content_extraction"] == "pdf_text_best_effort"
    assert "PDF text excerpt" in enriched.raw_text


MINCIT_ZF_SAMPLE_TEXT = (
    "ZONAS FRANCAS FECHA: 31 DE DICIEMBRE DE 2025 "
    "Fuente: Ministerio de Industria, Comercio y Turismo - DPC 29/01/2026 "
    "NIT NOMBRE ZONA FRANCA CLASE DE ZONA FRANCA TIPO DE USUARIO "
    "DEPARTAMENTO MUNICIPIO Resolución de declaratoria Resolución de prorroga CIIU "
    "800178052 Zona Franca Industrial de Bienes y Servicios La Candelaria "
    "Permanente Usuario Operador Bolívar Cartagena "
    "Res. 95 de 10 de febrero de 1993 Res. 1311 de 1 de diciembre de 2021 7020 "
    "90191119 3 Zona Franca Permanente Especial De Servicios Rionegro MRO "
    "Permanente especial Servicios Antioquia Rionegro "
    "Res. No. 2118 del 26 de diciembre de 2025 Vacía 3315"
)


def _mincit_zf_item() -> RawItem:
    return RawItem(
        id="mincit-zf-approved",
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas (Estadísticas)",
        source_type="regulatory",
        url="https://zf.mincit.gov.co/getattachment/estadisticas/zonas.pdf.aspx",
        title="Zonas Francas aprobadas",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-02-18T00:00:00Z",
        raw_text="Zonas Francas aprobadas Fecha de actualización: 18 de febrero de 2026",
        metadata={"extraction": "anchor"},
    )


def test_extract_mincit_zonas_francas_approved_rows_from_pdf_text() -> None:
    rows = _extract_mincit_zonas_francas_approved_rows_from_text(
        _mincit_zf_item(),
        MINCIT_ZF_SAMPLE_TEXT,
    )

    assert len(rows) == 2
    first = rows[0].metadata
    assert first["registry"] == "mincit_zonas_francas_aprobadas"
    assert first["content_extraction"] == "mincit_zonas_francas_approved_pdf"
    assert first["nit"] == "800178052"
    assert first["zona_franca_name"] == (
        "Zona Franca Industrial de Bienes y Servicios La Candelaria"
    )
    assert first["zone_class"] == "Permanente"
    assert first["user_type"] == "Usuario Operador"
    assert first["department"] == "Bolívar"
    assert first["municipality"] == "Cartagena"
    assert first["declaratory_resolution"] == "Res. 95 de 10 de febrero de 1993"
    assert first["extension_resolution"] == (
        "Res. 1311 de 1 de diciembre de 2021"
    )
    assert first["ciiu"] == "7020"
    assert first["snapshot_date"] == "2025-12-31T00:00:00Z"
    assert first["source_report_date"] == "2026-01-29T00:00:00Z"
    assert first["follow_up_sources"][1]["source_id"] == "diario_oficial"

    second = rows[1].metadata
    assert second["nit"] == "901911193"
    assert second["zone_class"] == "Permanente Especial"
    assert second["extension_resolution"] == "Vacía"


def test_extract_mincit_zonas_francas_handles_repeated_location_terms() -> None:
    text = (
        "ZONAS FRANCAS FECHA: 31 DE DICIEMBRE DE 2025 "
        "NIT NOMBRE ZONA FRANCA CLASE DE ZONA FRANCA TIPO DE USUARIO "
        "DEPARTAMENTO MUNICIPIO Resolución de declaratoria Resolución de prorroga CIIU "
        "800185347 Zona Franca de Bogotá Permanente Usuario Operador Bogotá Bogotá "
        "Res. 934 de 06 de agosto de 1993 Res. 888 de 26 de agosto de 2020 7020 "
        "900162578 Zona Franca de Las Américas S.A.S. Permanente Permanente "
        "Magdalena Santa Marta Res. 5657 de 27 de Junio de 2008 "
        "Res. 232 de 9 de febrero de 2022 6820 "
        "90191119 3 Zona Franca Permanente Especial De Servicios Rionegro MRO "
        "Permanente especial Servicios Antioquia Rionegro "
        "Res. No. 2118 del 26 de diciembre de 2025 Vacía 3315 "
        "RESUMEN ZONAS FRANCAS."
    )

    rows = _extract_mincit_zonas_francas_approved_rows_from_text(
        _mincit_zf_item(),
        text,
    )

    assert len(rows) == 3
    assert rows[0].metadata["department"] == "Bogotá"
    assert rows[0].metadata["municipality"] == "Bogotá"
    assert rows[1].metadata["user_type"] == "Permanente"
    assert rows[1].metadata["department"] == "Magdalena"
    assert rows[2].metadata["registry_key"] == "901911193"


class _FakeMinCITPdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(
            _minimal_operator_pdf(
                b"BT\n("
                + MINCIT_ZF_SAMPLE_TEXT.encode("latin-1")
                + b") Tj\nET"
            )
        )


def test_enrich_mincit_zonas_francas_expands_approved_pdf_to_registry_rows() -> None:
    enriched = _enrich_mincit_zonas_francas(
        [_mincit_zf_item()],
        _FakeMinCITPdfClient(),
    )

    assert [item.metadata["registry_key"] for item in enriched] == [
        "800178052",
        "901911193",
    ]
    assert all(
        item.metadata["content_extraction"] == "mincit_zonas_francas_approved_pdf"
        for item in enriched
    )
    assert "MinCIT Zonas Francas aprobadas" in enriched[0].title


def test_extract_corte_comunicados_reads_dated_links(sample_source) -> None:
    source = replace(sample_source, id="corte_constitucional_comunicados", type="legal")
    html = """
    <main>
      <div>
        <span>23 de abril de 2026</span>
        <a href="/comunicados/comunicado-18-abril-23-de-2026.pdf">
          Comunicado 18. Corte decide sobre reforma pensional
        </a>
      </div>
    </main>
    """
    items = _extract_corte_comunicados(
        html,
        "https://www.corteconstitucional.gov.co/comunicados/",
        source,
        "2026-04-27T12:00:00Z",
    )
    assert len(items) == 1
    assert items[0].published_at == "2026-04-23T00:00:00Z"
    assert items[0].metadata["extraction"] == "corte_comunicados_dated_anchor"


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


def test_extract_dian_regulatory_project_links_filters_navigation(sample_source) -> None:
    source = replace(sample_source, id="dian_proyectos_normas", type="regulatory")
    html = """
    <nav>
      <a href="/Paginas/Inicio.aspx">Portal DIAN</a>
      <a href="/normatividad/Paginas/Agenda-reglamentaria.aspx">
        Agenda Reglamentaria DIAN
      </a>
      <a href="/normatividad/Paginas/ProyectosNormas.aspx">
        Proyectos de Normas
      </a>
      <a href="/atencionciudadano/Paginas/Inicio.aspx">Atención</a>
    </nav>
    """

    items = _extract_dian_regulatory_project_links(
        html,
        "https://www.dian.gov.co/normatividad/Paginas/Inicio.aspx",
        source,
        "2026-05-15T12:00:00Z",
    )

    assert [item.title for item in items] == [
        "DIAN regulatory project index — Agenda Reglamentaria DIAN",
        "DIAN regulatory project index — Proyectos de Normas",
    ]
    assert items[0].published_at is None
    assert items[0].metadata["parser_status"] == "dynamic_or_undated_index"


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


class _FakeGacetaPdfClient:
    def __init__(self):
        self.posts = []

    def post(self, url, data=None):  # noqa: ANN001 - mirrors httpx.Client.post
        self.posts.append((url, data))
        text = (
            "Gaceta del Congreso 476. AL PROYECTO DE LEY NÚMERO 550 DE "
            "2026 CÁMARA Y SENADO por la cual se adiciona el Presupuesto "
            "General de la Nación de la vigencia fiscal de 2026. Página 1"
        )
        return _FakeBinaryResponse(
            _minimal_text_pdf(text),
            headers={"content-type": "application/pdf"},
        )


class _FakeDiarioPdfClient:
    def __init__(self):
        self.posts = []
        self.gets = []

    def post(self, url, data=None):  # noqa: ANN001 - mirrors httpx.Client.post
        self.posts.append((url, data))
        html = """
        <html><body>
          <object type="application/pdf"
            data="/diario/javax.faces.resource/dynamiccontent.properties.xhtml?ln=primefaces&amp;pfdrid=abc">
          </object>
        </body></html>
        """
        return _FakeBinaryResponse(
            html.encode("utf-8"),
            headers={"content-type": "text/html;charset=UTF-8"},
            url="https://svrpubindc.imprenta.gov.co/diario/view/detallesPdf.xhtml",
        )

    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        self.gets.append((url, params))
        text = (
            "Diario Oficial 53.490. Ministerio de Comercio, Industria y "
            "Turismo. Resolución 2118 de 2025 por la cual se declara la "
            "Zona Franca Permanente Especial De Servicios Rionegro MRO. "
            "Decreto 123 de 2026. Página 1"
        )
        return _FakeBinaryResponse(
            _minimal_text_pdf(text),
            headers={"content-type": "application/pdf"},
            url=url,
        )


def test_parse_diario_oficial_pdf_text_extracts_legal_act_identities() -> None:
    parsed = _parse_diario_oficial_pdf_text(
        "Diario Oficial. Ministerio de Comercio, Industria y Turismo. "
        "Resolución No. 2118 del 26 de diciembre de 2025 por la cual se "
        "declara la Zona Franca Permanente Especial De Servicios Rionegro MRO."
    )

    assert parsed is not None
    assert parsed["legal_act_records"][0]["label"] == "Resolución 2118 de 2025"


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

    enriched = _enrich_diario_oficial_pdfs(
        items,
        client,
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        max_items=1,
    )

    assert enriched[0].metadata["content_extraction"] == "diario_oficial_pdf_text"
    assert enriched[0].metadata["legal_act_records"][0]["label"] == (
        "Resolución 2118 de 2025"
    )
    assert "Rionegro MRO" in enriched[0].raw_text
    assert client.posts[0][1]["javax.faces.ViewState"] == "view-state-2"
    assert client.posts[0][1]["frmConDiario"] == "frmConDiario"
    assert "dynamiccontent.properties.xhtml" in client.gets[0][0]
    assert enriched[0].metadata["pdf_embedded_url"].endswith("pfdrid=abc")


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
    assert enriched[0].metadata["project_label"] == (
        "Proyecto de Ley 550 DE 2026 Cámara y Senado"
    )
    assert enriched[0].metadata["matched_project_labels"] == [
        "Proyecto de Ley 550 DE 2026 Cámara y Senado"
    ]
    assert "Presupuesto General de la Nación" in enriched[0].raw_text
    assert client.posts[0][1]["javax.faces.ViewState"] == "view-state-1"
    assert (
        "formResumen:dataTableResumen:0:btnDescargarPdf" in client.posts[0][1]
    )


class _FakeFeed:
    def __init__(self, entries):
        self.entries = entries
        self.feed = type("F", (), {"id": "feed-1"})()


def test_parse_rss_entries(sample_source) -> None:
    parsed = _FakeFeed(
        entries=[
            {
                "title": "Comunicado Junta Directiva",
                "link": "https://www.banrep.gov.co/comunicado-1",
                "summary": "<p>La junta directiva...</p>",
                "published_parsed": time.strptime(
                    "2026-04-27T11:00:00", "%Y-%m-%dT%H:%M:%S"
                ),
            },
            {  # missing link -> dropped
                "title": "ignored",
                "summary": "x",
            },
        ]
    )
    items = _parse_rss_entries(parsed, sample_source, fetched_at="2026-04-27T12:00:00Z")
    assert len(items) == 1
    assert items[0].title == "Comunicado Junta Directiva"
    assert items[0].published_at == "2026-04-27T11:00:00Z"
    assert items[0].source_id == sample_source.id


def test_recover_rss_entries_from_loose_xml(sample_source) -> None:
    xml = """
    <rss><channel>
      <item>
        <title>Función Pública expide nuevo decreto</title>
        <link>https://www.funcionpublica.gov.co/noticia</link>
        <pubDate>Thu, 09 Apr 2026 16:35:00 GMT</pubDate>
        <description>Texto suficiente para limpiar y clasificar el elemento.</description>
      </item>
    </channel></rss>
    """
    items = _recover_rss_entries(xml, sample_source, "2026-04-27T12:00:00Z")
    assert len(items) == 1
    assert items[0].published_at == "2026-04-09T16:35:00Z"
    assert items[0].metadata["extraction"] == "rss_recovery"


def test_parse_socrata_date_handles_iso_with_millis() -> None:
    assert _parse_socrata_date("2026-04-10T00:00:00.000") == "2026-04-10T00:00:00Z"
    assert _parse_socrata_date("2026-01-15T13:45:30.123") == "2026-01-15T13:45:30Z"


def test_parse_socrata_date_returns_none_on_garbage() -> None:
    assert _parse_socrata_date(None) is None
    assert _parse_socrata_date("") is None
    assert _parse_socrata_date("not-a-date") is None
    assert _parse_socrata_date(12345) is None
    # Calendar-invalid date.
    assert _parse_socrata_date("2026-02-31T00:00:00.000") is None


def test_socrata_params_compose_query() -> None:
    adapter = SocrataAdapter(
        date_field="fecha_de_publicacion_del",
        title_field="nombre_del_procedimiento",
        id_field="id_del_proceso",
        entity_field="entidad",
        label="SECOP II Proceso",
    )
    cutoff = datetime(2026, 4, 16, 0, 0, 0, tzinfo=timezone.utc)
    params = _socrata_params(adapter, cutoff=cutoff, limit=30)
    assert params["$where"] == (
        "fecha_de_publicacion_del >= '2026-04-16T00:00:00.000'"
    )
    assert params["$order"] == "fecha_de_publicacion_del DESC"
    assert params["$limit"] == "30"
    selected = set(params["$select"].split(","))
    assert selected == {
        "fecha_de_publicacion_del",
        "nombre_del_procedimiento",
        "id_del_proceso",
        "entidad",
    }


def test_socrata_row_to_item_synthesizes_url_and_title(sample_source) -> None:
    source = replace(
        sample_source,
        id="secop_ii_procesos",
        type="dataset",
        url="https://www.datos.gov.co/resource/p6dx-8zbt.json",
        trust_role="civic_signal",
    )
    adapter = SOCRATA_ADAPTERS["secop_ii_procesos"]
    row = {
        "fecha_de_publicacion_del": "2026-04-25T00:00:00.000",
        "nombre_del_procedimiento": "ADQUISICIÓN DE EQUIPOS DE COMPUTO",
        "id_del_proceso": "CO1.REQ.10337260",
        "entidad": "MUNICIPIO DE SUCRE",
    }
    item = _socrata_row_to_item(row, source, "2026-04-30T12:00:00Z", adapter)
    assert item is not None
    assert item.published_at == "2026-04-25T00:00:00Z"
    assert item.title.startswith("SECOP II Proceso — ADQUISICIÓN")
    assert "MUNICIPIO DE SUCRE" in item.title
    assert item.url == (
        "https://www.datos.gov.co/resource/p6dx-8zbt.json"
        "?id=CO1.REQ.10337260"
    )
    assert item.metadata["extraction"] == "socrata_api"
    assert item.metadata["id_value"] == "CO1.REQ.10337260"


def test_socrata_row_to_item_skips_rows_missing_required_fields(sample_source) -> None:
    source = replace(
        sample_source,
        id="secop_ii_procesos",
        type="dataset",
        url="https://www.datos.gov.co/resource/p6dx-8zbt.json",
    )
    adapter = SOCRATA_ADAPTERS["secop_ii_procesos"]
    # Missing date.
    assert _socrata_row_to_item(
        {
            "nombre_del_procedimiento": "X",
            "id_del_proceso": "abc",
        },
        source,
        "2026-04-30T12:00:00Z",
        adapter,
    ) is None
    # Missing title.
    assert _socrata_row_to_item(
        {
            "fecha_de_publicacion_del": "2026-04-25T00:00:00.000",
            "id_del_proceso": "abc",
        },
        source,
        "2026-04-30T12:00:00Z",
        adapter,
    ) is None
    # Missing id.
    assert _socrata_row_to_item(
        {
            "fecha_de_publicacion_del": "2026-04-25T00:00:00.000",
            "nombre_del_procedimiento": "X",
        },
        source,
        "2026-04-30T12:00:00Z",
        adapter,
    ) is None


def test_fetch_api_calls_socrata_with_expected_params(sample_source) -> None:
    source = replace(
        sample_source,
        id="secop_ii_procesos",
        type="dataset",
        url="https://www.datos.gov.co/resource/p6dx-8zbt.json",
        fetch_method="api",
        trust_role="civic_signal",
        max_items=5,
    )
    captured: dict = {}
    payload = [
        {
            "fecha_de_publicacion_del": "2026-04-28T00:00:00.000",
            "nombre_del_procedimiento": "PRESTACION DE SERVICIOS",
            "id_del_proceso": "CO1.REQ.111",
            "entidad": "ENT A",
        },
        {
            "fecha_de_publicacion_del": "2026-04-27T00:00:00.000",
            "nombre_del_procedimiento": "OBRA PUBLICA",
            "id_del_proceso": "CO1.REQ.222",
            "entidad": "ENT B",
        },
        # Duplicate id -> deduped.
        {
            "fecha_de_publicacion_del": "2026-04-26T00:00:00.000",
            "nombre_del_procedimiento": "PRESTACION DE SERVICIOS",
            "id_del_proceso": "CO1.REQ.111",
            "entidad": "ENT A",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        items = fetch_api(source, client)

    assert len(items) == 2
    assert items[0].title.startswith("SECOP II Proceso — PRESTACION DE SERVICIOS")
    assert items[1].title.startswith("SECOP II Proceso — OBRA PUBLICA")
    assert all(it.url.startswith(source.url + "?id=") for it in items)
    assert captured["params"]["$limit"] == "5"
    assert captured["params"]["$order"] == "fecha_de_publicacion_del DESC"
    assert captured["params"]["$where"].startswith(
        "fecha_de_publicacion_del >= '"
    )


def test_socrata_adapter_registry_covers_yaml_sources() -> None:
    """Every fetch_method=api source in the YAML must have an adapter."""
    from pathlib import Path

    import yaml

    config_path = Path(__file__).resolve().parents[1] / "config" / "metasources.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    enabled_api_ids = {
        entry["id"]
        for entry in raw["metasources"]
        if entry.get("enabled") and entry.get("fetch_method") == "api"
    }
    missing = enabled_api_ids - set(SOCRATA_ADAPTERS)
    assert not missing, (
        f"enabled api sources without SOCRATA_ADAPTERS entry: {sorted(missing)}"
    )


def test_cap_items_uses_source_max_items(sample_source, make_raw) -> None:
    source = replace(sample_source, max_items=2)
    items = [
        make_raw(id="1", url="https://example.com/1"),
        make_raw(id="2", url="https://example.com/2"),
        make_raw(id="3", url="https://example.com/3"),
    ]
    capped = _cap_items(source, items)
    assert [it.id for it in capped] == ["1", "2"]
