from __future__ import annotations

import io
import time
import zipfile
from dataclasses import replace
from datetime import datetime, timezone

import httpx

import colombia_forecasting_desk.fetchers as fetchers
import colombia_forecasting_desk.source_fetching.dane as dane_fetchers
import colombia_forecasting_desk.source_fetching.imprenta as imprenta_fetchers
import colombia_forecasting_desk.source_fetching.minhacienda as minhacienda_fetchers
from colombia_forecasting_desk.fetchers import (
    SOCRATA_ADAPTERS,
    SocrataAdapter,
    _enrich_dane_icoced_xlsx,
    _enrich_banrep_minutas_html,
    _enrich_banrep_minutas_html_with_browser,
    _enrich_diario_oficial_pdfs,
    _enrich_gaceta_pdfs,
    _enrich_mincit_zonas_francas,
    _enrich_pdf_text,
    _enrich_senado_agenda_pdfs,
    _annotate_legal_identity_items,
    _extract_anchors,
    _extract_banrep_minutas_metadata,
    _extract_corte_comunicados,
    _extract_dane_comunicados,
    _extract_dian_regulatory_project_links,
    _extract_eltiempo_colombia_section,
    _extract_imprenta_jsf_table,
    _extract_mincit_zonas_francas_approved_rows_from_text,
    _extract_minhacienda_decree_projects,
    _extract_minhacienda_decree_projects_from_reader_markdown,
    _extract_minhacienda_tes_auction_facts,
    _extract_minhacienda_tes_auction_rows_from_text,
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
    _fetch_dian_regulatory_projects_api,
    fetch_api,
    fetch_html,
    fetch_rss,
)
from colombia_forecasting_desk.models import RawItem
from colombia_forecasting_desk.observability import RunTrace


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


MINHACIENDA_TES_COP_TEXT = """
MINHACIENDA REALIZA SUBASTA DE TES COP POR
$6,0 BILLONES, LA MAYOR REALIZADA A LA FECHA
El Ministerio de Hacienda y Crédito Público Minhacienda emitió hoy $6,0
billones en la subasta de TES denominados en pesos (COP) con vencimiento a
cuatro, nueve, catorce y treinta y dos años.
Se recibieron órdenes de compra por $12,3 billones, 4,1 veces el monto
inicialmente ofrecido.
Las tasas de interés de corte de la subasta fueron de 14,790% para los
TES 2030, 14,300% para los TES 2035, 13,968% para los TES 2040 y
13,940% para los TES 2058.
Bogotá, 13 de mayo de 2026
Tabla 1
Resultados Subasta TES COP
Plazo al vencimiento 4 años 9 años 14 años 32 años
Fecha de Vencimiento 27-feb-30 24-ene-35 28-nov-40 13-mar-58
Tasa cupón 12.500% 11.750% 12.750% 12.000%
Tasa de corte 14.790% 14.300% 13.968% 13.940%
Ofertas Recibidas $5.4 billones $4.2 billones $740 mil millones $1.9 billones
Monto Aprobado $2.8 billones $2.2 billones $178 mil millones $890 mil millones
(Fin).
"""


IRC_TES_COP_TEXT = """
RESUMEN SUBASTA TES TASA FIJA
Subdirección Financiamiento Interno de la Nación
Dirección General de Crédito Público y Tesoro Nacional
Ministerio de Hacienda y Crédito Público
Subasta No. 9 13 de mayo de 2026
FECHA PLAZO AL VTO. TASA MÍNIMA TASA MÁXIMA TASA PROMEDIO TASA DE
CORTE TASA SEN PRECIA PRECIO CUPÓN "TI" TAIL PB
VENCIMIENTO (AÑOS)
27-feb-30 4 14,520% 15,100% 14,810% 14,790% 14,760% 14,546% 96,120 12,50% 14,785% 0,459
24-ene-35 9 14,099% 14,661% 14,380% 14,300% 14,260% 14,053% 91,087 11,75% 14,346% -4,572
28-nov-40 14 13,820% 14,411% 14,116% 13,968% 13,902% 13,768% 98,174 12,75% 14,098% -13,039
13-mar-58 32 13,650% 14,297% 13,974% 13,940% 13,850% 13,639% 88,200 12,00% 13,983% -4,332
MONTOS
SESION COMPETITIVA
FECHA MONTO OFERTADO MONTO OFERTADO MONTO APROBADO MONTO APROBADO
BID/COVER
VENCIMIENTO Valor Nominal Valor Costo Valor Nominal Valor Costo
27-feb-30 5.698.333.000.000 5.430.910.232.310 2.886.053.800.000 2.774.074.912.560
24-ene-35 4.677.500.000.000 4.187.531.875.000 2.369.000.000.000 2.157.851.030.000
28-nov-40 773.000.000.000 739.660.510.000 181.000.000.000 177.694.940.000
13-mar-58 2.228.500.000.000 1.917.802.530.000 1.009.500.000.000 890.379.000.000
TOTAL 13.377.333.000.000 12.275.905.147.310 6.445.553.800.000 5.999.999.882.560 4,1
"""


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
    assert _parse_date_text_to_iso("17.05.2026") == "2026-05-17T00:00:00Z"
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


def test_extract_minhacienda_tes_auction_rows_from_text() -> None:
    rows = _extract_minhacienda_tes_auction_rows_from_text(MINHACIENDA_TES_COP_TEXT)

    assert rows == [
        {
            "tenor_years": 4,
            "maturity_date": "27-feb-30",
            "maturity_year": 2030,
            "coupon_rate_pct": 12.5,
            "cutoff_rate_pct": 14.79,
            "demand_cop_billions": 5.4,
            "approved_cop_billions": 2.8,
        },
        {
            "tenor_years": 9,
            "maturity_date": "24-ene-35",
            "maturity_year": 2035,
            "coupon_rate_pct": 11.75,
            "cutoff_rate_pct": 14.3,
            "demand_cop_billions": 4.2,
            "approved_cop_billions": 2.2,
        },
        {
            "tenor_years": 14,
            "maturity_date": "28-nov-40",
            "maturity_year": 2040,
            "coupon_rate_pct": 12.75,
            "cutoff_rate_pct": 13.968,
            "demand_cop_billions": 0.74,
            "approved_cop_billions": 0.178,
        },
        {
            "tenor_years": 32,
            "maturity_date": "13-mar-58",
            "maturity_year": 2058,
            "coupon_rate_pct": 12.0,
            "cutoff_rate_pct": 13.94,
            "demand_cop_billions": 1.9,
            "approved_cop_billions": 0.89,
        },
    ]


def test_extract_irc_tes_auction_facts() -> None:
    facts = _extract_minhacienda_tes_auction_facts(
        IRC_TES_COP_TEXT,
        title="Subasta 09 COP Mayo 13 de 2026",
        pdf_url="https://www.irc.gov.co/documents/d/guest/subasta-9-cop-mayo-13-de-2026?download=true",
    )

    assert facts is not None
    assert facts["auction_date"] == "2026-05-13T00:00:00Z"
    assert facts["auction_type"] == "COP"
    assert facts["auction_number"] == "9"
    assert facts["total_issued_cop_billions"] == 6.0
    assert facts["total_demand_cop_billions"] == 12.276
    assert facts["bid_to_cover"] == 4.1
    assert facts["maturity_years"] == [2030, 2035, 2040, 2058]
    assert facts["maturity_rows"][0]["cutoff_rate_pct"] == 14.79
    assert facts["maturity_rows"][0]["coupon_rate_pct"] == 12.5
    assert facts["maturity_rows"][0]["approved_cop_billions"] == 2.774
    assert facts["max_cutoff_rate_pct"] == 14.79


def test_extract_minhacienda_tes_auction_facts_fail_closed_without_table() -> None:
    facts = _extract_minhacienda_tes_auction_facts(
        "El Ministerio emitió TES, pero este texto no contiene la tabla.",
        title="Informe TES subasta COP No. 09",
        pdf_url="https://www.minhacienda.gov.co/documents/d/portal/report?download=true",
    )

    assert facts is None


def test_fetch_minhacienda_tes_reports_enriches_pdf_text(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="minhacienda_tes_reports",
        name="MinHacienda — Informes TES 2026",
        type="economic_indicator",
        url="https://www.minhacienda.gov.co/informes-tes-2026",
        fetch_method="html",
        trust_role="official_signal",
        max_items=1,
    )
    index_html = """
    <main>
      <a href="/documents/d/portal/informe-tes-subasta-cop-no-09">
        Informe TES subasta COP No. 09
      </a>
      <p>El Ministerio de Hacienda y Crédito Público MinHacienda emitió hoy
      $6,0 billones en la subasta de...</p>
    </main>
    """

    def fake_pdf_text(content: bytes, *, max_chars: int) -> str:
        assert content == b"%PDF official report"
        return MINHACIENDA_TES_COP_TEXT

    monkeypatch.setattr(
        minhacienda_fetchers,
        "_extract_pdf_text_with_pdfplumber",
        fake_pdf_text,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/informes-tes-2026":
            return httpx.Response(200, text=index_html)
        if request.url.path == "/documents/d/portal/informe-tes-subasta-cop-no-09":
            assert request.url.params["download"] == "true"
            return httpx.Response(200, content=b"%PDF official report")
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    item = items[0]
    assert item.published_at == "2026-05-13T00:00:00Z"
    assert item.metadata["content_extraction"] == "minhacienda_tes_auction_pdf"
    assert item.metadata["total_issued_cop_billions"] == 6.0
    assert item.metadata["total_demand_cop_billions"] == 12.3
    assert item.metadata["bid_to_cover"] == 4.1
    assert item.metadata["maturity_years"] == [2030, 2035, 2040, 2058]
    assert item.metadata["max_cutoff_rate_pct"] == 14.79
    assert item.metadata["long_cutoff_rate_pct"] == 13.94
    assert "Source PDF:" in item.raw_text


def test_extract_minhacienda_tes_reports_derives_pdf_url_from_view_file(
    sample_source,
) -> None:
    source = replace(
        sample_source,
        id="minhacienda_tes_reports",
        name="MinHacienda — Informes TES 2026",
        type="economic_indicator",
        url="https://www.minhacienda.gov.co/informes-tes-2026",
    )
    html = """
    <table>
      <tr>
        <td>
          <a href="/informes-tes-2026/-/document_library/immw/view_file/3281840">
            Informe TES subasta COP No. 09
          </a>
        </td>
        <td>Hace 2 días</td>
      </tr>
    </table>
    """
    items = fetchers._extract_minhacienda_tes_reports(
        html,
        source.url,
        source,
        "2026-05-16T00:00:00Z",
    )

    assert len(items) == 1
    assert items[0].url == (
        "https://www.minhacienda.gov.co/documents/d/portal/"
        "informe-tes-subasta-cop-no-09?download=true"
    )


def test_extract_irc_tes_reports_pairs_titles_with_download_links(sample_source) -> None:
    source = replace(
        sample_source,
        id="minhacienda_tes_reports",
        name="MinHacienda / IRC — Subastas TES 2026",
        type="economic_indicator",
        url="https://www.irc.gov.co/424",
    )
    html = """
    <table>
      <tr>
        <td><a href="/424/-/document_library/sinf/view_file/3288677">
          Subasta 09 COP Mayo 13 de 2026
        </a></td>
        <td>Hace 2 días</td>
      </tr>
      <tr>
        <td><a href="/424/-/document_library/sinf/view_file/3288666">
          Subasta 08 COP Abril 29 de 2026
        </a></td>
        <td>Hace 2 semanas</td>
      </tr>
    </table>
    <a href="/documents/d/guest/subasta-9-cop-mayo-13-de-2026?download=true">
      Descargar (244 KB)
    </a>
    <a href="/documents/d/guest/subasta-8-cop-abril-29-de-2026-1?download=true">
      Descargar (235 KB)
    </a>
    """
    items = fetchers._extract_irc_tes_reports(
        html,
        source.url,
        source,
        "2026-05-16T00:00:00Z",
    )

    assert [item.title for item in items] == [
        "Subasta 09 COP Mayo 13 de 2026",
        "Subasta 08 COP Abril 29 de 2026",
    ]
    assert items[0].url == (
        "https://www.irc.gov.co/documents/d/guest/"
        "subasta-9-cop-mayo-13-de-2026?download=true"
    )
    assert items[0].published_at == "2026-05-13T00:00:00Z"


def test_fetch_minhacienda_tes_reports_uses_browser_on_bot_block(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="minhacienda_tes_reports",
        name="MinHacienda — Informes TES 2026",
        type="economic_indicator",
        url="https://www.minhacienda.gov.co/informes-tes-2026",
        fetch_method="html",
        max_items=1,
    )
    browser_item = RawItem(
        id="minhacienda-browser-item",
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url="https://www.minhacienda.gov.co/documents/d/portal/informe-tes-subasta-cop-no-09?download=true",
        title="Informe TES subasta COP No. 09",
        fetched_at="2026-05-16T00:00:00Z",
        published_at="2026-05-13T00:00:00Z",
        raw_text="Official MinHacienda TES auction report.",
        metadata={"content_extraction": "minhacienda_tes_auction_pdf"},
    )
    calls: list[tuple[str, int]] = []

    def fake_browser_fetch(source_arg, fetched_at, *, max_items):
        calls.append((source_arg.id, max_items))
        return [browser_item]

    monkeypatch.setattr(
        fetchers,
        "_fetch_minhacienda_tes_reports_with_browser",
        fake_browser_fetch,
    )

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>Radware Bot Manager</html>")
    )
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == [browser_item]
    assert calls == [("minhacienda_tes_reports", 1)]


def test_extract_minhacienda_decree_projects_requires_complete_project_fields(
    sample_source,
) -> None:
    source = replace(
        sample_source,
        id="minhacienda_proyectos_decreto",
        name="MinHacienda — Proyectos de Decreto",
        type="regulatory",
        url="https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026",
        fetch_method="html",
        trust_role="regulatory_signal",
    )
    html = """
    <main>
      <div class="project">
        <a href="/documents/20119/2873514/PD+garantias.pdf/abc?t=1">
          &#xf15c; PD. Por el cual se modifica el Decreto 1068 de 2015.
        </a>
        <div>mayo 13, 2026</div>
        <p>El proyecto de decreto tiene por objeto modificar garantias
        para bonos hipotecarios.</p>
        <p>El Ministerio de Hacienda informa que el Proyecto de Decreto
        esta para comentarios del 13 al 28 de mayo de 2026 hasta las 12
        de la noche.</p>
        <a href="/web/forms/shared/-/form/3277529">Comentar proyecto</a>
      </div>
      <div class="project">
        <a href="/documents/20119/2873514/PD+sin+formulario.pdf/def?t=2">
          &#xf15c; PD. Proyecto sin formulario publicado.
        </a>
        <div>mayo 12, 2026</div>
        <p>El proyecto de decreto tiene por objeto ajustar una regla fiscal.</p>
      </div>
    </main>
    """

    items = _extract_minhacienda_decree_projects(
        html,
        source.url,
        source,
        "2026-05-19T00:00:00Z",
    )

    assert len(items) == 2
    complete = items[0]
    assert complete.title == "PD. Por el cual se modifica el Decreto 1068 de 2015."
    assert complete.published_at == "2026-05-13T00:00:00Z"
    assert complete.url == (
        "https://www.minhacienda.gov.co/documents/20119/2873514/"
        "PD+garantias.pdf/abc?t=1"
    )
    assert (
        complete.metadata["content_extraction"]
        == "minhacienda_decree_project_browser"
    )
    assert complete.metadata["comment_form_url"] == (
        "https://www.minhacienda.gov.co/web/forms/shared/-/form/3277529"
    )
    assert "bonos hipotecarios" in complete.metadata["description"]
    assert "13 al 28 de mayo de 2026" in complete.metadata["comment_window_text"]
    assert "Proyecto PDF:" in complete.raw_text

    incomplete = items[1]
    assert "content_extraction" not in incomplete.metadata
    assert incomplete.metadata["content_extraction_error"] == (
        "missing required decree project fields: comment_form_url"
    )


def test_extract_minhacienda_decree_projects_from_reader_markdown(sample_source) -> None:
    source = replace(
        sample_source,
        id="minhacienda_proyectos_decreto",
        name="MinHacienda — Proyectos de Decreto",
        type="regulatory",
        url="https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026",
        fetch_method="html",
        trust_role="regulatory_signal",
    )
    markdown = """
    [PD. Por el cual se modifica el Decreto 1068 de 2015.](https://www.minhacienda.gov.co/documents/20119/2873514/PD+garantias.pdf/abc?t=1 "Documento")

    mayo 13, 2026

    El proyecto de decreto tiene por objeto modificar garantias para bonos
    hipotecarios.

    El Ministerio de Hacienda informa que el Proyecto de Decreto esta para
    comentarios del 13 al 28 de mayo de 2026 hasta las 12 de la noche.

    [Comentar proyecto](https://www.minhacienda.gov.co/web/forms/shared/-/form/3277529)

    [PD. Proyecto sin formulario publicado.](https://www.minhacienda.gov.co/documents/20119/2873514/PD+sin+formulario.pdf/def?t=2 "Documento")

    mayo 12, 2026

    El proyecto de decreto tiene por objeto ajustar una regla fiscal.

    Mostrando el intervalo 1 - 2 de 2 resultados.
    """

    items = _extract_minhacienda_decree_projects_from_reader_markdown(
        markdown,
        source.url,
        "https://r.jina.ai/" + source.url,
        source,
        "2026-05-19T00:00:00Z",
    )

    assert len(items) == 2
    complete = items[0]
    assert complete.published_at == "2026-05-13T00:00:00Z"
    assert (
        complete.metadata["content_extraction"]
        == "minhacienda_decree_project_reader"
    )
    assert complete.metadata["source_access"] == "jina_reader_proxy"
    assert complete.metadata["official_source_url"] == source.url
    assert complete.metadata["comment_form_url"] == (
        "https://www.minhacienda.gov.co/web/forms/shared/-/form/3277529"
    )
    assert "13 al 28 de mayo de 2026" in complete.metadata["comment_window_text"]

    incomplete = items[1]
    assert "content_extraction" not in incomplete.metadata
    assert incomplete.metadata["content_extraction_error"] == (
        "missing required decree project fields: comment_form_url"
    )


def test_fetch_minhacienda_decree_projects_uses_browser_on_bot_block(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="minhacienda_proyectos_decreto",
        name="MinHacienda — Proyectos de Decreto",
        type="regulatory",
        url="https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026",
        fetch_method="html",
        max_items=2,
    )
    browser_item = RawItem(
        id="minhacienda-decree-browser-item",
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url="https://www.minhacienda.gov.co/documents/20119/2873514/project.pdf/abc?t=1",
        title="PD. Por el cual se modifica el Decreto 1068 de 2015.",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-13T00:00:00Z",
        raw_text="Official MinHacienda decree project with comment window.",
        metadata={"content_extraction": "minhacienda_decree_project_browser"},
    )
    calls: list[tuple[str, int]] = []

    def fake_browser_fetch(source_arg, fetched_at, *, max_items):
        calls.append((source_arg.id, max_items))
        return [browser_item]

    monkeypatch.setattr(
        minhacienda_fetchers,
        "_fetch_minhacienda_decree_projects_with_browser",
        fake_browser_fetch,
    )

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>Radware Bot Manager</html>")
    )
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == [browser_item]
    assert calls == [("minhacienda_proyectos_decreto", 2)]


def test_fetch_minhacienda_decree_projects_uses_reader_after_browser_bot_block(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="minhacienda_proyectos_decreto",
        name="MinHacienda — Proyectos de Decreto",
        type="regulatory",
        url="https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026",
        fetch_method="html",
        max_items=2,
    )
    calls: list[str] = []

    def fake_browser_fetch(source_arg, fetched_at, *, max_items):
        calls.append(source_arg.id)
        raise fetchers.BotBlockError("browser fetch still bot-blocked: Radware Page")

    monkeypatch.setattr(
        minhacienda_fetchers,
        "_fetch_minhacienda_decree_projects_with_browser",
        fake_browser_fetch,
    )
    reader_markdown = """
    [PD. Por el cual se modifica el Decreto 1068 de 2015.](https://www.minhacienda.gov.co/documents/20119/2873514/project.pdf/abc?t=1)

    mayo 13, 2026

    El Ministerio de Hacienda informa que el Proyecto de Decreto esta para
    comentarios del 13 al 28 de mayo de 2026 hasta las 12 de la noche.

    [Comentar proyecto](https://www.minhacienda.gov.co/web/forms/shared/-/form/3277529)
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.minhacienda.gov.co":
            return httpx.Response(200, text="<title>Radware Page</title>")
        if request.url.host == "r.jina.ai":
            assert str(request.url).startswith(
                "https://r.jina.ai/https://www.minhacienda.gov.co/"
            )
            return httpx.Response(200, text=reader_markdown)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert calls == ["minhacienda_proyectos_decreto"]
    assert len(items) == 1
    assert items[0].metadata["source_access"] == "jina_reader_proxy"
    assert (
        items[0].metadata["content_extraction"]
        == "minhacienda_decree_project_reader"
    )


def test_fetch_banrep_junta_uses_browser_on_bot_block(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="banrep_junta_comunicados",
        name="BanRep Junta",
        type="official_updates",
        url="https://www.banrep.gov.co/es/comunicados-junta",
        fetch_method="html",
    )
    browser_item = RawItem(
        id="banrep-browser-item",
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url="https://www.banrep.gov.co/es/minutas",
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-06T00:00:00Z",
        raw_text="Official BanRep minutas.",
        metadata={"content_extraction": "banrep_minutas_html"},
    )
    calls: list[str] = []

    def fake_browser_fetch(source_arg, fetched_at):
        calls.append(source_arg.id)
        return [browser_item]

    monkeypatch.setattr(
        fetchers,
        "_fetch_banrep_junta_with_browser",
        fake_browser_fetch,
    )

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>Radware Bot Manager</html>")
    )
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == [browser_item]
    assert calls == ["banrep_junta_comunicados"]


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


BANREP_MINUTAS_DETAIL_HTML = """
<html><body><main>
  <h1>Minutas BanRep: La Junta Directiva del Banco de la República decidió por
  mayoría incrementar en 100 puntos básicos (pbs) la tasa de interés de política
  monetaria a 11,25%</h1>
  <a href="/es/print/pdf/node/65818">View PDF</a>
  <h2>Adjuntos</h2>
  <a href="https://d1b4gd4m8561gs.cloudfront.net/sites/default/files/anexo.pdf">
    Anexo estadístico
  </a>
  <p>Cuatro directores votaron a favor de esta decisión, dos por una reducción
  de 50 pbs y uno por mantenerla inalterada.</p>
  <p>Fecha de publicación: Martes, 07 de abril de 2026 19:44</p>
  <ul>
    <li>La inflación total en enero y febrero se situó en 5,4% y 5,3%,
    respectivamente, por encima del nivel observado al cierre de 2025.</li>
    <li>Las expectativas de inflación total continúan elevadas y alejadas de la
    meta, aunque las encuestas a analistas mostraron ligeras reducciones.</li>
  </ul>
  <p>El grupo mayoritario que votó por incrementar la tasa de interés de
  política en 100 pbs recordó que la decisión de enero no era suficiente. Los
  miembros de este grupo subrayaron el comportamiento de la inflación total y
  básica.</p>
  <p>Los directores que votaron por una reducción de 50 pbs de la tasa de
  interés de política destacaron que la inflación observada responde más a
  choques de oferta.</p>
  <p>El miembro de la Junta que votó por mantener inalterada la tasa de interés
  de política señaló que el ciclo de crisis provocado por la pandemia aún no se
  estabiliza.</p>
  <p>Próximas reuniones, minutas, informes y presentaciones ABR 30 Reunión tasa
  de interés de intervención.</p>
</main></body></html>
"""

BANREP_MINUTAS_DRUPAL_DETAIL_HTML = """
<html><body>
  <div class="block-page-title-block">
    <h1>Minutas BanRep: La Junta Directiva del Banco de la República decidió por
    unanimidad mantener inalterada la tasa de interés de política monetaria en
    11,25%</h1>
  </div>
  <div data-history-node-id="65916" class="node node--type-noticias">
    <div class="field--name-field-file">
      <a href="//d1b4gd4m8561gs.cloudfront.net/sites/default/files/paginas/anexo-estadistico-abril-2026.pdf">
        Anexo estadístico
      </a>
    </div>
    <div class="field-label">Fecha de publicación:</div>
    Miércoles, 06 de mayo de 2026
    <div class="body field-node--body">
      <p>La Junta Directiva tuvo en cuenta los siguientes elementos:</p>
      <ul>
        <li>En marzo la inflación total se situó en 5,6% superando en 46 pbs el
        dato de diciembre.</li>
        <li>El mercado laboral continúa dinámico, con niveles de desempleo
        históricamente bajos y tendencias crecientes en el empleo asalariado.</li>
      </ul>
      <p>La decisión adoptada por unanimidad de mantener inalterada la tasa de
      interés de política envía un mensaje de consenso entre los miembros de la
      Junta Directiva.</p>
      <p>Un grupo de cuatro directores manifestó su preocupación por el
      incremento que se ha venido observando en la inflación total y básica, y
      en sus expectativas. Subrayaron la persistencia inflacionaria.</p>
      <p>Los dos directores que abogan por una postura de política monetaria más
      relajada sostienen que la inflación anual ha descendido sustancialmente y
      que sus incrementos recientes obedecen a choques de oferta.</p>
      <p>Otro miembro de la Junta analiza que la inflación en marzo estuvo
      explicada por diversos factores entre los que predominan los choques de
      oferta.</p>
      <p>Asimismo, resaltaron que, en la sesión de Junta del próximo 30 de
      junio, se contará con información adicional valiosa.</p>
    </div>
  </div>
</body></html>
"""


def test_extract_banrep_minutas_metadata_reads_policy_body() -> None:
    metadata = _extract_banrep_minutas_metadata(
        BANREP_MINUTAS_DETAIL_HTML,
        "https://www.banrep.gov.co/es/noticias/minutas-banrep-marzo-2026",
    )

    assert metadata["content_extraction"] == "banrep_minutas_html"
    assert metadata["decision_action"] == "hike"
    assert metadata["rate_change_bps"] == 100
    assert metadata["policy_rate_pct"] == "11.25"
    assert metadata["publication_date"] == "2026-04-07T00:00:00Z"
    assert metadata["vote_result"] == "majority"
    assert "Cuatro directores votaron" in metadata["vote_summary"]
    assert len(metadata["key_bullets"]) == 2
    assert "incrementar la tasa" in metadata["board_blocs"]["majority"]
    assert "reducción de 50 pbs" in metadata["board_blocs"]["rate_cut_bloc"]
    assert "mantener inalterada" in metadata["board_blocs"]["hold_bloc"]
    assert metadata["official_links"][0]["url"].endswith("/es/print/pdf/node/65818")
    assert "ABR 30" in metadata["next_meeting_context"]


def test_extract_banrep_minutas_metadata_reads_drupal_node_body() -> None:
    metadata = _extract_banrep_minutas_metadata(
        BANREP_MINUTAS_DRUPAL_DETAIL_HTML,
        "https://www.banrep.gov.co/es/noticias/minutas-banrep-abril-2026",
    )

    assert metadata["content_extraction"] == "banrep_minutas_html"
    assert metadata["decision_action"] == "hold"
    assert metadata["policy_rate_pct"] == "11.25"
    assert metadata["publication_date"] == "2026-05-06T00:00:00Z"
    assert metadata["vote_result"] == "unanimous"
    assert "consenso" in metadata["vote_summary"]
    assert len(metadata["key_bullets"]) == 2
    assert "cuatro directores" in metadata["board_blocs"]["hawkish_bloc"]
    assert "dos directores" in metadata["board_blocs"]["dovish_bloc"]
    assert "Otro miembro" in metadata["board_blocs"]["single_member_bloc"]
    assert "30 de junio" in metadata["next_meeting_context"]
    assert metadata["official_links"][0]["url"].startswith(
        "https://d1b4gd4m8561gs.cloudfront.net/"
    )


class _FakeBanrepMinutasClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        if "minutas-banrep-marzo-2026" in url:
            return _FakeBinaryResponse(
                BANREP_MINUTAS_DETAIL_HTML.encode("utf-8"),
                url=url,
            )
        raise httpx.TransportError("detail unavailable")


class _FakeBanrepBotBlockClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(
            b"<html><title>Radware Bot Manager</title></html>",
            url="https://validate.perfdrive.com/challenge",
        )


def test_enrich_banrep_minutas_html_keeps_listing_item_on_detail_failure() -> None:
    minutas = RawItem(
        id="banrep-minutas-1",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-marzo-2026",
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-04-07T00:00:00Z",
        raw_text="07/04/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )
    comunicado = RawItem(
        id="banrep-comunicado-1",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/junta-directiva-marzo-2026",
        title="La Junta Directiva decidió incrementar la tasa",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-03-31T00:00:00Z",
        raw_text="31/03/2026 Comunicado Junta",
        metadata={"extraction": "anchor"},
    )
    missing = RawItem(
        id="banrep-minutas-2",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-enero-2026",
        title="Minutas BanRep: decisión anterior",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-02-04T00:00:00Z",
        raw_text="04/02/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )

    enriched = _enrich_banrep_minutas_html(
        [minutas, comunicado, missing],
        _FakeBanrepMinutasClient(),
        max_items=3,
    )

    assert enriched[0].metadata["content_extraction"] == "banrep_minutas_html"
    assert "BanRep minutas detail" in enriched[0].raw_text
    assert enriched[1] == comunicado
    assert enriched[2] == missing
    assert enriched[2].metadata == {"extraction": "anchor"}


def test_enrich_banrep_minutas_html_uses_browser_when_detail_is_bot_blocked(
    monkeypatch,
) -> None:
    minutas = RawItem(
        id="banrep-minutas-1",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-marzo-2026",
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-04-07T00:00:00Z",
        raw_text="07/04/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )
    browser_item = RawItem(
        id=minutas.id,
        source_id=minutas.source_id,
        source_name=minutas.source_name,
        source_type=minutas.source_type,
        url=minutas.url,
        title=minutas.title,
        fetched_at=minutas.fetched_at,
        published_at=minutas.published_at,
        raw_text="Browser parsed BanRep minutas detail.",
        metadata={"content_extraction": "banrep_minutas_html"},
    )
    calls: list[int] = []

    def fake_browser_enrich(items, *, max_items):
        calls.append(max_items)
        assert items == [minutas]
        return [browser_item]

    monkeypatch.setattr(
        dane_fetchers,
        "_enrich_banrep_minutas_html_with_browser_session",
        fake_browser_enrich,
    )

    enriched = _enrich_banrep_minutas_html(
        [minutas],
        _FakeBanrepBotBlockClient(),
        max_items=1,
    )

    assert enriched == [browser_item]
    assert calls == [1]


class _FakeBanrepBrowserPage:
    def __init__(self, html_by_url: dict[str, str]) -> None:
        self.html_by_url = html_by_url
        self.url = ""

    def goto(self, url, wait_until=None, timeout=None):  # noqa: ANN001
        self.url = url

    def wait_for_load_state(self, state, timeout=None):  # noqa: ANN001
        return None

    def content(self) -> str:
        return self.html_by_url[self.url]


def test_enrich_banrep_minutas_html_with_browser_uses_detail_parser() -> None:
    minutas_url = "https://www.banrep.gov.co/es/minutas"
    minutas = RawItem(
        id="banrep-minutas-browser",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url=minutas_url,
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-06T00:00:00Z",
        raw_text="06/05/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )
    page = _FakeBanrepBrowserPage({minutas_url: BANREP_MINUTAS_DETAIL_HTML})

    enriched = _enrich_banrep_minutas_html_with_browser(
        [minutas],
        page,
        max_items=1,
    )

    assert enriched[0].metadata["content_extraction"] == "banrep_minutas_html"
    assert enriched[0].metadata["vote_result"] == "majority"
    assert "BanRep minutas detail" in enriched[0].raw_text


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


def test_fetch_dian_regulatory_projects_api_emits_structured_rows(
    sample_source,
) -> None:
    source = replace(
        sample_source,
        id="dian_proyectos_normas",
        name="DIAN — Proyectos de Normas",
        type="regulatory",
        url="https://www.dian.gov.co/normatividad/Paginas/Inicio.aspx",
    )
    captured: dict[str, str] = {}
    payload = {
        "value": [
            {
                "FileLeafRef": "Proyecto Resolución 000000 de 05-05-2026.pdf",
                "Title": "Proyecto Resolución 000000 de 2026",
                "NumeroNorma": 0.0,
                "FechaDeEmision": "2026-05-05T05:00:00Z",
                "Descripcion": (
                    "Por la cual se reorganiza la estructura y funciones del "
                    "Comité Técnico de Programas, Campañas y Acciones de Control."
                ),
                "TipoDeNorma": "Proyecto Resolución",
                "Fecha_x0020_inicio": "2026-05-05T05:00:00Z",
                "Fecha_x0020_final": "2026-05-14T05:00:00Z",
                "Buz_x00f3_n": "dir_fiscalizacion@dian.gov.co. ",
                "Observaciones": {
                    "Description": "Observaciones",
                    "Url": (
                        "https://www.dian.gov.co/normatividad/"
                        "ObservacionesProyectosNormas/"
                        "Observaciones-Proyecto-Resolucion-05052026.pdf"
                    ),
                },
                "Anexos": {
                    "Description": "Anexos",
                    "Url": (
                        "https://www.dian.gov.co/normatividad/"
                        "Proyectosnormas/Anexo Resolución 000000.zip"
                    ),
                },
                "Created": "2026-05-05T13:00:02Z",
                "Modified": "2026-05-05T13:03:00Z",
                "FileRef": (
                    "/normatividad/Proyectosnormas/"
                    "Proyecto Resolución 000000 de 05-05-2026.pdf"
                ),
            }
        ]
    }

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["orderby"] = request.url.params.get("$orderby", "")
        captured["accept"] = request.headers.get("accept", "")
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = _fetch_dian_regulatory_projects_api(
            source,
            client,
            "2026-05-19T12:00:00Z",
        )

    assert len(items) == 1
    item = items[0]
    assert "getbytitle('Proyectos%20de%20normas')" in captured["url"]
    assert captured["accept"] == "application/json;odata=nometadata"
    assert captured["orderby"] == "Modified desc"
    assert item.title == "Proyecto Resolución 000000 de 2026"
    assert item.published_at == "2026-05-05T13:03:00Z"
    assert item.url.endswith(
        "/Proyecto%20Resoluci%C3%B3n%20000000%20de%2005-05-2026.pdf"
    )
    assert "Ventana de comentarios" in item.raw_text
    assert "dir_fiscalizacion@dian.gov.co" in item.raw_text
    assert item.metadata["content_extraction"] == (
        "dian_regulatory_project_sharepoint"
    )
    assert item.metadata["description"].startswith("Por la cual se reorganiza")
    assert item.metadata["issue_date"] == "2026-05-05T05:00:00Z"
    assert item.metadata["modified_at"] == "2026-05-05T13:03:00Z"
    assert item.metadata["comment_start"] == "2026-05-05T05:00:00Z"
    assert item.metadata["comment_end"] == "2026-05-14T05:00:00Z"
    assert item.metadata["observations_url"].endswith(
        "Observaciones-Proyecto-Resolucion-05052026.pdf"
    )
    assert item.metadata["annex_url"].endswith(
        "Anexo%20Resoluci%C3%B3n%20000000.zip"
    )


def test_fetch_html_uses_dian_sharepoint_api_instead_of_landing_page(
    sample_source,
) -> None:
    source = replace(
        sample_source,
        id="dian_proyectos_normas",
        name="DIAN — Proyectos de Normas",
        type="regulatory",
        url="https://www.dian.gov.co/normatividad/Paginas/Inicio.aspx",
    )
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "FileLeafRef": "Proyecto Resolución 000000.pdf",
                        "Title": "Proyecto Resolución 000000 de 2026",
                        "FechaDeEmision": "2026-05-05T05:00:00Z",
                        "Descripcion": "Proyecto normativo DIAN.",
                        "TipoDeNorma": "Proyecto Resolución",
                        "FileRef": "/normatividad/Proyectosnormas/Proyecto.pdf",
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    assert seen_paths == [
        "/normatividad/_api/web/lists/getbytitle('Proyectos de normas')/items"
    ]


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


def test_extract_eltiempo_colombia_section_parses_article_cards(sample_source) -> None:
    source = replace(sample_source, id="eltiempo_colombia")
    html = """
    <html><body>
      <article
        data-id="3556412"
        data-publicacion="2026-05-17"
        data-category="Colombia/otras ciudades"
        data-name="Recompensa por responsables de ataque en el Cauca"
      >
        <a
          class="c-articulo__titulo__txt"
          href="/colombia/otras-ciudades/recompensa-por-ataque-en-cauca-3556412"
        >Recompensa por responsables de ataque en el Cauca</a>
        <p class="c-articulo__resumen">Autoridades anunciaron una recompensa.</p>
      </article>
      <article data-publicacion="2026-05-17">
        <a class="c-articulo__titulo__txt" href="/mas-contenido/especial-comercial">
          Especial comercial de marca
        </a>
      </article>
      <article data-publicacion="2026-05-17">
        <a class="c-articulo__titulo__txt" href="https://example.com/colombia/noticia">
          Nota externa que no pertenece al sitio
        </a>
      </article>
    </body></html>
    """

    items = _extract_eltiempo_colombia_section(
        html,
        "https://www.eltiempo.com/colombia",
        source,
        fetched_at="2026-05-18T12:00:00Z",
    )

    assert len(items) == 1
    assert items[0].title == "Recompensa por responsables de ataque en el Cauca"
    assert items[0].url == (
        "https://www.eltiempo.com/colombia/otras-ciudades/"
        "recompensa-por-ataque-en-cauca-3556412"
    )
    assert items[0].published_at == "2026-05-17T00:00:00Z"
    assert items[0].raw_text == "Autoridades anunciaron una recompensa."
    assert items[0].metadata["extraction"] == "eltiempo_colombia_section_html"
    assert items[0].metadata["article_id"] == "3556412"


def test_fetch_eltiempo_rss_augments_with_section_cards(sample_source) -> None:
    source = replace(
        sample_source,
        id="eltiempo_colombia",
        name="El Tiempo — Colombia",
        url="https://www.eltiempo.com/rss/colombia.xml",
        fetch_method="rss",
    )
    rss = """
    <rss><channel>
      <item>
        <title>Última hora política desde Bogotá</title>
        <link>https://www.eltiempo.com/colombia/bogota/rss-story-123</link>
        <pubDate>Mon, 18 May 2026 11:34:48 -0500</pubDate>
        <description>Noticia desde el RSS.</description>
      </item>
    </channel></rss>
    """
    section_html = """
    <html><body>
      <article data-id="123" data-publicacion="2026-05-18">
        <a class="c-articulo__titulo__txt" href="/colombia/bogota/rss-story-123">
          Última hora política desde Bogotá
        </a>
        <p class="c-articulo__resumen">Duplicado desde la sección.</p>
      </article>
      <article data-id="456" data-publicacion="2026-05-17">
        <a class="c-articulo__titulo__txt" href="/colombia/otras-ciudades/older-story-456">
          Alcaldes anuncian nuevas medidas regionales
        </a>
        <p class="c-articulo__resumen">Artículo que ya salió del RSS corto.</p>
      </article>
    </body></html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == source.url:
            return httpx.Response(200, text=rss, request=request)
        if str(request.url) == "https://www.eltiempo.com/colombia":
            return httpx.Response(200, text=section_html, request=request)
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True) as client:
        items = fetch_rss(source, client)

    assert len(items) == 2
    assert items[0].url == "https://www.eltiempo.com/colombia/bogota/rss-story-123"
    assert items[0].published_at == "2026-05-18T16:34:48Z"
    assert items[1].url == (
        "https://www.eltiempo.com/colombia/otras-ciudades/older-story-456"
    )
    assert items[1].metadata["extraction"] == "eltiempo_colombia_section_html"


def test_fetch_all_records_source_trace_events(sample_source) -> None:
    source = replace(
        sample_source,
        id="trace_rss",
        name="Trace RSS",
        url="https://example.com/rss.xml",
        fetch_method="rss",
    )
    rss = """
    <rss><channel>
      <item>
        <title>Banco de la Republica anuncia nueva decision de tasas</title>
        <link>https://example.com/item-1</link>
        <pubDate>Mon, 18 May 2026 11:34:48 -0500</pubDate>
        <description>Comunicado con suficiente texto para trazabilidad.</description>
      </item>
    </channel></rss>
    """

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text=rss, request=request)
    )
    trace = RunTrace(run_date="2026-05-18", mode="daily")

    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items, failures = fetchers.fetch_all([source], client=client, trace=trace)

    assert len(items) == 1
    assert failures == []
    source_events = [
        event
        for event in trace.to_dict()["events"]
        if event["name"] == "fetch_source"
    ]
    assert len(source_events) == 1
    event = source_events[0]
    assert event["status"] == "ok"
    assert event["metadata"]["source_id"] == "trace_rss"
    assert event["metadata"]["fetch_method"] == "rss"
    assert event["counts"]["raw_items"] == 1


def test_fetch_all_records_failed_source_trace_event(sample_source) -> None:
    source = replace(sample_source, id="bad_source", fetch_method="unsupported")
    trace = RunTrace(run_date="2026-05-18", mode="daily")

    items, failures = fetchers.fetch_all([source], trace=trace)

    assert items == []
    assert len(failures) == 1
    source_events = [
        event
        for event in trace.to_dict()["events"]
        if event["name"] == "fetch_source"
    ]
    assert source_events[0]["status"] == "error"
    assert source_events[0]["metadata"]["source_id"] == "bad_source"
    assert source_events[0]["error_class"] == "ValueError"


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
