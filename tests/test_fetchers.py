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
    _enrich_pdf_text,
    _extract_anchors,
    _extract_corte_comunicados,
    _extract_dane_comunicados,
    _extract_imprenta_jsf_table,
    _extract_pdf_text,
    _cap_items,
    _parse_rss_entries,
    _parse_dane_icoced_xlsx,
    _parse_date_text_to_iso,
    _parse_socrata_date,
    _recover_rss_entries,
    _socrata_params,
    _socrata_row_to_item,
    _struct_time_to_iso,
    fetch_api,
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

    def __init__(self, content: bytes):
        self.content = content

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
