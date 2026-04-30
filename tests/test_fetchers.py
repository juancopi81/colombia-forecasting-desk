from __future__ import annotations

import time
from dataclasses import replace
from datetime import datetime, timezone

import httpx

from colombia_forecasting_desk.fetchers import (
    SOCRATA_ADAPTERS,
    SocrataAdapter,
    _extract_anchors,
    _extract_corte_comunicados,
    _extract_dane_comunicados,
    _cap_items,
    _parse_rss_entries,
    _parse_date_text_to_iso,
    _parse_socrata_date,
    _recover_rss_entries,
    _socrata_params,
    _socrata_row_to_item,
    _struct_time_to_iso,
    fetch_api,
)


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
