from __future__ import annotations

import time
from dataclasses import replace

from colombia_forecasting_desk.fetchers import (
    _extract_anchors,
    _extract_corte_comunicados,
    _extract_dane_comunicados,
    _cap_items,
    _parse_rss_entries,
    _parse_date_text_to_iso,
    _recover_rss_entries,
    _struct_time_to_iso,
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


def test_cap_items_uses_source_max_items(sample_source, make_raw) -> None:
    source = replace(sample_source, max_items=2)
    items = [
        make_raw(id="1", url="https://example.com/1"),
        make_raw(id="2", url="https://example.com/2"),
        make_raw(id="3", url="https://example.com/3"),
    ]
    capped = _cap_items(source, items)
    assert [it.id for it in capped] == ["1", "2"]
