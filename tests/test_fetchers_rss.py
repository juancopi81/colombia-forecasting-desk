from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


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
