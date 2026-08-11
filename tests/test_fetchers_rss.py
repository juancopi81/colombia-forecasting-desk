from __future__ import annotations

from pathlib import Path

import colombia_forecasting_desk.source_fetching.rss as rss_fetchers
from colombia_forecasting_desk.config_loader import load_metasources

from tests.fetcher_helpers import *  # noqa: F403


DANE_PUBLICATION_CALENDAR_URL = (
    "https://www.dane.gov.co/index.php?option=com_jevents&task=modlatest.rss"
    "&format=feed&type=rss&Itemid=1626&modid=0"
)
DANE_PUBLICATION_CALENDAR_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "dane_publication_calendar"
    / "2026-08-11.xml"
)


def _dane_publication_calendar_source(sample_source, **overrides):
    values = {
        "id": "dane_publication_calendar",
        "name": "DANE — Calendario de publicaciones",
        "url": DANE_PUBLICATION_CALENDAR_URL,
        "type": "calendar",
        "trust_role": "agenda_signal",
        "max_items": 10,
    }
    values.update(overrides)
    return replace(sample_source, **values)


def _rss_with_titles(*titles: str) -> str:
    items = "".join(
        f"""
        <item>
          <title>{title}</title>
          <link>https://www.dane.gov.co/index.php/calendario/evento-{index}</link>
          <description>Publicación programada por DANE.</description>
          <pubDate>Mon, 10 Aug 2026 08:00:00 -0500</pubDate>
        </item>
        """
        for index, title in enumerate(titles, start=1)
    )
    return f"<rss><channel>{items}</channel></rss>"


def _fetch_dane_calendar(source, feed: str, monkeypatch, fetched_at: str):
    monkeypatch.setattr(rss_fetchers, "_now_iso", lambda: fetched_at)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=feed, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        return fetch_rss(source, client)


def test_fetch_dane_calendar_uses_scheduled_title_timestamp(
    sample_source,
    monkeypatch,
) -> None:
    source = _dane_publication_calendar_source(sample_source)
    items = _fetch_dane_calendar(
        source,
        DANE_PUBLICATION_CALENDAR_FIXTURE.read_text(encoding="utf-8"),
        monkeypatch,
        "2026-08-11T16:00:00Z",
    )

    assert [item.title for item in items] == [
        "13 Ago 2026 14:00 : GEIH - Población fuera de la fuerza laboral"
    ]
    assert items[0].published_at == "2026-08-13T14:00:00-05:00"
    assert items[0].metadata["feed_published_at"] == "2025-11-18T16:54:14Z"
    assert items[0].metadata["event_type"] == "dane_economic_release"
    assert items[0].metadata["calendar"] == "dane_publication_calendar"


def test_fetch_dane_calendar_extracts_operation_url(
    sample_source,
    monkeypatch,
) -> None:
    source = _dane_publication_calendar_source(sample_source)
    [item] = _fetch_dane_calendar(
        source,
        DANE_PUBLICATION_CALENDAR_FIXTURE.read_text(encoding="utf-8"),
        monkeypatch,
        "2026-08-11T16:00:00Z",
    )

    assert item.metadata["operation_url"] == (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "mercado-laboral/poblacion-fuera-de-la-fuerza-laboral"
    )
    assert item.metadata["calendar_event_url"] == item.url


def test_fetch_dane_calendar_keeps_supported_economic_release_families(
    sample_source,
    monkeypatch,
) -> None:
    source = _dane_publication_calendar_source(sample_source, max_items=20)
    feed = _rss_with_titles(
        "18 Ago 2026 10:00 : Índice de Precios al Consumidor (IPC)",
        "18 Ago 2026 10:30 : Producto Interno Bruto (PIB)",
        "18 Ago 2026 11:00 : Indicador de Seguimiento a la Economía (ISE)",
        "18 Ago 2026 11:30 : Gran Encuesta Integrada de Hogares (GEIH)",
        "18 Ago 2026 12:00 : Índice de Costos de la Construcción de Edificaciones (ICOCED)",
        "18 Ago 2026 12:30 : Encuesta Mensual Manufacturera con Enfoque Territorial (EMMET)",
        "18 Ago 2026 13:00 : Encuesta Mensual de Comercio (EMC)",
        "18 Ago 2026 13:30 : Importaciones",
        "18 Ago 2026 14:00 : Exportaciones",
        "18 Ago 2026 14:30 : Estadísticas de Edificación Licencias de Construcción (ELIC)",
        "18 Ago 2026 15:00 : Pobreza Monetaria Departamental",
    )
    items = _fetch_dane_calendar(
        source, feed, monkeypatch, "2026-08-11T16:00:00Z"
    )

    assert [item.metadata["release_family"] for item in items] == [
        "ipc",
        "pib",
        "ise",
        "labor",
        "icoced",
        "emmet",
        "retail",
        "imports",
        "exports",
        "construction_licenses",
    ]


def test_fetch_dane_calendar_fails_closed_on_non_current_release_titles(
    sample_source,
    monkeypatch,
) -> None:
    source = _dane_publication_calendar_source(sample_source, max_items=20)
    feed = _rss_with_titles(
        "18 Ago 2026 09:59 : Producto Interno Bruto (PIB)",
        "18 Ago 2026 10:00 : Índice de Precios al Consumidor (IPC)",
        "32 Ago 2026 11:00 : Indicador de Seguimiento a la Economía (ISE)",
        "Próxima publicación: Encuesta Mensual de Comercio (EMC)",
        "18 Ago 2026 12:00 : Boletín diario de Precios de Alimentos (SIPSA)",
    )
    items = _fetch_dane_calendar(
        source, feed, monkeypatch, "2026-08-18T15:00:00Z"
    )

    assert [item.metadata["release_family"] for item in items] == ["ipc"]
    assert items[0].metadata["scheduled_at_local"] == "2026-08-18T10:00:00-05:00"


def test_fetch_dane_calendar_caps_nearest_events_through_source_max_items(
    sample_source,
    monkeypatch,
) -> None:
    source = _dane_publication_calendar_source(sample_source, max_items=2)
    feed = _rss_with_titles(
        "20 Ago 2026 10:00 : Exportaciones",
        "18 Ago 2026 10:00 : Producto Interno Bruto (PIB)",
        "19 Ago 2026 10:00 : Importaciones",
    )
    items = _fetch_dane_calendar(
        source, feed, monkeypatch, "2026-08-11T16:00:00Z"
    )

    assert [item.metadata["release_family"] for item in items] == ["pib", "imports"]


def test_real_config_enables_dane_publication_calendar() -> None:
    config_path = Path(__file__).resolve().parents[1] / "config" / "metasources.yaml"
    sources = load_metasources(config_path)

    source = next(item for item in sources if item.id == "dane_publication_calendar")

    assert source.url == DANE_PUBLICATION_CALENDAR_URL
    assert source.fetch_method == "rss"
    assert source.type == "calendar"
    assert source.trust_role == "agenda_signal"
    assert source.priority == "high"
    assert source.onboarding_status == "working"
    assert source.max_items == 20


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
