from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_extract_registraduria_news_cards(sample_source) -> None:
    source = replace(
        sample_source,
        id="registraduria_noticias",
        name="Registraduría — Noticias",
        type="official_updates",
        url="https://www.registraduria.gov.co/-2026-.html",
        fetch_method="html",
        trust_role="official_signal",
    )
    html = """
    <main>
      <li class="newsmodule">
        <div class="num-comunicado">No. 088</div>
        <div class="previewnew">
          <h4 class="titlepreview">
            Registrador Nacional le entregó detalles de las elecciones
            presidenciales 2026
          </h4>
          <p class="datenew">Martes 19 de mayo de 2026</p>
          <p class="captionnew">El proceso operativo avanza con normalidad.</p>
          <a class="seemorenew"
             href="Registrador-Nacional-le-entrego-detalles.html"></a>
        </div>
      </li>
      <li class="newsmodule">
        <div class="num-comunicado">No. 087</div>
        <div class="previewnew">
          <h4 class="titlepreview">Con éxito se desarrolló el simulacro nacional</h4>
          <p class="datenew">Sábado 16 de mayo de 2026</p>
          <p class="captionnew">Se evaluó la solución de preconteo.</p>
          <a class="seemorenew"
             href="/Con-exito-se-desarrollo-el-simulacro.html"></a>
        </div>
      </li>
    </main>
    """

    items = _extract_registraduria_news_cards(
        html,
        source.url,
        source,
        "2026-05-19T00:00:00Z",
        source_access="browser_official_html",
    )

    assert len(items) == 2
    assert items[0].title == (
        "Registrador Nacional le entregó detalles de las elecciones "
        "presidenciales 2026"
    )
    assert items[0].published_at == "2026-05-19T00:00:00Z"
    assert items[0].url == (
        "https://www.registraduria.gov.co/"
        "Registrador-Nacional-le-entrego-detalles.html"
    )
    assert items[0].metadata["comunicado_number"] == "No. 088"
    assert items[0].metadata["source_access"] == "browser_official_html"
    assert items[0].metadata["content_extraction"] == "registraduria_news_card"
    assert "normalidad" in items[0].raw_text


def test_extract_registraduria_news_article_detail() -> None:
    html = """
    <div class="maincollumn">
      <h2>Registrador Nacional le entregó detalles de las elecciones
      presidenciales 2026 al cuerpo diplomático</h2>
      <p class="date-news">Martes 19 de mayo de 2026 - Nacional</p>
      <blockquote class="spip">
        <p>El Registrador Nacional informó que el proceso operativo avanza
        con total normalidad.</p>
      </blockquote>
      <p>Además, recordó que la entidad cuenta con un Programa General de
      Auditorías.</p>
      <p>Síguenos para más noticias en Google News</p>
    </div>
    """

    detail = _extract_registraduria_news_article_detail(html)

    assert detail is not None
    assert detail["publication_date"] == "2026-05-19T00:00:00Z"
    assert (
        detail["content_extraction"]
        == "registraduria_news_article_html"
    )
    assert "proceso operativo" in detail["body_excerpt"]
    assert "Google News" not in detail["body_excerpt"]


def test_fetch_registraduria_noticias_uses_browser_on_403(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="registraduria_noticias",
        name="Registraduría — Noticias",
        type="official_updates",
        url="https://www.registraduria.gov.co/-2026-.html",
        fetch_method="html",
        max_items=6,
    )
    browser_item = RawItem(
        id="registraduria-browser-item",
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url="https://www.registraduria.gov.co/news.html",
        title="Registraduría news",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-19T00:00:00Z",
        raw_text="Official Registraduria news card.",
        metadata={"content_extraction": "registraduria_news_card"},
    )
    calls: list[tuple[str, int]] = []

    def fake_browser_fetch(source_arg, fetched_at, *, max_items):
        calls.append((source_arg.id, max_items))
        return [browser_item]

    monkeypatch.setattr(
        registraduria_fetchers,
        "_fetch_registraduria_noticias_with_browser",
        fake_browser_fetch,
    )

    transport = httpx.MockTransport(
        lambda request: httpx.Response(403, text="<title>Just a moment...</title>")
    )
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == [browser_item]
    assert calls == [("registraduria_noticias", 6)]
