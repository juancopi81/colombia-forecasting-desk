"""Fixture-based parser tests.

Each captured snapshot in `tests/fixtures/<source_id>/<date>.html` is exercised
through the existing fetcher extractors. Tests pin current behaviour so future
parser changes either stay equivalent or update the assertions deliberately.

Several priority sources currently extract nothing useful from their landing
URL (BanRep landing pages, Cámara agenda hub, Corte search form, Registraduría
behind Cloudflare). The tests document those known-unrankable cases so they
surface in CI and in the source-health table.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from colombia_forecasting_desk.fetchers import (
    _detect_bot_block,
    _detect_spa_shell,
    _extract_anchors,
    _extract_corte_comunicados,
    _extract_dated_anchors,
    _extract_imprenta_jsf_table,
)

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def _load(source_id: str, date: str = "2026-04-29") -> str:
    return (FIXTURE_DIR / source_id / f"{date}.html").read_text(encoding="utf-8")


def test_banrep_rss_landing_yields_dated_items_with_browser_headers(
    sample_source,
) -> None:
    """The fixture was captured using `DEFAULT_HEADERS` (browser-like UA +
    Spanish Accept-Language). With those headers BanRep serves the real
    landing page; without them it serves a 1.3KB Radware bot-block page.
    The parser already finds dated minutas/board-decision links on the real
    page, so the M1.1 'raw>0 but rankable=0' result was a bot-block silent
    failure, not a parser bug.
    """
    source = replace(sample_source, id="banrep_rss")
    html = _load("banrep_rss")
    items = _extract_dated_anchors(
        html,
        "https://www.banrep.gov.co/es/noticias-rss",
        source,
        "2026-04-29T00:00:00Z",
        "anchor",
    )
    assert len(items) >= 3, (
        "expected at least 3 dated BanRep items from the live HTML; "
        "if this fails the bot manager probably served a block page when "
        "the fixture was refreshed."
    )
    # The /noticias-rss URL surfaces Blog BanRep posts plus board/news links;
    # at least some BanRep-domain links should be present.
    assert all("banrep.gov.co" in it.url for it in items)


def test_banrep_junta_comunicados_yields_dated_items(sample_source) -> None:
    """`/es/comunicados-junta` likewise produces high-value dated items when
    fetched with browser headers. Locks in the minimum count.
    """
    source = replace(sample_source, id="banrep_junta_comunicados")
    html = _load("banrep_junta_comunicados")
    items = _extract_dated_anchors(
        html,
        "https://www.banrep.gov.co/es/comunicados-junta",
        source,
        "2026-04-29T00:00:00Z",
        "anchor",
    )
    assert len(items) >= 3
    assert all(it.published_at for it in items)
    titles = " ".join(it.title.lower() for it in items)
    assert "junta directiva" in titles or "minutas" in titles


def test_corte_comunicados_fragment_is_search_only(sample_source) -> None:
    """`/comunicados/` returns an empty search form with no embedded list.
    `_extract_corte_comunicados` correctly returns []. Test pins this so we
    notice if the page starts shipping data inline.
    """
    source = replace(
        sample_source, id="corte_constitucional_comunicados", type="legal"
    )
    html = _load("corte_constitucional_comunicados")
    assert "COMUNICADOS DE PRENSA" in html
    items = _extract_corte_comunicados(
        html,
        "https://www.corteconstitucional.gov.co/comunicados/",
        source,
        "2026-04-29T00:00:00Z",
    )
    assert items == []


def test_camara_agenda_landing_is_navigation_only(sample_source) -> None:
    """`/agenda-consolidada/` is a navigation hub linking to Excel/PDF agendas
    on sub-pages, not an inline agenda. The generic anchor extractor pulls nav
    items; the dated-anchor extractor finds no dates.
    """
    source = replace(sample_source, id="camara_agenda_consolidada", type="calendar")
    html = _load("camara_agenda_consolidada")
    anchors = _extract_anchors(
        html, "https://www.camara.gov.co/agenda-consolidada/"
    )
    assert anchors, "navigation anchors should still be present"
    dated = _extract_dated_anchors(
        html,
        "https://www.camara.gov.co/agenda-consolidada/",
        source,
        "2026-04-29T00:00:00Z",
        "anchor",
    )
    assert dated == [], (
        "Cámara agenda hub does not expose dates inline; refresh fixture and "
        "this assertion if the site adds a dated agenda widget."
    )


def test_registraduria_fixture_is_cloudflare_challenge() -> None:
    """The Registraduría URL returns a Cloudflare interactive challenge with
    `Just a moment...` rather than HTML content. Captured here so the
    bot-block detector tests have a real-world sample to assert against.
    """
    html = _load("registraduria_noticias")
    assert "Just a moment" in html
    # The challenge page is not currently caught by our BOT_BLOCK_MARKERS
    # because Cloudflare uses generic copy. Acceptable: the live fetcher gets
    # a 403 status code which surfaces as an explicit failure already.
    assert _detect_bot_block(html) is None


def test_diario_oficial_jsf_table_yields_dated_editions(sample_source) -> None:
    """`svrpubindc.imprenta.gov.co/diario/` renders the latest editions in a
    PrimeFaces datatable. Each data row is `Número | Tipo | DD/MM/YYYY |
    download-button`. The download button is a JSF postback, so we synthesize
    `?edicion=NNNNN` URLs so dedupe stays per-edition.
    """
    source = replace(
        sample_source,
        id="diario_oficial",
        type="legal",
        url="https://svrpubindc.imprenta.gov.co/diario/",
    )
    html = _load("diario_oficial")
    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/diario/",
        source,
        "2026-04-29T00:00:00Z",
        edition_label="Diario Oficial",
        query_param="edicion",
    )
    assert len(items) >= 5
    assert all(it.published_at for it in items)
    assert all(it.title.startswith("Diario Oficial ") for it in items)
    assert all("?edicion=" in it.url for it in items)
    # Different editions should produce different URLs
    assert len({it.url for it in items}) == len(items)


def test_gacetas_congreso_jsf_table_yields_dated_gacetas(sample_source) -> None:
    """`svrpubindc.imprenta.gov.co/gacetas/index.xhtml` shares the Imprenta
    Nacional datatable layout but with five columns (Número | Entidad | Fecha
    | Documento | Acciones). Same parser handles both.
    """
    source = replace(
        sample_source,
        id="gacetas_congreso",
        type="legal",
        url="https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
    )
    html = _load("gacetas_congreso")
    items = _extract_imprenta_jsf_table(
        html,
        "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml",
        source,
        "2026-04-29T00:00:00Z",
        edition_label="Gaceta del Congreso",
        query_param="gaceta",
    )
    assert len(items) >= 5
    assert all(it.published_at for it in items)
    assert all(it.title.startswith("Gaceta del Congreso ") for it in items)
    titles = " ".join(it.title for it in items)
    assert "Senado" in titles or "Cámara" in titles
    assert all("?gaceta=" in it.url for it in items)


def test_detect_bot_block_flags_radware_marker() -> None:
    snippet = (
        "<html><head><title>Radware Bot Manager Block</title></head>"
        "<body>blocked</body></html>"
    )
    assert _detect_bot_block(snippet) is not None


def test_detect_spa_shell_flags_small_app_root() -> None:
    shell = "<html><body><app-root></app-root></body></html>"
    assert _detect_spa_shell(shell) is True


def test_detect_spa_shell_does_not_flag_large_pages_with_app_root() -> None:
    big = "<html><body>" + "x" * 25_000 + "<app-root></app-root></body></html>"
    assert _detect_spa_shell(big) is False


@pytest.mark.parametrize("source_id", [
    "banrep_rss",
    "banrep_junta_comunicados",
    "camara_agenda_consolidada",
])
def test_priority_fixtures_exist(source_id: str) -> None:
    path = FIXTURE_DIR / source_id / "2026-04-29.html"
    assert path.exists(), f"missing fixture for {source_id}"
    assert path.stat().st_size > 1000, f"fixture for {source_id} is suspiciously small"
