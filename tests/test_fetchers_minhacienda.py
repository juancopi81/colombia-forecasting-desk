from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


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
