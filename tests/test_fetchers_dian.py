from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


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
