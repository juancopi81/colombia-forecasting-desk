from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


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
