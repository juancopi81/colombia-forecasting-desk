from __future__ import annotations

import json

import pytest

from tests.fetcher_helpers import *  # noqa: F403


def _corte_source(sample_source, *, max_items: int = 15):
    return replace(
        sample_source,
        id="corte_constitucional_comunicados",
        name="Corte Constitucional — Comunicados",
        type="legal",
        url="https://www.corteconstitucional.gov.co/comunicados/",
        max_items=max_items,
    )


def _api_response(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "datos": json.dumps(rows),
        "error": False,
        "msn": "Consulta exitosa",
        "status": 200,
    }


def test_fetch_corte_comunicados_api_parses_rows_and_enriches_newest_pdf(
    sample_source,
) -> None:
    source = _corte_source(sample_source, max_items=2)
    api_rows = [
        {
            "Id": 38191,
            "Titulo": "Comunicado 26 - Agosto 13 de 2026",
            "RutaArchivo": (
                "https://www.corteconstitucional.gov.co/comunicados/"
                "comunicado-26-agosto-13-de-2026.pdf"
            ),
            "Fecha": "2026-08-14T21:06:17",
            "FechaIniPub": "2026-08-14T21:04:34.203",
            "Estado": True,
        },
        {
            "Id": 38093,
            "Titulo": "Comunicado 24 - Julio 23 de 2026",
            "RutaArchivo": (
                "https://www.corteconstitucional.gov.co/comunicados/"
                "comunicado-24-julio-23-de-2026.pdf"
            ),
            "Fecha": "2026-07-24T14:44:01",
            "FechaIniPub": "2026-07-24T14:43:27.033",
            "Estado": True,
        },
    ]
    pdf = _minimal_text_pdf(
        "COMUNICADO 26. Sentencia C-259 de 2026. La Corte Constitucional "
        "declaro que las medidas adoptadas por el Gobierno son exequibles, "
        "condicionadas e inexequibles y ordeno ajustar su implementacion."
    )
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.path.endswith("/Eventos/ObtenerComunicado"):
            assert request.method == "POST"
            assert json.loads(request.content) == {
                "pFecha": "2025-07-22",
                "pFechaFin": "2026-08-26",
            }
            return httpx.Response(200, json=_api_response(api_rows))
        if request.url.path.endswith("comunicado-26-agosto-13-de-2026.pdf"):
            return httpx.Response(200, content=pdf)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = _fetch_corte_comunicados_api(
            source,
            client,
            "2026-08-26T15:00:00Z",
            pdf_parse_limit=1,
        )

    assert [item.title for item in items] == [
        "Comunicado 26 - Agosto 13 de 2026",
        "Comunicado 24 - Julio 23 de 2026",
    ]
    assert items[0].published_at == "2026-08-15T02:04:34Z"
    assert items[0].metadata["extraction"] == "corte_comunicados_api"
    assert items[0].metadata["source_record_id"] == 38191
    assert items[0].metadata["content_extraction"] == "corte_comunicado_pdf"
    assert "Sentencia C-259 de 2026" in items[0].raw_text
    assert "content_extraction" not in items[1].metadata
    assert seen == [
        ("POST", "/webapi/api/Eventos/ObtenerComunicado"),
        ("GET", "/comunicados/comunicado-26-agosto-13-de-2026.pdf"),
    ]


def test_fetch_corte_comunicados_api_discards_inactive_and_year_mismatch_rows(
    sample_source,
) -> None:
    source = _corte_source(sample_source)
    api_rows = [
        {
            "Id": 38190,
            "Titulo": "Comunicado 26 - Agosto 13 de 2026",
            "RutaArchivo": (
                "https://www.corteconstitucional.gov.co/comunicados/"
                "comunicado-26-agosto-13-de-2025.pdf"
            ),
            "FechaIniPub": "2026-08-14T21:04:10.84",
            "Estado": True,
        },
        {
            "Id": 38189,
            "Titulo": "Comunicado 25 - Agosto 12 de 2026",
            "RutaArchivo": (
                "https://www.corteconstitucional.gov.co/comunicados/"
                "comunicado-25-agosto-12-de-2026.pdf"
            ),
            "FechaIniPub": "2026-08-14T17:35:18.873",
            "Estado": False,
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_api_response(api_rows))

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = _fetch_corte_comunicados_api(
            source,
            client,
            "2026-08-26T15:00:00Z",
            pdf_parse_limit=0,
        )

    assert items == []


def test_fetch_corte_comunicados_api_rejects_unexpected_nested_payload(
    sample_source,
) -> None:
    source = _corte_source(sample_source)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"datos": "not-json", "error": False, "status": 200},
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        with pytest.raises(ValueError, match="unexpected Corte communications API"):
            _fetch_corte_comunicados_api(
                source,
                client,
                "2026-08-26T15:00:00Z",
            )


def test_fetch_html_uses_corte_api_instead_of_spa_landing_page(sample_source) -> None:
    source = _corte_source(sample_source, max_items=1)
    api_rows = [
        {
            "Id": 38191,
            "Titulo": "Comunicado 26 - Agosto 13 de 2026",
            "RutaArchivo": (
                "https://www.corteconstitucional.gov.co/comunicados/"
                "comunicado-26-agosto-13-de-2026.pdf"
            ),
            "FechaIniPub": "2026-08-14T21:04:34.203",
            "Estado": True,
        }
    ]
    pdf = _minimal_text_pdf(
        "COMUNICADO 26. La Corte Constitucional adopto una decision oficial "
        "con suficiente contenido para el analista y la revision publica."
    )
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path.endswith("/Eventos/ObtenerComunicado"):
            return httpx.Response(200, json=_api_response(api_rows))
        if request.url.path.endswith(".pdf"):
            return httpx.Response(200, content=pdf)
        raise AssertionError(f"unexpected request: {request.url}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    assert seen_paths[0] == "/webapi/api/Eventos/ObtenerComunicado"
    assert "/comunicados/" not in seen_paths
