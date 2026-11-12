"""Current IPC bulletin fallback, independent of archived forecast resolution."""
from __future__ import annotations

import hashlib
import textwrap

import httpx
import pytest

from colombia_forecasting_desk.indicator_watch import (
    IPC_URL,
    _fetch_dane_html_observation,
    ipc_observation_from_html,
)


BULLETIN_URL = "https://www.dane.gov.co/files/operaciones/IPC/oct2026/bol-IPC-oct2026.pdf"
LANDING = f"""
<h1>Información octubre 2026</h1>
<table><tr><td>Boletín técnico</td><td>09/11/2026</td><td>PDF</td>
<td>283 KB</td><td><a href="{BULLETIN_URL}">Descargar</a></td></tr></table>
<h2>Información histórica</h2>
<a href="/files/operaciones/IPC/sep2026/bol-IPC-sep2026.pdf">Septiembre</a>
"""
# Relevant narrative excerpts from DANE's October 2026 bulletin,
# SHA256 5c89ffaa17bee2d3ed6782898fa7eabb8e5eabbc680dd5f3abc5f256e47f4abd.
# Empty intervening pages preserve original evidence page numbers (2, 4, 6).
TITLE = "Boletín técnico\nÍndice de Precios al Consumidor (IPC)\nOctubre 2026\n"
MONTHLY = (
    "En el mes de octubre de 2026, el IPC registró una variación de 0,20% "
    "en comparación con septiembre de 2026, cinco divisiones se ubicaron "
    "por encima del promedio nacional."
)
YTD = (
    "En lo corrido del año, (enero - octubre), cinco divisiones de bienes y "
    "servicios se ubicaron por encima del promedio nacional (5,94%): "
    "Alimentos y bebidas no alcohólicas (8,20%)."
)
ANNUAL = (
    "En el mes de octubre de 2026, el IPC registró una variación de 6,30% "
    "en comparación con octubre de 2025."
)
PAGES = [
    "Bogotá\n9 de noviembre de 2026\n" + TITLE + "Gráfico 1. IPC Variaciones\nTotal Nacional\nOctubre 2025 - 2026",
    TITLE + MONTHLY,
    TITLE,
    TITLE + YTD,
    TITLE,
    TITLE + ANNUAL,
]


def _pdf(pages: list[str]) -> bytes:
    """Small genuine text PDFs keep extraction tests portable without new deps."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        (f"<< /Type /Pages /Count {len(pages)} /Kids ["
         + " ".join(f"{4 + 2 * i} 0 R" for i in range(len(pages))) + "] >>").encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
    ]
    for index, page in enumerate(pages):
        lines = [line for paragraph in page.splitlines() for line in textwrap.wrap(paragraph, width=90)]
        stream = ("BT /F1 10 Tf 40 800 Td 13 TL\n"
                  + "\n".join(f"<{line.encode('cp1252').hex()}> Tj T*" for line in lines)
                  + "\nET").encode()
        objects.extend([
            ("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
             "/Resources << /Font << /F1 3 0 R >> >> "
             f"/Contents {5 + 2 * index} 0 R >>").encode(),
            f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream",
        ])
    result = b"%PDF-1.4\n"
    offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(result))
        result += f"{index} 0 obj\n".encode() + obj + b"\nendobj\n"
    startxref = len(result)
    result += f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode()
    result += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return result + (f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\n"
                     f"startxref\n{startxref}\n%%EOF").encode()


def _collect(*, landing=LANDING, content=None, bulletin_handler=None, follow_redirects=False):
    requests = []
    content = _pdf(PAGES) if content is None else content

    def handler(request):
        requests.append(str(request.url))
        if str(request.url) == IPC_URL:
            return httpx.Response(200, text=landing)
        if bulletin_handler:
            return bulletin_handler(request)
        return httpx.Response(200, content=content)

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=follow_redirects) as client:
        observation = _fetch_dane_html_observation(client, "ipc_inflation", IPC_URL, ipc_observation_from_html)
    return observation, requests


def test_link_only_current_page_fetches_pdf_headline_and_preserves_provenance():
    content = _pdf(PAGES)
    observation, requests = _collect(content=content)
    assert requests == [IPC_URL, BULLETIN_URL]
    assert observation.status == "observed"
    assert observation.period == "2026-10"
    assert observation.release_date == "2026-11-09T00:00:00Z"
    assert observation.source_url == BULLETIN_URL
    assert observation.values["monthly_variation_pct"] == 0.20
    assert observation.values["year_to_date_variation_pct"] == 5.94
    assert observation.values["annual_variation_pct"] == 6.30
    assert observation.values["source_sha256"] == hashlib.sha256(content).hexdigest()
    assert observation.values["landing_page_url"] == IPC_URL
    assert observation.values["linked_bulletin_url"] == BULLETIN_URL
    assert observation.values["series_variant"] == "national_headline"
    assert "first_release_verified" not in observation.values
    assert {key: matches[0]["page"] for key, matches in observation.values["metric_provenance"].items()} == {
        "monthly_variation_pct": 2, "year_to_date_variation_pct": 4, "annual_variation_pct": 6,
    }


def test_valid_html_does_not_fetch_bulletin_or_change_existing_observation():
    landing = LANDING + """<p>En octubre de 2026 la variación mensual del IPC fue
        0,20%, la variación año corrido fue 5,94% y la anual 6,30%.</p>"""
    observation, requests = _collect(landing=landing)
    assert requests == [IPC_URL]
    assert observation == ipc_observation_from_html(landing)


def test_reference_period_values_and_previous_month_are_not_hardcoded():
    landing = LANDING.replace("octubre 2026", "enero 2027").replace("oct2026", "ene2027").replace("09/11/2026", "09/02/2027")
    pages = [
        page.replace("Octubre 2026", "Enero 2027")
        .replace("octubre de 2026", "enero de 2027")
        .replace("septiembre de 2026", "diciembre de 2026")
        .replace("octubre de 2025", "enero de 2026")
        .replace("enero - octubre", "enero - enero")
        .replace("9 de noviembre de 2026", "9 de febrero de 2027")
        .replace("0,20%", "-0,10%").replace("5,94%", "-0,10%").replace("6,30%", "4,20%")
        for page in PAGES
    ]
    observation, requests = _collect(landing=landing, content=_pdf(pages))
    assert requests == [IPC_URL, BULLETIN_URL.replace("oct2026", "ene2027")]
    assert observation.status == "observed"
    assert observation.period == "2027-01"
    assert observation.release_date == "2027-02-09T00:00:00Z"
    assert observation.values["monthly_variation_pct"] == -0.10
    assert observation.values["year_to_date_variation_pct"] == -0.10
    assert observation.values["annual_variation_pct"] == 4.20


@pytest.mark.parametrize("landing", [
    LANDING.replace("Información octubre 2026", "Información"),
    LANDING.replace(BULLETIN_URL, BULLETIN_URL.replace("oct2026", "sep2026")),
    LANDING.replace(BULLETIN_URL, "https://example.com/bol-IPC-oct2026.pdf"),
    LANDING.replace("https://www.dane.gov.co/", "https://www.dane.gov.co.evil.example/"),
    LANDING.replace("bol-IPC-oct2026.pdf", "bol-IPC-oct2026-core.pdf"),
    LANDING.replace("09/11/2026", "31/11/2026"),
    LANDING.replace("09/11/2026", "09/11/2026 10/11/2026"),
    LANDING.replace("<table>", "<h2>Información histórica</h2><table>"),
    LANDING + LANDING.replace("octubre 2026", "septiembre 2026"),
    LANDING.replace("</table>", "</table>" + LANDING.split("<h2>")[0].replace("<h1>Información octubre 2026</h1>", "").replace("/IPC/oct2026/", "/IPC/")),
])
def test_missing_ambiguous_nonofficial_or_stale_links_fail_without_document_fetch(landing):
    observation, requests = _collect(landing=landing)
    assert observation.status == "failed"
    assert observation.values == {}
    assert requests == [IPC_URL]


@pytest.mark.parametrize("mutate", [
    lambda pages: [page.replace("Octubre 2026", "Septiembre 2026") for page in pages],
    lambda pages: [page.replace("9 de noviembre", "10 de noviembre") for page in pages],
    lambda pages: [page.replace("Total Nacional", "Bogotá") for page in pages],
    lambda pages: [page.replace("Consumidor (IPC)", "Consumidor (IPC) sin alimentos") for page in pages],
    lambda pages: [page.replace("cinco divisiones de bienes", "el IPC sin alimentos, cinco divisiones de bienes") for page in pages],
    lambda pages: [page + ("\n" + ANNUAL.replace("6,30%", "6,31%") if i == 5 else "") for i, page in enumerate(pages)],
    lambda pages: [page + ("\n" + MONTHLY.replace("0,20%", "0,21%") if i == 1 else "") for i, page in enumerate(pages)],
    lambda pages: [page + ("\n" + YTD.replace("5,94%", "5,95%") if i == 3 else "") for i, page in enumerate(pages)],
    lambda pages: [page.replace(YTD, "") for page in pages],
    lambda pages: [page.replace("comparación con septiembre de 2026", "comparación con agosto de 2026") for page in pages],
])
def test_wrong_period_series_publication_missing_or_conflicting_values_fail_closed(mutate):
    observation, requests = _collect(content=_pdf(mutate(PAGES)))
    assert requests == [IPC_URL, BULLETIN_URL]
    assert observation.status == "failed"
    assert observation.values == {}


@pytest.mark.parametrize("content", [b"<html>Service unavailable</html>", b"%PDF-broken", _pdf([""])])
def test_unreadable_pdf_fails_gracefully(content):
    observation, _ = _collect(content=content)
    assert observation.status == "failed"
    assert observation.values == {}


@pytest.mark.parametrize("destination", [
    "https://example.com/bol-IPC-oct2026.pdf",
    BULLETIN_URL.replace("oct2026", "sep2026"),
    BULLETIN_URL.replace("https://", "http://"),
    BULLETIN_URL.replace("bol-IPC-oct2026.pdf", "anex-IPC-oct2026.pdf"),
])
def test_redirect_targets_are_validated_before_fetch_even_with_redirecting_client(destination):
    observation, requests = _collect(
        bulletin_handler=lambda request: httpx.Response(302, headers={"location": destination}),
        follow_redirects=True,
    )
    assert observation.status == "failed"
    assert observation.values == {}
    assert requests == [IPC_URL, BULLETIN_URL]


def test_valid_official_redirect_preserves_final_source_url_and_hash():
    final_url = BULLETIN_URL.replace("www.dane.gov.co", "dane.gov.co")
    content = _pdf(PAGES)

    def handler(request):
        if str(request.url) == BULLETIN_URL:
            return httpx.Response(302, headers={"location": final_url})
        return httpx.Response(200, content=content)

    observation, requests = _collect(bulletin_handler=handler)
    assert requests == [IPC_URL, BULLETIN_URL, final_url]
    assert observation.status == "observed"
    assert observation.source_url == final_url
    assert observation.values["linked_bulletin_url"] == BULLETIN_URL
    assert observation.values["source_sha256"] == hashlib.sha256(content).hexdigest()


@pytest.mark.parametrize("failure", ["timeout", "status"])
def test_bulletin_network_failures_remain_failed_observations(failure):
    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("fixture timeout", request=request)
        return httpx.Response(503)

    observation, _ = _collect(bulletin_handler=handler)
    assert observation.status == "failed"
    assert observation.values == {}


def test_download_and_redirect_limits_fail_closed():
    for handler in (
        lambda request: httpx.Response(200, content=b"%PDF" + b" " * 25_000_000),
        lambda request: httpx.Response(302, headers={"location": BULLETIN_URL}),
    ):
        observation, requests = _collect(bulletin_handler=handler)
        assert observation.status == "failed"
        assert observation.values == {}
        assert len(requests) <= 5
