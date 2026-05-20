from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def _assert_parsed_contract(item: RawItem, extraction: str) -> None:
    assert item.metadata["content_extraction"] == extraction
    assert "content_extraction_error" not in item.metadata
    assert item.raw_text


def _assert_fail_closed_contract(item: RawItem, error_substring: str) -> None:
    assert "content_extraction" not in item.metadata
    assert error_substring in item.metadata["content_extraction_error"]


class _ShortPdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(_minimal_text_pdf("Corto"))


def test_rawitem_parser_metadata_contract_marks_parsed_records(sample_source) -> None:
    pdf_item = RawItem(
        id="pdf-1",
        source_id="dane_comunicados_prensa",
        source_name="DANE",
        source_type="official_updates",
        url="https://www.dane.gov.co/files/prensa/comunicados/demo.pdf",
        title="DANE publica comunicado tecnico",
        fetched_at="2026-05-06T00:00:00Z",
        published_at="2026-05-05T00:00:00Z",
        raw_text="DANE publica comunicado tecnico 05/05/2026 PDF Descargar",
        metadata={"extraction": "dane_comunicados_table"},
    )
    parsed_pdf = _enrich_pdf_text([pdf_item], _FakePdfClient(), max_items=1)[0]
    _assert_parsed_contract(parsed_pdf, "pdf_text_best_effort")

    registraduria_source = replace(
        sample_source,
        id="registraduria_noticias",
        name="Registraduria — Noticias",
        url="https://www.registraduria.gov.co/-2026-.html",
        type="official_updates",
    )
    registraduria_items = _extract_registraduria_news_cards(
        """
        <li class="newsmodule">
          <span class="num-comunicado">No. 088</span>
          <a class="seemorenew" href="/Comunicado-088.html">Ver noticia</a>
          <h3 class="titlepreview">Registrador Nacional presenta calendario electoral</h3>
          <p class="datenew">Martes 19 de mayo de 2026</p>
          <p class="captionnew">La entidad entrego detalles operativos del proceso electoral.</p>
        </li>
        """,
        registraduria_source.url,
        registraduria_source,
        "2026-05-19T00:00:00Z",
        source_access="browser_official_html",
    )
    _assert_parsed_contract(registraduria_items[0], "registraduria_news_card")

    minhacienda_source = replace(
        sample_source,
        id="minhacienda_proyectos_decreto",
        name="MinHacienda — Proyectos de Decreto",
        type="regulatory",
        url="https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026",
        fetch_method="html",
        trust_role="regulatory_signal",
    )
    decree_items = _extract_minhacienda_decree_projects(
        """
        <main>
          <div class="project">
            <a href="/documents/20119/2873514/PD+garantias.pdf/abc?t=1">
              PD. Por el cual se modifica el Decreto 1068 de 2015.
            </a>
            <div>mayo 13, 2026</div>
            <p>El proyecto de decreto tiene por objeto modificar garantias.</p>
            <p>El Proyecto de Decreto esta para comentarios del 13 al 28 de mayo.</p>
            <a href="/web/forms/shared/-/form/3277529">Comentar proyecto</a>
          </div>
        </main>
        """,
        minhacienda_source.url,
        minhacienda_source,
        "2026-05-19T00:00:00Z",
    )
    _assert_parsed_contract(
        decree_items[0],
        "minhacienda_decree_project_browser",
    )


def test_rawitem_parser_metadata_contract_keeps_failures_link_level(
    sample_source,
) -> None:
    pdf_item = RawItem(
        id="pdf-short",
        source_id="dane_comunicados_prensa",
        source_name="DANE",
        source_type="official_updates",
        url="https://www.dane.gov.co/files/prensa/comunicados/short.pdf",
        title="DANE comunicado corto",
        fetched_at="2026-05-06T00:00:00Z",
        published_at="2026-05-05T00:00:00Z",
        raw_text="DANE comunicado corto PDF Descargar",
        metadata={"extraction": "dane_comunicados_table"},
    )
    failed_pdf = _enrich_pdf_text([pdf_item], _ShortPdfClient(), max_items=1)[0]
    _assert_fail_closed_contract(failed_pdf, "pdf text excerpt too short")

    minhacienda_source = replace(
        sample_source,
        id="minhacienda_proyectos_decreto",
        name="MinHacienda — Proyectos de Decreto",
        type="regulatory",
        url="https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026",
        fetch_method="html",
        trust_role="regulatory_signal",
    )
    decree_items = _extract_minhacienda_decree_projects(
        """
        <main>
          <div class="project">
            <a href="/documents/20119/2873514/PD+sin+formulario.pdf/def?t=2">
              PD. Proyecto sin formulario publicado.
            </a>
            <div>mayo 12, 2026</div>
            <p>El proyecto de decreto tiene por objeto ajustar una regla fiscal.</p>
          </div>
        </main>
        """,
        minhacienda_source.url,
        minhacienda_source,
        "2026-05-19T00:00:00Z",
    )
    _assert_fail_closed_contract(decree_items[0], "comment_form_url")
