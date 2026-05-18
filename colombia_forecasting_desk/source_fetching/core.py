from __future__ import annotations

from ..observability import RunTrace
from .common import *
from .dane import *
from .html import *
from .imprenta import *
from .mincit import *
from .minhacienda import *
from .pdf import *
from .registries import *
from .rss import *
from .senado import *
from .socrata import *


def fetch_html(source: Metasource, client: httpx.Client) -> list[RawItem]:
    fetched_at = _now_iso()
    if source.id == "minhacienda_tes_reports" and "irc.gov.co" in source.url:
        return _fetch_minhacienda_tes_reports_with_browser(
            source,
            fetched_at,
            max_items=source.max_items
            if source.max_items is not None
            else MINHACIENDA_TES_PARSE_LIMIT,
        )
    response = _http_get(client, source.url)
    marker = _detect_bot_block(response.text)
    if marker:
        if source.id == "minhacienda_tes_reports":
            browser_items = _fetch_minhacienda_tes_reports_with_browser(
                source,
                fetched_at,
                max_items=source.max_items
                if source.max_items is not None
                else MINHACIENDA_TES_PARSE_LIMIT,
            )
            if browser_items:
                return browser_items
        raise BotBlockError(f"bot block detected: {marker}")
    if _detect_spa_shell(response.text):
        raise DynamicShellError(
            "page is a JS app shell with no static content; "
            "needs a headless renderer or a different URL"
        )
    if source.id == "senado_leyes_registry":
        return _fetch_senado_leyes_registry(source, client, fetched_at)
    if source.id == "camara_proyectos_ley_registry":
        return _fetch_camara_proyectos_ley_registry(
            source,
            client,
            response.text,
            fetched_at,
        )
    if source.id == "dane_comunicados_prensa":
        items = _extract_dane_comunicados(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return _enrich_pdf_text(items, client)
    if source.id == "mincit_zonas_francas":
        items = _extract_dated_anchors(
            response.text,
            str(response.url),
            source,
            fetched_at,
            "anchor",
            require_date=False,
        )
        if items:
            return _enrich_mincit_zonas_francas(items, client)
    if source.id == "senado_agenda_legislativa":
        items = _extract_dated_anchors(
            response.text,
            str(response.url),
            source,
            fetched_at,
            "anchor",
            require_date=False,
        )
        if items:
            return _enrich_senado_agenda_pdfs(items, client)
    if source.id == "dane_icoced":
        items = _extract_dane_icoced(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return _enrich_dane_icoced_xlsx(
                items,
                client,
                max_items=source.max_items,
            )
    if source.id == "corte_constitucional_comunicados":
        items = _extract_corte_comunicados(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return items
    if source.id == "dian_proyectos_normas":
        items = _extract_dian_regulatory_project_links(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return items
    if source.id == "minhacienda_tes_reports":
        items = _extract_minhacienda_tes_reports(
            response.text, str(response.url), source, fetched_at
        )
        if items:
            return _enrich_minhacienda_tes_reports(
                items,
                client,
                max_items=source.max_items
                if source.max_items is not None
                else MINHACIENDA_TES_PARSE_LIMIT,
            )
    if source.id == "banrep_junta_comunicados":
        items = _extract_dated_anchors(
            response.text,
            str(response.url),
            source,
            fetched_at,
            "anchor",
            require_date=False,
        )
        if items:
            return _enrich_banrep_minutas_html(items, client)
    if source.id == "diario_oficial":
        items = _extract_imprenta_jsf_table(
            response.text,
            str(response.url),
            source,
            fetched_at,
            edition_label="Diario Oficial",
            query_param="edicion",
        )
        if items:
            return _enrich_diario_oficial_pdfs(
                items,
                client,
                response.text,
                str(response.url),
                max_items=source.max_items
                if source.max_items is not None
                else PDF_TEXT_PARSE_LIMIT,
            )
    if source.id == "gacetas_congreso":
        items = _extract_imprenta_jsf_table(
            response.text,
            str(response.url),
            source,
            fetched_at,
            edition_label="Gaceta del Congreso",
            query_param="gaceta",
        )
        if items:
            return _enrich_gaceta_pdfs(items, client, response.text, str(response.url))
    items = _extract_dated_anchors(
        response.text,
        str(response.url),
        source,
        fetched_at,
        "anchor",
        require_date=False,
    )
    if source.id in {"gestor_normativo_fp", "suin_juriscol", "suin_juriscol_normas"}:
        return _annotate_legal_identity_items(items)
    return items



def _cap_items(source: Metasource, items: list[RawItem]) -> list[RawItem]:
    if source.max_items is None or source.max_items < 0:
        return items
    if len(items) <= source.max_items:
        return items
    logger.info("Capping %s items: %d -> %d", source.id, len(items), source.max_items)
    return items[: source.max_items]


def _fetch_one(source: Metasource, client: httpx.Client) -> list[RawItem]:
    if source.fetch_method == "rss":
        return fetch_rss(source, client)
    if source.fetch_method == "html":
        return fetch_html(source, client)
    if source.fetch_method == "api":
        return fetch_api(source, client)
    raise ValueError(f"unsupported fetch_method: {source.fetch_method}")


def fetch_all(
    sources: list[Metasource],
    client: httpx.Client | None = None,
    trace: RunTrace | None = None,
) -> tuple[list[RawItem], list[SourceFailure]]:
    items: list[RawItem] = []
    failures: list[SourceFailure] = []

    owns_client = client is None
    if client is None:
        client = httpx.Client(
            timeout=HTTP_TIMEOUT,
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
        )

    try:
        for source in sources:
            source_client = client
            source_owns_client = False
            if source.verify_ssl is False:
                source_client = httpx.Client(
                    timeout=HTTP_TIMEOUT,
                    follow_redirects=True,
                    verify=False,
                    headers=DEFAULT_HEADERS,
                )
                source_owns_client = True
            try:
                if trace is None:
                    fetched = _cap_items(source, _fetch_one(source, source_client))
                else:
                    with trace.span(
                        "fetch_source",
                        category="source_fetch",
                        metadata={
                            "source_id": source.id,
                            "source_name": source.name,
                            "fetch_method": source.fetch_method,
                            "trust_role": source.trust_role,
                            "priority": source.priority,
                            "verify_ssl": source.verify_ssl,
                        },
                    ) as span:
                        fetched = _cap_items(source, _fetch_one(source, source_client))
                        span.set_counts(raw_items=len(fetched))
                items.extend(fetched)
                logger.info("Fetched %d items from %s", len(fetched), source.id)
            except Exception as exc:  # noqa: BLE001 — boundary catch by design
                failures.append(
                    SourceFailure(
                        source_id=source.id,
                        source_name=source.name,
                        url=source.url,
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        occurred_at=_now_iso(),
                    )
                )
                logger.warning(
                    "FAILED %s: %s: %s",
                    source.id,
                    exc.__class__.__name__,
                    exc,
                )
            finally:
                if source_owns_client:
                    source_client.close()
    finally:
        if owns_client:
            client.close()

    return items, failures


__all__ = [name for name in globals() if not name.startswith("__")]
