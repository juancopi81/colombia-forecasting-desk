from __future__ import annotations

import json

from ..court_rulings import (
    COURT_COMMUNICATION_KIND,
    COURT_DEADLINE_PENDING_STATUS,
    court_text_signals,
)
from .common import *
from .pdf import _extract_pdf_text_with_pdfplumber, _looks_like_pdf_excerpt

CORTE_COMUNICADOS_API_URL = (
    "https://www.corteconstitucional.gov.co/"
    "webapi/api/Eventos/ObtenerComunicado"
)
CORTE_COMUNICADOS_LOOKBACK_DAYS = 400
CORTE_COMUNICADOS_DEFAULT_LIMIT = 15
CORTE_COMUNICADOS_PDF_PARSE_LIMIT = 3
CORTE_COMUNICADOS_TEXT_MAX_CHARS = 12_000
CORTE_TIMEZONE = timezone(timedelta(hours=-5))


def _corte_api_datetime(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return _parse_date_text_to_iso(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CORTE_TIMEZONE)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _corte_query_window(fetched_at: str) -> tuple[str, str]:
    try:
        end = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    except ValueError:
        end = datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    end = end.astimezone(timezone.utc)
    start = end - timedelta(days=CORTE_COMUNICADOS_LOOKBACK_DAYS)
    return start.date().isoformat(), end.date().isoformat()


def _corte_pdf_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    resolved = urljoin("https://www.corteconstitucional.gov.co/", value.strip())
    parts = urlsplit(resolved)
    if (
        parts.scheme != "https"
        or parts.netloc.lower() != "www.corteconstitucional.gov.co"
    ):
        return None
    if not parts.path.lower().startswith("/comunicados/"):
        return None
    if ".pdf" not in parts.path.lower():
        return None
    return resolved


def _corte_title_url_years_match(title: str, url: str) -> bool:
    title_years = re.findall(r"\b20\d{2}\b", title)
    url_years = re.findall(r"\b20\d{2}\b", urlsplit(url).path)
    if not title_years or not url_years:
        return True
    return title_years[-1] == url_years[-1]


def _corte_api_rows(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
) -> list[Mapping[str, Any]]:
    start_date, end_date = _corte_query_window(fetched_at)
    response: httpx.Response | None = None
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.post(
                CORTE_COMUNICADOS_API_URL,
                json={"pFecha": start_date, "pFechaFin": end_date},
                headers={"Accept": "application/json"},
            )
        except httpx.TransportError as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS)
                continue
            raise
        if response.status_code >= 500 and attempt < MAX_RETRIES:
            time.sleep(BACKOFF_SECONDS)
            continue
        break
    if response is None:
        if last_exc:
            raise last_exc
        raise RuntimeError(f"{source.id} API request did not return a response")
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, Mapping) or payload.get("error") is True:
        raise ValueError("unexpected Corte communications API response")
    nested = payload.get("datos")
    try:
        rows = json.loads(nested) if isinstance(nested, str) else nested
    except json.JSONDecodeError as exc:
        raise ValueError("unexpected Corte communications API payload") from exc
    if not isinstance(rows, list):
        raise ValueError("unexpected Corte communications API payload")
    return [row for row in rows if isinstance(row, Mapping)]


def _corte_link_item(
    row: Mapping[str, Any],
    source: Metasource,
    fetched_at: str,
) -> RawItem | None:
    if row.get("Estado") is False:
        return None
    title = normalize_whitespace(str(row.get("Titulo") or ""))
    url = _corte_pdf_url(row.get("RutaArchivo"))
    if not title or url is None or not _corte_title_url_years_match(title, url):
        return None

    published_at = _corte_api_datetime(row.get("FechaIniPub"))
    document_date = _corte_api_datetime(row.get("Fecha"))
    metadata = {
        "extraction": "corte_comunicados_api",
        "api_endpoint": CORTE_COMUNICADOS_API_URL,
        "source_record_id": row.get("Id"),
        "document_date": document_date,
        "source_published_at": row.get("FechaIniPub"),
        "parser_status": "document_link",
        "court_document_kind": COURT_COMMUNICATION_KIND,
        "written_ruling_available": False,
        "deadline_status": COURT_DEADLINE_PENDING_STATUS,
    }
    return RawItem(
        id=_make_id(source.id, url, title),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=url,
        title=title,
        fetched_at=fetched_at,
        published_at=published_at or document_date,
        raw_text=(
            f"{title}. Comunicado oficial de la Corte Constitucional. "
            "Documento publicado: "
            f"{published_at or document_date or 'fecha no disponible'}."
        ),
        metadata=metadata,
    )


def _corte_item_with_metadata(item: RawItem, metadata: dict[str, Any]) -> RawItem:
    return RawItem(
        id=item.id,
        source_id=item.source_id,
        source_name=item.source_name,
        source_type=item.source_type,
        url=item.url,
        title=item.title,
        fetched_at=item.fetched_at,
        published_at=item.published_at,
        raw_text=item.raw_text,
        metadata=metadata,
    )


def _enrich_corte_comunicado_pdfs(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int = CORTE_COMUNICADOS_PDF_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        if parsed_count >= max_items:
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            text = normalize_whitespace(
                _extract_pdf_text_with_pdfplumber(
                    response.content,
                    max_chars=CORTE_COMUNICADOS_TEXT_MAX_CHARS,
                )
            )
        except Exception as exc:  # noqa: BLE001 - preserve the official link
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(_corte_item_with_metadata(item, metadata))
            parsed_count += 1
            continue
        if not _looks_like_pdf_excerpt(text):
            metadata["content_extraction_error"] = "no readable Court PDF text found"
            enriched.append(_corte_item_with_metadata(item, metadata))
            parsed_count += 1
            continue
        metadata.update(
            {
                "content_extraction": "corte_comunicado_pdf",
                "parser_status": "parsed_content",
                "pdf_text_chars": len(text),
                **court_text_signals(text),
            }
        )
        enriched.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=f"{item.raw_text} PDF text: {text}",
                metadata=metadata,
            )
        )
        parsed_count += 1
    return enriched


def _fetch_corte_comunicados_api(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
    *,
    pdf_parse_limit: int = CORTE_COMUNICADOS_PDF_PARSE_LIMIT,
) -> list[RawItem]:
    rows = _corte_api_rows(source, client, fetched_at)
    items = [
        item
        for row in rows
        if (item := _corte_link_item(row, source, fetched_at)) is not None
    ]
    items = _dedupe_raw_items_by_url(_sort_raw_items_newest_first(items))
    limit = source.max_items or CORTE_COMUNICADOS_DEFAULT_LIMIT
    return _enrich_corte_comunicado_pdfs(
        items[:limit],
        client,
        max_items=min(pdf_parse_limit, limit),
    )


__all__ = [name for name in globals() if not name.startswith("__")]
