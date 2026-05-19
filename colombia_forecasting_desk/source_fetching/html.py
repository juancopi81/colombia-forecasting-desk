from __future__ import annotations

from urllib.parse import quote, urlunsplit

from .common import *

def _extract_anchors(html: str, base_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("main") or soup.find("article") or soup.body or soup
    base_host = urlsplit(base_url).netloc.lower()
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for a in container.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        resolved = urljoin(base_url, href)
        parts = urlsplit(resolved)
        if parts.scheme not in ("http", "https"):
            continue
        if parts.netloc and parts.netloc.lower() != base_host:
            continue
        text = normalize_whitespace(a.get_text(separator=" ", strip=True))
        if len(text) < MIN_ANCHOR_TEXT:
            continue
        if text.lower() in NAV_TEXT:
            continue
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        out.append((text, resolved))
        if len(out) >= ANCHORS_PER_SOURCE:
            break
    return out


def _same_site_url(url: str, base_url: str) -> str | None:
    resolved = urljoin(base_url, url.strip())
    parts = urlsplit(resolved)
    if parts.scheme not in ("http", "https"):
        return None
    base_host = urlsplit(base_url).netloc.lower()
    if parts.netloc and parts.netloc.lower() != base_host:
        return None
    return resolved


def _extract_dated_anchors(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
    extraction: str,
    require_date: bool = True,
) -> list[RawItem]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("main") or soup.find("article") or soup.body or soup
    seen: set[str] = set()
    items: list[RawItem] = []
    for a in container.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        resolved = _same_site_url(href, base_url)
        if resolved is None:
            continue
        title = normalize_whitespace(a.get_text(separator=" ", strip=True))
        if len(title) < MIN_ANCHOR_TEXT or title.lower() in NAV_TEXT:
            continue
        parent_text = normalize_whitespace(
            (a.find_parent(["tr", "li", "article", "div", "section"]) or a)
            .get_text(separator=" ", strip=True)
        )
        published_at = _parse_date_text_to_iso(parent_text[:DATE_CONTEXT_CHARS])
        if require_date and not published_at:
            continue
        canon = canonicalize_url(resolved)
        if canon in seen:
            continue
        seen.add(canon)
        items.append(
            RawItem(
                id=_make_id(source.id, resolved, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=resolved,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=parent_text,
                metadata={"extraction": extraction},
            )
        )
        if len(items) >= ANCHORS_PER_SOURCE:
            break
    return items



def _extract_corte_comunicados(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    items = _extract_dated_anchors(
        html, base_url, source, fetched_at, "corte_comunicados_dated_anchor"
    )
    if items:
        return [
            it
            for it in items
            if "comunicado" in fold_accents((it.title + " " + it.url).lower())
        ] or items
    return []


_DIAN_REGULATORY_LINK_MARKERS = (
    "agenda-reglamentaria",
    "proyectosnormas",
)
DIAN_PROJECTS_API_URL = (
    "https://www.dian.gov.co/normatividad/_api/web/lists/"
    "getbytitle('Proyectos de normas')/items"
)
DIAN_PROJECTS_API_FIELDS = (
    "Title",
    "FileLeafRef",
    "FileRef",
    "Created",
    "Modified",
    "NumeroNorma",
    "FechaDeEmision",
    "Descripcion",
    "TipoDeNorma",
    "Fecha_x0020_inicio",
    "Fecha_x0020_final",
    "Buz_x00f3_n",
    "Observaciones",
    "Anexos",
)
DIAN_PROJECTS_API_TOP = "30"


def _quote_http_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            quote(parts.path, safe="/%"),
            quote(parts.query, safe="=&?/%:'(),$"),
            quote(parts.fragment, safe=""),
        )
    )


def _dian_absolute_url(value: str | None) -> str | None:
    if not value:
        return None
    return _quote_http_url(urljoin("https://www.dian.gov.co", value))


def _dian_url_field(value: Any) -> str | None:
    if isinstance(value, Mapping):
        url = value.get("Url") or value.get("url")
        if isinstance(url, str):
            return _quote_http_url(url)
    if isinstance(value, str) and value.strip():
        return _quote_http_url(value.strip())
    return None


def _dian_api_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return _parse_date_text_to_iso(value)


def _dian_project_raw_text(
    *,
    title: str,
    description: str,
    norm_type: str | None,
    issue_date: str | None,
    modified_at: str | None,
    comment_start: str | None,
    comment_end: str | None,
    mailbox: str | None,
    observations_url: str | None,
    annex_url: str | None,
) -> str:
    parts = [title]
    if norm_type:
        parts.append(f"Tipo de norma: {norm_type}.")
    if issue_date:
        parts.append(f"Fecha de emision: {issue_date}.")
    if modified_at:
        parts.append(f"Ultima actualizacion DIAN: {modified_at}.")
    if description:
        parts.append(description)
    if comment_start or comment_end:
        parts.append(
            "Ventana de comentarios: "
            f"{comment_start or 'sin fecha de inicio'} a "
            f"{comment_end or 'sin fecha final'}."
        )
    if mailbox:
        parts.append(f"Buzon de comentarios: {mailbox}.")
    if observations_url:
        parts.append(f"Respuesta a observaciones: {observations_url}.")
    if annex_url:
        parts.append(f"Anexos: {annex_url}.")
    return normalize_whitespace(" ".join(parts))


def _fetch_dian_regulatory_projects_api(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
) -> list[RawItem]:
    params = {
        "$select": ",".join(DIAN_PROJECTS_API_FIELDS),
        "$orderby": "Modified desc",
        "$top": DIAN_PROJECTS_API_TOP,
    }
    last_exc: Exception | None = None
    response: httpx.Response | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.get(
                DIAN_PROJECTS_API_URL,
                params=params,
                headers={"Accept": "application/json;odata=nometadata"},
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
        raise RuntimeError("DIAN projects API request did not return a response")
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("value")
    if not isinstance(rows, list):
        raise ValueError("unexpected DIAN projects API payload")

    items: list[RawItem] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        file_ref = row.get("FileRef")
        file_url = _dian_absolute_url(file_ref if isinstance(file_ref, str) else None)
        if not file_url:
            continue
        canonical = canonicalize_url(file_url)
        if canonical in seen:
            continue
        seen.add(canonical)

        title = normalize_whitespace(
            str(
                row.get("Title")
                or row.get("FileLeafRef")
                or "DIAN regulatory project"
            )
        )
        description = normalize_whitespace(str(row.get("Descripcion") or ""))
        norm_type = normalize_whitespace(str(row.get("TipoDeNorma") or "")) or None
        issue_date = _dian_api_date(row.get("FechaDeEmision"))
        created_at = _dian_api_date(row.get("Created"))
        modified_at = _dian_api_date(row.get("Modified"))
        published_at = modified_at or created_at or issue_date
        comment_start = _dian_api_date(row.get("Fecha_x0020_inicio"))
        comment_end = _dian_api_date(row.get("Fecha_x0020_final"))
        mailbox = normalize_whitespace(str(row.get("Buz_x00f3_n") or "")) or None
        observations_url = _dian_url_field(row.get("Observaciones"))
        annex_url = _dian_url_field(row.get("Anexos"))
        raw_text = _dian_project_raw_text(
            title=title,
            description=description,
            norm_type=norm_type,
            issue_date=issue_date,
            modified_at=modified_at,
            comment_start=comment_start,
            comment_end=comment_end,
            mailbox=mailbox,
            observations_url=observations_url,
            annex_url=annex_url,
        )
        metadata = {
            "content_extraction": "dian_regulatory_project_sharepoint",
            "api_endpoint": DIAN_PROJECTS_API_URL,
            "file_ref": file_ref,
            "file_name": row.get("FileLeafRef"),
            "description": description,
            "norm_type": norm_type,
            "norm_number": row.get("NumeroNorma"),
            "issue_date": issue_date,
            "comment_start": comment_start,
            "comment_end": comment_end,
            "comment_mailbox": mailbox,
            "observations_url": observations_url,
            "annex_url": annex_url,
            "created_at": created_at,
            "modified_at": modified_at,
        }
        items.append(
            RawItem(
                id=_make_id(source.id, file_url, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=file_url,
                title=title,
                fetched_at=fetched_at,
                published_at=published_at,
                raw_text=raw_text,
                metadata=metadata,
            )
        )
    return items


def _extract_dian_regulatory_project_links(
    html: str,
    base_url: str,
    source: Metasource,
    fetched_at: str,
) -> list[RawItem]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[RawItem] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, str(anchor.get("href") or ""))
        anchor_text = normalize_whitespace(anchor.get_text(" ", strip=True))
        searchable = fold_accents(" ".join([anchor_text, href]).lower())
        if not any(marker in searchable for marker in _DIAN_REGULATORY_LINK_MARKERS):
            continue
        canon = canonicalize_url(href)
        if canon in seen:
            continue
        seen.add(canon)
        title_text = anchor_text or "DIAN regulatory project index"
        title = f"DIAN regulatory project index — {title_text[:140]}"
        metadata = {
            "extraction": "dian_regulatory_project_index_link",
            "parser_status": "dynamic_or_undated_index",
            "target_url": href,
        }
        items.append(
            RawItem(
                id=_make_id(source.id, href, title),
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                url=href,
                title=title,
                fetched_at=fetched_at,
                published_at=None,
                raw_text=(
                    f"{title}. DIAN exposes this regulatory-project lead from "
                    "the normativity landing page, but the static HTML does not "
                    "include dated project rows. Treat as parser feasibility "
                    "metadata, not rankable evidence."
                ),
                metadata=metadata,
            )
        )
    return items




__all__ = [name for name in globals() if not name.startswith("__")]
