from __future__ import annotations

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
