from __future__ import annotations

from .common import *

def _field_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", fold_accents(label.lower())).strip("_")


def _parse_detail_datetime_to_iso(value: str | None) -> str | None:
    if not value:
        return None
    clean = normalize_whitespace(value)
    try:
        parsed = datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except ValueError:
        return _parse_date_text_to_iso(clean)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _registry_year(value: str) -> str:
    year = int(value)
    if year < 100:
        year += 2000
    return str(year)


_REGISTRY_PROJECT_RE = re.compile(
    r"\b(?P<number>\d{1,4})\s*/\s*(?P<year>\d{2,4})(?:\s*(?P<suffix>[CS]))?",
    re.IGNORECASE,
)


def _normalize_project_number(value: str) -> str:
    stripped = value.lstrip("0")
    return stripped or "0"


def _registry_project_records(
    *,
    numero_senado: str | None = None,
    numero_camara: str | None = None,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for text, default_chamber in (
        (numero_senado or "", "Senado"),
        (numero_camara or "", "Cámara"),
    ):
        for match in _REGISTRY_PROJECT_RE.finditer(text):
            suffix = (match.group("suffix") or "").upper()
            chamber = (
                "Senado"
                if suffix == "S"
                else "Cámara"
                if suffix == "C"
                else default_chamber
            )
            record = {
                "number": _normalize_project_number(match.group("number")),
                "year": _registry_year(match.group("year")),
                "chamber": chamber,
            }
            key = (record["number"], record["year"], record["chamber"])
            if key in seen:
                continue
            seen.add(key)
            records.append(record)
    return records


def _registry_project_kind(value: str | None) -> str:
    folded = fold_accents((value or "").lower())
    return "Acto Legislativo" if "acto legislativo" in folded else "Ley"


def _registry_project_label(
    records: list[dict[str, str]],
    *,
    kind: str = "Ley",
) -> str:
    if not records:
        return ""
    record = records[0]
    return (
        f"Proyecto de {kind} {record['number']} de {record['year']} "
        f"{record['chamber']}"
    )


def _extract_detail_label_values(html_fragment: str) -> dict[str, str]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    values: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        for idx in range(0, len(cells) - 1, 2):
            label = normalize_whitespace(cells[idx]).rstrip(":")
            value = normalize_whitespace(cells[idx + 1])
            if label and value:
                values[_field_key(label)] = value
    return values


def _extract_senado_publication_links(
    html_fragment: str,
    base_url: str,
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    links: list[dict[str, str]] = []
    for label_cell in soup.select("td.celda-etiqueta"):
        value_cell = label_cell.find_next_sibling("td")
        if value_cell is None:
            continue
        link = value_cell.find("a")
        title = normalize_whitespace(
            link.get_text(" ", strip=True) if link else value_cell.get_text(" ", strip=True)
        )
        if not title:
            continue
        label = normalize_whitespace(label_cell.get_text(" ", strip=True)).rstrip(":")
        href = link.get("href") if link else ""
        links.append(
            {
                "type": label,
                "title": title,
                "url": urljoin(base_url, href) if href else "",
            }
        )
    return links


def _extract_senado_text_radicado_url(html_fragment: str, base_url: str) -> str:
    soup = BeautifulSoup(html_fragment, "html.parser")
    button = soup.find(id="textoRadicadoBtn")
    if button is None:
        return ""
    link = button.get("data-link") or ""
    return urljoin(base_url, link) if link else ""


def _senado_registry_row_to_item(
    row: Mapping[str, Any],
    detail_html: str,
    *,
    source: Metasource,
    fetched_at: str,
    detail_url: str,
) -> RawItem | None:
    title = normalize_whitespace(str(row.get("titulo") or ""))
    numero_senado = normalize_whitespace(str(row.get("numero_senado") or ""))
    numero_camara = normalize_whitespace(str(row.get("numero_camara") or ""))
    if not title or not (numero_senado or numero_camara):
        return None
    fields = _extract_detail_label_values(detail_html)
    kind = _registry_project_kind(fields.get("tipo_de_ley") or "Ley")
    records = _registry_project_records(
        numero_senado=numero_senado,
        numero_camara=numero_camara,
    )
    project_label = _registry_project_label(records, kind=kind)
    if not project_label:
        return None
    status = normalize_whitespace(
        fields.get("estado") or str(row.get("estado") or "")
    )
    commission = normalize_whitespace(
        fields.get("comision") or str(row.get("comision") or "")
    )
    filing_date = fields.get("fecha_de_presentacion")
    published_at = _parse_date_text_to_iso(filing_date) if filing_date else None
    publication_links = _extract_senado_publication_links(detail_html, source.url)
    text_radicado_url = _extract_senado_text_radicado_url(detail_html, source.url)
    evidence_parts = [
        project_label,
        title,
        f"Estado: {status}" if status else "",
        f"Comisión: {commission}" if commission else "",
        f"Fecha de presentación: {filing_date}" if filing_date else "",
    ]
    if publication_links:
        evidence_parts.append(
            "Publicaciones: "
            + "; ".join(link["title"] for link in publication_links[:4])
        )
    metadata: dict[str, Any] = {
        "content_extraction": "senado_leyes_registry",
        "parsed_content": True,
        "legislative_registry": "senado_leyes",
        "registry_detail_url": detail_url,
        "project_label": project_label,
        "project_records": records,
        "project_identity_status": "clean",
        "has_clean_project_identity": True,
        "bill_title": title,
        "status": status,
        "commission": commission,
        "author": normalize_whitespace(str(row.get("autor") or "")),
        "legislature": fields.get("legislatura") or _current_legislature_label(),
        "cuatrenio": fields.get("cuatrenio") or str(row.get("cuatrenio") or ""),
        "source_row_id": str(row.get("id") or ""),
    }
    if publication_links:
        metadata["publication_links"] = publication_links
    if text_radicado_url:
        metadata["text_radicado_url"] = text_radicado_url
    return RawItem(
        id=_make_id(source.id, detail_url, project_label),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=detail_url,
        title=f"Senado registry — {project_label} — {title}",
        fetched_at=fetched_at,
        published_at=published_at,
        raw_text=". ".join(part for part in evidence_parts if part),
        metadata=metadata,
    )


def _fetch_senado_leyes_registry(
    source: Metasource,
    client: httpx.Client,
    fetched_at: str,
) -> list[RawItem]:
    search_url = urljoin(source.url, "api/search_pdly.php")
    detail_base = urljoin(source.url, "api/get_detalle_pdly.php")
    response = _http_post_form(
        client,
        search_url,
        {"legislatura": _current_legislature_label()},
    )
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError("unexpected Senado registry payload")
    limit = source.max_items or LEGISLATIVE_REGISTRY_DEFAULT_LIMIT
    def row_sort_key(row: object) -> int:
        if not isinstance(row, dict):
            return -1
        try:
            return int(str(row.get("id") or "0"))
        except ValueError:
            return -1

    items: list[RawItem] = []
    for row in sorted(rows, key=row_sort_key, reverse=True)[:limit]:
        if not isinstance(row, dict):
            continue
        row_id = row.get("id")
        if row_id is None:
            continue
        detail_url = f"{detail_base}?id={row_id}"
        detail = _http_get(client, detail_base, params={"id": str(row_id)})
        item = _senado_registry_row_to_item(
            row,
            detail.text,
            source=source,
            fetched_at=fetched_at,
            detail_url=detail_url,
        )
        if item is not None:
            items.append(item)
    return items


def _extract_camara_pl_nonce(html_text: str) -> str:
    match = re.search(r"PL_NONCE\s*:\s*['\"]([^'\"]+)['\"]", html_text)
    return match.group(1) if match else ""


def _extract_camara_legislature_id(html_text: str, label: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    select = soup.find("select", id="legislaturaField")
    if select is None:
        return "All"
    for option in select.find_all("option"):
        text = normalize_whitespace(option.get_text(" ", strip=True))
        if text == label:
            return option.get("value") or "All"
    return "All"


def _camara_pack_names(pack: str | None) -> str:
    if not pack:
        return ""
    names: list[str] = []
    for entry in str(pack).split("::"):
        parts = entry.split("||")
        if len(parts) >= 2 and parts[1].strip():
            names.append(normalize_whitespace(parts[1]))
    return ", ".join(names)


def _extract_camara_detail_fields(html_text: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    fields: dict[str, Any] = {}
    match = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html_text)
    if match:
        fields["date_published"] = match.group(1)
    for card in soup.select(".pl-card"):
        title_el = card.select_one(".pl-title")
        body_el = card.select_one(".pl-body")
        if title_el is None or body_el is None:
            continue
        key = _field_key(title_el.get_text(" ", strip=True))
        fields[key] = normalize_whitespace(body_el.get_text(" ", strip=True))
        if key == "publicacion":
            fields["publication_links"] = [
                {
                    "title": normalize_whitespace(a.get_text(" ", strip=True)),
                    "url": a.get("href") or "",
                }
                for a in body_el.find_all("a")
                if a.get("href")
            ]
    for title_el in soup.select(".pl-nums-title"):
        if "fecha de radicacion" not in fold_accents(
            title_el.get_text(" ", strip=True).lower()
        ):
            continue
        parent = title_el.find_parent(class_="pl-nums-group")
        if parent is None:
            continue
        for card in parent.select(".pl-kpi-card"):
            label_el = card.select_one(".pl-kpi-label")
            value_el = card.select_one(".pl-kpi-value")
            if label_el is None or value_el is None:
                continue
            value = normalize_whitespace(value_el.get_text(" ", strip=True))
            if value and value not in {"-", "—"}:
                fields["fecha_de_radicacion"] = value
                break
    return fields


def _camara_registry_row_to_item(
    row: Mapping[str, Any],
    detail_html: str,
    *,
    source: Metasource,
    fetched_at: str,
    detail_url: str,
) -> RawItem | None:
    title = normalize_whitespace(str(row.get("titulo") or ""))
    short_title = normalize_whitespace(str(row.get("proyecto") or ""))
    numero_senado = normalize_whitespace(str(row.get("nro_senado") or ""))
    numero_camara = normalize_whitespace(str(row.get("nro_camara") or ""))
    if not title or not (numero_senado or numero_camara):
        return None
    fields = _extract_camara_detail_fields(detail_html)
    kind = _registry_project_kind(str(row.get("tipo") or fields.get("tipo_de_ley") or ""))
    records = _registry_project_records(
        numero_senado=numero_senado,
        numero_camara=numero_camara,
    )
    project_label = _registry_project_label(records, kind=kind)
    if not project_label:
        return None
    published_at = _parse_date_text_to_iso(str(fields.get("fecha_de_radicacion") or ""))
    if not published_at:
        published_at = _parse_detail_datetime_to_iso(str(fields.get("date_published") or ""))
    status = normalize_whitespace(str(row.get("estado") or ""))
    commission = _camara_pack_names(str(row.get("comisiones_pack") or ""))
    authors = _camara_pack_names(str(row.get("autores_pack") or ""))
    other_authors = normalize_whitespace(str(row.get("otros_autores") or ""))
    object_text = normalize_whitespace(str(fields.get("objeto_del_proyecto") or ""))
    publication_links = [
        {
            "title": str(link.get("title") or ""),
            "url": urljoin(detail_url, str(link.get("url") or "")),
        }
        for link in (fields.get("publication_links") or [])
        if isinstance(link, dict) and link.get("url")
    ]
    display_title = short_title or title
    evidence_parts = [
        project_label,
        display_title,
        title,
        f"Estado: {status}" if status else "",
        f"Comisión: {commission}" if commission else "",
        f"Fecha de radicación: {fields.get('fecha_de_radicacion')}"
        if fields.get("fecha_de_radicacion")
        else "",
        f"Objeto: {object_text}" if object_text else "",
    ]
    metadata: dict[str, Any] = {
        "content_extraction": "camara_proyectos_ley_registry",
        "parsed_content": True,
        "legislative_registry": "camara_proyectos_ley",
        "registry_detail_url": detail_url,
        "project_label": project_label,
        "project_records": records,
        "project_identity_status": "clean",
        "has_clean_project_identity": True,
        "bill_title": title,
        "short_title": short_title,
        "status": status,
        "commission": commission,
        "authors": ", ".join(p for p in [authors, other_authors] if p),
        "legislature": str(row.get("vigencia") or ""),
        "origin": str(row.get("origen") or ""),
        "bill_type": str(row.get("tipo") or ""),
    }
    if object_text:
        metadata["object"] = object_text
    if publication_links:
        metadata["publication_links"] = publication_links
    return RawItem(
        id=_make_id(source.id, detail_url, project_label),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=detail_url,
        title=f"Cámara registry — {project_label} — {display_title}",
        fetched_at=fetched_at,
        published_at=published_at,
        raw_text=". ".join(part for part in evidence_parts if part),
        metadata=metadata,
    )


def _fetch_camara_proyectos_ley_registry(
    source: Metasource,
    client: httpx.Client,
    home_html: str,
    fetched_at: str,
) -> list[RawItem]:
    nonce = _extract_camara_pl_nonce(home_html)
    if not nonce:
        raise ValueError("Camara proyectos page missing PL_NONCE")
    legislature = _extract_camara_legislature_id(
        home_html,
        _current_legislature_label(),
    )
    limit = source.max_items or LEGISLATIVE_REGISTRY_DEFAULT_LIMIT
    ajax_url = urljoin(source.url, "/wp-admin/admin-ajax.php")
    response = _http_post_form(
        client,
        ajax_url,
        {
            "action": "get_proyectos_ley_page",
            "_ajax_nonce": nonce,
            "page": "1",
            "per_page": str(limit),
            "term": "",
            "comision": "",
            "tipo": "All",
            "estado": "All",
            "origen": "All",
            "legislatura": legislature,
            "ley_numero": "",
            "ley_fecha": "",
            "comision_adv": "All",
        },
    )
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    rows = data.get("items") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("unexpected Camara proyectos payload")
    items: list[RawItem] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        link_web = normalize_whitespace(str(row.get("link_web") or ""))
        if not link_web:
            continue
        split = urlsplit(source.url)
        site_root = f"{split.scheme}://{split.netloc}/"
        detail_url = urljoin(site_root, link_web)
        detail = _http_get(client, detail_url)
        item = _camara_registry_row_to_item(
            row,
            detail.text,
            source=source,
            fetched_at=fetched_at,
            detail_url=detail_url,
        )
        if item is not None:
            items.append(item)
    return items




__all__ = [name for name in globals() if not name.startswith("__")]
