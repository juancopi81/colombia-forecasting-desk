from __future__ import annotations

from .common import *

SOCRATA_FRESHNESS_DAYS = 14
SOCRATA_DEFAULT_LIMIT = 30
_SOCRATA_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})")


@dataclass(frozen=True, slots=True)
class SocrataAdapter:
    """Per-dataset configuration for the Socrata API fetcher.

    Each entry on `datos.gov.co` has its own column names, so we keep the
    column-to-RawItem mapping in code rather than YAML — same dispatch style
    we use for source-specific HTML extractors.
    """

    date_field: str
    title_field: str
    id_field: str
    label: str
    entity_field: str | None = None
    title_max_chars: int = 160


SOCRATA_ADAPTERS: dict[str, SocrataAdapter] = {
    "secop_ii_procesos": SocrataAdapter(
        date_field="fecha_de_publicacion_del",
        title_field="nombre_del_procedimiento",
        id_field="id_del_proceso",
        entity_field="entidad",
        label="SECOP II Proceso",
    ),
    "secop_ii_contratos": SocrataAdapter(
        date_field="fecha_de_firma",
        title_field="descripcion_del_proceso",
        id_field="id_contrato",
        entity_field="nombre_entidad",
        label="SECOP II Contrato",
    ),
    "secop_i_procesos": SocrataAdapter(
        date_field="fecha_de_cargue_en_el_secop",
        title_field="detalle_del_objeto_a_contratar",
        id_field="uid",
        entity_field="nombre_entidad",
        label="SECOP I Proceso",
    ),
    "secop_ii_adiciones": SocrataAdapter(
        date_field="fecharegistro",
        title_field="descripcion",
        id_field="identificador",
        label="SECOP II Adición",
    ),
    "secop_multas_sanciones": SocrataAdapter(
        date_field="fecha_de_publicacion",
        title_field="nombre_contratista",
        id_field="numero_de_resolucion",
        entity_field="nombre_entidad",
        label="Multa/Sanción SECOP I",
    ),
}


def _parse_socrata_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = _SOCRATA_DATE_RE.match(value)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(x) for x in match.groups())
    try:
        return datetime(
            year, month, day, hour, minute, second, tzinfo=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _socrata_row_to_item(
    row: Mapping[str, Any],
    source: Metasource,
    fetched_at: str,
    adapter: SocrataAdapter,
) -> RawItem | None:
    published_at = _parse_socrata_date(row.get(adapter.date_field))
    if not published_at:
        return None
    title_raw = normalize_whitespace((row.get(adapter.title_field) or "")).strip()
    if not title_raw:
        return None
    id_value = normalize_whitespace((row.get(adapter.id_field) or "")).strip()
    if not id_value:
        return None
    entity = ""
    if adapter.entity_field:
        entity = normalize_whitespace((row.get(adapter.entity_field) or "")).strip()
    title_body = title_raw[: adapter.title_max_chars]
    title_parts = [adapter.label, title_body]
    if entity:
        title_parts.append(entity[:80])
    title = " — ".join(p for p in title_parts if p)
    synthetic_url = f"{source.url}?id={id_value}"
    raw_text = " | ".join(p for p in [title_raw, entity] if p)
    metadata = {
        "extraction": "socrata_api",
        "dataset_url": source.url,
        "id_value": id_value,
        "date_field": adapter.date_field,
        "title_field": adapter.title_field,
    }
    if entity:
        metadata["entity"] = entity
    return RawItem(
        id=_make_id(source.id, synthetic_url, title),
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=synthetic_url,
        title=title,
        fetched_at=fetched_at,
        published_at=published_at,
        raw_text=raw_text,
        metadata=metadata,
    )


def _socrata_params(
    adapter: SocrataAdapter,
    *,
    cutoff: datetime,
    limit: int,
) -> dict[str, str]:
    select_cols = {
        adapter.date_field,
        adapter.title_field,
        adapter.id_field,
    }
    if adapter.entity_field:
        select_cols.add(adapter.entity_field)
    cutoff_text = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000")
    return {
        "$select": ",".join(sorted(select_cols)),
        "$where": f"{adapter.date_field} >= '{cutoff_text}'",
        "$order": f"{adapter.date_field} DESC",
        "$limit": str(limit),
    }


def fetch_api(source: Metasource, client: httpx.Client) -> list[RawItem]:
    adapter = SOCRATA_ADAPTERS.get(source.id)
    if adapter is None:
        raise ValueError(
            f"no Socrata adapter configured for source.id={source.id!r}; "
            "add an entry in SOCRATA_ADAPTERS"
        )
    fetched_at = _now_iso()
    cutoff = datetime.now(timezone.utc) - timedelta(days=SOCRATA_FRESHNESS_DAYS)
    limit = source.max_items if source.max_items and source.max_items > 0 else SOCRATA_DEFAULT_LIMIT
    params = _socrata_params(adapter, cutoff=cutoff, limit=limit)
    response = _http_get(client, source.url, params=params)
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(
            f"unexpected Socrata payload for {source.id}: "
            f"expected list, got {type(payload).__name__}"
        )
    items: list[RawItem] = []
    seen: set[str] = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        item = _socrata_row_to_item(row, source, fetched_at, adapter)
        if item is None:
            continue
        canon = canonicalize_url(item.url)
        if canon in seen:
            continue
        seen.add(canon)
        items.append(item)
    return items




__all__ = [name for name in globals() if not name.startswith("__")]
