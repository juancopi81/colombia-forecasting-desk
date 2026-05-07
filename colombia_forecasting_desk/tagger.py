from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from typing import Any

CANONICAL_ENTITIES = (
    "banrep",
    "dane",
    "dian",
    "congreso",
    "registraduria",
    "cne",
    "corte_constitucional",
    "minhacienda",
    "anh",
    "secop",
    "presidencia",
)

CANONICAL_TOPICS = (
    "monetary_policy",
    "inflation",
    "labor_market",
    "fiscal_tax",
    "external_trade",
    "energy",
    "hydrocarbons",
    "legislative",
    "electoral",
    "regulatory",
    "constitutional_court",
    "procurement",
    "construction",
    "security",
)

_ENTITY_ALIASES: dict[str, tuple[str, ...]] = {
    "banrep": (
        "banrep",
        "banco de la republica",
        "banco republica",
        "junta directiva del banco de la republica",
    ),
    "dane": (
        "dane",
        "departamento administrativo nacional de estadistica",
    ),
    "dian": (
        "dian",
        "direccion de impuestos y aduanas nacionales",
    ),
    "congreso": (
        "congreso",
        "congreso de la republica",
        "senado",
        "senado de la republica",
        "camara de representantes",
        "gaceta del congreso",
    ),
    "registraduria": (
        "registraduria",
        "registraduria nacional",
        "registraduria nacional del estado civil",
    ),
    "cne": (
        "cne",
        "consejo nacional electoral",
    ),
    "corte_constitucional": (
        "corte constitucional",
    ),
    "minhacienda": (
        "minhacienda",
        "ministerio de hacienda",
        "ministerio de hacienda y credito publico",
        "min hacienda",
    ),
    "anh": (
        "anh",
        "agencia nacional de hidrocarburos",
    ),
    "secop": (
        "secop",
        "secop i",
        "secop ii",
        "colombia compra eficiente",
    ),
    "presidencia": (
        "presidencia",
        "presidencia de la republica",
        "presidente de la republica",
        "casa de narino",
    ),
}

_TOPIC_ALIASES: dict[str, tuple[str, ...]] = {
    "monetary_policy": (
        "politica monetaria",
        "tasa de interes",
        "tasa de politica monetaria",
        "tasa de referencia",
        "junta directiva del banco de la republica",
    ),
    "inflation": (
        "inflacion",
        "ipc",
        "indice de precios al consumidor",
        "precios al consumidor",
        "costo de vida",
    ),
    "labor_market": (
        "mercado laboral",
        "empleo",
        "desempleo",
        "ocupacion",
        "geih",
        "reforma laboral",
    ),
    "fiscal_tax": (
        "fiscal",
        "tributaria",
        "impuesto",
        "impuestos",
        "recaudo",
        "presupuesto",
        "hacienda",
        "dian",
    ),
    "external_trade": (
        "comercio exterior",
        "exportaciones",
        "importaciones",
        "balanza comercial",
        "arancel",
        "aranceles",
        "aduanas",
    ),
    "energy": (
        "energia",
        "electricidad",
        "sector electrico",
        "tarifa de energia",
        "tarifas de energia",
        "gas natural",
    ),
    "hydrocarbons": (
        "hidrocarburos",
        "petroleo",
        "gas natural",
        "crudo",
        "anh",
    ),
    "legislative": (
        "congreso",
        "senado",
        "camara de representantes",
        "gaceta del congreso",
        "proyecto de ley",
        "ponencia",
        "primer debate",
        "segundo debate",
        "plenaria",
        "comision",
    ),
    "electoral": (
        "registraduria",
        "cne",
        "consejo nacional electoral",
        "eleccion",
        "elecciones",
        "electoral",
        "escrutinio",
        "votacion",
        "censo electoral",
    ),
    "regulatory": (
        "regulacion",
        "regulatorio",
        "decreto",
        "resolucion",
        "circular",
        "norma",
        "proyecto de norma",
        "consulta publica",
        "superintendencia",
    ),
    "constitutional_court": (
        "corte constitucional",
        "constitucionalidad",
        "sentencia",
        "tutela",
    ),
    "procurement": (
        "secop",
        "contratacion",
        "licitacion",
        "contrato",
        "contratos",
        "colombia compra eficiente",
    ),
    "construction": (
        "construccion",
        "licencias de construccion",
        "vivienda",
        "edificaciones",
        "cemento",
        "concreto",
    ),
    "security": (
        "seguridad",
        "policia",
        "fuerza publica",
        "crimen",
        "homicidio",
        "secuestro",
        "defensa",
        "ejercito",
        "narcotrafico",
    ),
}

_ALIAS_TOKEN_RE = re.compile(r"[a-z0-9]+")


def fold_accents(text: str) -> str:
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )


def _alias_pattern(alias: str) -> re.Pattern[str]:
    tokens = _ALIAS_TOKEN_RE.findall(fold_accents(alias).lower())
    if not tokens:
        raise ValueError(f"Alias has no searchable tokens: {alias!r}")
    body = r"[^a-z0-9]+".join(re.escape(token) for token in tokens)
    return re.compile(rf"(?<![a-z0-9]){body}(?![a-z0-9])")


_ENTITY_PATTERNS = {
    entity: tuple(_alias_pattern(alias) for alias in aliases)
    for entity, aliases in _ENTITY_ALIASES.items()
}
_TOPIC_PATTERNS = {
    topic: tuple(_alias_pattern(alias) for alias in aliases)
    for topic, aliases in _TOPIC_ALIASES.items()
}


def _metadata_text(metadata: Mapping[str, Any] | None) -> str:
    if not metadata:
        return ""
    parts: list[str] = []
    for key, value in sorted(metadata.items()):
        if value is None:
            continue
        if isinstance(value, str):
            parts.append(f"{key} {value}")
        elif isinstance(value, (int, float, bool)):
            parts.append(f"{key} {value}")
        elif isinstance(value, (list, tuple, set)):
            parts.extend(str(item) for item in value if item is not None)
    return " ".join(parts)


def _searchable_text(
    title: str,
    clean_text: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    joined = " ".join((title or "", clean_text or "", _metadata_text(metadata)))
    return fold_accents(joined).lower()


def _matches(
    patterns_by_tag: dict[str, tuple[re.Pattern[str], ...]],
    canonical_order: tuple[str, ...],
    text: str,
) -> list[str]:
    return [
        tag
        for tag in canonical_order
        if any(pattern.search(text) for pattern in patterns_by_tag[tag])
    ]


def sort_entity_tags(tags: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    known = [tag for tag in CANONICAL_ENTITIES if tag in tags]
    unknown = sorted(tag for tag in tags if tag not in CANONICAL_ENTITIES)
    return known + unknown


def sort_topic_tags(tags: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    known = [tag for tag in CANONICAL_TOPICS if tag in tags]
    unknown = sorted(tag for tag in tags if tag not in CANONICAL_TOPICS)
    return known + unknown


def tag_item(
    title: str,
    clean_text: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> tuple[list[str], list[str]]:
    text = _searchable_text(title, clean_text, metadata)
    entities = _matches(_ENTITY_PATTERNS, CANONICAL_ENTITIES, text)
    topics = _matches(_TOPIC_PATTERNS, CANONICAL_TOPICS, text)
    return entities, topics
