from __future__ import annotations

import re

from .cleaner import fold_accents
from .models import Cluster

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)

_FORECASTABLE_SOURCE_TYPES = {
    "calendar",
    "dataset",
    "economic_indicator",
    "legal",
    "official_updates",
    "polling",
    "regulatory",
}
_FORECASTABLE_SIGNAL_TYPES = {
    "calendar_event",
    "court_or_regulatory_movement",
    "economic_indicator",
    "legislative_movement",
    "market_move",
    "new_data",
    "official_update",
    "poll",
}
_DECISION_TERMS = {
    "aprobacion",
    "aprueba",
    "banrep",
    "calendario",
    "cne",
    "congreso",
    "corte",
    "decreto",
    "demanda",
    "dian",
    "eleccion",
    "electoral",
    "encuesta",
    "fallo",
    "gaceta",
    "ibr",
    "ipc",
    "junta",
    "ley",
    "minhacienda",
    "norma",
    "ponencia",
    "proyecto",
    "registraduria",
    "reforma",
    "resolucion",
    "sentencia",
    "tasa",
    "trm",
}
_DATA_TERMS = {
    "anh",
    "contrato",
    "dane",
    "deficit",
    "desempleo",
    "energia",
    "exportaciones",
    "importaciones",
    "inflacion",
    "manufacturera",
    "petroleo",
    "recaudo",
    "secop",
}
_LOW_FORECASTABILITY_TERMS = {
    "asesinar",
    "asesinato",
    "atraco",
    "captura",
    "capturaron",
    "escopolamina",
    "festival",
    "hipopotamo",
    "homicidio",
    "robo",
    "veterinario",
}
_SECOP_SOURCE_PREFIXES = ("secop_",)
_GENERIC_IMPRENTA_TITLE_RE = re.compile(
    r"^(?:gaceta del congreso\s+\d+|diario oficial\s+[\d.]+)"
    r"(?:\s+[-]\s+[^-]+)?$",
    re.IGNORECASE,
)


def _has_secop_source(cluster: Cluster) -> bool:
    return any(
        source_id.startswith(_SECOP_SOURCE_PREFIXES)
        for source_id in cluster.member_source_ids
    )


def _is_opaque_imprenta_cluster(cluster: Cluster) -> bool:
    if not set(cluster.member_source_ids) <= {"diario_oficial", "gacetas_congreso"}:
        return False
    for title in cluster.member_titles or [cluster.title]:
        normalized = title.lower().replace("—", "-").replace("–", "-")
        normalized = fold_accents(normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not _GENERIC_IMPRENTA_TITLE_RE.match(normalized):
            return False
    return True


def _terms(text: str) -> set[str]:
    return set(_TOKEN_RE.findall(fold_accents(text.lower())))


def cluster_terms(cluster: Cluster) -> set[str]:
    return _terms(" ".join([cluster.title, cluster.summary]))


def forecastability_reasons(cluster: Cluster) -> list[str]:
    terms = cluster_terms(cluster)
    reasons: list[str] = []
    if set(cluster.source_types) & _FORECASTABLE_SOURCE_TYPES:
        reasons.append("primary or structured source")
    if set(cluster.signal_types) & _FORECASTABLE_SIGNAL_TYPES:
        reasons.append("forecastable signal type")
    if terms & _DECISION_TERMS:
        reasons.append("decision/resolution terms")
    if terms & _DATA_TERMS:
        reasons.append("measurable data terms")
    if cluster.source_count >= 2:
        reasons.append("multi-source corroboration")
    return reasons


def noise_reasons(cluster: Cluster) -> list[str]:
    terms = cluster_terms(cluster)
    reasons: list[str] = []
    if _has_secop_source(cluster) and cluster.source_count == 1:
        reasons.append(
            "individual SECOP row belongs in the procurement pulse unless it has a national hook"
        )
    if _is_opaque_imprenta_cluster(cluster):
        reasons.append(
            "official publication index lacks document title or parsed text"
        )
    if terms & _LOW_FORECASTABILITY_TERMS:
        reasons.append("low-forecastability human-interest/local story")
    if "blog" in terms and cluster.source_count == 1:
        reasons.append("commentary/blog item rather than an unresolved event")
    if (
        cluster.source_count == 1
        and set(cluster.source_types) == {"news"}
        and not (terms & (_DECISION_TERMS | _DATA_TERMS))
    ):
        reasons.append("single-source media narrative without a clear resolution path")
    return reasons


def forecastability_score(cluster: Cluster) -> float:
    score = 0.0
    score += 1.5 * len(forecastability_reasons(cluster))
    score -= 2.0 * len(noise_reasons(cluster))
    if _has_secop_source(cluster) and cluster.source_count == 1:
        score -= 4.0
    if _is_opaque_imprenta_cluster(cluster):
        score -= 8.0
    if cluster.confidence == "high":
        score += 0.5
    elif cluster.confidence == "low":
        score -= 0.5
    return score


def is_forecastable_candidate(cluster: Cluster) -> bool:
    return (
        forecastability_score(cluster) >= 3.0
        and bool(forecastability_reasons(cluster))
        and not noise_reasons(cluster)
    )


def resolution_hint(cluster: Cluster) -> str:
    terms = cluster_terms(cluster)
    if {"banrep", "tasa", "junta"} & terms:
        return "Banco de la Republica board statement, minutes, and rate series."
    if {"gaceta", "congreso", "ponencia", "proyecto", "reforma", "ley"} & terms:
        return "Congreso agenda, Gacetas del Congreso, and final legislative votes."
    if {"dane", "ipc", "desempleo", "inflacion"} & terms:
        return "Next official DANE release for the named indicator."
    if {"cne", "encuesta", "electoral", "registraduria", "eleccion"} & terms:
        return "CNE/Registraduria publication or official electoral calendar."
    if {"dian", "minhacienda", "decreto", "norma", "resolucion"} & terms:
        return "DIAN/MinHacienda project page, final decree/resolution, or Diario Oficial."
    if {"secop", "contrato"} & terms:
        return "SECOP dataset refresh and Colombia Compra process detail."
    return "Primary official source named in the signal, plus a dated follow-up check."


def deadline_hint(cluster: Cluster) -> str:
    terms = cluster_terms(cluster)
    if {"banrep", "tasa", "junta"} & terms:
        return "Next scheduled BanRep board decision."
    if {"gaceta", "congreso", "ponencia", "proyecto", "reforma", "ley"} & terms:
        return "Next committee/plenary agenda window, or 30-60 days if no date is known."
    if {"cne", "encuesta", "electoral", "registraduria", "eleccion"} & terms:
        return "Next official electoral milestone or poll-filing window."
    if {"dane", "ipc", "desempleo", "inflacion"} & terms:
        return "Next monthly release for the indicator."
    return "Use a concrete 7/30/60-day window unless the source provides a deadline."


def question_seed(cluster: Cluster) -> str:
    terms = cluster_terms(cluster)
    if {"banrep", "tasa", "junta"} & terms:
        return "Will Banco de la Republica change the policy rate at its next board decision?"
    if {"gaceta", "congreso", "ponencia", "proyecto", "reforma", "ley"} & terms:
        return "Will the referenced legislative item advance to its next formal stage within the next 30-60 days?"
    if {"cne", "encuesta", "electoral", "registraduria", "eleccion"} & terms:
        return "Will the referenced electoral milestone or poll filing be confirmed by the official source in the next reporting window?"
    if {"dian", "minhacienda", "decreto", "norma", "resolucion"} & terms:
        return "Will the referenced regulatory proposal be issued or materially revised before its next official deadline?"
    if {"dane", "ipc", "desempleo", "inflacion"} & terms:
        return "Will the next official data release confirm the direction implied by this signal?"
    return "Can this signal be converted into a dated yes/no event with a primary resolution source?"
