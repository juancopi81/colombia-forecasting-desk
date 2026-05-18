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
    "declaratoria",
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
    "mincit",
    "norma",
    "ponencia",
    "prorroga",
    "proyecto",
    "registraduria",
    "reforma",
    "resolucion",
    "sentencia",
    "tasa",
    "trm",
    "zona",
    "franca",
    "francas",
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
_DIARIO_FINAL_ROW_TYPE = "diario_legal_act"
_UNRESOLVED_ANCHOR_SOURCE_IDS = {
    "camara_agenda_consolidada",
    "camara_proyectos_ley_registry",
    "dian_proyectos_normas",
    "gacetas_congreso",
    "mincit_zonas_francas",
    "minhacienda_proyectos_decreto",
    "senado_agenda_legislativa",
    "senado_leyes_registry",
}
LEGISLATIVE_REGISTRY_SOURCE_IDS = {
    "senado_leyes_registry",
    "camara_proyectos_ley_registry",
}
_UNRESOLVED_ANCHOR_METADATA_KEYS = {
    "agenda_action_type",
    "bill_title",
    "declaratory_resolution",
    "legislative_registry",
    "project_label",
    "project_records",
    "registry_change_type",
    "registry_detail_url",
    "zona_franca_name",
}
_GENERIC_IMPRENTA_TITLE_RE = re.compile(
    r"^(?:gaceta del congreso\s+\d+|diario oficial\s+[\d.]+)"
    r"(?:\s+[-]\s+(?:senado de la republica|camara de representantes|"
    r"edicion ordinaria|edicion extraordinaria|ordinaria|extraordinaria))?$",
    re.IGNORECASE,
)
_DIARIO_FINAL_ACT_TITLE_RE = re.compile(
    r"\bdiario oficial\s+[\d.]+\s+-\s+"
    r"(?:(?:ordinaria|extraordinaria)\s+-\s+)?"
    r"(?:acto legislativo|decreto(?:\s+ley)?|ley|"
    r"resolucion(?:\s+\w+){0,3}|circular(?:\s+\w+){0,2}|acuerdo|directiva)"
    r"\s+(?:no\.?\s*)?[a-z]?\d",
    re.IGNORECASE,
)
_GENERIC_SENADO_AGENDA_TITLE_RE = re.compile(
    r"^agenda legislativa del .*\(\s*pdf\b",
    re.IGNORECASE,
)
_SENADO_AGENDA_ENTRY_TITLE_RE = re.compile(
    r"^senado agenda .*\bproyecto de\b",
    re.IGNORECASE,
)
_CLEAN_SENADO_PROJECT_RE = re.compile(
    r"\bProyecto\s+de\s+(?:Ley|Acto\s+Legislativo)\s+"
    r"\d{1,4}\s+de\s+\d{4}\s+(?:Senado|C[aá]mara)"
    r"(?:\s*/\s*\d{1,4}\s+de\s+\d{4}\s+(?:Senado|C[aá]mara))?",
    re.IGNORECASE,
)
_LOSSY_SENADO_TITLE_MARKERS = (
    "elacual",
    "modificael",
    "laley",
    "deyse",
    "otrasdisposiciones",
)


def _has_secop_source(cluster: Cluster) -> bool:
    return any(
        source_id.startswith(_SECOP_SOURCE_PREFIXES)
        for source_id in cluster.member_source_ids
    )


def _has_only_secop_sources(cluster: Cluster) -> bool:
    return bool(cluster.member_source_ids) and all(
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


def _normalized_text(text: str) -> str:
    normalized = text.lower().replace("—", "-").replace("–", "-")
    normalized = fold_accents(normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _has_diario_final_publication_member(cluster: Cluster) -> bool:
    titles = cluster.member_titles or [cluster.title]
    metadata_items = cluster.member_metadata or [{} for _ in titles]
    source_ids = cluster.member_source_ids or ["diario_oficial" for _ in titles]
    for source_id, title, metadata in zip(source_ids, titles, metadata_items):
        if source_id != "diario_oficial":
            continue
        if metadata.get("document_row_type") == _DIARIO_FINAL_ROW_TYPE:
            return True
        if (
            metadata.get("content_extraction") == "diario_oficial_pdf_text"
            and metadata.get("legal_act_records")
        ):
            return True
        if _DIARIO_FINAL_ACT_TITLE_RE.search(_normalized_text(title)):
            return True
    return False


def _is_diario_final_publication_cluster(cluster: Cluster) -> bool:
    return (
        set(cluster.member_source_ids) == {"diario_oficial"}
        and _has_diario_final_publication_member(cluster)
    )


def _has_clean_unresolved_decision_identity(cluster: Cluster) -> bool:
    cluster_sources = set(cluster.member_source_ids)
    if not cluster_sources & _UNRESOLVED_ANCHOR_SOURCE_IDS:
        return False
    for metadata in cluster.member_metadata:
        if any(metadata.get(key) for key in _UNRESOLVED_ANCHOR_METADATA_KEYS):
            return True
    texts = [cluster.title, cluster.summary, *cluster.member_titles]
    if "gacetas_congreso" in cluster_sources and any(
        re.search(r"\bproyecto\s+de\s+(?:ley|acto\s+legislativo)\s+\d", text, re.I)
        for text in texts
    ):
        return True
    if "senado_agenda_legislativa" in cluster_sources and any(
        _has_clean_senado_project_identity(text) for text in texts
    ):
        return True
    if cluster_sources & LEGISLATIVE_REGISTRY_SOURCE_IDS and any(
        re.search(
            r"\bproyecto\s+de\s+(?:ley|acto\s+legislativo)\s+"
            r"\d{1,4}\s+de\s+\d{4}\s+(?:senado|c[aá]mara)",
            text,
            re.I,
        )
        for text in texts
    ):
        return True
    return False


def _is_mixed_diario_final_without_unresolved_identity(cluster: Cluster) -> bool:
    return (
        _has_diario_final_publication_member(cluster)
        and set(cluster.member_source_ids) != {"diario_oficial"}
        and not _has_clean_unresolved_decision_identity(cluster)
    )


def _is_generic_senado_agenda_cluster(cluster: Cluster) -> bool:
    if set(cluster.member_source_ids) != {"senado_agenda_legislativa"}:
        return False
    titles = cluster.member_titles or [cluster.title]
    normalized_titles = [
        re.sub(
            r"\s+",
            " ",
            fold_accents(title.lower()).replace("—", "-").replace("–", "-"),
        ).strip()
        for title in titles
    ]
    if any("proyecto de ley" in title for title in normalized_titles):
        return False
    return all(
        _GENERIC_SENADO_AGENDA_TITLE_RE.match(title)
        for title in normalized_titles
    )


def _has_clean_senado_project_identity(text: str) -> bool:
    folded = fold_accents(text.lower())
    if any(marker in folded for marker in _LOSSY_SENADO_TITLE_MARKERS):
        return False
    match = _CLEAN_SENADO_PROJECT_RE.search(text)
    if not match:
        return False
    tail = text[match.end() :]
    title_match = re.search(r"(?:—|-)\s*([^—-]{24,220})", tail)
    if not title_match:
        return False
    title = title_match.group(1).strip(" .,:;-")
    title_terms = _terms(title)
    return bool(
        title_terms
        & {
            "adopta",
            "codigo",
            "crea",
            "dictan",
            "establece",
            "expide",
            "modifica",
            "promueve",
            "reforma",
            "regimen",
            "sistema",
        }
    )


def _is_weak_senado_agenda_entry_cluster(cluster: Cluster) -> bool:
    if set(cluster.member_source_ids) != {"senado_agenda_legislativa"}:
        return False
    texts = [cluster.title, cluster.summary, *cluster.member_titles]
    normalized_texts = [
        normalize
        for text in texts
        if (
            normalize := re.sub(
                r"\s+",
                " ",
                fold_accents(text.lower()).replace("—", "-").replace("–", "-"),
            ).strip()
        )
    ]
    if not any(_SENADO_AGENDA_ENTRY_TITLE_RE.match(text) for text in normalized_texts):
        return False
    return not any(_has_clean_senado_project_identity(text) for text in texts)


def _has_official_followup_match(cluster: Cluster) -> bool:
    return any(
        metadata.get("official_followup_match_count", 0)
        or metadata.get("official_followup_matches")
        for metadata in cluster.member_metadata
    )


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
    if _has_official_followup_match(cluster):
        reasons.append("official follow-up matched")
    return reasons


def noise_reasons(cluster: Cluster) -> list[str]:
    terms = cluster_terms(cluster)
    reasons: list[str] = []
    if _has_only_secop_sources(cluster):
        reasons.append(
            "SECOP-only procurement rows belong in the procurement pulse unless they have a national hook"
        )
    if _is_opaque_imprenta_cluster(cluster):
        reasons.append(
            "official publication index lacks document title or parsed text"
        )
    if _is_diario_final_publication_cluster(cluster):
        reasons.append(
            "final Diario Oficial publication is resolution evidence, not an unresolved forecast"
        )
    if _is_mixed_diario_final_without_unresolved_identity(cluster):
        reasons.append(
            "mixed cluster includes final Diario Oficial publication without a clean unresolved decision identity"
        )
    if _is_generic_senado_agenda_cluster(cluster):
        reasons.append("Senado agenda PDF lacks a parsed bill/action entry")
    if _is_weak_senado_agenda_entry_cluster(cluster):
        reasons.append("Senado agenda entry lacks a clean project number or bill title")
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
    if _has_only_secop_sources(cluster):
        score -= 4.0
    if _is_opaque_imprenta_cluster(cluster):
        score -= 8.0
    if _is_diario_final_publication_cluster(cluster):
        score -= 8.0
    if _is_mixed_diario_final_without_unresolved_identity(cluster):
        score -= 8.0
    if _is_generic_senado_agenda_cluster(cluster):
        score -= 8.0
    if _is_weak_senado_agenda_entry_cluster(cluster):
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
    if {"mincit", "zona", "franca", "francas", "declaratoria", "prorroga"} & terms:
        return "MinCIT approved-zones registry, Diario Oficial, SUIN, or Gestor Normativo."
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
    if {"mincit", "zona", "franca", "francas", "declaratoria", "prorroga"} & terms:
        return "Next MinCIT statistics snapshot or a 30-60 day legal-publication check."
    return "Use a concrete 7/30/60-day window unless the source provides a deadline."


def question_seed(cluster: Cluster) -> str:
    terms = cluster_terms(cluster)
    if {"banrep", "tasa", "junta"} & terms:
        return "Will Banco de la Republica change the policy rate at its next board decision?"
    if {"gaceta", "congreso", "ponencia", "proyecto", "reforma", "ley"} & terms:
        return "Will the referenced legislative item advance to its next formal stage within the next 30-60 days?"
    if {"cne", "encuesta", "electoral", "registraduria", "eleccion"} & terms:
        return "Will the referenced electoral milestone or poll filing be confirmed by the official source in the next reporting window?"
    if {"mincit", "zona", "franca", "francas", "declaratoria", "prorroga"} & terms:
        return "Will the named zona-franca declaration or extension be confirmed in the next official follow-up window?"
    if {"dian", "minhacienda", "decreto", "norma", "resolucion"} & terms:
        return "Will the referenced regulatory proposal be issued or materially revised before its next official deadline?"
    if {"dane", "ipc", "desempleo", "inflacion"} & terms:
        return "Will the next official data release confirm the direction implied by this signal?"
    return "Can this signal be converted into a dated yes/no event with a primary resolution source?"
