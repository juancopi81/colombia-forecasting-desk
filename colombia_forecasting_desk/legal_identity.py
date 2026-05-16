from __future__ import annotations

import re
from typing import Any, Iterable

from .cleaner import fold_accents, normalize_whitespace

OFFICIAL_LEGAL_SOURCE_IDS = frozenset(
    {
        "diario_oficial",
        "suin_juriscol",
        "suin_juriscol_normas",
        "gestor_normativo_fp",
        "gestor_normativo_funcion_publica",
    }
)

_LEGAL_ACT_RE = re.compile(
    r"\b(?P<kind>"
    r"Acto\s+Legislativo|Decreto[-\s]+Ley|Decreto|Ley|"
    r"Resoluci[oó]n|Res\.?|"
    r"Circular(?:\s+(?:Externa|Conjunta))?|"
    r"Acuerdo|Directiva|Concepto"
    r")\s*(?:No\.?|N[úu]m(?:ero)?\.?|Nro\.?)?\s*"
    r"(?P<number>[A-Z]?\d{1,6}(?:[-/]\d{1,5})?)\s*"
    r"(?:de|del)\s+"
    r"(?:(?:\d{1,2})(?:[°º])?\s+(?:de\s+)?"
    r"(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|setiembre|octubre|noviembre|diciembre)\s+"
    r"(?:de\s+)?)?"
    r"(?P<year>19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)


def normalize_legal_act_kind(value: str) -> str:
    folded = fold_accents(normalize_whitespace(value).lower()).replace(".", "")
    if folded.startswith("res"):
        return "Resolución"
    if folded.startswith("decreto ley") or folded.startswith("decreto-ley"):
        return "Decreto Ley"
    if folded.startswith("decreto"):
        return "Decreto"
    if folded.startswith("ley"):
        return "Ley"
    if folded.startswith("acto legislativo"):
        return "Acto Legislativo"
    if folded.startswith("circular conjunta"):
        return "Circular Conjunta"
    if folded.startswith("circular externa"):
        return "Circular Externa"
    if folded.startswith("circular"):
        return "Circular"
    if folded.startswith("acuerdo"):
        return "Acuerdo"
    if folded.startswith("directiva"):
        return "Directiva"
    if folded.startswith("concepto"):
        return "Concepto"
    return normalize_whitespace(value).title()


def normalize_legal_act_number(value: str) -> str:
    clean = normalize_whitespace(value).upper().replace("/", "-")
    parts = [part.lstrip("0") or "0" for part in clean.split("-")]
    return "-".join(parts)


def legal_act_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        fold_accents(str(record.get("kind") or "").lower()),
        normalize_legal_act_number(str(record.get("number") or "")),
        str(record.get("year") or "").strip(),
    )


def legal_act_label(record: dict[str, Any]) -> str:
    kind = normalize_legal_act_kind(str(record.get("kind") or ""))
    number = normalize_legal_act_number(str(record.get("number") or ""))
    year = str(record.get("year") or "").strip()
    return normalize_whitespace(f"{kind} {number} de {year}")


def parse_legal_act_records(
    *texts: str | None,
    max_records: int = 30,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    haystack = normalize_whitespace(" ".join(text or "" for text in texts))
    for match in _LEGAL_ACT_RE.finditer(haystack):
        record = {
            "kind": normalize_legal_act_kind(match.group("kind")),
            "number": normalize_legal_act_number(match.group("number")),
            "year": match.group("year"),
            "matched_text": normalize_whitespace(match.group(0)),
        }
        key = legal_act_key(record)
        if key in seen:
            continue
        seen.add(key)
        record["label"] = legal_act_label(record)
        records.append(record)
        if len(records) >= max_records:
            break
    return records


def legal_act_keys(records: object) -> set[tuple[str, str, str]]:
    if not isinstance(records, list):
        return set()
    keys: set[tuple[str, str, str]] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        key = legal_act_key(record)
        if all(key):
            keys.add(key)
    return keys


def annotate_legal_identity(
    metadata: dict[str, Any],
    *texts: str | None,
    max_records: int = 30,
) -> dict[str, Any]:
    records = parse_legal_act_records(*texts, max_records=max_records)
    if not records:
        return metadata
    metadata["legal_act_records"] = records
    metadata["legal_act_record_count"] = len(records)
    return metadata


def best_shared_legal_act_record(
    first: Iterable[dict[str, Any]],
    second: Iterable[dict[str, Any]],
) -> dict[str, str] | None:
    second_by_key = {legal_act_key(record): record for record in second}
    for record in first:
        match = second_by_key.get(legal_act_key(record))
        if match:
            return {
                "kind": str(record.get("kind") or match.get("kind") or ""),
                "number": str(record.get("number") or match.get("number") or ""),
                "year": str(record.get("year") or match.get("year") or ""),
                "label": str(
                    record.get("label")
                    or match.get("label")
                    or legal_act_label(record)
                ),
            }
    return None
