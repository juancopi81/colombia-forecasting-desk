"""Organization identifiers and deterministic cross-source watchlist matching."""
from __future__ import annotations

from datetime import date, datetime
import json
import math
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import yaml

from .config_loader import load_metasources
from .legal_identity import parse_legal_act_records
from .schemas.watchlist_hits import KINDS, validate

ARTIFACTS = ("raw_items.json", "cleaned_items.json", "legislative_reconciler.json", "indicator_watch.json", "analyst_leads.json", "m2_review_packet.json")
_NIT_FIELDS = {"nit", "registry_key", "nit_entidad", "nit_de_la_entidad", "documento_proveedor", "identificacion_del_contratista", "documento_contratista"}
_DOCUMENT_FIELDS = {"tipodocproveedor", "tipo_identifi_del_contratista", "tipo_documento", "tipo_documento_proveedor"}
_BILL = re.compile(r"\b(?:pl|proyecto de ley)\s*(?:no\.?\s*)?0*(\d+)\s*(?:/|de\s*)\s*(20\d{2})\s*(camara|senado)\b")


def normalize_name(value: str) -> str:
    text = "".join(c for c in unicodedata.normalize("NFKD", str(value).casefold()) if not unicodedata.combining(c))
    text = re.sub(r"\bs\s*\.\s*a\s*\.\s*s\s*\.?\b", " sas ", text)
    text = re.sub(r"\be\s*\.\s*s\s*\.\s*p\s*\.?\b", " esp ", text)
    text = re.sub(r"\bs\s*\.\s*a\s*\.?\b", " sa ", text)
    text = " ".join(re.sub(r"[^\w\s]", " ", text).split())
    while re.search(r"\s(?:sas|sa|esp|bic|ltda|limitada)$", text):
        text = re.sub(r"\s(?:sas|sa|esp|bic|ltda|limitada)$", "", text)
    return text


def nit_check_digit(number: str) -> str:
    if not re.fullmatch(r"\d{1,15}", number):
        raise ValueError("NIT base must contain one to fifteen digits")
    weights = (3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71)
    remainder = sum(int(digit) * weight for digit, weight in zip(number[::-1], weights)) % 11
    return str(remainder if remainder < 2 else 11 - remainder)


def normalize_nit(value: str, *, expected_base: str | None = None) -> str | None:
    """Separate an explicit DV; bare long IDs are not guessed to include one."""
    raw = str(value).strip().replace(".", "").replace(" ", "")
    if not re.fullmatch(r"\d+(?:-\d)?", raw):
        return None
    if "-" in raw:
        number, check = raw.rsplit("-", 1)
        if len(number) > 15 or check != nit_check_digit(number):
            return None
    elif expected_base and raw in {expected_base, expected_base + nit_check_digit(expected_base)}:
        number = expected_base
    elif len(raw) <= 9:
        number = raw
    else:
        return None
    return number if expected_base is None or number == expected_base else None


def nit_variants(value: str) -> list[str]:
    number = normalize_nit(value)
    if number is None:
        raise ValueError("NIT needs an unambiguous base or an explicit valid check digit")
    check = nit_check_digit(number)
    formatted = f"{int(number):,}".replace(",", ".")
    return list(dict.fromkeys([number, number + check, number + "-" + check, formatted + "-" + check]))


def normalize_divipola(value: str | int) -> str | None:
    raw = str(value).strip()
    if not raw.isdigit():
        return None
    if len(raw) <= 2:
        return raw.zfill(2)
    return raw.zfill(5) if len(raw) <= 5 else None


def load_watchlist(path: Path) -> dict:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict) or not isinstance(config.get("entries"), list):
        raise ValueError("watchlist requires an entries list")
    seen = set()
    for entry in config["entries"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("kind"), str) or entry["kind"] not in KINDS:
            raise ValueError("unsupported watchlist kind")
        identity = entry.get("watch_id", "")
        if not isinstance(identity, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", identity) or identity in seen:
            raise ValueError("watch_id must be unique and path-safe")
        seen.add(identity)
        if not entry.get("label") or not entry.get("why") or not isinstance(entry.get("identifiers"), dict):
            raise ValueError(f"{identity}: label, why and identifiers are required")
        identifiers = entry["identifiers"]
        if identifiers.get("nit") and normalize_nit(str(identifiers["nit"])) is None:
            raise ValueError(f"{identity}: ambiguous or invalid NIT/check digit")
        if entry["kind"] in {"company", "public_entity"} and not identifiers.get("nit") and not identifiers.get("names"):
            raise ValueError(f"{identity}: an organization needs a NIT or exact name")
        for key in ("names", "keywords", "bills", "decrees", "divipola"):
            if key in identifiers and (not isinstance(identifiers[key], list) or not all(isinstance(x, str) and x.strip() for x in identifiers[key])):
                raise ValueError(f"{identity}: {key} must be a text list")
        if any(normalize_divipola(code) is None for code in identifiers.get("divipola", [])):
            raise ValueError(f"{identity}: invalid DIVIPOLA code")
    settings = config.setdefault("settings", {})
    if not isinstance(settings, dict):
        raise ValueError("watchlist settings must be an object")
    for key, default, maximum in (("window_months", 24, 120), ("group_limit", 500, 5000), ("contract_id_limit", 500, 5000), ("buyer_limit", 5, 20)):
        value = settings.get(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
            raise ValueError(f"{key} must be an integer between one and {maximum}")
    thresholds = settings.get("thresholds", {})
    if not isinstance(thresholds, dict):
        raise ValueError("thresholds must be an object")
    for key, value in thresholds.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{key} must be a finite nonnegative threshold")
    if thresholds.get("supplier_share", .25) > 1:
        raise ValueError("supplier_share cannot exceed one")
    return config


def _fields(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key.casefold(), item
            yield from _fields(item)
    elif isinstance(value, list):
        for item in value:
            yield from _fields(item)


def _bill_ids(text: str) -> set[tuple[str, str, str]]:
    folded = "".join(c for c in unicodedata.normalize("NFKD", text.casefold()) if not unicodedata.combining(c))
    found = {(str(int(number)), year, chamber) for number, year, chamber in _BILL.findall(folded)}
    found.update((str(int(number)), year, chamber) for year, chamber, number in re.findall(r"bill:(20\d{2}):(camara|senado):0*(\d+)\b", folded))
    return found


def match_record(entry: dict, record: dict, *, divipola: list[dict] = ()) -> str | None:
    identifiers = entry["identifiers"]
    fields = list(_fields(record))
    expected = normalize_nit(str(identifiers.get("nit", ""))) if identifiers.get("nit") else None
    doc_types = [normalize_name(value) for key, value in fields if key in _DOCUMENT_FIELDS and isinstance(value, str)]
    if expected:
        for key, value in fields:
            if key not in _NIT_FIELDS or not isinstance(value, (str, int)):
                continue
            if key in {"documento_proveedor", "identificacion_del_contratista", "documento_contratista"} and any(value not in {"nit", "n i t"} for value in doc_types):
                continue
            if normalize_nit(str(value), expected_base=expected) == expected:
                return "nit"
    haystack = json.dumps(record, ensure_ascii=False, sort_keys=True)
    if identifiers.get("bills") and _bill_ids(haystack) & _bill_ids(" ".join(identifiers["bills"])):
        return "bill"
    if identifiers.get("decrees"):
        keys = lambda texts: {(x["kind"], x["number"], x["year"]) for x in parse_legal_act_records(re.sub(r"(Decreto\s*\d+)/(20\d{2})", r"\1 de \2", texts, flags=re.I))}
        if keys(haystack) & keys(" ".join(identifiers["decrees"])):
            return "decree"
    codes = {normalize_divipola(c) for c in identifiers.get("divipola", [])}
    if codes:
        for key, value in fields:
            if key in {"cod_mpio", "codigo_municipio", "divipola", "divipola_code", "cod_dpto", "codigo_departamento"} and normalize_divipola(value) in codes:
                return "divipola"
    normalized = normalize_name(haystack)
    names = list(identifiers.get("names", []))
    for row in divipola:
        if row.get("cod_mpio") in codes:
            names.append(row.get("nom_mpio", ""))
        if row.get("cod_dpto") in codes:
            names.append(row.get("dpto", ""))
    if any(name and re.search(r"(?<!\w)" + re.escape(normalize_name(name)) + r"(?!\w)", normalized) for name in names):
        return "name_candidate"
    if any(re.search(r"(?<!\w)" + re.escape(normalize_name(word)) + r"(?!\w)", normalized) for word in identifiers.get("keywords", [])):
        return "keyword_candidate"
    return None


def _records(payload):
    if isinstance(payload, list):
        for item in payload:
            yield from _records(item)
    elif isinstance(payload, dict):
        if any(isinstance(payload.get(key), str) and payload[key].startswith("https://") for key in ("url", "source_url")):
            yield payload
        else:
            for value in payload.values():
                if isinstance(value, (list, dict)):
                    yield from _records(value)


def build_watchlist_hits(run_dir: Path, config: dict, *, cache_dir: Path, metasources_path: Path | None = None) -> dict:
    run_date = date.fromisoformat(run_dir.name).isoformat()
    official_ids = None
    if metasources_path:
        official_ids = {source.id for source in load_metasources(metasources_path) if source.trust_role not in {"media_signal", "polling_signal"}}
    catalog_path = cache_dir / "divipola.json"
    catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else {}
    payload = {"schema_version": "watchlist_hits.v1", "run_date": run_date, "hits": [], "patterns": [], "coverage_gaps": [], "demo_watch_ids": []}
    if catalog:
        payload["divipola_vintage"] = {key: catalog.get(key) for key in ("vintage", "rows_updated_at", "fetched_at", "source_url")}
    if any(entry["identifiers"].get("divipola") for entry in config["entries"]) and not catalog.get("rows"):
        payload["coverage_gaps"].append("DANE DIVIPOLA cache unavailable; only configured names and exact codes can match.")
    seen = set()
    for entry in config["entries"]:
        if not entry.get("active", True):
            continue
        if entry.get("demo"):
            payload["demo_watch_ids"].append(entry["watch_id"])
        for artifact in ARTIFACTS:
            path = run_dir / artifact
            if not path.exists():
                payload["coverage_gaps"].append(f"{artifact} unavailable for {entry['watch_id']}")
                continue
            for record in _records(json.loads(path.read_text(encoding="utf-8"))):
                source_id = str(record.get("source_id") or record.get("indicator_id") or "official_record")
                if official_ids is not None and source_id not in official_ids and not record.get("indicator_id") and source_id != "official_record":
                    continue
                matched = match_record(entry, record, divipola=catalog.get("rows", []))
                if not matched:
                    continue
                url = record.get("url") or record.get("source_url")
                if not isinstance(url, str) or urlsplit(url).scheme != "https":
                    continue
                key = (entry["watch_id"], source_id, url)
                if key in seen:
                    continue
                seen.add(key)
                # Contract text can name an individual even when a place/topic
                # was the match. Do not copy any supplier identity into hits.
                procurement = source_id.startswith("secop")
                title = f"Public contract record matching {entry['label']}" if procurement else str(record.get("title") or record.get("name") or entry["label"])
                payload["hits"].append({"watch_id": entry["watch_id"], "kind": entry["kind"], "source_id": source_id,
                    "title": title, "url": url, "matched_on": matched,
                    "summary": f"{'A candidate text match' if matched.endswith('candidate') else 'An exact identifier match'} for {entry['label']}; review the official record.",
                    "evidence_ref": {"artifact": artifact, "locator": f"id={record.get('id') or record.get('record_id') or url}"}, "demo": bool(entry.get("demo"))})
        history_path = cache_dir / f"{entry['watch_id']}.json"
        if history_path.exists():
            history = json.loads(history_path.read_text(encoding="utf-8"))
            try:
                fetched_day = datetime.fromisoformat(history.get("fetched_at", "").replace("Z", "+00:00")).astimezone(ZoneInfo("America/Bogota")).date().isoformat()
            except ValueError:
                fetched_day = "9999"
            if history.get("as_of", "9999") <= run_date and fetched_day <= run_date:
                payload["patterns"].extend(history.get("patterns", []))
                payload["coverage_gaps"].extend(f"{entry['watch_id']}: {gap}" for gap in history.get("coverage_gaps", []))
                payload.setdefault("history_vintages", []).append({"watch_id": entry["watch_id"], "as_of": history.get("as_of"), "fetched_at": history.get("fetched_at")})
                if history["as_of"] < run_date:
                    payload["coverage_gaps"].append(f"{entry['watch_id']}: history ends {history['as_of']}; later changes are not in this cache")
            else:
                payload["coverage_gaps"].append(f"{entry['watch_id']}: later history cache excluded from this run")
        elif entry["kind"] in {"company", "public_entity"}:
            payload["coverage_gaps"].append(f"{entry['watch_id']}: history has not been fetched")
    payload["coverage_gaps"] = list(dict.fromkeys(payload["coverage_gaps"]))
    issues = validate(payload)
    if issues:
        raise ValueError("; ".join(issues))
    return payload


def render_watchlist_hits(payload: dict) -> str:
    lines = [f"# Watchlist — {payload['run_date']}", ""]
    if payload.get("demo_watch_ids"):
        lines += ["Includes clearly labeled demo organizations; replace them with your own watchlist.", ""]
    for hit in payload["hits"]:
        lines.append(f"- [{hit['title']}]({hit['url']}) — {hit['summary']} Match: {hit['matched_on']}.")
    if not payload["hits"]:
        lines.append("No matching records were captured in these daily artifacts.")
    for pattern in payload["patterns"]:
        lines.extend(["", f"- {pattern['plain_text']} {pattern['caveat']}"])
    if payload.get("coverage_gaps"):
        lines += ["", "Data coverage:"] + [f"- {gap}" for gap in payload["coverage_gaps"]]
    return "\n".join(lines) + "\n"
