"""Validate story memory, find candidate evidence, and freeze daily snapshots."""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import unicodedata
from urllib.parse import urlsplit

import yaml
from bs4 import BeautifulSoup

from .schemas.case_file import validate

ARTIFACTS = {
    "legislative_reconciler.json": None,
    "cleaned_items.json": None,
    "indicator_watch.json": None,
    "analyst_leads.json": "leads",
    "m2_review_packet.json": "review_items",
}
_BILL = re.compile(r"\b(?:PL|Proyecto\s+de\s+Ley)\s*(?:No\.?\s*)?0*(\d+)\s*(?:/|de\s*)\s*(\d{4})\s*(Camara|Senado)\b", re.I)
_DECREE = re.compile(r"\bDecreto\s*(?:numero\s*)?0*(\d+)\s*(?:/|de\s*)\s*(\d{4})\b", re.I)


def _normalize(value: str) -> str:
    return " ".join("".join(c for c in unicodedata.normalize("NFKD", value.casefold()) if not unicodedata.combining(c)).split())


def _json_dates(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_dates(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_dates(item) for item in value]
    return value


def load_cases(case_dir: Path) -> list[dict]:
    cases = []
    for path in sorted(case_dir.glob("*.yaml")):
        case = _json_dates(yaml.safe_load(path.read_text(encoding="utf-8")))
        issues = validate(case)
        if issues:
            raise ValueError(f"{path}: {'; '.join(issues)}")
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", case["case_id"]):
            raise ValueError(f"{path}: unsafe case_id")
        if path.stem != case["case_id"]:
            raise ValueError(f"{path}: filename must equal case_id")
        cases.append(case)
    ids = [case["case_id"] for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate case_id")
    return cases


def _bill_ids(record: dict) -> set[tuple[str, str, str]]:
    result = set()
    for item in [record.get("origin_project"), *record.get("linked_projects", [])]:
        if isinstance(item, dict) and all(item.get(k) for k in ("number", "year", "chamber")):
            number = str(item["number"])
            if number.isdigit():
                result.add((str(int(number)), str(item["year"]), _normalize(item["chamber"])))
    text = _normalize(json.dumps(record, ensure_ascii=False))
    result.update((str(int(n)), year, chamber.casefold()) for n, year, chamber in _BILL.findall(text))
    for year, chamber, number in re.findall(r"bill:(\d{4}):(camara|senado):0*(\d+)\b", text):
        result.add((str(int(number)), year, chamber))
    return result


def _urls(value) -> list[str]:
    if isinstance(value, dict):
        return [url for key, item in value.items() for url in ([item] if key in {"url", "source_url"} and isinstance(item, str) else _urls(item))]
    if isinstance(value, list):
        return [url for item in value for url in _urls(item)]
    return []


def _official_url(record: dict) -> str | None:
    for url in _urls(record):
        host = urlsplit(url).hostname or ""
        if url.startswith("https://") and (host.endswith(".gov.co") or host == "xm.com.co" or host.endswith(".xm.com.co")):
            return url
    return None


def match_case(case: dict, record: dict) -> list[str]:
    """Return exact identifier and bounded keyword matches, never conclusions."""
    identifiers = case["identifiers"]
    text = _normalize(json.dumps(record, ensure_ascii=False))
    matched = []
    record_bills = _bill_ids(record)
    for bill in identifiers["bills"]:
        if record_bills & _bill_ids({"title": bill}):
            matched.append(f"bill:{bill}")
    decree_ids = {(str(int(n)), year) for n, year in _DECREE.findall(text)}
    for decree in identifiers["decrees"]:
        keys = {(str(int(n)), year) for n, year in _DECREE.findall(_normalize(decree))}
        if keys & decree_ids:
            matched.append(f"decree:{decree}")
    for indicator in identifiers["indicator_ids"]:
        if record.get("indicator_id") == indicator:
            matched.append(f"indicator:{indicator}")
    for nit in identifiers["nits"]:
        digits = re.sub(r"\D", "", nit)
        if digits and re.search(r"(?<!\d)" + re.escape(digits) + r"(?!\d)", re.sub(r"(?<=\d)[.\-](?=\d)", "", text)):
            matched.append(f"nit:{nit}")
    # Full contextual text may contain adjacent bills. A keyword hit is explicitly
    # a weaker candidate, with its excerpt preserved for editorial review.
    for keyword in identifiers["keywords"]:
        if re.search(r"(?<!\w)" + re.escape(_normalize(keyword)) + r"(?!\w)", text):
            matched.append(f"keyword:{keyword}")
    return sorted(set(matched))


def build_case_updates(run_dir: Path, cases: list[dict]) -> dict:
    run_date = date.fromisoformat(run_dir.name)
    rows = []
    gaps = []
    for name, key in ARTIFACTS.items():
        path = run_dir / name
        if not path.exists():
            gaps.append(f"{name} is unavailable; no absence inference is possible.")
            continue
        value = json.loads(path.read_text(encoding="utf-8"))
        items = value.get(key, []) if key and isinstance(value, dict) else value
        if not isinstance(items, list):
            raise ValueError(f"{path} must contain a record list")
        rows.extend((name, index, row) for index, row in enumerate(items) if isinstance(row, dict))
    # A bounded follow-up can preserve official evidence that the ticker omitted.
    # Accept only same-run, hash-verified successful captures; a receipt alone is
    # not source content and must never create a match.
    for receipt_path in sorted((run_dir / "sources").glob("*_receipt.json")):
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("status") != 200 or receipt.get("tls_verification") is not True:
            continue
        artifact = str(receipt.get("artifact", ""))
        prefix = f"runs/{run_dir.name}/"
        relative = artifact[len(prefix):] if artifact.startswith(prefix) else artifact
        captured = (run_dir / relative).resolve()
        if not captured.is_relative_to(run_dir.resolve()) or not captured.is_file() or captured.suffix.lower() != ".html":
            continue
        raw = captured.read_bytes()
        if hashlib.sha256(raw).hexdigest() != receipt.get("sha256"):
            gaps.append(f"{receipt_path.name} has no matching source hash.")
            continue
        soup = BeautifulSoup(raw, "html.parser")
        body = soup.find("main") or soup
        record = {"id": receipt_path.stem, "source_id": receipt.get("source_id"), "url": receipt.get("url"), "title": soup.title.get_text(" ", strip=True) if soup.title else "Captured official follow-up", "clean_text": body.get_text(" ", strip=True)}
        rows.append((str(captured.relative_to(run_dir.resolve())), 0, record))
    results = []
    for case in cases:
        candidates = []
        seen = set()
        for artifact, index, record in rows:
            url = _official_url(record)
            matches = match_case(case, record) if url else []
            if not matches:
                continue
            identity = str(record.get("id") or record.get("canonical_bill_id") or record.get("indicator_id") or record.get("lead_id") or record.get("packet_item_id") or index)
            fingerprint = (artifact, identity, url)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            excerpt = str(record.get("clean_text") or record.get("raw_text") or record.get("headline") or record.get("display_title") or record.get("claim_or_question") or record.get("title") or "Official candidate evidence")[:1800]
            candidates.append({"artifact": artifact, "locator": f"index={index};id={identity}", "matched_on": matches, "source_url": url, "source_id": str(record.get("source_id") or record.get("source_name") or "official"), "excerpt": excerpt})
        last_update = max([date.fromisoformat(case["current_view"]["date"]), *[date.fromisoformat(u["date"]) for u in case["updates"]]])
        if last_update > run_date:
            raise ValueError(f"{case['case_id']}: future knowledge cannot enter {run_date}")
        idle_days = (run_date - last_update).days
        park = not candidates and not gaps and idle_days >= case["park_after_days"] and case["status"] == "active"
        results.append({"case_id": case["case_id"], "plain_title": case["plain_title"], "current_view": case["current_view"], "status": case["status"], "candidates": candidates, "days_since_update": idle_days, "suggested_status": "parked" if park else case["status"]})
    return {"schema_version": "case_updates.v1", "run_date": run_date.isoformat(), "cases": results, "coverage_gaps": gaps}


def snapshot_cases(case_dir: Path, run_dir: Path) -> Path:
    cases = load_cases(case_dir)
    day = date.fromisoformat(run_dir.name).isoformat()
    for case in cases:
        dates = [case["current_view"]["date"], *[u["date"] for u in case["updates"]]]
        if any(value > day for value in dates):
            raise ValueError(f"{case['case_id']}: cannot snapshot later knowledge into {day}")
    content = {f"{case['case_id']}.yaml": (case_dir / f"{case['case_id']}.yaml").read_bytes() for case in cases}
    manifest = {"schema_version": "cases_snapshot.v1", "run_date": day, "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(content.items())}}
    content["manifest.json"] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    target = run_dir / "cases_snapshot"
    if target.exists():
        existing = {path.name: path.read_bytes() for path in target.iterdir() if path.is_file()}
        if existing != content:
            raise ValueError(f"{target} is frozen; refusing to replace a different snapshot")
        return target
    run_dir.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".cases_snapshot-", dir=run_dir))
    try:
        for name, data in content.items():
            (stage / name).write_bytes(data)
        os.rename(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return target
