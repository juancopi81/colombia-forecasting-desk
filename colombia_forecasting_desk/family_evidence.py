"""Official source intake for comparators and archived first-release values.

Network belongs here, never in a planner, validator, renderer or resolver.
Collected source bytes are preserved and rechecked before resolution.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
from pathlib import Path
import posixpath
import re
import unicodedata
from urllib.parse import urljoin, urlsplit
import xml.etree.ElementTree as ET
import zipfile

from bs4 import BeautifulSoup
import httpx
import pdfplumber

from .forecast_families import BOGOTA, _instant, _number, _official_url, _read, _rows, _write, load_forecast_families
from .indicator_watch import ipc_observation_from_html, ise_observation_from_html
from .source_fetching.dane import _xlsx_cell_text, _xlsx_shared_strings

MONTHS = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12}
FULL_MONTHS = {name: index for index, name in enumerate(("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"), 1)}
NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
DANE_SPECS = {
    "ipc_annual_up": ("IPC", "ipc_inflation", "annual_variation_pct", "headline_annual"),
    "ise_annual_up": ("ISE", "ise_activity", "annual_growth_pct", "original_total_annual"),
    "unemployment_down_yoy": ("GEIH", "labor_market", "national_unemployment_rate_pct", "national_original_unemployment"),
}


def _fold(value: str) -> str:
    return " ".join("".join(char for char in unicodedata.normalize("NFKD", value.casefold()) if not unicodedata.combining(char)).split())


def _column_number(column: str) -> int:
    total = 0
    for char in column:
        total = total * 26 + ord(char) - ord("A") + 1
    return total


def _date_in_text(text: str) -> str | None:
    match = re.search(r"\b(\d{1,2})\s+de\s+(" + "|".join(FULL_MONTHS) + r")\s+de\s+(20\d{2})\b", _fold(text))
    if match:
        return date(int(match[3]), FULL_MONTHS[match[2]], int(match[1])).isoformat()
    return None


def parse_geih_history(content: bytes) -> dict:
    """Parse only monthly national TD, with exact raw-cell provenance."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if sum(info.file_size for info in archive.infolist()) > 100_000_000:
            raise ValueError("GEIH workbook exceeds the bounded extraction size")
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        rels = {row.attrib["Id"]: row.attrib["Target"] for row in ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))}
        sheet = next((row for row in workbook.findall(".//s:sheet", NS) if _fold(row.attrib.get("name", "")) == "total nacional"), None)
        if sheet is None:
            raise ValueError("GEIH annex has no exact Total nacional worksheet")
        target = rels.get(sheet.attrib.get(REL, ""), "")
        path = target.lstrip("/") if target.startswith("/") else posixpath.normpath(posixpath.join("xl", target))
        if not path.startswith("xl/"):
            raise ValueError("Invalid GEIH worksheet relationship")
        strings = _xlsx_shared_strings(archive)
        rows = []
        for element in ET.fromstring(archive.read(path)).findall(".//s:row", NS):
            cells = {}
            for cell in element.findall("s:c", NS):
                ref = cell.attrib.get("r", "")
                column = re.match(r"[A-Z]+", ref)
                value = _xlsx_cell_text(cell, strings)
                if column and value:
                    cells[column[0]] = value
            if cells:
                rows.append((element.attrib.get("r", ""), cells))
    section = next((index for index, (_, cells) in enumerate(rows) if "serie mensual" in _fold(cells.get("A", ""))), None)
    if section is None or not any("total nacional" == _fold(cells.get("A", "")) for _, cells in rows[:section]):
        raise ValueError("The first national monthly GEIH section is not identified")
    header = next((index for index in range(section + 1, min(section + 5, len(rows))) if _fold(rows[index][1].get("A", "")) == "concepto"), None)
    if header is None or header + 1 >= len(rows):
        raise ValueError("GEIH national monthly year/month headers are missing")
    years, months = rows[header][1], rows[header + 1][1]
    rate = None
    for row_number, cells in rows[header + 2:header + 12]:
        if "serie" in _fold(cells.get("A", "")) or _fold(cells.get("A", "")).startswith("total"):
            break
        if _fold(cells.get("A", "")) in ("tasa de desocupacion (td)", "tasa de desempleo (td)"):
            rate = (row_number, cells)
            break
    if rate is None:
        raise ValueError("GEIH national monthly TD row is missing")
    year = None
    observations = []
    for column in sorted(months, key=_column_number):
        if column in years and re.fullmatch(r"20\d{2}", years[column]):
            year = int(years[column])
        month = MONTHS.get(_fold(months[column])[:3])
        value = rate[1].get(column)
        if year is None or month is None or value is None:
            continue
        try:
            raw_value = float(value.replace(",", "."))
        except ValueError:
            continue
        if not _number(raw_value) or not 0 <= raw_value <= 100:
            raise ValueError("Invalid national unemployment percentage in official annex")
        observations.append({"period": f"{year:04d}-{month:02d}", "national_unemployment_rate_pct": round(raw_value, 1), "raw_value": raw_value, "precision_decimal_places": 1, "locator": f"Total nacional!{column}{rate[0]}"})
    if not observations or len({row["period"] for row in observations}) != len(observations):
        raise ValueError("GEIH monthly history is empty or has ambiguous periods")
    publication = next((_date_in_text(cells.get("A", "")) for _, cells in rows if _fold(cells.get("A", "")).startswith("actualizado")), None)
    if publication is None:
        raise ValueError("GEIH annex has no identifiable official update date")
    for row in observations:
        row["published_at"] = publication
    return {"rows": observations, "published_at": publication, "sheet_name": sheet.attrib["name"], "series_variant": "national_original_unemployment", "caveat": "Current official revised history, rounded to DANE's published one-decimal national percentage. The original cell value is preserved. Revised/backcast history is not an archived first-release resolver and methodology changes limit long-window comparisons."}


def pdf_text_and_metadata(content: bytes) -> tuple[str, dict]:
    if not content.startswith(b"%PDF"):
        raise ValueError("The selected first-release source is not a PDF")
    with pdfplumber.open(io.BytesIO(content)) as document:
        text = "\n".join(page.extract_text() or "" for page in document.pages[:12])
        metadata = dict(document.metadata or {})
    if len(text.strip()) < 60:
        raise ValueError("The official bulletin has no readable first-release text")
    return text, metadata


def _document_period(source_url: str, operation: str) -> str | None:
    filename = urlsplit(source_url).path.rsplit("/", 1)[-1]
    match = re.fullmatch(r"bol-" + operation + r"-([a-z]{3})(20\d{2})\.pdf", filename, re.I)
    if match and match[1].lower() in MONTHS:
        return f"{match[2]}-{MONTHS[match[1].lower()]:02d}"
    return None


def parse_first_release(content: bytes, *, source_url: str, event: dict, family: str, last_modified: str | None = None) -> dict:
    operation, _, value_key, variant = DANE_SPECS[family]
    if not re.fullmatch(r"20\d{2}-(?:0[1-9]|1[0-2])", str(event.get("reference_period", ""))):
        raise ValueError("The declared reference period must be an exact calendar month")
    if not _official_url(source_url, family) or _document_period(source_url, operation) != event["reference_period"]:
        raise ValueError("A specific official technical bulletin for the exact target period is required")
    if event.get("series_variant") != variant:
        raise ValueError("The bulletin intake does not match the declared series or geography")
    text, metadata = pdf_text_and_metadata(content)
    folded = _fold(text)
    if "boletin tecnico" not in folded[:300]:
        raise ValueError("The source is not an identifiable DANE technical bulletin")
    publication = _date_in_text(text[:500])
    if publication != event["event_date"]:
        raise ValueError("Bulletin publication date does not match the declared release event")
    # Routine notes about historical revisions/provisional data are legitimate.
    # A correction of this bulletin or later file modification is different.
    if re.search(r"fe de erratas|boletin (?:corregido|actualizado)|correccion (?:del|de este) boletin", folded[:1400]):
        raise ValueError("The target bulletin identifies a correction; first-release value needs review")
    if last_modified:
        try:
            modified = parsedate_to_datetime(last_modified).astimezone(BOGOTA).date().isoformat()
        except (ValueError, TypeError):
            raise ValueError("The source has an unreadable Last-Modified timestamp") from None
        if modified > publication:
            raise ValueError("The official bulletin was modified after the first-release date")
    for key in ("ModDate", "CreationDate"):
        match = re.match(r"D:(\d{4})(\d{2})(\d{2})", str(metadata.get(key, "")))
        if match and f"{match[1]}-{match[2]}-{match[3]}" > publication:
            raise ValueError("PDF metadata shows a later document version")
    value = None
    if family == "ipc_annual_up":
        if "total nacional" not in folded[:1500]:
            raise ValueError("The IPC bulletin does not identify the national headline series")
        if "sin alimentos" in folded[:500] or "sin regulados" in folded[:500]:
            raise ValueError("Core inflation is not the headline IPC series")
        month_name = next(name for name, number in FULL_MONTHS.items() if number == int(event["reference_period"][5:7]))
        annual = re.search(rf"en (?:el mes de )?{month_name} de {event['reference_period'][:4]}\s*,?.{{0,100}}?variacion anual del ipc fue\s+(-?\d+(?:[,.]\d+)?)\s*%", folded)
        if annual is None:
            annual = re.search(rf"en (?:el mes de )?{month_name} de {event['reference_period'][:4]}\s*,?\s*el ipc registro una variacion de\s+(-?\d+(?:[,.]\d+)?)\s*%\s+en comparacion con {month_name} de {int(event['reference_period'][:4]) - 1}", folded)
        if annual:
            value = float(annual[1].replace(",", "."))
            observation = None
        else:
            observation = ipc_observation_from_html(text)
    elif family == "ise_annual_up":
        observation = ise_observation_from_html(text)
    else:
        month_name = next(name for name, number in FULL_MONTHS.items() if number == int(event["reference_period"][5:7]))
        year = event["reference_period"][:4]
        pattern = rf"(?:para (?:el mes de )?|en ){month_name} de {year}\w*\s*,?\s*la tasa de (?:desocupacion|desempleo) del total nacional fue\s+(-?\d+(?:[,.]\d+)?)\s*%"
        matches = list(re.finditer(pattern, folded))
        if not matches or len({match[1] for match in matches}) != 1:
            raise ValueError("Exact national monthly unemployment headline is missing or contradictory")
        value = float(matches[0][1].replace(",", "."))
        observation = None
    if family == "ise_annual_up" or family == "ipc_annual_up" and observation is not None:
        if observation is None or observation.period != event["reference_period"]:
            raise ValueError("The bulletin does not expose the exact target-period series")
        value = observation.values.get(value_key)
    if not _number(value):
        raise ValueError("The official first-release value is unavailable")
    if family == "unemployment_down_yoy":
        value = round(value, 1)
    return {"value": float(value), "period": event["reference_period"], "series_variant": variant, "value_key": value_key, "event_date": publication, "published_at": publication + "T00:00:00-05:00", "source_url": source_url, "last_modified": last_modified, "pdf_metadata": metadata, "document_kind": "dated_original_technical_bulletin", "first_release_verified": True, "extraction_scope": "first twelve pages; exact period and original national series", "text_sha256": hashlib.sha256(text.encode()).hexdigest()}


def _download(client: httpx.Client, url: str) -> tuple[bytes, str, str | None]:
    if not _official_url(url, "ipc_annual_up"):
        raise ValueError("Evidence must be fetched from an HTTPS DANE URL")
    response = client.get(url)
    response.raise_for_status()
    if not _official_url(str(response.url), "ipc_annual_up"):
        raise ValueError("The official evidence URL redirected outside DANE")
    if len(response.content) > 25_000_000:
        raise ValueError("Official evidence exceeds the bounded 25 MB download")
    return response.content, str(response.url), response.headers.get("last-modified")


def _preserve(run_dir: Path, content: bytes, suffix: str) -> tuple[str, str]:
    digest = hashlib.sha256(content).hexdigest()
    relative = Path("sources/family_evidence") / (digest + suffix)
    path = run_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() != content:
        raise ValueError("A preserved evidence hash already has different bytes")
    path.write_bytes(content)
    return str(relative), digest


def _card(run_dir: Path, identity: str) -> dict:
    cards = _read(run_dir / "indicator_watch.json", [])
    return next((card for card in cards if card.get("indicator_id") == identity), {})


def discover_document_urls(run_dir: Path, identity: str) -> list[str]:
    card = _card(run_dir, identity)
    documents = (card.get("values") or {}).get("official_documents") or []
    return sorted({document["url"] for document in documents if isinstance(document, dict) and _official_url(document.get("url"), "ipc_annual_up")})


def capture_geih_history(run_dir: Path, *, client: httpx.Client, source_url: str | None = None, now: datetime | None = None) -> dict:
    clock = now or datetime.now(timezone.utc)
    urls = [source_url] if source_url else [url for url in discover_document_urls(run_dir, "labor_market") if re.search(r"/anex-GEIH-[a-z]{3}20\d{2}\.xlsx$", urlsplit(url).path, re.I)]
    if len(urls) != 1:
        raise ValueError("Identify the current GEIH national annex in the captured labor card, or supply --history-url")
    content, final_url, modified = _download(client, urls[0])
    try:
        parsed = parse_geih_history(content)
    except (zipfile.BadZipFile, ET.ParseError, KeyError) as exc:
        raise ValueError(f"The official GEIH annex is not a readable national workbook: {exc}") from exc
    if parsed["published_at"] > run_dir.name or clock.astimezone(BOGOTA).date().isoformat() != run_dir.name:
        raise ValueError("History intake must record its actual capture date, without future knowledge")
    artifact_path, digest = _preserve(run_dir, content, ".xlsx")
    artifact = {"schema_version": "family_reference_history.v1", "run_date": run_dir.name, "captured_at": clock.isoformat(), "family_id": "unemployment_down_yoy", "source_url": final_url, "source_artifact": artifact_path, "source_sha256": digest, "last_modified": modified, **parsed}
    path = run_dir / "family_reference_history.json"
    if path.exists() and _read(path).get("source_sha256") != digest:
        raise ValueError("Same-day reference history is already frozen with a different official document")
    if not path.exists():
        _write(path, artifact)
    return _read(path)


def reference_history_for_run(run_dir: Path) -> dict | None:
    """Find the latest already-known, byte-backed national history snapshot."""
    candidates = sorted((path for path in run_dir.parent.iterdir() if path.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name) and path.name <= run_dir.name), reverse=True)
    for directory in candidates:
        artifact = _read(directory / "family_reference_history.json")
        if not isinstance(artifact, dict) or artifact.get("schema_version") != "family_reference_history.v1":
            continue
        try:
            capture_date = _instant(str(artifact.get("captured_at", ""))).astimezone(BOGOTA).date().isoformat()
        except ValueError:
            continue
        if artifact.get("series_variant") != "national_original_unemployment" or not _official_url(artifact.get("source_url"), "unemployment_down_yoy") or capture_date > run_dir.name:
            continue
        relative = Path(str(artifact.get("source_artifact", "")))
        if relative.is_absolute() or ".." in relative.parts:
            continue
        source = directory / relative
        if not source.exists() or hashlib.sha256(source.read_bytes()).hexdigest() != artifact.get("source_sha256"):
            continue
        parsed = parse_geih_history(source.read_bytes())
        if parsed["rows"] != artifact.get("rows"):
            continue
        return artifact
    return None


def capture_first_release(run_dir: Path, row: dict, *, client: httpx.Client, source_url: str | None = None, now: datetime | None = None) -> dict:
    clock = now or datetime.now(timezone.utc)
    family = row.get("forecast_family") or row.get("family_id")
    event = row.get("family_event") or row
    if family not in DANE_SPECS or event["event_date"] > run_dir.name or clock.astimezone(BOGOTA).date().isoformat() != run_dir.name:
        raise ValueError("Only an occurred DANE event can have a first-release capture on the actual run date")
    operation, indicator, _, _ = DANE_SPECS[family]
    urls = [source_url] if source_url else [url for url in discover_document_urls(run_dir, indicator) if _document_period(url, operation) == event["reference_period"]]
    if not urls and source_url is None:
        # IPC currently exposes bulletin links on its operation page rather than
        # in the card values. Follow that captured official page, preserving it.
        page_url = _card(run_dir, indicator).get("source_url")
        if page_url:
            page, final_page_url, _ = _download(client, page_url)
            _preserve(run_dir, page, ".html")
            urls = sorted({urljoin(final_page_url, anchor["href"]) for anchor in BeautifulSoup(page, "html.parser").find_all("a", href=True) if _document_period(urljoin(final_page_url, anchor["href"]), operation) == event["reference_period"]})
    if len(urls) != 1:
        raise ValueError(f"The exact {operation} {event['reference_period']} bulletin is unavailable or ambiguous; supply --release-url after official follow-up")
    content, final_url, modified = _download(client, urls[0])
    parsed = parse_first_release(content, source_url=final_url, event=event, family=family, last_modified=modified)
    artifact_path, digest = _preserve(run_dir, content, ".pdf")
    observation = {"slot_id": row.get("slot_id") or f"{family}:{event['event_date']}", "family_id": family, "captured_at": clock.isoformat(), "source_artifact": artifact_path, "source_sha256": digest, **parsed}
    path = run_dir / "family_release_evidence.json"
    artifact = _read(path, {"schema_version": "family_release_evidence.v1", "run_date": run_dir.name, "observations": [], "notices": []})
    prior = next((item for item in artifact["observations"] if item["slot_id"] == observation["slot_id"]), None)
    if prior and prior["source_sha256"] != digest:
        raise ValueError("Conflicting versions of the same first release need review; preserved evidence is immutable")
    if not prior:
        artifact["observations"].append(observation)
        _write(path, artifact)
    return prior or observation


def verified_release_for_row(row: dict, run_dir: Path) -> dict | None:
    artifact = _read(run_dir / "family_release_evidence.json", {})
    if not isinstance(artifact, dict) or artifact.get("schema_version") != "family_release_evidence.v1":
        return None
    for recorded in artifact.get("observations", []):
        if recorded.get("slot_id") != row.get("slot_id") or recorded.get("family_id") != row.get("forecast_family") or recorded.get("first_release_verified") is not True:
            continue
        relative = Path(str(recorded.get("source_artifact", "")))
        if relative.is_absolute() or ".." in relative.parts:
            continue
        path = run_dir / relative
        if not path.exists():
            continue
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != recorded.get("source_sha256"):
            continue
        try:
            parsed = parse_first_release(content, source_url=recorded["source_url"], event=row["family_event"], family=row["forecast_family"], last_modified=recorded.get("last_modified"))
            if _instant(recorded["captured_at"]).astimezone(BOGOTA).date().isoformat() != run_dir.name:
                continue
        except (ValueError, KeyError, TypeError):
            continue
        if any(recorded.get(key) != value for key, value in parsed.items()):
            continue
        return {**parsed, "sha256": recorded["source_sha256"], "artifact": f"runs/{run_dir.name}/{recorded['source_artifact']}", "locator": f"family_release_evidence.json:slot_id={row['slot_id']}", "captured_at": recorded["captured_at"]}
    return None
