"""Bounded, read-only capture of the Cámara commission minutes JSON index.

Index dates and downloaded bytes are evidence for subsequent human review, not
certified session/vote outcomes. No ledger or historical run is modified.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
import httpx


INDEX_URL = "https://www.camara.gov.co/comision-tercera-o-de-hacienda-y-credito-publico/actas-y-otros-comision-tercera-constitucional/"
PAGE_SIZE = 10
MAX_PAGES = 5
MAX_DOCUMENTS = 3
HOSTS = {"www.camara.gov.co", "camara.gov.co"}
COMMISSION = "Comisión Tercera Constitucional Permanente"
CAVEAT = (
    "Completeness describes the returned index query only. An empty or stale "
    "index does not establish absence, a vote result, or NO. Index dates are "
    "unverified document/session dates; PDFs remain unread. No automatic "
    "bill-status or forecast resolution is performed."
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def official_url(value: str, *, base: str = INDEX_URL, endpoint: bool = False) -> str:
    """Resolve official URLs without allowing credentials, foreign hosts or HTTP."""
    if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
        raise ValueError("Missing or invalid official URL")
    parsed = urlsplit(urljoin(base, value.strip()))
    if (parsed.scheme != "https" or parsed.hostname not in HOSTS
            or parsed.username is not None or parsed.password is not None
            or parsed.port not in (None, 443) or "\\" in value):
        raise ValueError("URL is outside the HTTPS Cámara allowlist")
    if endpoint and (parsed.path != "/wp-admin/admin-ajax.php" or parsed.query):
        raise ValueError("Unexpected Cámara AJAX endpoint")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def parse_config(html: bytes) -> dict[str, str]:
    """Read the public page's current config; never execute JavaScript."""
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    for script in soup.find_all("script"):
        for match in re.finditer(r"window\.AP_CFG\s*=\s*\{(.*?)\}\s*;", script.get_text(), re.S):
            config = {}
            for key in ("AJAX_URL", "AY_NONCE", "COMISION"):
                values = re.findall(rf'\b{key}\s*:\s*("(?:\\.|[^"\\])*")', match.group(1))
                if len(values) != 1:
                    raise ValueError(f"Missing or ambiguous AP_CFG {key}")
                config[key] = json.loads(values[0])
                if not isinstance(config[key], str) or not config[key].strip():
                    raise ValueError(f"Empty AP_CFG {key}")
            config["AJAX_URL"] = official_url(config["AJAX_URL"], endpoint=True)
            if config["COMISION"] != COMMISSION:
                raise ValueError("AP_CFG commission is not Comisión Tercera Constitucional Permanente")
            matches.append(config)
    if len(matches) != 1:
        raise ValueError("Missing or ambiguous window.AP_CFG; index shell is not result evidence")
    return matches[0]


def parse_page(content: bytes, page: int) -> tuple[list, int, int]:
    try:
        payload = json.loads(content)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("AJAX returned non-JSON content (possible access/nonce failure)") from exc
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise ValueError("AJAX reported failure (possible access/nonce failure)")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise ValueError("Malformed AJAX data/items")
    total, total_pages = data.get("total"), data.get("total_pages")
    if any(type(value) is not int or value < 0 for value in (total, total_pages)):
        raise ValueError("Malformed pagination totals")
    if total_pages != (total + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("Inconsistent total/total_pages")
    expected = min(PAGE_SIZE, max(0, total - (page - 1) * PAGE_SIZE))
    if len(data["items"]) != expected or (total_pages and page > total_pages):
        raise ValueError("Page length disagrees with pagination totals")
    return data["items"], total, total_pages


def parse_record(raw: object, page: int, position: int) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("Record is not an object")
    identifier = raw.get("id")
    if isinstance(identifier, bool) or not isinstance(identifier, (str, int)) or not str(identifier).isdigit():
        raise ValueError("Record has no numeric id")
    iso = raw.get("fecha_iso")
    if not isinstance(iso, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso):
        raise ValueError("Record has no exact fecha_iso")
    date.fromisoformat(iso)
    for key in ("tipo_publicacion", "titulo"):
        if not isinstance(raw.get(key), str) or not raw[key].strip():
            raise ValueError(f"Record has no {key}")
    result = {
        "id": str(identifier), "index_date": iso, "date_source": "fecha_iso",
        "date_verified_in_document": False, "tipo_publicacion": raw["tipo_publicacion"],
        "titulo": raw["titulo"], "numero": raw.get("numero"),
        "fecha": raw.get("fecha"), "descripcion": raw.get("descripcion"),
        "gaceta_numero": raw.get("gaceta_numero"), "gaceta_anio": raw.get("gaceta_anio"),
        "gaceta_fecha": raw.get("gaceta_fecha"),
        "source_page": page, "source_position": position, "document_url": None,
        "document_status": "no_document_link", "link_notice": None,
    }
    # Secondary links remain in the raw JSON. Only Cámara document links can
    # become download targets; generic Gaceta portals are not document evidence.
    if raw.get("enlace"):
        try:
            result["document_url"] = official_url(raw["enlace"], base="https://www.camara.gov.co/")
            result["document_status"] = "not_downloaded"
        except ValueError as exc:
            result["document_status"] = "rejected_link"
            result["link_notice"] = str(exc)
    return result


class _Capture:
    def __init__(self, output_dir: Path, client: httpx.Client, report: dict):
        self.output_dir, self.client, self.report = output_dir, client, report

    def fetch(self, url: str, *, label: str, kind: str, data: dict | None = None) -> bytes:
        method = "POST" if data is not None else "GET"
        url = official_url(url, endpoint=kind == "json")
        limit = 25 * 1024 * 1024 if kind == "pdf" else 3 * 1024 * 1024
        for hop in range(4):
            receipt = {
                "requested_url": url, "method": method, "request_started_at": _utc_now(),
                "retrieved_at": None,
                "tls_certificate_verification_enabled": True, "status_code": None,
                "query": {key: value for key, value in (data or {}).items() if key != "_ajax_nonce"},
            }
            self.report["receipts"].append(receipt)
            try:
                with self.client.stream(method, url, data=data, follow_redirects=False) as response:
                    receipt.update(status_code=response.status_code, response_url=str(response.url),
                                   content_type=response.headers.get("content-type"),
                                   last_modified=response.headers.get("last-modified"))
                    body = bytearray()
                    complete = True
                    for chunk in response.iter_bytes():
                        if len(body) + len(chunk) > limit:
                            body.extend(chunk[:limit - len(body)])
                            complete = False
                            break
                        body.extend(chunk)
                    artifact = f"{label}-hop-{hop}.{kind}"
                    (self.output_dir / artifact).write_bytes(body)
                    receipt.update(artifact=artifact, sha256=hashlib.sha256(body).hexdigest(),
                                   byte_count=len(body), body_complete=complete,
                                   retrieved_at=_utc_now(),
                                   body_encoding="HTTP content-decoded response bytes")
                    if not complete:
                        raise ValueError("Response exceeds bounded byte limit; saved bytes are partial")
                    if response.is_redirect:
                        if hop == 3:
                            raise ValueError("Redirect limit exceeded")
                        target = official_url(response.headers.get("location", ""), base=url,
                                              endpoint=kind == "json")
                        if data is not None and response.status_code not in (307, 308):
                            raise ValueError("AJAX redirect would change POST semantics")
                        receipt["redirect_url"] = target
                        url = target
                        continue
                    response.raise_for_status()
                    return bytes(body)
            except (httpx.HTTPError, ValueError) as exc:
                receipt["error"] = str(exc)
                raise
        raise ValueError("Redirect limit exceeded")


def capture_minutes(
    output_dir: Path, *, date_from: str, date_to: str, max_pages: int = 2,
    max_documents: int = 1, transport: httpx.BaseTransport | None = None,
) -> dict:
    """Capture one filtered query; output directory must not already exist.

    The optional transport is for offline regression tests. Production always
    constructs its own TLS-verifying client and validates redirects manually.
    """
    for value in (date_from, date_to):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("Dates must use YYYY-MM-DD")
        date.fromisoformat(value)
    if date_from > date_to:
        raise ValueError("date_from must not follow date_to")
    if not 1 <= max_pages <= MAX_PAGES or not 0 <= max_documents <= MAX_DOCUMENTS:
        raise ValueError(f"Limits: max_pages 1..{MAX_PAGES}; max_documents 0..{MAX_DOCUMENTS}")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": "camara_minutes_capture.v1", "started_at": _utc_now(),
        "index_url": INDEX_URL, "transport": "injected_test_transport" if transport else "network",
        "query": {"fecha_desde": date_from, "fecha_hasta": date_to, "per_page": PAGE_SIZE,
                  "tipo": "All", "term": ""},
        "limits": {"max_pages": max_pages, "max_documents": max_documents},
        "index_status": "unavailable", "total": None, "total_pages": None,
        "pages_captured": 0, "records": [], "documents": [], "receipts": [],
        "diagnostic": None, "notices": [], "caveat": CAVEAT,
    }
    with httpx.Client(timeout=30, verify=True, follow_redirects=False, transport=transport,
                      headers={"User-Agent": "ColombiaForecastingDesk/0.1 bounded-official-evidence"}) as client:
        capture = _Capture(output_dir, client, report)
        try:
            config = parse_config(capture.fetch(INDEX_URL, label="index", kind="html"))
            report["query"]["comision"] = config["COMISION"]
            report["ajax_url"] = config["AJAX_URL"]
            params = {**report["query"], "action": "get_actas_y_otros_page", "_ajax_nonce": config["AY_NONCE"]}
            seen_ids = set()
            record_errors = False
            for page in range(1, max_pages + 1):
                body = capture.fetch(config["AJAX_URL"], label=f"filtered-page-{page}", kind="json",
                                     data={**params, "page": page})
                items, total, total_pages = parse_page(body, page)
                if page > 1 and (total, total_pages) != (report["total"], report["total_pages"]):
                    raise ValueError("Pagination totals changed between pages; snapshot is inconsistent")
                report.update(total=total, total_pages=total_pages, pages_captured=page,
                              index_status="truncated")
                for position, raw in enumerate(items, 1):
                    try:
                        record = parse_record(raw, page, position)
                        if record["id"] in seen_ids:
                            raise ValueError("Duplicate record id across/within pages")
                        seen_ids.add(record["id"])
                        if not date_from <= record["index_date"] <= date_to:
                            raise ValueError("Record date is outside requested filter")
                    except ValueError as exc:
                        record_errors = True
                        report["notices"].append(f"Page {page}, record {position}: {exc}; inspect raw response")
                        continue
                    if record["link_notice"]:
                        report["notices"].append(f"Record {record['id']}: {record['link_notice']}")
                    report["records"].append(record)
                if page >= total_pages:
                    if not record_errors:
                        report["index_status"] = "complete"
                    break
            if report["index_status"] == "truncated":
                report["notices"].append("Index query is incomplete: page cap or invalid/duplicate records")
            if report["index_status"] == "complete" and report["total"] == 0:
                report["notices"].append("No matching index rows; coverage remains unknown, never NO")
                diagnostic = {"scope": "unfiltered first page only; not matching evidence", "status": "unavailable"}
                report["diagnostic"] = diagnostic
                try:
                    body = capture.fetch(config["AJAX_URL"], label="diagnostic-page-1", kind="json",
                                         data={**params, "page": 1, "fecha_desde": "", "fecha_hasta": ""})
                    items, total, total_pages = parse_page(body, 1)
                    dates = [parse_record(raw, 1, i)["index_date"] for i, raw in enumerate(items, 1)]
                    diagnostic.update(status="complete" if total_pages <= 1 else "truncated", total=total,
                                      total_pages=total_pages, returned_dates=dates,
                                      date_min=min(dates, default=None), date_max=max(dates, default=None))
                except (ValueError, httpx.HTTPError) as exc:
                    diagnostic["error"] = str(exc)
                    report["notices"].append(f"Unfiltered diagnostic unavailable: {exc}")
        except (ValueError, httpx.HTTPError) as exc:
            report["notices"].append(f"Index capture incomplete: {exc}")

        downloaded_urls = {}
        for record in report["records"]:
            url = record["document_url"]
            if not url:
                continue
            if url in downloaded_urls:
                record["document_status"] = downloaded_urls[url]["status"]
                record["document_capture"] = downloaded_urls[url]["capture_id"]
                continue
            if not urlsplit(url).path.lower().endswith(".pdf"):
                record["document_status"] = "unsupported_document_type"
                continue
            if len(downloaded_urls) >= max_documents:
                record["document_status"] = "not_downloaded_limit"
                continue
            document = {"capture_id": f"document-{len(downloaded_urls) + 1}", "url": url,
                        "status": "unavailable", "text_extraction": "not_attempted"}
            downloaded_urls[url] = document
            report["documents"].append(document)
            try:
                body = capture.fetch(url, label=document["capture_id"], kind="pdf")
                if not body.startswith(b"%PDF-"):
                    raise ValueError("Document response is not a PDF; retained raw response")
                receipt = report["receipts"][-1]
                document.update(status="downloaded_unread", artifact=receipt["artifact"], sha256=receipt["sha256"])
            except (ValueError, httpx.HTTPError) as exc:
                document["error"] = str(exc)
                report["notices"].append(f"Document {record['id']} unavailable: {exc}")
            record["document_status"] = document["status"]
            record["document_capture"] = document["capture_id"]
    report["finished_at"] = _utc_now()
    (output_dir / "capture.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    # The daily/weekly evidence reader discovers sources/**/receipts.json.
    # Keep the detailed report and expose its same receipts using that contract.
    sources = []
    for receipt in report["receipts"]:
        source = {
            **receipt,
            "url": receipt.get("response_url", receipt["requested_url"]),
            "tls_verified": receipt["tls_certificate_verification_enabled"] if receipt["status_code"] is not None else None,
        }
        if receipt.get("artifact"):
            source["artifact"] = str((output_dir / receipt["artifact"]).resolve())
        sources.append(source)
    manifest = {"schema_version": "camara_minutes_receipts.v1", "finished_at": report["finished_at"], "sources": sources}
    (output_dir / "receipts.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report
