"""Source-checked charts for reader briefs. No network or probability work."""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import html
import json
import math
from pathlib import Path
import re
import shutil
import textwrap
from typing import Any
from urllib.parse import quote, urlsplit
from xml.etree import ElementTree as ET
from zipfile import ZipFile
from zoneinfo import ZoneInfo

BOGOTA = ZoneInfo("America/Bogota")
DATA_FILE = "brief_chart_data.json"
PNG_FILE = "brief_chart.png"
SVG_FILE = "brief_chart.svg"
_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stamp(value: Any) -> datetime:
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("chart timestamps must be timezone-aware")
    return stamp


def day_cutoff(day: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time.max, BOGOTA)


def _path(root: Path, name: Any) -> Path:
    if not isinstance(name, str) or Path(name).is_absolute():
        raise ValueError("chart evidence paths must be relative to the dated run")
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("chart evidence path is missing or outside the dated run")
    return path


def _text(value: Any, label: str, cap: int = 240) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > cap or re.search(r"<[^>]+>", value):
        raise ValueError(f"chart {label} needs bounded plain text")
    return " ".join(value.split())


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise ValueError("chart values must be finite numbers; missing is not zero")
    return value


def _period(value: Any, frequency: str) -> str:
    # A list permits separate year / quarter cells, including merged year headers.
    tokens = value if isinstance(value, list) else [value]
    text = " ".join(str(int(token)) if isinstance(token, (int, float)) and not isinstance(token, bool) and float(token).is_integer() else str(token).strip() for token in tokens)
    text = re.sub(r"(?<=\d)(?:pr|p)\b", "", text, flags=re.I)
    if frequency == "quarterly":
        match = re.fullmatch(r"(\d{4})[\s/-]*(?:Q([1-4])|([IV]+)|([1-4]))", text, re.I)
        if match:
            quarter = match[2] or {"I": "1", "II": "2", "III": "3", "IV": "4"}.get((match[3] or "").upper()) or match[4]
            if quarter:
                return f"{match[1]}-Q{quarter}"
    elif frequency == "annual" and re.fullmatch(r"\d{4}", text):
        return text
    elif frequency == "monthly" and re.fullmatch(r"\d{4}-\d{2}", text):
        date.fromisoformat(text + "-01")
        return text
    elif frequency == "daily" and date.fromisoformat(text).isoformat() == text:
        return text
    raise ValueError("chart period does not match the declared frequency")


def _period_start(period: str) -> date:
    if "-Q" in period:
        year, quarter = period.split("-Q")
        return date(int(year), 3 * int(quarter) - 2, 1)
    return date.fromisoformat(period + ("-01-01" if len(period) == 4 else "-01" if len(period) == 7 else ""))


def _json_value(data: Any, pointer: str) -> Any:
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise ValueError("JSON chart locators must be JSON pointers")
    for part in pointer[1:].split("/"):
        key = part.replace("~1", "/").replace("~0", "~")
        data = data[int(key)] if isinstance(data, list) else data[key]
    return data


def _xlsx_values(path: Path) -> dict[str, Any]:
    """Read original XLSX cells, with no spreadsheet engine or new dependency."""
    result: dict[str, Any] = {}
    with ZipFile(path) as archive:
        percent_styles = set()
        if "xl/styles.xml" in archive.namelist():
            styles = ET.fromstring(archive.read("xl/styles.xml"))
            formats = {int(node.attrib["numFmtId"]): node.attrib.get("formatCode", "") for node in styles.findall("s:numFmts/s:numFmt", _NS)}
            for index, style in enumerate(styles.findall("s:cellXfs/s:xf", _NS)):
                number_format = int(style.attrib.get("numFmtId", "0"))
                if number_format in {9, 10} or "%" in formats.get(number_format, ""):
                    percent_styles.add(index)
        strings = []
        if "xl/sharedStrings.xml" in archive.namelist():
            strings = ["".join(node.itertext()) for node in ET.fromstring(archive.read("xl/sharedStrings.xml")).findall("s:si", _NS)]
        rels = {node.attrib["Id"]: node.attrib["Target"] for node in ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))}
        for sheet in ET.fromstring(archive.read("xl/workbook.xml")).findall("s:sheets/s:sheet", _NS):
            target = rels[sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]]
            member = target.lstrip("/") if target.startswith("/") else "xl/" + target
            for cell in ET.fromstring(archive.read(member)).findall(".//s:sheetData/s:row/s:c", _NS):
                # Formula caches can be missing/stale. Never silently plot one.
                if cell.find("s:f", _NS) is not None:
                    continue
                value = cell.findtext("s:v", namespaces=_NS)
                kind = cell.attrib.get("t")
                if kind == "s" and value is not None:
                    value = strings[int(value)]
                elif kind == "inlineStr":
                    value = "".join(cell.find("s:is", _NS).itertext())
                elif kind in (None, "n") and value is not None:
                    value = float(Decimal(value) * (100 if int(cell.attrib.get("s", "0")) in percent_styles else 1))
                else:
                    continue
                result[sheet.attrib["name"] + "!" + cell.attrib["r"]] = value
    return result


def compile_chart_data(request: dict, run: Path, *, generated_at: str, cutoff: datetime) -> dict:
    """Extract every mark from one archived source and its verified receipt."""
    if request.get("schema_version") != "brief_chart_request.v1":
        raise ValueError("chart request must use brief_chart_request.v1")
    slug = request.get("id", "")
    if not isinstance(slug, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", slug):
        raise ValueError("chart request id must be a short lowercase slug")
    frequency = request.get("frequency")
    if frequency not in {"daily", "monthly", "quarterly", "annual"}:
        raise ValueError("chart frequency must be daily, monthly, quarterly or annual")
    receipt_path = _path(run, request.get("source_receipt"))
    receipt = json.loads(receipt_path.read_text())
    url = receipt.get("source_url")
    host = urlsplit(url).hostname if isinstance(url, str) else None
    if not host or any(c.isspace() or c in '<>"' for c in url) or urlsplit(url).scheme != "https" or urlsplit(url).username or urlsplit(url).password or not (host.endswith(".gov.co") or host.endswith(".xm.com.co")):
        raise ValueError("chart source must be an official Colombian HTTPS source")
    if receipt.get("status_code") != 200 or receipt.get("tls_verified") is not True:
        raise ValueError("chart source receipt needs HTTP 200 and verified TLS")
    captured_at = receipt.get("captured_at") or receipt.get("fetched_at")
    captured = _stamp(captured_at)
    generated = _stamp(generated_at)
    if captured > cutoff or generated > cutoff or captured > generated:
        raise ValueError("chart source or extraction postdates the cutoff")
    published_at = receipt.get("published_at")
    if published_at and _stamp(published_at) > cutoff:
        raise ValueError("chart publication postdates the cutoff")
    original = _path(run, receipt.get("artifact"))
    if digest(original) != receipt.get("sha256"):
        raise ValueError("chart original source hash mismatch")
    content_type = str(receipt.get("content_type", "")).split(";", 1)[0].strip().lower()
    if original.suffix == ".json" and (content_type != "application/json" or urlsplit(url).path.lower().endswith((".pdf", ".xlsx"))):
        raise ValueError("chart JSON must be captured source JSON, not a PDF/XLSX transcription")
    if original.suffix == ".xlsx" and content_type not in {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/octet-stream", "application/zip"}:
        raise ValueError("chart XLSX receipt has an incompatible content type")
    if original.suffix == ".json":
        document = json.loads(original.read_text())
        get = lambda locator: _json_value(document, locator)
    elif original.suffix == ".xlsx":
        document = _xlsx_values(original)
        get = lambda locator: document[locator]
    else:
        raise ValueError("chart source must be captured JSON or XLSX; PDF transcriptions are not verified chart data")
    series = _text(get(request.get("series_locator")), "source series")
    unit = _text(get(request.get("unit_locator")), "source unit", 80)
    title = _text(request.get("title"), "title", 100)
    refs = request.get("points")
    if not isinstance(refs, list) or not 2 <= len(refs) <= 24:
        raise ValueError("chart needs 2–24 source observations")
    points = []
    for ref in refs:
        locators = ref.get("period_locators")
        if not isinstance(locators, list) or not 1 <= len(locators) <= 2:
            raise ValueError("each chart point needs one or two period locators")
        period = _period([get(locator) for locator in locators], frequency)
        value = _number(get(ref.get("value_locator")))
        if _period_start(period) > cutoff.astimezone(BOGOTA).date():
            raise ValueError("chart observation is in the future")
        if original.suffix == ".xlsx":
            # A selected series row/column must actually contain the value cell.
            split = lambda loc: re.fullmatch(r"(.+)!([A-Z]+)([1-9]\d*)", loc)
            cell, header = split(ref["value_locator"]), split(request["series_locator"])
            periods = [split(loc) for loc in locators]
            aligned = lambda a, b: a and b and a[1] == b[1] and (a[2] == b[2] or a[3] == b[3])
            if not aligned(cell, header) or not any(aligned(cell, p) for p in periods):
                raise ValueError("chart cells do not align with the selected series and period")
        else:
            record = ref["value_locator"].rsplit("/", 1)[0]
            series_parent = request["series_locator"].rsplit("/", 1)[0]
            unit_parent = request["unit_locator"].rsplit("/", 1)[0]
            if series_parent != unit_parent or not record.startswith(series_parent + "/") or any(loc.rsplit("/", 1)[0] != record for loc in locators):
                raise ValueError("chart JSON values, periods, series and units must belong to the same series records")
        points.append({"period": period, "value": value})
    periods = [point["period"] for point in points]
    if periods != sorted(set(periods)):
        raise ValueError("chart periods must be unique and chronological")
    # Gaps must not be disguised as adjacent equally spaced observations.
    ordinals = [_ordinal(p, frequency) for p in periods]
    if any(b - a != 1 for a, b in zip(ordinals, ordinals[1:])):
        raise ValueError("chart periods must be consecutive; select a complete bounded window")
    return {"schema_version": "brief_chart_data.v1", "data_id": f"{run.name}:{slug}",
            "run_date": run.name, "generated_at": generated_at, "title": title,
            "series": series, "unit": unit, "frequency": frequency, "points": points,
            "source": {"source_url": url, "artifact": str(original.relative_to(run.resolve())),
                       "sha256": digest(original), "captured_at": captured_at,
                       "receipt_sha256": digest(receipt_path)}, "request": request}


def _ordinal(period: str, frequency: str) -> int:
    if frequency == "quarterly":
        return int(period[:4]) * 4 + int(period[-1])
    if frequency == "monthly":
        return int(period[:4]) * 12 + int(period[-2:])
    if frequency == "annual":
        return int(period)
    return date.fromisoformat(period).toordinal()


def load_chart_data(run: Path, *, cutoff: datetime) -> dict:
    data = json.loads((run / DATA_FILE).read_text())
    expected = compile_chart_data(data["request"], run, generated_at=data["generated_at"], cutoff=cutoff)
    if data != expected:
        raise ValueError("prepared chart differs from the captured source, series, units or periods")
    return data


def validate_chart_decision(brief: dict, *, chart_data: dict | None = None) -> list[str]:
    kind = str(brief.get("schema_version", "")).split(".")[0]
    if brief.get("schema_version") == kind + ".v1":
        return ["chart_decision requires a v2 brief"] if "chart_decision" in brief else []
    decision = brief.get("chart_decision")
    if not isinstance(decision, dict):
        return ["v2 briefs require chart_decision with a reason and chart or null"]
    try:
        _text(decision.get("reason"), "decision reason", 500)
        if "chart" not in decision:
            raise ValueError("chart_decision must explicitly select a chart or null")
        chart = decision["chart"]
        if chart is None:
            return []
        if not isinstance(chart, dict) or not isinstance(chart.get("data_id"), str):
            raise ValueError("select exactly one chart using its prepared data_id")
        stories = brief.get("changes" if kind == "reader_brief" else "top_stories", [])
        index = chart.get("story_index")
        if type(index) is not int or not 0 <= index < len(stories):
            raise ValueError("chart must attach to one existing story")
        _text(chart.get("why_it_matters"), "caption", 350)
        if chart_data is not None:
            if chart_data.get("schema_version") != "brief_chart_data.v1" or chart_data.get("data_id") != chart["data_id"]:
                raise ValueError("selected chart does not match the prepared data")
            as_of = brief.get("run_date") or brief.get("as_of")
            if chart_data.get("run_date", "9999") > as_of or _stamp(chart_data["generated_at"]) > day_cutoff(as_of):
                raise ValueError("chart data postdates the brief")
            points = chart_data.get("points", [])
            if not 2 <= len(points) <= 24:
                raise ValueError("chart needs 2–24 observations")
            for point in points:
                _number(point["value"])
    except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
        return [str(exc)]
    return []


def selected_chart(brief: dict) -> dict | None:
    return (brief.get("chart_decision") or {}).get("chart")


def validate_frozen_chart_data(data: Any, *, cutoff: datetime) -> list[str]:
    """Validate the copied envelope without touching a later source file."""
    try:
        if not isinstance(data, dict) or data.get("schema_version") != "brief_chart_data.v1":
            raise ValueError("invalid frozen chart dataset")
        _text(data["title"], "title", 100)
        _text(data["series"], "series")
        _text(data["unit"], "unit", 80)
        day = date.fromisoformat(data["run_date"])
        if not re.fullmatch(re.escape(day.isoformat()) + r":[a-z0-9][a-z0-9-]{0,63}", data["data_id"]):
            raise ValueError("chart id must identify its dated source run")
        generated, captured = _stamp(data["generated_at"]), _stamp(data["source"]["captured_at"])
        if captured > generated or generated > cutoff or day > cutoff.astimezone(BOGOTA).date():
            raise ValueError("frozen chart postdates the cutoff")
        for key in ("sha256", "receipt_sha256"):
            if not re.fullmatch(r"[a-f0-9]{64}", data["source"][key]):
                raise ValueError("frozen chart lacks original and receipt hashes")
        if not 2 <= len(data["points"]) <= 24:
            raise ValueError("frozen chart needs 2–24 observations")
        frequency = data["frequency"]
        periods = []
        for point in data["points"]:
            _number(point["value"])
            period = _period(point["period"], frequency)
            if _period_start(period) > cutoff.astimezone(BOGOTA).date():
                raise ValueError("frozen chart has future observations")
            periods.append(_ordinal(period, frequency))
        if any(b - a != 1 for a, b in zip(periods, periods[1:])):
            raise ValueError("frozen chart periods must be consecutive")
    except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
        return [str(exc)]
    return []


def chart_markdown(brief: dict, data: dict | None, image_path: str = PNG_FILE) -> str:
    chart = selected_chart(brief)
    if not chart:
        return ""
    if data is None:
        raise ValueError("selected chart needs validated prepared data")
    escape = lambda s: re.sub(r"([\\`*_{}\[\]()#+!<>])", r"\\\1", s)
    url = quote(data["source"]["source_url"], safe=":/?#[]@!$&'*+,;=%")
    return (f'![{escape(data["title"])}]({image_path})\n\n'
            f'{escape(chart["why_it_matters"])} '
            f'[Chart source]({url}).')


def render_chart_svg(data: dict) -> str:
    """A bounded, accessible line chart; zero baseline and straight segments."""
    points = data["points"]
    values = [_number(p["value"]) for p in points]
    low, high = min(0, min(values)), max(0, max(values))
    if low == high:
        high = low + 1
    span = high - low
    if not math.isfinite(span):
        raise ValueError("chart range is too large to render")
    step = 10 ** math.floor(math.log10(span)) / 2
    low = math.floor(low / step) * step
    high = math.ceil((high + span * .14) / step) * step
    title_lines = textwrap.wrap(data["title"], width=40)
    series_lines = textwrap.wrap(data["series"], width=48)
    unit_lines = textwrap.wrap(data["unit"], width=48) if len(data["unit"]) > 20 else []
    top = 36 + len(title_lines) * 29
    bottom = top + 235
    height = bottom + 78 + (len(series_lines) + len(unit_lines)) * 25
    x = lambda i: 75 + i * 475 / (len(points) - 1)
    y = lambda v: top + (high - v) * 235 / (high - low)
    esc = lambda s: html.escape(str(s), quote=True)
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 {height}" width="640" height="{height}" role="img" aria-labelledby="chart-title chart-description">',
             f'<title id="chart-title">{esc(data["title"])}</title>',
             '<desc id="chart-description">' + esc(data["series"] + "; " + data["unit"] + ". " + "; ".join(f'{p["period"]}: {p["value"]:g}' for p in points)) + '</desc>',
             '<rect width="100%" height="100%" fill="#ffffff"/>',
             '<g font-family="Arial, sans-serif" font-size="24" fill="#172b35">']
    for i, line in enumerate(title_lines):
        parts.append(f'<text x="18" y="{30 + i * 29}">{esc(line)}</text>')
    grid_step = step * max(1, math.ceil((high - low) / (step * 4)))
    for tick in [low + i * grid_step for i in range(1 + int((high - low) / grid_step))]:
        parts.append(f'<line x1="75" y1="{y(tick):.2f}" x2="550" y2="{y(tick):.2f}" stroke="#d5dce0"/>')
        parts.append(f'<text x="63" y="{y(tick) + 8:.2f}" text-anchor="end">{tick:.4g}</text>')
    if not unit_lines:
        parts.append(f'<text x="75" y="{top - 10}" font-size="22">{esc(data["unit"])}</text>')
    path = " ".join(f'{"M" if i == 0 else "L"}{x(i):.2f},{y(p["value"]):.2f}' for i, p in enumerate(points))
    parts.append(f'<path d="{path}" fill="none" stroke="#008d87" stroke-width="4"/>')
    for i, point in enumerate(points):
        parts.append(f'<circle cx="{x(i):.2f}" cy="{y(point["value"]):.2f}" r="4" fill="#008d87"/>')
    # Directly label the comparison that motivated the chart, without crowding.
    label_y = [y(values[-2]) - 14, y(values[-1]) - 14]
    if abs(label_y[0] - label_y[1]) < 30 and len(points) > 8:
        label_y[1] += 32
        if label_y[1] > bottom - 5:
            label_y = [label_y[0] - 32, label_y[1] - 32]
    for offset_index, i in enumerate([len(points) - 2, len(points) - 1]):
        label = f"{values[i]:.3g}"
        anchor = "start" if i == len(points) - 1 and len(label) <= 6 else "end"
        offset = 8 if anchor == "start" else -8
        parts.append(f'<text x="{x(i) + offset:.2f}" y="{label_y[offset_index]:.2f}" text-anchor="{anchor}">{label}</text>')
    ticks = sorted({0, (len(points) - 1) // 2, len(points) - 1})
    for i in ticks:
        label = points[i]["period"].replace("-Q", " Q")
        anchor = "start" if i == 0 else "end" if i == len(points) - 1 else "middle"
        parts.append(f'<text x="{x(i):.2f}" y="{bottom + 35}" text-anchor="{anchor}" font-size="22">{esc(label)}</text>')
    for i, line in enumerate([*series_lines, *unit_lines]):
        parts.append(f'<text x="18" y="{bottom + 76 + i * 25}" font-size="22">{esc(line)}</text>')
    parts.append('</g></svg>')
    return "".join(parts)


def chart_html(brief: dict, data: dict | None) -> str:
    if not selected_chart(brief):
        return ""
    if data is None:
        raise ValueError("selected chart needs validated prepared data")
    caption = html.escape(selected_chart(brief)["why_it_matters"])
    url = html.escape(data["source"]["source_url"], quote=True)
    return ('<figure class="brief-chart" style="margin:18px 0">' +
            render_chart_svg(data).replace('<svg ', '<svg style="display:block;width:100%;height:auto" ', 1) +
            f'<figcaption>{caption} <a href="{url}">Chart source</a>.</figcaption></figure>')


def write_chart_images(data: dict, folder: Path) -> list[str]:
    """Rasterize locally using the already-declared Playwright dependency."""
    from playwright.sync_api import sync_playwright
    svg = render_chart_svg(data)
    with sync_playwright() as playwright:
        kwargs: dict[str, Any] = {"headless": True}
        if not Path(playwright.chromium.executable_path).is_file():
            candidates = [shutil.which("chromium"), shutil.which("google-chrome"),
                          "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
            executable = next((p for p in candidates if p and Path(p).is_file()), None)
            if not executable:
                raise ValueError("chart PNG rendering needs Chromium: run playwright install chromium")
            kwargs["executable_path"] = executable
        browser = playwright.chromium.launch(**kwargs)
        try:
            page = browser.new_page(viewport={"width": 680, "height": 800}, device_scale_factor=2)
            page.route("**/*", lambda route: route.abort())
            page.set_content(svg)
            png = page.locator("svg").screenshot(animations="disabled")
        finally:
            browser.close()
    # Write only after successful rasterization; invalid input never creates an image.
    (folder / SVG_FILE).write_text(svg, encoding="utf-8")
    (folder / PNG_FILE).write_bytes(png)
    return [SVG_FILE, PNG_FILE]
