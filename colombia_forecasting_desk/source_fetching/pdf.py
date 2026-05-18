from __future__ import annotations

from .common import *

_PDF_STREAM_RE = re.compile(rb"stream\r?\n(.*?)\r?\nendstream", re.DOTALL)
_PDF_LITERAL_RE = re.compile(rb"\((?:\\.|[^\\()])*\)")
_PDF_TEXT_OBJECT_RE = re.compile(rb"\bBT\b(.*?)\bET\b", re.DOTALL)
_PDF_TEXT_TOKEN_RE = re.compile(
    rb"\((?:\\.|[^\\()])*\)|<\s*(?:[0-9A-Fa-f]{2}\s*)+>"
)
_PDF_ACTUAL_TEXT_RE = re.compile(
    rb"/ActualText\s*(\((?:\\.|[^\\()])*\)|<\s*(?:[0-9A-Fa-f]{2}\s*)+>)"
)
_PDF_TEXT_ALLOWED_PUNCT = set(".,;:!?¿¡()[]{}%$#/+-_=°'\"@&\n\r\t ")
_PDF_TEXT_ALLOWED_NONASCII = set("áéíóúÁÉÍÓÚñÑüÜ")
_PDF_COMMON_SPANISH_TERMS = {
    "con",
    "de",
    "del",
    "el",
    "en",
    "la",
    "las",
    "los",
    "para",
    "por",
    "que",
    "una",
}


def _decode_pdf_literal(raw: bytes) -> str:
    body = raw[1:-1]
    out = bytearray()
    i = 0
    while i < len(body):
        ch = body[i]
        if ch != 0x5C:
            out.append(ch)
            i += 1
            continue
        i += 1
        if i >= len(body):
            break
        esc = body[i]
        if esc in b"nrtbf":
            out.append({ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12}[esc])
            i += 1
            continue
        if esc in b"()\\":
            out.append(esc)
            i += 1
            continue
        if esc in b"\r\n":
            i += 1
            if esc == 13 and i < len(body) and body[i] == 10:
                i += 1
            continue
        if 48 <= esc <= 55:
            digits = bytes([esc])
            i += 1
            for _ in range(2):
                if i < len(body) and 48 <= body[i] <= 55:
                    digits += bytes([body[i]])
                    i += 1
                else:
                    break
            out.append(int(digits, 8) & 0xFF)
            continue
        out.append(esc)
        i += 1
    return bytes(out).decode("latin-1", errors="ignore")


def _decode_pdf_cmap_hex(cleaned: str, cmap: dict[str, str]) -> str:
    if not cmap:
        return ""
    for width in (4, 2):
        if len(cleaned) % width:
            continue
        chars: list[str] = []
        hits = 0
        for i in range(0, len(cleaned), width):
            code = cleaned[i : i + width].upper()
            value = cmap.get(code)
            if value:
                chars.append(value)
                hits += 1
            else:
                chars.append(" ")
        if hits:
            return normalize_whitespace("".join(chars))
    return ""


def _decode_pdf_hex(raw: bytes, cmap: dict[str, str] | None = None) -> str:
    cleaned = re.sub(rb"\s+", b"", raw[1:-1])
    if not cleaned or len(cleaned) % 2:
        return ""
    cleaned_text = cleaned.decode("ascii", errors="ignore").upper()
    if cmap:
        decoded = _decode_pdf_cmap_hex(cleaned_text, cmap)
        if decoded:
            return decoded
    try:
        content = bytes.fromhex(cleaned_text)
    except ValueError:
        return ""
    if content.startswith(b"\xfe\xff"):
        return content[2:].decode("utf-16-be", errors="ignore")

    candidates = [
        content.decode("utf-16-be", errors="ignore"),
        content.decode("utf-8", errors="ignore"),
        content.decode("latin-1", errors="ignore"),
    ]
    candidates = [normalize_whitespace(candidate) for candidate in candidates]
    return max(candidates, key=_text_signal_score, default="")


def _decode_pdf_text_token(
    raw: bytes, cmap: dict[str, str] | None = None
) -> str:
    if raw.startswith(b"("):
        return _decode_pdf_literal(raw)
    if raw.startswith(b"<"):
        return _decode_pdf_hex(raw, cmap)
    return ""


def _text_signal_score(text: str) -> int:
    return sum(
        1
        for ch in text
        if (ch.isascii() and ch.isalpha()) or ch in _PDF_TEXT_ALLOWED_NONASCII
    )


def _looks_like_text_fragment(text: str) -> bool:
    normalized = normalize_whitespace(text)
    if not normalized or "\x00" in normalized:
        return False
    if _is_pdf_noise_fragment(normalized):
        return False
    useful = sum(
        1
        for ch in normalized
        if (
            (ch.isascii() and (ch.isalnum() or ch.isspace()))
            or ch in _PDF_TEXT_ALLOWED_NONASCII
            or ch in _PDF_TEXT_ALLOWED_PUNCT
        )
    )
    return useful / len(normalized) >= 0.85 and _text_signal_score(normalized) > 0


def _is_pdf_noise_fragment(text: str) -> bool:
    folded = fold_accents(text.lower())
    return (
        "identityadobe" in folded
        or "microsoft sans" in folded
        or "segoe ui" in folded
        or ("arial" in folded and "proyecto" not in folded)
        or ("calibri" in folded and "proyecto" not in folded)
    )


def _extract_pdf_cmap(chunks: list[bytes]) -> dict[str, str]:
    cmap: dict[str, str] = {}
    for chunk in chunks:
        text = chunk.decode("latin-1", errors="ignore")
        for block in re.findall(r"beginbfchar(.*?)endbfchar", text, flags=re.DOTALL):
            for source, target in re.findall(
                r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", block
            ):
                try:
                    decoded = bytes.fromhex(target).decode("utf-16-be", errors="ignore")
                except ValueError:
                    continue
                if decoded:
                    cmap[source.upper()] = decoded
        for block in re.findall(r"beginbfrange(.*?)endbfrange", text, flags=re.DOTALL):
            for start, end, target in re.findall(
                r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>",
                block,
            ):
                try:
                    start_i = int(start, 16)
                    end_i = int(end, 16)
                    target_i = int(target, 16)
                except ValueError:
                    continue
                width = len(start)
                if end_i - start_i > 256:
                    continue
                for offset, code in enumerate(range(start_i, end_i + 1)):
                    try:
                        decoded = chr(target_i + offset)
                    except ValueError:
                        continue
                    cmap[f"{code:0{width}X}"] = decoded
    return cmap


def _extract_pdf_operator_lines(
    chunk: bytes,
    cmap: dict[str, str] | None = None,
) -> list[str]:
    lines: list[str] = []
    for text_object in _PDF_TEXT_OBJECT_RE.findall(chunk):
        fragments: list[str] = []
        for token in _PDF_TEXT_TOKEN_RE.findall(text_object):
            fragment = _decode_pdf_text_token(token, cmap)
            if _looks_like_text_fragment(fragment):
                fragments.append(fragment)
        line = normalize_whitespace("".join(fragments))
        if _looks_like_text_fragment(line):
            lines.append(line)
    return lines


def _extract_pdf_actual_text(
    chunk: bytes,
    cmap: dict[str, str] | None = None,
) -> list[str]:
    fragments: list[str] = []
    for token in _PDF_ACTUAL_TEXT_RE.findall(chunk):
        fragment = normalize_whitespace(_decode_pdf_text_token(token, cmap))
        if _looks_like_text_fragment(fragment):
            fragments.append(fragment)
    return fragments


def _looks_like_text(text: str) -> bool:
    normalized = normalize_whitespace(text)
    if len(normalized) < 8:
        return False
    useful = sum(
        1
        for ch in normalized
        if (
            (ch.isascii() and (ch.isalnum() or ch.isspace()))
            or ch in _PDF_TEXT_ALLOWED_NONASCII
            or ch in _PDF_TEXT_ALLOWED_PUNCT
        )
    )
    letters = sum(
        1
        for ch in normalized
        if (ch.isascii() and ch.isalpha()) or ch in _PDF_TEXT_ALLOWED_NONASCII
    )
    return useful / len(normalized) >= 0.85 and letters / len(normalized) >= 0.35


def _looks_like_pdf_excerpt(text: str) -> bool:
    if len(text) < PDF_TEXT_MIN_CHARS:
        return False
    folded_tokens = set(re.findall(r"\w+", fold_accents(text.lower())))
    if len(folded_tokens & _PDF_COMMON_SPANISH_TERMS) < 3:
        return False
    if text.count("Identity") > 1 or text.count("Segoe UI") > 1:
        return False
    if "endstream" in text or text.count("\\") > 2:
        return False
    return True


def _extract_pdf_text(content: bytes, *, max_chars: int = PDF_TEXT_EXCERPT_CHARS) -> str:
    """Best-effort PDF text extraction using only the standard library.

    This intentionally handles only common text streams. It is not a full PDF
    parser, but it gives M1 a useful excerpt when official PDFs expose readable
    literal strings without adding another production dependency.
    """
    chunks: list[bytes] = []
    for match in _PDF_STREAM_RE.finditer(content[:PDF_TEXT_MAX_BYTES]):
        stream = match.group(1).strip(b"\r\n")
        try:
            chunks.append(zlib.decompress(stream))
        except zlib.error:
            chunks.append(stream)
    if not chunks:
        chunks.append(content[:PDF_TEXT_MAX_BYTES])

    primary_texts: list[str] = []
    fallback_texts: list[str] = []
    cmap = _extract_pdf_cmap(chunks)
    for chunk in chunks:
        primary_texts.extend(_extract_pdf_operator_lines(chunk, cmap))
        primary_texts.extend(_extract_pdf_actual_text(chunk, cmap))
        for literal in _PDF_LITERAL_RE.findall(chunk):
            text = _decode_pdf_literal(literal)
            if _looks_like_text(text):
                fallback_texts.append(text)

    excerpt = normalize_whitespace(" ".join(primary_texts))
    if _looks_like_pdf_excerpt(excerpt):
        return excerpt[:max_chars]

    excerpt = normalize_whitespace(" ".join([*primary_texts, *fallback_texts]))
    if not _looks_like_pdf_excerpt(excerpt):
        return ""
    return excerpt[:max_chars]


_SENADO_AGENDA_PROJECT_RE = re.compile(
    r"\bProyecto\s+de\s+(?P<kind>Ley|Acto\s+Legislativo)\s+No\.?\s+"
    r"(?P<first_number>\d{1,4})\s+(?:de|del)\s+"
    r"(?P<first_year>\d{4})\s+(?P<first_chamber>Senado|C[aá]mara)"
    r"(?:[,;\s]+(?P<second_number>\d{1,4})\s+(?:de|del)\s+"
    r"(?P<second_year>\d{4})\s+(?P<second_chamber>Senado|C[aá]mara))?",
    re.IGNORECASE,
)
_SENADO_AGENDA_LOOSE_PROJECT_RE = re.compile(
    r"\bProyecto\s+de\s+Ley\s+(?:No\.?\s*)?"
    r"(?P<chamber>Senado|C[aá]mara)?\s*(?P<body>.{24,320}?)(?="
    r"\b(?:Autores?|Ponente|Publicaci[oó]n|Proyecto\s+de\s+Ley|Hora|"
    r"Lugar|Transmisi[oó]n|TEMA:|$))",
    re.IGNORECASE,
)
_SENADO_AGENDA_DAY_RE = re.compile(
    r"\b(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)"
    r"\s+(\d{1,2})\s+de\s+"
    r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|"
    r"setiembre|octubre|noviembre|diciembre)"
    r"(?:\s+de\s+(\d{4}))?",
    re.IGNORECASE,
)
_SENADO_TITLE_REPAIR_PATTERNS = (
    (r"\belacual\b", "el cual"),
    (r"\bmodificael\b", "modifica el"),
    (r"\blaleydeyse\b", "la ley de y se"),
    (r"\blaley\b", "la ley"),
    (r"\bdeyse\b", "de y se"),
    (r"\botrasdisposiciones\b", "otras disposiciones"),
)
_SENADO_LOSSY_TITLE_MARKERS = (
    "elacual",
    "modificael",
    "laley",
    "deyse",
    "otrasdisposiciones",
)
_GACETAS_CONGRESO_URL = "https://svrpubindc.imprenta.gov.co/gacetas/index.xhtml"



def _enrich_pdf_text(
    items: list[RawItem],
    client: httpx.Client,
    *,
    max_items: int = PDF_TEXT_PARSE_LIMIT,
) -> list[RawItem]:
    enriched: list[RawItem] = []
    parsed_count = 0
    for item in items:
        path = urlsplit(item.url).path.lower()
        if parsed_count >= max_items or ".pdf" not in path:
            enriched.append(item)
            continue
        metadata = dict(item.metadata)
        try:
            response = _http_get(client, item.url)
            excerpt = _extract_pdf_text(response.content)
        except Exception as exc:  # noqa: BLE001 - preserve link-level item
            metadata["content_extraction_error"] = f"{exc.__class__.__name__}: {exc}"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        if len(excerpt) < PDF_TEXT_MIN_CHARS:
            metadata["content_extraction_error"] = "pdf text excerpt too short"
            enriched.append(
                RawItem(
                    id=item.id,
                    source_id=item.source_id,
                    source_name=item.source_name,
                    source_type=item.source_type,
                    url=item.url,
                    title=item.title,
                    fetched_at=item.fetched_at,
                    published_at=item.published_at,
                    raw_text=item.raw_text,
                    metadata=metadata,
                )
            )
            parsed_count += 1
            continue
        metadata.update(
            {
                "content_extraction": "pdf_text_best_effort",
                "pdf_text_chars": len(excerpt),
            }
        )
        enriched.append(
            RawItem(
                id=item.id,
                source_id=item.source_id,
                source_name=item.source_name,
                source_type=item.source_type,
                url=item.url,
                title=item.title,
                fetched_at=item.fetched_at,
                published_at=item.published_at,
                raw_text=f"{item.raw_text} PDF text excerpt: {excerpt}",
                metadata=metadata,
            )
        )
        parsed_count += 1
    return enriched


def _extract_pdf_text_with_pdfplumber(
    content: bytes,
    *,
    max_chars: int = PDF_TEXT_FULL_CHARS,
) -> str:
    """Extract PDF text with layout when pdfplumber is available.

    MinHacienda TES auction reports are numeric tables; preserving spaces and
    line breaks is materially better than the generic no-dependency extractor.
    The fallback keeps the source fail-closed in environments that have not
    synced the dependency yet.
    """
    try:
        import pdfplumber
    except ImportError:
        return _extract_pdf_text_objects_text(content, max_chars=max_chars)

    pages: list[str] = []
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(layout=True) or page.extract_text() or ""
                if page_text:
                    pages.append(page_text)
    except Exception:  # noqa: BLE001 - keep best-effort PDF extraction fail-closed
        return _extract_pdf_text_objects_text(content, max_chars=max_chars)
    return "\n".join(pages)[:max_chars]



def _extract_pdf_text_objects_text(
    content: bytes,
    *,
    max_chars: int = PDF_TEXT_FULL_CHARS,
) -> str:
    """Extract PDF text-object content without dropping numeric-only fragments.

    The generic PDF excerpt parser intentionally rejects many numeric fragments
    to avoid false positives. MinCIT's approved-zones PDF is a numeric table, so
    the source-specific parser needs the text object stream with NITs,
    resolution numbers, dates, and CIIU codes intact.
    """
    chunks: list[bytes] = []
    for match in _PDF_STREAM_RE.finditer(content[:PDF_TEXT_MAX_BYTES]):
        stream = match.group(1).strip(b"\r\n")
        try:
            chunks.append(zlib.decompress(stream))
        except zlib.error:
            chunks.append(stream)
    if not chunks:
        chunks.append(content[:PDF_TEXT_MAX_BYTES])

    lines: list[str] = []
    cmap = _extract_pdf_cmap(chunks)
    for chunk in chunks:
        for text_object in _PDF_TEXT_OBJECT_RE.findall(chunk):
            fragments = [
                _decode_pdf_text_token(token, cmap)
                for token in _PDF_TEXT_TOKEN_RE.findall(text_object)
            ]
            line = normalize_whitespace("".join(fragments))
            if line:
                lines.append(line)
    return normalize_whitespace(" ".join(lines))[:max_chars]




__all__ = [name for name in globals() if not name.startswith("__")]
