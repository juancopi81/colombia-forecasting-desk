from __future__ import annotations

import logging
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .cleaner import fold_accents
from .models import CleanedItem

logger = logging.getLogger(__name__)

_TRACKING_PREFIXES = ("utm_",)
_TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src"}
_PRIMARY_TRUST_ROLES = {"official_signal", "resolution_source"}


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower() or "https"
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=False)
        if not (k.lower() in _TRACKING_KEYS or k.lower().startswith(_TRACKING_PREFIXES))
    ]
    query = urlencode(sorted(query_pairs))
    return urlunsplit((scheme, netloc, path, query, ""))


def _normalize_title(title: str) -> str:
    return fold_accents(title.lower()).strip()


def _trust_rank(item: CleanedItem) -> int:
    return 1 if item.trust_role in _PRIMARY_TRUST_ROLES else 0


def dedupe(items: list[CleanedItem]) -> list[CleanedItem]:
    by_url: dict[str, CleanedItem] = {}
    for item in items:
        key = canonicalize_url(item.url)
        if not key:
            key = f"__no_url__:{item.source_id}:{_normalize_title(item.title)}"
        existing = by_url.get(key)
        if existing is None:
            by_url[key] = item
            continue
        if _trust_rank(item) > _trust_rank(existing):
            by_url[key] = item
        # otherwise keep the first-seen item (stable order)

    seen_per_source: dict[tuple[str, str], CleanedItem] = {}
    for item in by_url.values():
        key = (item.source_id, _normalize_title(item.title))
        if key in seen_per_source:
            continue
        seen_per_source[key] = item

    deduped = list(seen_per_source.values())
    logger.info(
        "dedupe: %d input items -> %d unique items",
        len(items),
        len(deduped),
    )
    return deduped
