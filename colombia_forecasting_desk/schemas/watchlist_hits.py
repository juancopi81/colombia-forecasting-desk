from __future__ import annotations
from typing import Any
from ._common import base, objects, text, https

KINDS = {"company", "public_entity", "place", "topic", "bill", "decree"}
PATTERNS = {"supplier_share", "repeat_direct_award", "large_addition", "sanctioned_supplier"}


def validate(payload: Any) -> list[str]:
    issues = base(payload, "watchlist_hits", ("hits", "patterns"))
    if not isinstance(payload, dict):
        return issues
    for hit in objects(payload, "hits", issues):
        if hit.get("kind") not in KINDS:
            issues.append("unsupported watchlist hit kind")
        for field in ("watch_id", "source_id", "title", "matched_on", "summary"):
            if not text(hit.get(field)):
                issues.append(f"hits.{field} is required")
        if not https(hit.get("url")):
            issues.append("hits.url must use https")
    for pattern in objects(payload, "patterns", issues):
        if pattern.get("kind") not in PATTERNS:
            issues.append("unsupported pattern kind")
        for field in ("pattern_id", "watch_id", "plain_text", "caveat"):
            if not text(pattern.get(field)):
                issues.append(f"patterns.{field} is required")
        urls = pattern.get("evidence_urls")
        if not isinstance(urls, list) or not urls or not all(https(url) for url in urls):
            issues.append("patterns needs https evidence_urls")
        if not isinstance(pattern.get("numbers"), dict):
            issues.append("patterns.numbers must be an object")
    return issues
