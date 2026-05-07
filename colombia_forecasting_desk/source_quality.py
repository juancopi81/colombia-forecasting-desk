from __future__ import annotations

from .models import SourceHealth

LINK_ONLY_CONTENT_MODES = frozenset(
    {
        "pdf_links_only",
        "spreadsheet_links_only",
        "document_links_only",
    }
)


def is_unparsed_link_only_source(health: SourceHealth) -> bool:
    return (
        health.content_mode in LINK_ONLY_CONTENT_MODES
        and health.document_link_count > 0
        and health.parsed_content_count == 0
    )
