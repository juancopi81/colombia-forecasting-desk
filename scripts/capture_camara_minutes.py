"""Capture the official Cámara Comisión Tercera minutes index and bounded PDFs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.camara_minutes import capture_minutes  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-from", required=True, help="Inclusive index date, YYYY-MM-DD")
    parser.add_argument("--date-to", required=True, help="Inclusive index date, YYYY-MM-DD")
    parser.add_argument("--output-dir", type=Path, required=True, help="New capture directory; never overwritten")
    parser.add_argument("--max-pages", type=int, default=2, choices=range(1, 6))
    parser.add_argument("--max-documents", type=int, default=1, choices=range(0, 4))
    args = parser.parse_args(argv)
    try:
        report = capture_minutes(args.output_dir, date_from=args.date_from, date_to=args.date_to,
                                 max_pages=args.max_pages, max_documents=args.max_documents)
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"report": str(args.output_dir / "capture.json"), "index_status": report["index_status"],
                      "matching_records": len(report["records"]), "total": report["total"],
                      "documents": len(report["documents"]), "notices": report["notices"]}, ensure_ascii=False))
    return 2 if report["notices"] or report["index_status"] != "complete" else 0


if __name__ == "__main__":
    raise SystemExit(main())
