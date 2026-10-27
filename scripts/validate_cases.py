"""Validate the persistent story files without modifying them."""
from __future__ import annotations
import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from colombia_forecasting_desk.cases import load_cases


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases-dir", type=Path, default=REPO_ROOT / "cases")
    args = parser.parse_args(argv)
    try:
        cases = load_cases(args.cases_dir)
        if not cases:
            raise ValueError("no case files found")
        print(f"Validated {len(cases)} stories.")
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
