"""Compare two generated run folders for stable artifact parity.

This is a refactor guard. It intentionally ignores volatile timestamps and git
metadata while comparing the artifacts that carry M1/M2 behavior.
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from pathlib import Path
from typing import Any

DEFAULT_ARTIFACTS = (
    "raw_items.json",
    "cleaned_items.json",
    "clusters.json",
    "indicator_watch.json",
    "source_failures.json",
    "source_health.json",
    "legislative_reconciler.json",
    "m2_ranked_questions.json",
    "m2_review_packet.json",
    "m1_candidates.json",
    "acceptance_report.json",
    "run_summary.json",
    "run_manifest.json",
    "metasource_brief.md",
    "m2_handoff.md",
    "m2_review_packet.md",
)

VOLATILE_KEYS = {
    "fetched_at",
    "finished_at",
    "generated_at",
    "occurred_at",
    "started_at",
}
REDACTED = "<volatile>"
ISO_Z_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


def normalize_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        normalized: dict[str, Any] = {}
        for key, value in payload.items():
            if key in VOLATILE_KEYS:
                normalized[key] = REDACTED
            elif key == "git" and isinstance(value, dict):
                normalized[key] = {
                    subkey: REDACTED if subkey in {"commit", "dirty_tracked_files"} else subvalue
                    for subkey, subvalue in value.items()
                }
            else:
                normalized[key] = normalize_payload(value)
        return normalized
    if isinstance(payload, list):
        return [normalize_payload(item) for item in payload]
    return payload


def normalize_text(text: str) -> str:
    return ISO_Z_RE.sub(REDACTED, text)


def normalized_artifact(path: Path) -> str:
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return json.dumps(
            normalize_payload(payload),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        ) + "\n"
    return normalize_text(path.read_text(encoding="utf-8"))


def compare_artifact(baseline: Path, candidate: Path, artifact: str) -> list[str]:
    left_path = baseline / artifact
    right_path = candidate / artifact
    if not left_path.exists() and not right_path.exists():
        return []
    if not left_path.exists() or not right_path.exists():
        return [
            f"{artifact}: existence differs "
            f"(baseline={left_path.exists()}, candidate={right_path.exists()})"
        ]

    left = normalized_artifact(left_path)
    right = normalized_artifact(right_path)
    if left == right:
        return []

    diff = difflib.unified_diff(
        left.splitlines(),
        right.splitlines(),
        fromfile=str(left_path),
        tofile=str(right_path),
        lineterm="",
        n=3,
    )
    return [f"{artifact}: content differs", *list(diff)]


def check_parity(
    baseline: Path,
    candidate: Path,
    artifacts: tuple[str, ...] = DEFAULT_ARTIFACTS,
) -> list[str]:
    failures: list[str] = []
    for artifact in artifacts:
        failures.extend(compare_artifact(baseline, candidate, artifact))
        if failures:
            break
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare stable M1/M2 artifacts from two run directories."
    )
    parser.add_argument("baseline", type=Path, help="Expected run directory")
    parser.add_argument("candidate", type=Path, help="Candidate run directory")
    parser.add_argument(
        "--artifact",
        action="append",
        dest="artifacts",
        help="Artifact filename to compare. May be repeated. Defaults to core artifacts.",
    )
    args = parser.parse_args(argv)

    artifacts = tuple(args.artifacts) if args.artifacts else DEFAULT_ARTIFACTS
    failures = check_parity(args.baseline, args.candidate, artifacts)
    if failures:
        print("\n".join(failures))
        return 1

    print(f"Artifact parity OK: {args.baseline} == {args.candidate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
