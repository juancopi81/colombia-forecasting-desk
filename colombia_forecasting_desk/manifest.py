from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .models import RunSummary
from .observability import SCHEMA_VERSION as RUN_TRACE_SCHEMA_VERSION

SCHEMA_VERSION = "run_manifest.v1"
REPO_ROOT = Path(__file__).resolve().parent.parent


def build_run_manifest(
    run_dir: Path,
    run_summary: RunSummary,
    *,
    config_path: str | Path,
    strict_requested: bool,
    acceptance_report: dict[str, Any],
    m1_candidates: dict[str, Any],
    legislative_reconciliations: list[dict[str, Any]],
    m2_ranked_questions: dict[str, Any],
    m2_review_packet: dict[str, Any],
    indicator_tension_cards: list[dict[str, Any]] | None = None,
    cooccurrence_bundles: list[dict[str, Any]] | None = None,
    analyst_leads: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Describe which code and artifact contracts produced a run."""
    analyst_leads = analyst_leads or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "generated_at": run_summary.finished_at,
        "config_path": str(config_path),
        "strict_requested": strict_requested,
        "strict_pass": bool(acceptance_report.get("strict_pass")),
        "acceptance_status": str(acceptance_report.get("status") or "unknown"),
        "git": _git_context(),
        "counts": {
            "sources_checked": run_summary.sources_checked,
            "sources_failed": run_summary.sources_failed,
            "raw_items": run_summary.raw_items,
            "cleaned_items": run_summary.cleaned_items,
            "clusters": run_summary.clusters,
            "m1_candidates": len(m1_candidates.get("candidates") or []),
            "legislative_reconciler_records": len(legislative_reconciliations),
            "m2_ranked_questions": len(
                m2_ranked_questions.get("ranked_questions") or []
            ),
            "m2_review_items": len(m2_review_packet.get("review_items") or []),
            "indicator_tension_cards": len(indicator_tension_cards or []),
            "cooccurrence_bundles": len(cooccurrence_bundles or []),
            "analyst_leads": len(analyst_leads.get("leads") or []),
        },
        "capabilities": {
            "source_health": True,
            "indicator_watch": True,
            "m1_candidates": True,
            "legislative_reconciler": bool(legislative_reconciliations),
            "legislative_m2_ranking": True,
            "heuristic_audit": bool(m2_ranked_questions.get("heuristic_audit")),
            "m2_review_packet": True,
            "indicator_tension_cards": True,
            "cooccurrence_bundles": True,
            "analyst_leads": True,
        },
        "artifact_schemas": {
            "m1_candidates.json": str(
                m1_candidates.get("schema_version") or "unknown"
            ),
            "legislative_reconciler.json": _schema_from_records(
                legislative_reconciliations
            ),
            "m2_ranked_questions.json": str(
                m2_ranked_questions.get("schema_version") or "unknown"
            ),
            "m2_review_packet.json": str(
                m2_review_packet.get("schema_version") or "unknown"
            ),
            "indicator_tension_cards.json": _schema_from_cards(
                indicator_tension_cards or []
            ),
            "cooccurrence_bundles.json": _schema_from_bundles(
                cooccurrence_bundles or []
            ),
            "analyst_leads.json": str(
                analyst_leads.get("schema_version") or "unknown"
            ),
            "acceptance_report.json": str(
                acceptance_report.get("schema_version") or "unknown"
            ),
            "run_trace.json": RUN_TRACE_SCHEMA_VERSION,
        },
        "artifacts": _artifact_inventory(run_dir),
        "comparison_note": (
            "Use this manifest when comparing historical daily runs. Artifact "
            "availability and parser capabilities may differ across dates."
        ),
    }


def _git_context() -> dict[str, Any]:
    commit = _git(["rev-parse", "HEAD"])
    branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    status = _git(["status", "--short", "--untracked-files=no"])
    return {
        "commit": commit,
        "branch": branch,
        "dirty_tracked_files": bool(status.strip()),
    }


def _git(args: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _schema_from_records(records: list[dict[str, Any]]) -> str:
    for record in records:
        if isinstance(record, dict) and record.get("schema_version"):
            return str(record["schema_version"])
    return "unknown"


def _schema_from_cards(cards: list[dict[str, Any]]) -> str:
    for card in cards:
        if isinstance(card, dict) and card.get("schema_version"):
            return str(card["schema_version"])
    return "indicator_tension_cards.v1"


def _schema_from_bundles(bundles: list[dict[str, Any]]) -> str:
    for bundle in bundles:
        if isinstance(bundle, dict) and bundle.get("schema_version"):
            return str(bundle["schema_version"])
    return "cooccurrence_bundles.v1"


def _artifact_inventory(run_dir: Path) -> list[dict[str, Any]]:
    expected = [
        "raw_items.json",
        "cleaned_items.json",
        "clusters.json",
        "indicator_watch.json",
        "indicator_tension_cards.json",
        "indicator_tension_cards.md",
        "cooccurrence_bundles.json",
        "cooccurrence_bundles.md",
        "source_failures.json",
        "source_health.json",
        "legislative_reconciler.json",
        "m2_ranked_questions.json",
        "m2_review_packet.json",
        "m2_review_packet.md",
        "analyst_leads.json",
        "analyst_leads.md",
        "m1_candidates.json",
        "acceptance_report.json",
        "metasource_brief.md",
        "m2_handoff.md",
        "run_summary.json",
        "run_trace.json",
        "run_manifest.json",
    ]
    artifacts: list[dict[str, Any]] = []
    for name in expected:
        path = run_dir / name
        artifacts.append(
            {
                "path": name,
                "exists": path.exists() or name == "run_manifest.json",
            }
        )
    return artifacts
