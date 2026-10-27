"""Plan exact internal forecast slots from already captured official calendars."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from colombia_forecasting_desk.forecast_families import DEFAULT_CONFIG, plan_family_forecasts  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--policy-rate-fallback", type=Path, help="Verified official communiqué provenance JSON; never inferred from a survey median.")
    args = parser.parse_args(argv)
    try:
        run_dir = args.runs_dir / args.date
        if not run_dir.is_dir():
            raise ValueError(f"Missing run directory: {run_dir}")
        fallback = json.loads(args.policy_rate_fallback.read_text()) if args.policy_rate_fallback else None
        artifact = plan_family_forecasts(run_dir, config_path=args.config, policy_fallback=fallback)
        path = run_dir / "family_forecast_slots.json"
        path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(path)
    print(f"slots: {len(artifact['slots'])}; enabled: {sum(slot['enabled'] for slot in artifact['slots'])}")
    for notice in artifact["notices"]:
        print(notice)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
