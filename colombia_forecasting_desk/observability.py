from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Iterator

SCHEMA_VERSION = "run_trace.v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean_mapping(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}


@dataclass
class TraceSpan:
    event: dict[str, Any]
    _started_counter: float

    def set_counts(self, **counts: Any) -> None:
        self.event.setdefault("counts", {}).update(_clean_mapping(counts))

    def set_metadata(self, **metadata: Any) -> None:
        self.event.setdefault("metadata", {}).update(_clean_mapping(metadata))

    def mark_error(self, exc: BaseException) -> None:
        self.event["status"] = "error"
        self.event["error_class"] = exc.__class__.__name__
        self.event["error_message"] = str(exc)

    def finish(self) -> None:
        if self.event.get("status") == "running":
            self.event["status"] = "ok"
        self.event["finished_at"] = _now_iso()
        self.event["duration_ms"] = round(
            (perf_counter() - self._started_counter) * 1000,
            3,
        )


@dataclass
class RunTrace:
    run_date: str
    mode: str
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: str = field(default_factory=_now_iso)
    _started_counter: float = field(default_factory=perf_counter)
    events: list[dict[str, Any]] = field(default_factory=list)

    @contextmanager
    def span(
        self,
        name: str,
        *,
        category: str = "pipeline",
        metadata: dict[str, Any] | None = None,
    ) -> Iterator[TraceSpan]:
        event: dict[str, Any] = {
            "name": name,
            "category": category,
            "status": "running",
            "started_at": _now_iso(),
        }
        if metadata:
            event["metadata"] = _clean_mapping(metadata)
        span = TraceSpan(event=event, _started_counter=perf_counter())
        try:
            yield span
        except Exception as exc:
            span.mark_error(exc)
            raise
        finally:
            span.finish()
            self.events.append(event)

    def to_dict(self) -> dict[str, Any]:
        finished_at = _now_iso()
        return {
            "schema_version": SCHEMA_VERSION,
            "run_date": self.run_date,
            "mode": self.mode,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "duration_ms": round((perf_counter() - self._started_counter) * 1000, 3),
            "metadata": _clean_mapping(self.metadata),
            "events": self.events,
        }
