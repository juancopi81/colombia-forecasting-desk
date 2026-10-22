"""Bound the complete TES browser attempt, including browser/driver shutdown."""
from __future__ import annotations

from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from ..models import Metasource, RawItem

TES_BROWSER_DEADLINE_SECONDS = 120.0
TES_BROWSER_TERMINATE_GRACE_SECONDS = 2.0
_POLL_SECONDS = 0.1
_KILL_WAIT_SECONDS = 1.0
_PS_TIMEOUT_SECONDS = 0.5
logger = logging.getLogger(__name__)


class TesBrowserTimeoutError(TimeoutError):
    """The TES worker did not finish its entire lifecycle before the deadline."""

    def __init__(self) -> None:
        super().__init__(
            "MinHacienda TES browser attempt exceeded "
            f"{TES_BROWSER_DEADLINE_SECONDS:g}s lifecycle deadline "
            "(startup, retrieval, parsing and cleanup); partial results discarded"
        )


class TesBrowserWorkerError(RuntimeError):
    """A TES worker failed; its original exception type remains in the message."""


def _process_snapshot() -> dict[int, tuple[int, int, str]]:
    # Chromium is detached by Playwright on POSIX, so worker killpg alone is
    # insufficient. Keep birth times as well as PIDs to reject reused identities.
    with tempfile.TemporaryFile() as output:
        process = subprocess.Popen(
            ["ps", "-axo", "pid=,ppid=,pgid=,stat=,lstart="],
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL,
        )
        try:
            returncode = process.wait(timeout=_PS_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=_PS_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                pass
            raise
        if returncode:
            raise subprocess.CalledProcessError(returncode, process.args)
        output.seek(0)
        text = output.read().decode("utf-8")
    processes = {}
    for line in text.splitlines():
        fields = line.split(None, 4)
        if len(fields) == 5 and not fields[3].startswith("Z"):
            processes[int(fields[0])] = (int(fields[1]), int(fields[2]), fields[4])
    return processes


def _track_descendants(
    root_pid: int,
    owned: dict[int, tuple[int, str]],
) -> dict[int, tuple[int, int, str]]:
    processes = _process_snapshot()
    if not owned and root_pid in processes:
        _, pgid, birth = processes[root_pid]
        owned[root_pid] = (pgid, birth)
    alive = {
        pid for pid, (pgid, birth) in owned.items()
        if pid in processes and processes[pid][1:] == (pgid, birth)
    }
    while True:
        # A known group member keeps its group identity alive even if its leader
        # exits first. Only groups created inside this attempt can be signalled.
        groups = {processes[pid][1] for pid in alive}
        descendants = {
            pid for pid, (ppid, pgid, _) in processes.items()
            if ppid in alive or pgid in groups
        }
        added = descendants - alive
        if not added:
            break
        alive.update(added)
        for pid in added:
            _, pgid, birth = processes[pid]
            owned[pid] = (pgid, birth)
    return processes


def _signal_owned(
    root_pid: int,
    owned: dict[int, tuple[int, str]],
    sig: int,
) -> bool:
    processes = _track_descendants(root_pid, owned)
    groups = {
        pgid for pid, (pgid, birth) in owned.items()
        if pid in processes and processes[pid][1:] == (pgid, birth)
        # The worker starts a new session; observed detached group leaders also
        # belong to this attempt. Never signal an inherited, unrelated group.
        and (pgid == root_pid or pgid in owned)
    }
    for pgid in groups:
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass
    return bool(groups)


def _stop_worker(
    process: subprocess.Popen,
    owned: dict[int, tuple[int, str]],
    grace_seconds: float,
) -> None:
    cleanup_error = None
    try:
        has_descendants = _signal_owned(process.pid, owned, signal.SIGTERM)
        until = time.monotonic() + grace_seconds
        while has_descendants and time.monotonic() < until:
            time.sleep(min(_POLL_SECONDS, max(0, until - time.monotonic())))
            has_descendants = _signal_owned(process.pid, owned, 0)
        _signal_owned(process.pid, owned, signal.SIGKILL)
        until = time.monotonic() + _KILL_WAIT_SECONDS
        while _signal_owned(process.pid, owned, 0):
            if time.monotonic() >= until:
                raise TesBrowserWorkerError(
                    "TES browser descendants survived forced termination"
                )
            time.sleep(min(_POLL_SECONDS, max(0, until - time.monotonic())))
    except (OSError, subprocess.SubprocessError) as exc:
        cleanup_error = TesBrowserWorkerError(
            f"TES descendant cleanup could not be verified: {exc}"
        )
    finally:
        # Popen retains the direct child's identity until it is reaped. This
        # fallback remains bounded if process enumeration itself failed.
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=_KILL_WAIT_SECONDS)
        except subprocess.TimeoutExpired:
            cleanup_error = TesBrowserWorkerError(
                "TES worker did not exit after forced termination"
            )
    if cleanup_error is not None:
        raise cleanup_error


def _worker_command(directory: Path) -> list[str]:
    return [sys.executable, "-m", __name__, str(directory)]


def fetch_tes_reports(
    source: Metasource,
    fetched_at: str,
    *,
    max_items: int,
) -> list[RawItem]:
    if os.name != "posix":
        raise TesBrowserWorkerError(
            "TES browser deadline requires POSIX process-group supervision; "
            "browser was not started on this platform"
        )
    try:
        _process_snapshot()
    except (OSError, subprocess.SubprocessError) as exc:
        raise TesBrowserWorkerError(
            "TES browser supervision requires process enumeration; "
            f"browser was not started: {exc}"
        ) from exc
    with tempfile.TemporaryDirectory(prefix="tes-browser-") as temporary:
        directory = Path(temporary)
        (directory / "request.json").write_text(json.dumps({
            "source": asdict(source), "fetched_at": fetched_at,
            "max_items": max_items,
        }), encoding="utf-8")
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(filter(None, (
            str(Path(__file__).resolve().parents[2]),
            environment.get("PYTHONPATH", ""),
        )))
        deadline = time.monotonic() + TES_BROWSER_DEADLINE_SECONDS
        # Files, not pipes: a surviving descendant cannot keep communicate()
        # waiting for EOF, and no result is read before a successful child exit.
        with (directory / "worker.log").open("wb") as log:
            process = subprocess.Popen(
                _worker_command(directory), stdin=subprocess.DEVNULL,
                stdout=log, stderr=log, env=environment, start_new_session=True,
            )
            owned: dict[int, tuple[int, str]] = {}
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TesBrowserTimeoutError()
                    _track_descendants(process.pid, owned)
                    if time.monotonic() >= deadline:
                        continue
                    if process.poll() is not None:
                        break
                    time.sleep(min(_POLL_SECONDS, remaining))
                if process.returncode != 0:
                    raise TesBrowserWorkerError(
                        f"TES browser worker exited with status {process.returncode}"
                    )
                result = json.loads((directory / "result.json").read_text("utf-8"))
                if result["status"] == "error":
                    raise TesBrowserWorkerError(
                        f"{result['error_class']}: {result['error_message']}"
                    )
                items = [RawItem(**item) for item in result["items"]]
                if time.monotonic() >= deadline:
                    raise TesBrowserTimeoutError()
                return items
            finally:
                primary_error = sys.exc_info()[1]
                try:
                    _stop_worker(process, owned, TES_BROWSER_TERMINATE_GRACE_SECONDS)
                except Exception as exc:
                    if primary_error is None:
                        raise
                    logger.warning("TES cleanup also failed: %s", exc)


def _worker_main(directory: Path) -> None:
    from .minhacienda import _fetch_minhacienda_tes_reports_in_browser

    request = json.loads((directory / "request.json").read_text("utf-8"))
    try:
        items = _fetch_minhacienda_tes_reports_in_browser(
            Metasource(**request["source"]), request["fetched_at"],
            max_items=request["max_items"],
        )
        result = {"status": "ok", "items": [asdict(item) for item in items]}
    except Exception as exc:
        result = {"status": "error", "error_class": type(exc).__name__,
                  "error_message": str(exc)}
    partial = directory / "result.partial.json"
    partial.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    partial.replace(directory / "result.json")


if __name__ == "__main__":
    _worker_main(Path(sys.argv[1]))
