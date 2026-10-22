"""Offline subprocess exercising the real TES worker and browser lifecycle."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace


def _hang() -> None:
    while True:
        time.sleep(0.1)


def _descendant(state: Path, name: str) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    (state / f"{name}.pid").write_text(str(os.getpid()))
    if name in {"driver", "browser"}:
        child = "browser" if name == "driver" else "renderer"
        subprocess.Popen(
            [sys.executable, "-m", "tests.tes_browser_worker_fixture",
             "descendant", str(state), child],
            # Match Playwright: driver shares worker group; Chromium detaches.
            start_new_session=name == "driver",
        )
    _hang()


def _run(directory: Path, mode: str, state: Path) -> None:
    from colombia_forecasting_desk.source_fetching import minhacienda, tes_browser

    (state / "worker.pid").write_text(str(os.getpid()))

    def stage(name: str) -> None:
        (state / name).touch()
        if mode == f"{name}_hang":
            _hang()

    class Page:
        url = ""

        def goto(self, url, **kwargs):
            self.url = url
            stage("fetch")
            if mode == "fetch_and_close_fail":
                raise ValueError("primary TES fetch failure")

        def wait_for_load_state(self, *args, **kwargs):
            pass

        def content(self):
            return '''<main><a href="/documents/d/portal/tes-20261014">
                Subasta 17 COP Octubre 14 de 2026
            </a></main>'''

    class Context:
        request = SimpleNamespace(get=lambda *a, **k: SimpleNamespace(
            ok=True, body=lambda: b"%PDF offline fixture",
        ))

        def new_page(self):
            return Page()

        def close(self):
            stage("context_close")
            if mode in {"fetch_and_close_fail", "close_fail"}:
                raise RuntimeError("context cleanup failed")

    class Browser:
        def new_context(self):
            return Context()

        def close(self):
            stage("browser_close")
            if mode in {"fetch_and_close_fail", "close_fail"}:
                raise RuntimeError("browser cleanup failed")

    def launch(**kwargs):
        stage("launch")
        if mode in {"tree_hang", "worker_exits_first"}:
            subprocess.Popen([
                sys.executable, "-m", "tests.tes_browser_worker_fixture",
                "descendant", str(state), "driver",
            ])
            until = time.monotonic() + 3
            while not (state / "renderer.pid").exists():
                if time.monotonic() > until:
                    raise RuntimeError("offline process tree did not start")
                time.sleep(0.01)
            # Let the supervisor observe both process groups before leader exit.
            time.sleep(0.25)
            if mode == "tree_hang":
                _hang()
        return Browser()

    def stop():
        stage("driver_stop")
        if mode in {"fetch_and_close_fail", "close_fail"}:
            raise RuntimeError("driver cleanup failed")

    def start():
        stage("startup")
        return SimpleNamespace(chromium=SimpleNamespace(launch=launch), stop=stop)

    def parse(content, **kwargs):
        stage("parse")
        return (Path(__file__).parent / "fixtures" / "minhacienda_tes_reports"
                / "tes_20261014_excerpt.txt").read_text()

    sys.modules["playwright.sync_api"] = SimpleNamespace(
        TimeoutError=TimeoutError,
        sync_playwright=lambda: SimpleNamespace(start=start),
    )
    minhacienda._extract_pdf_text_with_pdfplumber = parse
    tes_browser._worker_main(directory)
    if mode == "result_then_hang":
        # Even a complete result file is unusable until worker exit succeeds.
        _hang()
    if mode == "result_then_fail":
        raise RuntimeError("worker failed after writing a result")


if __name__ == "__main__":
    if sys.argv[1] == "descendant":
        _descendant(Path(sys.argv[2]), sys.argv[3])
    else:
        _run(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]))
