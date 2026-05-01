# Source HTML fixtures

Each `<source_id>/<YYYY-MM-DD>.html` file is a captured response from the live URL
in `config/metasources.yaml`. They are used by `tests/test_fixture_parsers.py` to
exercise parsers without hitting the network.

To refresh a fixture:

```bash
uv run python - <<'PY'
import httpx, pathlib
from colombia_forecasting_desk.fetchers import DEFAULT_HEADERS
url = "https://www.example.com/feed"
sid = "example_source"
date = "2026-04-29"
with httpx.Client(timeout=20.0, follow_redirects=True, headers=DEFAULT_HEADERS) as c:
    r = c.get(url)
out = pathlib.Path(f"tests/fixtures/{sid}/{date}.html")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_bytes(r.content)
print(r.status_code, len(r.content))
PY
```

Some fixtures intentionally capture failure modes (bot-block pages, SPA shells)
so the detector tests have something to assert against. Do not delete them.

## Synthetic fixtures

A small number of fixtures could not be captured live (the runtime that created
them could not reach the host). These files are built from documented page
structure and clearly marked with a comment at the top of the file.

| source_id | file | reason |
| --- | --- | --- |
| `dane_icoced` | `dane_icoced/2026-05-01.html` | `dane.gov.co` blocked by host allowlist |

Synthetic fixtures still provide useful offline test coverage: the ICOCED
parser keys off the Excel filename pattern (`anex-ICOCED-{mes}{anio}.xlsx`),
not on live HTML structure, so the test exercises the real parser logic.
Replace with a live capture when convenient using the refresh script above.
