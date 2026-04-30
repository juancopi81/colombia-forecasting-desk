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
