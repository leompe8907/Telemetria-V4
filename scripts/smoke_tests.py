from __future__ import annotations

import json
from pathlib import Path

import requests

try:
    from dotenv import dotenv_values
except Exception:  # pragma: no cover
    dotenv_values = None


def main() -> int:
    base = "http://127.0.0.1:8001"

    env = {}
    if dotenv_values is not None:
        env = {k: v for k, v in (dotenv_values(Path(".env")) or {}).items() if k and v}

    ro = (env.get("TELEMETRIA_API_KEY_RO") or "").strip()
    rw = (env.get("TELEMETRIA_API_KEY_RW") or "").strip()
    assert ro and rw, "Missing TELEMETRIA_API_KEY_RO/RW in .env"

    def req(method: str, path: str, key: str | None, **kwargs):
        headers = kwargs.pop("headers", {})
        if key is not None:
            headers = {**headers, "X-Telemetria-Key": key}
        url = base + path
        r = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            try:
                body = r.json()
            except Exception:
                body = r.text
        else:
            body = r.text
        return r.status_code, body, r.headers

    def show(name: str, status: int, body):
        print(f"\n[{name}] status={status}")
        if isinstance(body, (dict, list)):
            print(json.dumps(body, ensure_ascii=False)[:1200])
        else:
            print(str(body)[:600])

    st, body, _ = req("GET", "/delancert/health/", ro)
    show("health (RO)", st, body)

    st, body, _ = req("GET", "/delancert/health/", rw)
    show("health (RW)", st, body)

    st, body, _ = req(
        "POST",
        "/delancert/telemetry/run/",
        ro,
        json={"limit": 1, "batch_size": 1, "merge_batch_size": 1, "backfill_last_n": 0},
    )
    show("run (RO) expect 403/401", st, body)

    st, body, _ = req(
        "POST",
        "/delancert/telemetry/run/",
        rw,
        json={"limit": 1, "batch_size": 50, "merge_batch_size": 50, "backfill_last_n": 0},
    )
    show("run (RW)", st, body)

    st, body, headers = req("POST", "/delancert/telemetry/run/", rw, json={"limit": 1})
    show("run again (rate limit expected)", st, body)
    if "Retry-After" in headers:
        print("Retry-After:", headers.get("Retry-After"))

    st, body, _ = req("GET", "/delancert/jobs/runs/?limit=5", ro)
    show("jobs/runs (RO)", st, body)

    st, body, _ = req("GET", "/delancert/dashboard/overview/", ro)
    show("dashboard/overview (RO)", st, body)

    st, body, _ = req("GET", "/delancert/health/", None)
    show("health (no key) expect 403/401", st, body)

    print("\nDONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

