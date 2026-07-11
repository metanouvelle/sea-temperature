"""SwimTemp — server-side beach search proxy with SQLite cache.

WHY: browsers on mobile networks share carrier IPs (CGNAT) and get
rate-limited by public Overpass servers. Proxying through our Fly.io
backend gives every device a clean datacenter IP, and the cache makes
repeat searches for popular cities instant and Overpass-free.

INSTALL:
  1. Copy this file to app/routes_beach_search.py
  2. In app/main.py add:
         from app.routes_beach_search import router as beach_search_router
         app.include_router(beach_search_router)
  3. Deploy. No new dependencies (stdlib urllib only).

Endpoint:  GET /api/beaches/search?lat=44.11&lon=15.23&radius_km=50
Returns:   {"elements": [...]}  — same shape as raw Overpass, so the
           frontend's processOverpass() works unchanged.
"""

import json
import os
import sqlite3
import time
import urllib.parse
import urllib.request

from fastapi import APIRouter, HTTPException

router = APIRouter()

# Fly.io volume in production; SWIMTEMP_DB env var or a local file for dev.
_DEFAULT_DB = "/data/sst.sqlite"
_LOCAL_DEV_DB = os.path.join(os.path.dirname(__file__), "dev-cache.sqlite")
DB_PATH = os.environ.get("SWIMTEMP_DB") or (
    _DEFAULT_DB if os.path.isdir(os.path.dirname(_DEFAULT_DB)) else _LOCAL_DEV_DB
)
CACHE_TTL_SECONDS = 24 * 3600  # beaches don't move; refresh daily
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
USER_AGENT = "SwimTemp/1.0 (+https://swimtemp.com)"


def _db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS beach_search_cache (
               key        TEXT PRIMARY KEY,
               payload    TEXT NOT NULL,
               updated_at INTEGER NOT NULL
           )""")
    return con


def _overpass_query(
    lat: float, lon: float, radius_m: int, include_resorts: bool
) -> str:
    around = f"(around:{radius_m},{lat},{lon})"
    resort = (
        (
            f'node["leisure"="beach_resort"]{around};'
            f'way["leisure"="beach_resort"]{around};'
        )
        if include_resorts
        else ""
    )
    return (
        "[out:json][timeout:20];("
        f'node["natural"="beach"]{around};'
        f'way["natural"="beach"]{around};'
        f'node["leisure"="beach"]{around};'
        f'way["leisure"="beach"]{around};'
        f"{resort}"
        ");out center tags;"
    )


@router.get("/api/beaches/search")
def beaches_search(
    lat: float, lon: float, radius_km: float = 50, include_resorts: int = 0
) -> dict:
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise HTTPException(status_code=422, detail="invalid coordinates")
    radius_km = max(1.0, min(radius_km, 100.0))

    # ~1km grid so nearby searches share a cache entry
    key = f"{lat:.2f}:{lon:.2f}:{int(radius_km)}:{1 if include_resorts else 0}"

    con = _db()
    try:
        row = con.execute(
            "SELECT payload, updated_at FROM beach_search_cache WHERE key = ?",
            (key,),
        ).fetchone()
        if row and time.time() - row[1] < CACHE_TTL_SECONDS:
            return json.loads(row[0])

        query = _overpass_query(lat, lon, int(radius_km * 1000), bool(include_resorts))
        body = urllib.parse.urlencode({"data": query}).encode()

        data = None
        for base in OVERPASS_MIRRORS:
            try:
                req = urllib.request.Request(
                    base, data=body, headers={"User-Agent": USER_AGENT}
                )
                with urllib.request.urlopen(req, timeout=25) as resp:
                    candidate = json.loads(resp.read())
                # Overpass reports internal timeouts as HTTP 200 + "remark".
                remark = candidate.get("remark", "")
                if not candidate.get("elements") and remark:
                    continue  # degraded answer — try the next mirror
                data = candidate
                break
            except Exception:
                continue

        if data is None:
            # Serve stale cache over an error if we have it
            if row:
                return json.loads(row[0])
            raise HTTPException(status_code=503, detail="overpass_unavailable")

        payload = {"elements": data.get("elements", [])}
        con.execute(
            "INSERT OR REPLACE INTO beach_search_cache (key, payload, updated_at) "
            "VALUES (?, ?, ?)",
            (key, json.dumps(payload), int(time.time())),
        )
        con.commit()
        return payload
    finally:
        con.close()
