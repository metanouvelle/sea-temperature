"""
app/scripts/update_warmest.py
─────────────────────────────────────────────────────────────────────────────
Nightly cron job: find warmest beaches per ocean region using OpenStreetMap
data and your existing Copernicus SST cache.

Flow per region:
  1. Query Overpass API for real beaches (natural=beach / leisure=beach)
  2. For each beach, look up nearest SST from sst_grid in SQLite
  3. Sort by temperature, store top 50 in warmest_beaches table

Run manually:
    python -m app.scripts.update_warmest

Add to cron (runs after prewarm at 5am UTC):
    0 5 * * * cd /app && python -m app.scripts.update_warmest >> /data/warmest.log 2>&1
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sqlite3
import time
import logging
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from pathlib import Path

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

DB_PATH = Path(os.getenv("SST_DB_PATH", "/data/sst.sqlite"))

# ── Ocean region bounding boxes ────────────────────────────────────────────────
# Format: (min_lat, max_lat, min_lon, max_lon)
REGIONS = {
    "mediterranean": (30, 47, -6, 34),
    "adriatic": (39, 46, 12, 21),
    "atlantic": (-40, 65, -80, 0),
    "pacific": (-50, 60, 120, 180),
    "indian": (-50, 25, 30, 120),
    "redsea": (12, 30, 32, 44),
}

# How many beaches to store per region
TOP_N = 50

# Overpass endpoints (fallback to mirror)
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]


def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    return R * 2 * asin(sqrt(a))


def overpass_query(min_lat, max_lat, min_lon, max_lon) -> list[dict]:
    """Query Overpass for beaches in a bounding box."""
    # Overpass bbox format: south, west, north, east
    bbox = f"{min_lat},{min_lon},{max_lat},{max_lon}"
    query = f"""
        [out:json][timeout:60];
        (
          node["natural"="beach"]({bbox});
          way["natural"="beach"]({bbox});
          node["leisure"="beach"]({bbox});
          way["leisure"="beach"]({bbox});
          node["leisure"="beach_resort"]({bbox});
          way["leisure"="beach_resort"]({bbox});
        );
        out center tags;
    """

    for url in OVERPASS_URLS:
        try:
            resp = httpx.post(
                url,
                data={"data": query},
                timeout=90,
                headers={"User-Agent": "SwimTemp/1.0 (swimtemp.com)"},
            )
            resp.raise_for_status()
            elements = resp.json().get("elements", [])
            log.info("Overpass returned %d elements from %s", len(elements), url)
            return elements
        except Exception as e:
            log.warning("Overpass failed on %s: %s", url, e)

    return []


def parse_beaches(elements: list[dict]) -> list[dict]:
    """Extract name, lat, lon from Overpass elements. Deduplicate by name."""
    seen = set()
    beaches = []
    for el in elements:
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        if not lat or not lon:
            continue
        tags = el.get("tags") or {}
        name = tags.get("name:en") or tags.get("name") or ""
        # Skip if name is not latin script (rough check)
        if name and not any(c.isascii() and c.isalpha() for c in name[:10]):
            # Try alt names
            name = (
                tags.get("name:en")
                or tags.get("int_name")
                or tags.get("alt_name")
                or ""
            )
        if not name:
            continue  # skip unnamed beaches
        if name in seen:
            continue
        seen.add(name)
        beaches.append({"name": name, "lat": float(lat), "lon": float(lon)})
    return beaches


def get_latest_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT date FROM sst_grid ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def lookup_sst(
    conn: sqlite3.Connection, lat: float, lon: float, date: str, radius_deg: float = 1.0
) -> float | None:
    """Find nearest SST from sst_grid within radius_deg degrees."""
    rows = conn.execute(
        """
        SELECT lat, lon, temp_c
        FROM sst_grid
        WHERE date = ?
          AND lat BETWEEN ? AND ?
          AND lon BETWEEN ? AND ?
          AND temp_c IS NOT NULL
        LIMIT 50
        """,
        (date, lat - radius_deg, lat + radius_deg, lon - radius_deg, lon + radius_deg),
    ).fetchall()

    if not rows:
        return None

    # Find closest point
    best = min(rows, key=lambda r: haversine(lat, lon, r[0], r[1]))
    dist_km = haversine(lat, lon, best[0], best[1])
    if dist_km > 50:  # too far — no data nearby
        return None
    return round(float(best[2]), 1)


def update_region(
    conn: sqlite3.Connection, region: str, bounds: tuple, date: str
) -> int:
    min_lat, max_lat, min_lon, max_lon = bounds
    log.info("── %s: querying Overpass...", region.upper())

    elements = overpass_query(min_lat, max_lat, min_lon, max_lon)
    beaches = parse_beaches(elements)
    log.info("%s: %d named beaches found", region, len(beaches))

    if not beaches:
        log.warning("%s: no beaches — skipping", region)
        return 0

    # Look up SST for each beach
    with_temp = []
    for b in beaches:
        temp = lookup_sst(conn, b["lat"], b["lon"], date)
        if temp is not None:
            with_temp.append({**b, "temp_c": temp})

    log.info("%s: %d beaches have SST data", region, len(with_temp))

    # Sort warmest first, deduplicate by ~1° cell, take top N
    with_temp.sort(key=lambda x: x["temp_c"], reverse=True)
    seen_cells = set()
    deduped = []
    for b in with_temp:
        cell = (round(b["lat"]), round(b["lon"]))
        if cell in seen_cells:
            continue
        seen_cells.add(cell)
        deduped.append(b)
    top = deduped[:TOP_N]

    # Delete old entries for this region and insert fresh
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    cur.execute("DELETE FROM warmest_beaches WHERE region = ?", (region,))
    cur.executemany(
        """
        INSERT INTO warmest_beaches (region, name, lat, lon, temp_c, date, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(region, b["name"], b["lat"], b["lon"], b["temp_c"], date, now) for b in top],
    )
    conn.commit()
    log.info("%s: stored %d warmest beaches", region, len(top))
    return len(top)


def is_data_fresh(date: str | None) -> bool:
    """Check if SST data is from today or yesterday (UTC)."""
    if not date:
        return False
    from datetime import date as date_cls, timedelta

    today = date_cls.today().isoformat()
    yesterday = (date_cls.today() - timedelta(days=1)).isoformat()
    return date in (today, yesterday)


def run():
    log.info("=== update_warmest starting ===")
    start = time.time()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    date = get_latest_date(conn)

    if not date:
        log.error("No SST data in sst_grid — run prewarm first")
        conn.close()
        return

    if not is_data_fresh(date):
        log.warning(
            "SST data is stale (%s) — prewarm may not have run yet. Skipping.", date
        )
        conn.close()
        return

    log.info("SST data is fresh (%s) — proceeding", date)

    log.info("Using SST date: %s", date)

    total = 0
    for region, bounds in REGIONS.items():
        try:
            count = update_region(conn, region, bounds, date)
            total += count
        except Exception as e:
            log.error("%s failed: %s", region, e)
        # Respect Overpass rate limit between regions
        time.sleep(5)

    conn.close()
    elapsed = round(time.time() - start, 1)
    log.info("=== update_warmest done: %d beaches stored in %.1fs ===", total, elapsed)


if __name__ == "__main__":
    run()
