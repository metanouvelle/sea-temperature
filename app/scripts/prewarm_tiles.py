"""
Pre-warm SST tile cache for all major coastal regions.

Run this nightly so yesterday's data is always ready before users arrive.
No user should ever wait for a Copernicus fetch.

Usage:
    python -m app.scripts.prewarm_tiles

Fly.io cron (add to fly.toml):
    [processes]
      app = "uvicorn app.main:app --host 0.0.0.0 --port 8080"

    [[statics]]  # not needed but shown for context

Add this to fly.toml to run nightly at 3am UTC:
    [deploy]
      release_command = "python -m app.scripts.prewarm_tiles"

Or as a separate cron machine — see README for setup.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.database import init_db
from app.logger import get_logger
from app.scripts.render_tiles import render
from app.services.sst_cache import (
    ensure_tile,
    login_copernicus,
    tile_id_for,
    yesterday_utc,
)

log = get_logger(__name__)


# ── Coastal tile regions ──────────────────────────────────────────────────────
# Each entry is (name, lat_min, lat_max, lon_min, lon_max)
# We only include coastal strips — no point caching mid-ocean tiles
# that no swimmer will ever check.
#
# Strategy: 2° tiles means each region below generates a manageable number
# of tiles. The full list is ~800 tiles globally — about 50-100 MB of data.

COASTAL_REGIONS = [
    # ── Mediterranean ─────────────────────────────────────────────────────────
    ("Mediterranean West", 30, 46, -6, 16),
    ("Mediterranean East", 30, 42, 16, 37),
    ("Black Sea", 40, 47, 28, 42),
    ("Adriatic Sea", 38, 46, 12, 20),
    # ── Atlantic Europe ───────────────────────────────────────────────────────
    ("Canary Islands", 26, 32, -18, -1),
    ("Portugal & Spain Atlantic", 34, 44, -10, -1),
    ("Bay of Biscay", 42, 48, -10, 0),
    ("UK & Ireland", 48, 60, -11, 3),
    # ── Caribbean & Gulf of Mexico ────────────────────────────────────────────
    ("Caribbean", 10, 24, -86, -60),
    ("Gulf of Mexico", 18, 30, -98, -80),
    ("Bahamas & Florida", 22, 30, -82, -72),
    # ── North America East Coast ──────────────────────────────────────────────
    ("US East Coast", 24, 46, -82, -66),
    # ── North America West Coast ──────────────────────────────────────────────
    ("US West Coast", 30, 50, -130, -116),
    ("Baja California", 22, 32, -118, -108),
    # ── South America ─────────────────────────────────────────────────────────
    ("Brazil Coast", -24, 6, -50, -34),
    ("Argentina & Uruguay", -42, -24, -66, -50),
    # ── West Africa ───────────────────────────────────────────────────────────
    ("West Africa", -4, 16, -18, 8),
    # ── East Africa & Red Sea ─────────────────────────────────────────────────
    ("East Africa", -26, 12, 38, 52),
    ("Red Sea", 12, 30, 32, 44),
    ("Persian Gulf", 22, 30, 48, 58),
    # ── South & Southeast Asia ────────────────────────────────────────────────
    ("India West Coast", 8, 24, 68, 78),
    ("India East Coast", 8, 22, 78, 90),
    ("Sri Lanka", 5, 10, 78, 84),
    ("Thailand & Malaysia", 1, 16, 98, 106),
    ("Indonesia & Bali", -10, 6, 104, 120),
    ("Philippines", 5, 20, 116, 128),
    ("Vietnam Coast", 8, 22, 104, 110),
    # ── East Asia ─────────────────────────────────────────────────────────────
    ("Japan", 30, 46, 128, 146),
    ("South Korea", 32, 40, 124, 132),
    ("China Coast", 18, 40, 108, 124),
    # ── Pacific ───────────────────────────────────────────────────────────────
    ("Hawaii", 18, 24, -162, -154),
    ("French Polynesia", -20, -8, -154, -138),
    ("Fiji & Vanuatu", -22, -8, 164, 180),
    ("Australia East", -38, -16, 148, 156),
    ("Australia West", -36, -16, 112, 122),
    ("Australia North", -18, -8, 128, 140),
    ("New Zealand", -48, -34, 164, 178),
    # ── Indian Ocean Islands ──────────────────────────────────────────────────
    ("Maldives", -2, 8, 72, 74),
    ("Seychelles", -6, 6, 54, 58),
    ("Mauritius & Reunion", -22, -18, 54, 58),
]


def tiles_for_region(lat_min, lat_max, lon_min, lon_max):
    """Generate all 2°x2° tile IDs covering a region."""
    tiles = []
    lat = lat_min
    while lat < lat_max:
        lon = lon_min
        while lon < lon_max:
            tiles.append(tile_id_for(lat, lon))
            lon += 2.0
        lat += 2.0
    return list(set(tiles))  # deduplicate


def prewarm(date: str, max_workers: int = 6) -> dict:
    """
    Fetch all coastal tiles for a given date.
    Uses a thread pool — Copernicus fetches are I/O bound so parallelism helps.
    max_workers=6 is conservative; Copernicus rate limits aggressively.
    """
    # Collect all unique tiles across all regions
    all_tiles: set[str] = set()
    for name, lat_min, lat_max, lon_min, lon_max in COASTAL_REGIONS:
        region_tiles = tiles_for_region(lat_min, lat_max, lon_min, lon_max)
        all_tiles.update(region_tiles)
        log.info("%-35s  %d tiles", name, len(region_tiles))

    total = len(all_tiles)
    log.info("Total unique tiles to warm: %d", total)

    results = {"ok": 0, "skipped": 0, "failed": 0, "total": total}
    start = time.time()

    def fetch_one(tile_id: str) -> tuple[str, str]:
        """Returns (tile_id, 'ok'|'skipped'|'failed')"""
        try:
            fetched = ensure_tile(date, tile_id)
            return tile_id, "skipped" if fetched == 0 else "ok"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.warning("Failed tile %s: %s", tile_id, exc)
            return tile_id, "failed"

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_one, tid): tid for tid in all_tiles}
        done = 0
        for future in as_completed(futures):
            tile_id, status = future.result()
            results[status] += 1
            done += 1
            if done % 20 == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log.info(
                    "Progress: %d/%d  ✓%d  ↷%d  ✗%d  rate=%.1f/min  ETA=%.0fmin",
                    done,
                    total,
                    results["ok"],
                    results["skipped"],
                    results["failed"],
                    rate * 60,
                    eta / 60,
                )

    elapsed = time.time() - start
    log.info(
        "Done in %.1f min — fetched=%d  skipped(cached)=%d  failed=%d",
        elapsed / 60,
        results["ok"],
        results["skipped"],
        results["failed"],
    )
    return results


if __name__ == "__main__":
    log.info("Initialising database...")
    init_db()

    log.info("Logging into Copernicus...")
    login_copernicus()

    date = yesterday_utc()
    log.info("Pre-warming tiles for date: %s", date)

    results = prewarm(date)

    if results["failed"] > results["total"] * 0.1:
        # More than 10% failed — something is wrong
        raise SystemExit(f"Too many failures: {results['failed']}/{results['total']}")

    log.info("Pre-warm complete ✓")

    log.info("Rendering static tiles for date: %s", date)
    render(date)
    log.info("Static tile render complete ✓")
