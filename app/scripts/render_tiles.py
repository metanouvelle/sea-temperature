"""
app/scripts/render_tiles.py

Renders SST data from SQLite into XYZ map tiles (256x256 PNG).
Run after prewarm_tiles.py to generate static tiles for fast map display.

Usage:
    python -m app.scripts.render_tiles

Output:
    /data/tiles/{date}/{z}/{x}/{y}.png
    /data/tiles/latest -> symlink to most recent date folder

Zoom levels rendered: 3-7
    Zoom 3: 64 tiles   (~world overview)
    Zoom 4: 256 tiles  (~continent level)
    Zoom 5: 1024 tiles (~country level)
    Zoom 6: 4096 tiles (~region level)
    Zoom 7: 16384 tiles (~city level, only tiles with data)
"""

import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from PIL import Image

load_dotenv()
from app.logger import get_logger

log = get_logger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
TILE_SIZE   = 256
ZOOM_MIN    = 3
ZOOM_MAX    = 7
TILES_DIR   = Path(os.getenv("SST_TILES_DIR", "/data/tiles"))
DB_PATH     = Path(os.getenv("SST_DB_PATH",   "/data/sst.sqlite"))
 
# ── Temperature band colors (RGBA) ────────────────────────────────────────────
# Matches frontend: cold=blue brave=cyan nice=green perfect=amber very_warm=red
COLORS = [
    (-99, 17,  (37,  99,  235, 200)),   # cold      #2563eb
    (17,  20,  (14,  165, 233, 200)),   # brave     #0ea5e9
    (20,  23,  (34,  197, 94,  200)),   # nice      #22c55e
    (23,  26,  (245, 158, 11,  200)),   # perfect   #f59e0b
    (26,  99,  (239, 68,  68,  200)),   # very_warm #ef4444
]
 
def temp_to_rgba(temp: float) -> tuple:
    for t_min, t_max, rgba in COLORS:
        if t_min <= temp < t_max:
            return rgba
    return COLORS[-1][2]  # fallback to very_warm
 
# ── XYZ tile math ─────────────────────────────────────────────────────────────
def tile_to_bbox(x: int, y: int, z: int) -> tuple:
    """Convert XYZ tile to (lon_min, lat_min, lon_max, lat_max)."""
    n = 2 ** z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon_min, lat_min, lon_max, lat_max
 
def lon_lat_to_pixel(lon: float, lat: float,
                     lon_min: float, lat_min: float,
                     lon_max: float, lat_max: float) -> tuple:
    """Convert lon/lat to pixel coordinates within a tile."""
    # Mercator projection for pixel y
    def lat_to_mercator(lat_deg):
        lat_rad = math.radians(lat_deg)
        return math.log(math.tan(math.pi / 4 + lat_rad / 2))
 
    merc_min = lat_to_mercator(lat_min)
    merc_max = lat_to_mercator(lat_max)
    merc_pt  = lat_to_mercator(lat)
 
    px = int((lon - lon_min) / (lon_max - lon_min) * TILE_SIZE)
    py = int((merc_max - merc_pt) / (merc_max - merc_min) * TILE_SIZE)
    return px, py
 
def latlon_to_tile(lat: float, lon: float, z: int) -> tuple:
    """Get the XYZ tile that contains a lat/lon point."""
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y
 
# ── Load all SST data for a date ──────────────────────────────────────────────
def load_sst_data(date: str) -> np.ndarray:
    """
    Load all SST points for a date from SQLite.
    Returns numpy array of shape (N, 3) — [lat, lon, temp_c]
    """
    import sqlite3
    log.info("Loading SST data for %s from %s...", date, DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT lat, lon, temp_c FROM sst_grid WHERE date=?", (date,)
    ).fetchall()
    conn.close()
    log.info("Loaded %d points", len(rows))
    return np.array(rows, dtype=np.float32)
 
# ── Render a single tile ──────────────────────────────────────────────────────
def render_tile(points: np.ndarray, x: int, y: int, z: int,
                out_path: Path) -> bool:
    """
    Render SST points that fall within tile x,y,z into a 256x256 PNG.
    Returns True if tile had data, False if empty (no file written).
    """
    lon_min, lat_min, lon_max, lat_max = tile_to_bbox(x, y, z)
 
    # Filter points to this tile's bbox (with small buffer for edge blending)
    buf = (lon_max - lon_min) * 0.1
    mask = (
        (points[:, 0] >= lat_min - buf) &
        (points[:, 0] <= lat_max + buf) &
        (points[:, 1] >= lon_min - buf) &
        (points[:, 1] <= lon_max + buf)
    )
    tile_points = points[mask]
 
    if len(tile_points) == 0:
        return False
 
    # Create RGBA image — transparent background
    img_array = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
 
    # Calculate pixel size in degrees to determine dot radius
    lon_span = lon_max - lon_min
    px_per_deg = TILE_SIZE / lon_span
    # At 0.05° resolution, each point should cover ~0.05° worth of pixels
    radius = max(1, int(0.06 * px_per_deg))
 
    for lat, lon, temp in tile_points:
        px, py = lon_lat_to_pixel(lon, lat, lon_min, lat_min, lon_max, lat_max)
        rgba = temp_to_rgba(float(temp))
 
        # Paint a small square of radius pixels
        x0 = max(0, px - radius)
        x1 = min(TILE_SIZE, px + radius + 1)
        y0 = max(0, py - radius)
        y1 = min(TILE_SIZE, py + radius + 1)
 
        if x0 < x1 and y0 < y1:
            img_array[y0:y1, x0:x1] = rgba
 
    if img_array[:, :, 3].max() == 0:
        return False  # all transparent, skip
 
    # Apply slight gaussian blur for smooth blending between cells
    # Must blur RGB and alpha separately to preserve transparency
    from PIL import ImageFilter
    img = Image.fromarray(img_array, 'RGBA')
    blur_radius = max(1, radius // 2)
    r, g, b, a = img.split()
    r = r.filter(ImageFilter.GaussianBlur(blur_radius))
    g = g.filter(ImageFilter.GaussianBlur(blur_radius))
    b = b.filter(ImageFilter.GaussianBlur(blur_radius))
    a = a.filter(ImageFilter.GaussianBlur(blur_radius))
    img = Image.merge('RGBA', (r, g, b, a))
 
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if img.mode != 'RGBA':
        img = img.convert('RGBA')
    log.info("Pre-save mode: %s", img.mode)
    if img.mode != 'RGBA':
        log.warning("Mode was %s, forcing RGBA conversion", img.mode)
        img = img.convert('RGBA')
    log.info("Post-convert mode: %s", img.mode)
    img.save(out_path, 'PNG')
    
    # Verify immediately after save
    verify = Image.open(out_path)
    log.info("Post-save mode: %s", verify.mode)
    img.save(out_path, 'PNG')
    return True
 
# ── Render all tiles for a zoom level ────────────────────────────────────────
def render_zoom(points: np.ndarray, date: str, z: int) -> dict:
    """Render all tiles for a given zoom level."""
    n = 2 ** z
    out_dir = TILES_DIR / date / str(z)
 
    log.info("Zoom %d: rendering up to %dx%d=%d tiles...", z, n, n, n * n)
 
    # Find which tiles actually have data — avoid rendering empty tiles
    tiles_with_data = set()
    for lat, lon, _ in points:
        tx, ty = latlon_to_tile(float(lat), float(lon), z)
        tx = max(0, min(n - 1, tx))
        ty = max(0, min(n - 1, ty))
        tiles_with_data.add((tx, ty))
 
    log.info("Zoom %d: %d tiles have SST data", z, len(tiles_with_data))
 
    results = {"rendered": 0, "empty": 0, "failed": 0}
    start = time.time()
    done = 0
    total = len(tiles_with_data)
 
    def render_one(args):
        tx, ty = args
        out_path = out_dir / str(tx) / f"{ty}.png"
        if out_path.exists():
            return "skipped"
        try:
            had_data = render_tile(points, tx, ty, z, out_path)
            return "rendered" if had_data else "empty"
        except Exception as e:
            log.warning("Failed tile z=%d x=%d y=%d: %s", z, tx, ty, e)
            return "failed"

 
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(render_one, tile): tile for tile in tiles_with_data}
        for future in as_completed(futures):
            result = future.result()
            if result == "rendered": results["rendered"] += 1
            elif result == "empty":  results["empty"] += 1
            elif result == "failed": results["failed"] += 1
            done += 1
            if done % 100 == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 1
                eta = (total - done) / rate
                log.info("Zoom %d: %d/%d  rendered=%d  ETA=%.0fs",
                         z, done, total, results["rendered"], eta)
 
    return results
 
# ── Main ──────────────────────────────────────────────────────────────────────
def render(date: str) -> None:
    """Render all zoom levels for a given date."""
    start_total = time.time()
    log.info("Starting tile render for date: %s", date)
    log.info("Output directory: %s", TILES_DIR / date)
 
    points = load_sst_data(date)
    if len(points) == 0:
        log.error("No SST data found for date %s — run prewarm first", date)
        return
 
    total_rendered = 0
    for z in range(ZOOM_MIN, ZOOM_MAX + 1):
        results = render_zoom(points, date, z)
        total_rendered += results["rendered"]
        log.info("Zoom %d done — rendered=%d empty=%d failed=%d",
                 z, results["rendered"], results["empty"], results["failed"])
 
    # Update symlink: /data/tiles/latest -> /data/tiles/{date}
    latest = TILES_DIR / "latest"
    if latest.is_symlink():
        latest.unlink()
    latest.symlink_to(TILES_DIR / date)
    log.info("Updated symlink: latest -> %s", date)
 
    elapsed = (time.time() - start_total) / 60
    log.info("Render complete — %d tiles in %.1f min", total_rendered, elapsed)
 
 
if __name__ == "__main__":
    import sys
    from app.database import init_db
 
    init_db()
 
    # Use date from arg or yesterday
    if len(sys.argv) > 1:
        date = sys.argv[1]
    else:
        from app.scripts.prewarm_tiles import yesterday_utc
        date = yesterday_utc()
 
    render(date)
