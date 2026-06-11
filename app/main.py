"""Main entry point for the sea temperature project."""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.templating import Jinja2Templates

from app.content.beaches import BEACH_MONTHLY_AVG, BEACHES, BEACHES_BY_SLUG
from app.database import init_db
from app.logger import get_logger
from app.middleware import TimingMiddleware
from app.scripts.prewarm_tiles import prewarm, yesterday_utc
from app.services.optimized_query import query_points_in_bbox_optimized
from app.services.sst_cache import (
    ensure_tile,
    login_copernicus,
    point_temperature,
    tile_exists,
    tile_id_for,
)

log = get_logger(__name__)

PREWARM_SECRET = os.getenv("PREWARM_SECRET", "")

_tile_executor = ThreadPoolExecutor(max_workers=12)

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.add_middleware(TimingMiddleware)


# ── Helpers ───────────────────────────────────────────────────────────────────


def get_latest_available_date() -> str:
    """Return most recent date with SST data, fallback to yesterday."""
    try:
        from app.database import connect

        conn = connect()
        result = conn.execute(
            "SELECT date FROM sst_tile ORDER BY date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if result:
            return result[0]
    except Exception:
        pass
    return yesterday_utc()


# ── Startup ───────────────────────────────────────────────────────────────────


@app.on_event("startup")
def _startup():
    """Init database and Copernicus on startup."""
    init_db()
    login_copernicus()


# ── Background prewarm ────────────────────────────────────────────────────────
def _cleanup_old_data():
    """Delete SST data older than 3 days to keep database small."""
    try:
        from app.database import connect

        conn = connect()
        # Keep only last 3 days in case of prewarm failures
        deleted = conn.execute(
            "DELETE FROM sst_grid WHERE date < date('now', '-3 days')"
        ).rowcount
        conn.execute("DELETE FROM sst_tile WHERE date < date('now', '-3 days')")
        conn.execute("VACUUM")  # reclaim disk space after deletion
        conn.commit()
        conn.close()
        log.info("Cleanup complete — deleted %d old grid rows", deleted)
    except Exception as e:
        log.error("Cleanup failed: %s", e)


def _background_prewarm():
    log.info("Background prewarm starting...")
    try:
        # Always fetch yesterday's fresh data, not what's already cached
        date = yesterday_utc()
        start = time.time()
        results = prewarm(date)
        elapsed = round((time.time() - start) / 60, 1)

        log.info(
            "Prewarm complete — %s fetched, %s skipped, %s failed, %.1f min",
            results.get("ok", 0),
            results.get("skipped", 0),
            results.get("failed", 0),
            elapsed,
        )

        Path("/data/prewarm_status.json").write_text(
            json.dumps(
                {
                    "last_run": date,
                    "elapsed_minutes": elapsed,
                    "fetched": results.get("ok", 0),
                    "skipped": results.get("skipped", 0),
                    "failed": results.get("failed", 0),
                    "total": results.get("total", 0),
                    "success": True,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        # Clean up old data to keep database small
        _cleanup_old_data()
    except Exception as e:
        log.error("Background prewarm failed: %s", e)


# ── Pages ─────────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/map", response_class=HTMLResponse)
def map_page(request: Request):
    return templates.TemplateResponse("sea-temp-map.html", {"request": request})


@app.get("/about", response_class=HTMLResponse)
def about(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})


@app.get("/beaches", response_class=HTMLResponse)
def beaches_page(request: Request):
    return templates.TemplateResponse("beaches.html", {"request": request})


@app.get("/beach/{slug}", response_class=HTMLResponse)
def beach_page(request: Request, slug: str):
    beach = BEACHES_BY_SLUG.get(slug)
    if not beach:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(
        "beach.html",
        {
            "request": request,
            "beach": beach,
            "monthly_avg": BEACH_MONTHLY_AVG.get(slug, []),
            "current_month": datetime.now().month,  # 1-indexed for Jinja
        },
    )


# ── API ───────────────────────────────────────────────────────────────────────


@app.get("/api/point")
def api_point(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(10.0, gt=0, le=50),
):
    d = get_latest_available_date()
    try:
        return point_temperature(d, lat, lon, radius_km)
    except Exception as exc:
        log.error("api_point error lat=%s lon=%s: %s", lat, lon, exc)
        raise HTTPException(
            status_code=503, detail="Service temporarily unavailable"
        ) from exc


@app.get("/api/grid")
def get_grid(bbox: str, zoom: float = Query(8.0)):
    """
    Return cached SST grid points for a bounding box.
    bbox format: south,west,north,east
    """
    south, west, north, east = map(float, bbox.split(","))
    date = get_latest_available_date()
    bounds = {"min_lat": south, "max_lat": north, "min_lon": west, "max_lon": east}
    pending = 0
    lat = south
    while lat <= north:
        lon = west
        while lon <= east:
            tid = tile_id_for(lat, lon)
            if not tile_exists(date, tid):
                f = _tile_executor.submit(ensure_tile, date, tid)
                f.add_done_callback(
                    lambda fut, d=date, t=tid: (
                        log.error(
                            "bg tile fetch %s:%s failed: %s", d, t, fut.exception()
                        )
                        if fut.exception()
                        else None
                    )
                )
                pending += 1
            lon += 2.0
        lat += 2.0
    points = query_points_in_bbox_optimized(date, bounds, zoom=zoom)
    return {
        "points": [
            {"lat": p[0], "lon": p[1], "temp_c": round(p[2], 2)} for p in points
        ],
        "pending": pending,
    }


@app.get("/api/beaches")
def api_beaches():
    """Return all beaches with current SST temperature, sorted warmest first."""
    date = get_latest_available_date()

    def fetch(beach):
        try:
            result = point_temperature(date, beach["lat"], beach["lon"], radius_km=25)
            temp = result["mean_c"] if result and result.get("status") == "ok" else None
        except Exception:
            temp = None
        return {**beach, "temp_c": temp, "date": date}

    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(fetch, b) for b in BEACHES]
        results = [f.result() for f in as_completed(futures)]

    results.sort(
        key=lambda x: x["temp_c"] if x["temp_c"] is not None else -99, reverse=True
    )
    return {"beaches": results, "date": date}


@app.get("/api/beach/{slug}")
def api_beach(slug: str):
    """Return temperature for a single beach."""
    beach = BEACHES_BY_SLUG.get(slug)
    if not beach:
        raise HTTPException(status_code=404)
    date = get_latest_available_date()
    result = point_temperature(date, beach["lat"], beach["lon"], radius_km=25)
    temp = result["mean_c"] if result and result.get("status") == "ok" else None
    return {**beach, "temp_c": temp, "date": date}


@app.get("/api/status")
def api_status():
    """Health + prewarm status endpoint."""
    status_file = Path("/data/prewarm_status.json")
    prewarm_info = {}
    if status_file.exists():
        try:
            prewarm_info = json.loads(status_file.read_text(encoding="utf-8"))
        except Exception:
            prewarm_info = {"error": "could not read status file"}
    return {"status": "ok", "prewarm": prewarm_info}


@app.post("/api/admin/prewarm")
def trigger_prewarm(authorization: str = Header(None)):
    """Trigger background prewarm. Protected by secret token."""
    if not PREWARM_SECRET or authorization != f"Bearer {PREWARM_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    # Clear status so polling knows a fresh run is in progress
    Path("/data/prewarm_status.json").write_text(
        json.dumps({"last_run": "running", "success": False}), encoding="utf-8"
    )
    threading.Thread(target=_background_prewarm, daemon=True).start()
    return {"status": "started"}


# ── Sitemap ───────────────────────────────────────────────────────────────────

SITE_URL = "https://swimtemp.com"


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return f"""User-agent: *
Allow: /

Sitemap: {SITE_URL}/sitemap.xml
"""


@app.get("/sitemap.xml")
def sitemap_xml():
    static_pages = [
        {"loc": f"{SITE_URL}/", "priority": "1.0"},
        {"loc": f"{SITE_URL}/beaches", "priority": "0.9"},
        {"loc": f"{SITE_URL}/map", "priority": "0.7"},
        {"loc": f"{SITE_URL}/about", "priority": "0.6"},
        {"loc": f"{SITE_URL}/privacy", "priority": "0.3"},
    ]

    beach_pages = [
        {
            "loc": f"{SITE_URL}/beach/{beach['slug']}",
            "priority": "0.8",
        }
        for beach in BEACHES
    ]

    urls = static_pages + beach_pages

    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]

    for url in urls:
        xml_parts.append(f"""  <url>
    <loc>{escape(url["loc"])}</loc>
    <changefreq>daily</changefreq>
    <priority>{url["priority"]}</priority>
  </url>""")

    xml_parts.append("</urlset>")

    return Response(
        content="\n".join(xml_parts),
        media_type="application/xml",
    )


@app.get("/privacy", response_class=HTMLResponse)
def privacy(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.head("/sitemap.xml")
def sitemap_head():
    return Response(
        status_code=200,
        media_type="application/xml",
    )


@app.head("/robots.txt")
def robots_head():
    return Response(
        status_code=200,
        media_type="text/plain",
    )
