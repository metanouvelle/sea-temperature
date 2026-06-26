"""Main entry point for the sea temperature project."""

import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.content.beaches import BEACH_MONTHLY_AVG, BEACHES, BEACHES_BY_SLUG, get_gyg_url
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

load_dotenv()

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
    now = datetime.now()
    return templates.TemplateResponse(
        "beach.html",
        {
            "request": request,
            "beach": beach,
            "monthly_avg": BEACH_MONTHLY_AVG.get(slug, []),
            "current_month": now.month,
            "current_month_name": now.strftime("%B"),
            "gyg_link": get_gyg_url(slug),
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


@app.get("/embed", response_class=HTMLResponse)
def embed_page(request: Request):
    return templates.TemplateResponse("embed.html", {"request": request})


@app.get("/widget/{slug}", response_class=HTMLResponse)
def widget_slug(request: Request, slug: str):
    beach = BEACHES_BY_SLUG.get(slug)
    if not beach:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(
        "widget.html",
        {
            "request": request,
            "name": beach["name"],
            "lat": beach["lat"],
            "lon": beach["lon"],
            "slug": slug,
        },
    )


@app.get("/widget", response_class=HTMLResponse)
def widget_coords(
    request: Request,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    name: str = Query("Sea Temperature"),
):
    return templates.TemplateResponse(
        "widget.html",
        {
            "request": request,
            "name": name,
            "lat": lat,
            "lon": lon,
            "slug": "",
        },
    )


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


@app.get("/api/sst-history")
async def sst_history(lat: float, lon: float):
    """
    Returns ~1 year of daily SST history (REP + NRT merged) plus a 7-day
    Open-Meteo Marine SST forecast for the given coordinates.

    Results are cached in SQLite for 24 h, keyed to 0.1° rounded lat/lon.
    """
    import httpx

    from app.services.sst_cache import fetch_nrt_history, fetch_rep_history

    # Round to 0.1° for cache key
    lat_r = round(lat, 1)
    lon_r = round(lon, 1)
    cache_key = f"sst_history_{lat_r}_{lon_r}"

    # ── 1. Check SQLite cache ──────────────────────────────────────────────
    db_path = os.environ.get("SST_DB_PATH", "/data/sst.sqlite")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sst_history_cache (
            cache_key TEXT PRIMARY KEY,
            data      TEXT NOT NULL,
            cached_at TEXT NOT NULL
        )
    """)
    conn.commit()

    cur.execute(
        "SELECT data, cached_at FROM sst_history_cache WHERE cache_key = ?",
        (cache_key,),
    )
    row = cur.fetchone()
    if row:
        cached_at = datetime.fromisoformat(row["cached_at"])
        if datetime.now(timezone.utc) - cached_at < timedelta(hours=24):
            conn.close()
            return json.loads(row["data"])

    # ── 2. Fetch historical SST (REP + NRT) ───────────────────────────────
    today = datetime.now(timezone.utc).date()
    year_ago = today - timedelta(days=365)

    try:
        rep_points = await fetch_rep_history(lat_r, lon_r, year_ago, today)
    except Exception:
        rep_points = []

    try:
        nrt_points = await fetch_nrt_history(lat_r, lon_r, year_ago, today)
    except Exception:
        nrt_points = []

    # Merge: prefer NRT for recent dates (NRT wins on duplicates)
    merged: dict[str, float] = {}
    for pt in rep_points:
        merged[pt["date"]] = pt["sst"]
    for pt in nrt_points:
        merged[pt["date"]] = pt["sst"]  # NRT overwrites REP for same date

    history = [{"date": d, "sst": round(merged[d], 2)} for d in sorted(merged)]

    # ── 3. Fetch 7-day SST forecast from Open-Meteo Marine ────────────────
    forecast = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://marine-api.open-meteo.com/v1/marine",
                params={
                    "latitude": lat_r,
                    "longitude": lon_r,
                    "daily": "sea_surface_temperature_max",
                    "timezone": "UTC",
                    "forecast_days": 7,
                },
            )
            resp.raise_for_status()
            marine = resp.json()
            dates = marine["daily"]["time"]
            ssts = marine["daily"]["sea_surface_temperature_max"]
            forecast = [
                {"date": d, "sst": round(s, 2)}
                for d, s in zip(dates, ssts)
                if s is not None
            ]
    except Exception:
        forecast = []

    # ── 4. Build response ─────────────────────────────────────────────────
    result = {
        "lat": lat_r,
        "lon": lon_r,
        "history": history,  # solid line
        "forecast": forecast,  # dashed line
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── 5. Cache in SQLite ────────────────────────────────────────────────
    cur.execute(
        """
        INSERT INTO sst_history_cache (cache_key, data, cached_at)
        VALUES (?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET data=excluded.data, cached_at=excluded.cached_at
        """,
        (cache_key, json.dumps(result), datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

    return result


# ─────────────────────────────────────────────────────────────────────────────
# ADD TO app/main.py  —  Auth + Saves API stubs
#
# These endpoints are intentionally minimal stubs. They:
#   - Return 501 Not Implemented so the frontend can detect "not yet live"
#   - Have correct route signatures so no URL changes are needed when implemented
#   - Include the full docstring spec so the implementer knows exactly what to build
#
# Implementation order when ready:
#   1. /api/auth/register  — email + password, send verification email
#   2. /api/auth/login     — return session token in httpOnly cookie
#   3. /api/auth/me        — used by frontend to hydrate saved/liked state on load
#   4. /api/saves/*        — the actual save/like toggle endpoints
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import Cookie
from fastapi.responses import JSONResponse

# ── Auth ──────────────────────────────────────────────────────────────────────


@app.post("/api/auth/register")
async def auth_register(request: Request):
    """
    Register with email + password.
    Body: { email, password }
    - Hash password with bcrypt
    - Insert into users table
    - Send verification email (use SendGrid or Resend)
    - Return 201 with { message: "Check your email" }
    - On duplicate email: 409
    """
    return JSONResponse({"detail": "Coming soon"}, status_code=501)


@app.post("/api/auth/login")
async def auth_login(request: Request):
    """
    Login with email + password.
    Body: { email, password }
    - Verify password hash
    - Create session row, set httpOnly cookie 'st_session'
    - Return { user: { id, email, display_name, tier } }
    """
    return JSONResponse({"detail": "Coming soon"}, status_code=501)


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    """
    Delete session row, clear cookie.
    """
    return JSONResponse({"detail": "Coming soon"}, status_code=501)


@app.get("/api/auth/me")
async def auth_me(request: Request):
    """
    Return current user + their saved/liked beach slugs.
    Called on page load to hydrate UI state.
    Response: {
        user: { id, email, display_name, tier } | null,
        saved: ["slug1", "slug2", ...],
        liked: ["slug3", ...]
    }
    If not logged in: { user: null, saved: [], liked: [] }
    This shape means the frontend never needs to branch on auth state
    for rendering — it just gets empty arrays when logged out.
    """
    return JSONResponse({"user": None, "saved": [], "liked": []}, status_code=200)


@app.get("/api/auth/verify")
async def auth_verify(token: str):
    """
    Verify email address from link in verification email.
    - Look up auth_tokens row, check not expired/used
    - Set users.verified = 1
    - Mark token used
    - Redirect to /map?verified=1
    """
    return JSONResponse({"detail": "Coming soon"}, status_code=501)


# ── Saves ─────────────────────────────────────────────────────────────────────


@app.post("/api/saves/beach/{slug}")
async def save_beach(slug: str, request: Request):
    """
    Toggle saved status for a beach.
    Body: { action: "save" | "unsave" | "like" | "unlike" }
    - Requires valid session cookie
    - Upsert user_saved_beaches row
    - Return { saved: bool, liked: bool }
    Frontend calls this on heart/bookmark icon click.
    If not authenticated: 401 — frontend shows login modal.
    """
    return JSONResponse({"detail": "Login required"}, status_code=401)


@app.get("/api/saves/beaches")
async def get_saved_beaches(request: Request):
    """
    Return all saved + liked beaches for current user, with live SST.
    Response: {
        saved: [{ ...beach, temp_c, saved_at }],
        liked: [{ ...beach, temp_c, liked_at }]
    }
    This powers a "My beaches" page or panel.
    """
    return JSONResponse({"saved": [], "liked": []}, status_code=200)


@app.post("/api/saves/point")
async def save_point(request: Request):
    """
    Save an arbitrary map point.
    Body: { lat, lon, label? }
    - Requires valid session
    - Insert into user_saved_points
    - Return { id, lat, lon, label }
    """
    return JSONResponse({"detail": "Login required"}, status_code=401)


@app.delete("/api/saves/point/{point_id}")
async def delete_saved_point(point_id: str, request: Request):
    """
    Delete a saved map point.
    - Requires valid session + ownership check
    """
    return JSONResponse({"detail": "Login required"}, status_code=401)


app.mount("/static", StaticFiles(directory="app/static"), name="static")
