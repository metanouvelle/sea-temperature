"""Main entry point for the sea temperature project."""

import json
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import init_db
from app.logger import get_logger
from app.middleware import SecurityHeadersMiddleware, TimingMiddleware
from app.scripts.prewarm_tiles import prewarm, yesterday_utc
from app.services.optimized_query import query_points_in_bbox_optimized
from app.services.sst_cache import (
    ensure_tile,
    login_copernicus,
    point_temperature,
    tile_exists,
    tile_id_for,
)
from app.routes_beach_search import router as beach_search_router

load_dotenv()

log = get_logger(__name__)

PREWARM_SECRET = os.getenv("PREWARM_SECRET", "")


def runtime_data_dir() -> Path:
    """Return the writable runtime data directory.

    Fly mounts /data. Local development falls back to ./data unless
    SST_DB_PATH explicitly points somewhere else.
    """
    explicit = os.getenv("SST_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve().parent
    fly_data = Path("/data")
    if fly_data.exists() and os.access(fly_data, os.W_OK):
        return fly_data
    return Path("data").resolve()


def sst_db_path() -> Path:
    explicit = os.getenv("SST_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return runtime_data_dir() / "sst.sqlite"


STATUS_PATH = runtime_data_dir() / "prewarm_status.json"

_tile_executor = ThreadPoolExecutor(max_workers=12)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize local storage while keeping cached-only development usable."""
    init_db()
    try:
        login_copernicus()
    except RuntimeError as exc:
        log.warning("Copernicus credentials unavailable at startup: %s", exc)
    yield


app = FastAPI(
    title="SwimTemp",
    description="Sea-surface temperature and marine conditions for swimmers.",
    version="1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)
templates = Jinja2Templates(directory="app/templates")
app.add_middleware(SecurityHeadersMiddleware)
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



# ── Background prewarm ────────────────────────────────────────────────────────
def _cleanup_old_data():
    """Delete old data to keep database small."""
    try:
        from app.database import connect

        conn = connect()

        # SST tile/grid — keep only last 2 days
        deleted = conn.execute(
            "DELETE FROM sst_grid WHERE date < date('now', '-2 days')"
        ).rowcount
        conn.execute("DELETE FROM sst_tile WHERE date < date('now', '-2 days')")
        log.info("SST cleanup — deleted %d old grid rows", deleted)

        # SST history cache — keep only last 7 days
        conn.execute("""
            DELETE FROM sst_history_cache
            WHERE cached_at < datetime('now', '-7 days')
        """)

        # Beach search cache stores Unix timestamps.
        try:
            cutoff_epoch = int(time.time()) - (7 * 24 * 60 * 60)
            conn.execute(
                "DELETE FROM beach_search_cache WHERE updated_at < ?",
                (cutoff_epoch,),
            )
        except Exception:
            pass  # table may not exist in all environments

        # Warmest beaches — keep only last 2 days
        try:
            conn.execute("""
                DELETE FROM warmest_beaches
                WHERE updated_at < datetime('now', '-2 days')
            """)
        except Exception:
            pass


        # Finish the write transaction before checkpoint/VACUUM. SQLite
        # rejects VACUUM while a transaction is active.
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
        conn.close()
        log.info("Cleanup complete")
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

        STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATUS_PATH.write_text(
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
    return templates.TemplateResponse(request, "explore.html")


@app.get("/map")
def map_page():
    return RedirectResponse(url="/", status_code=308)


@app.get("/about", response_class=HTMLResponse)
def about(request: Request):
    return templates.TemplateResponse(request, "about.html")


@app.get("/beaches")
def beaches_page():
    return RedirectResponse(url="/", status_code=308)


@app.get("/location", response_class=HTMLResponse)
def location_page(
    request: Request,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    name: str = Query("Sea temperature"),
    place: str = Query(""),
):
    return templates.TemplateResponse(
        request,
        "beach.html",
        {
            "name": name.strip()[:120] or "Sea temperature",
            "place": place.strip()[:160],
            "lat": lat,
            "lon": lon,
        },
    )


# ── API ───────────────────────────────────────────────────────────────────────


@app.get("/api/point")
def api_point(
    response: Response,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(10.0, gt=0, le=50),
):
    d = get_latest_available_date()
    try:
        result = point_temperature(d, lat, lon, radius_km)
        response.headers["Cache-Control"] = "public, max-age=900, stale-if-error=86400"
        return result
    except Exception as exc:
        log.error("api_point error lat=%s lon=%s: %s", lat, lon, exc)
        raise HTTPException(
            status_code=503, detail="Service temporarily unavailable"
        ) from exc


@app.get("/api/grid")
def get_grid(response: Response, bbox: str, zoom: float = Query(8.0)):
    """
    Return cached SST grid points for a bounding box.
    bbox format: south,west,north,east
    """
    try:
        south, west, north, east = map(float, bbox.split(","))
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="bbox must be south,west,north,east")
    if not (-90 <= south < north <= 90 and -180 <= west < east <= 180):
        raise HTTPException(status_code=422, detail="invalid bbox bounds")
    if (north - south) > 30 or (east - west) > 60:
        raise HTTPException(status_code=422, detail="bbox is too large")
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
    response.headers["Cache-Control"] = (
        "no-store" if pending else "public, max-age=300"
    )
    return {
        "points": [
            {"lat": p[0], "lon": p[1], "temp_c": round(p[2], 2)} for p in points
        ],
        "pending": pending,
    }


@app.get("/healthz")
def healthz():
    """Deployment health check that verifies the SQLite store is reachable."""
    try:
        from app.database import connect

        conn = connect()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"status": "ok"}
    except Exception as exc:
        log.error("health check failed: %s", exc)
        raise HTTPException(status_code=503, detail="database unavailable") from exc


@app.get("/api/status")
def api_status():
    """Small operational health endpoint for deploy checks."""
    prewarm_info = {}
    if STATUS_PATH.exists():
        try:
            prewarm_info = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        except Exception:
            prewarm_info = {"error": "could not read status file"}
    return {
        "status": "ok",
        "latest_sst_date": get_latest_available_date(),
        "prewarm": prewarm_info,
    }


@app.post("/api/admin/prewarm")
def trigger_prewarm(authorization: str = Header(None)):
    """Trigger background prewarm. Protected by secret token."""
    if not PREWARM_SECRET or authorization != f"Bearer {PREWARM_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    # Clear status so polling knows a fresh run is in progress
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(
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
    urls = [
        {"loc": f"{SITE_URL}/", "priority": "1.0"},
        {"loc": f"{SITE_URL}/about", "priority": "0.6"},
        {"loc": f"{SITE_URL}/privacy", "priority": "0.3"},
        {"loc": f"{SITE_URL}/embed", "priority": "0.4"},
    ]
    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for url in urls:
        xml_parts.append(
            f'  <url>\n    <loc>{escape(url["loc"])}</loc>\n'
            f'    <changefreq>weekly</changefreq>\n    <priority>{url["priority"]}</priority>\n  </url>'
        )
    xml_parts.append("</urlset>")
    return Response(content="\n".join(xml_parts), media_type="application/xml")


@app.get("/privacy", response_class=HTMLResponse)
def privacy(request: Request):
    return templates.TemplateResponse(request, "privacy.html")


@app.get("/embed", response_class=HTMLResponse)
def embed_page(request: Request):
    return templates.TemplateResponse(request, "embed.html")


@app.get("/widget", response_class=HTMLResponse)
def widget_coords(
    request: Request,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    name: str = Query("Sea Temperature"),
):
    return templates.TemplateResponse(
        request,
        "widget.html",
        {
            "name": name.strip()[:80] or "Sea Temperature",
            "lat": lat,
            "lon": lon,
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
async def sst_history(
    response: Response,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
):
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
    db_file = sst_db_path()
    db_file.parent.mkdir(parents=True, exist_ok=True)
    db_path = str(db_file)
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
            response.headers["Cache-Control"] = "public, max-age=3600, stale-if-error=86400"
            response.headers["X-SwimTemp-Cache"] = "hit"
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

    response.headers["Cache-Control"] = "public, max-age=3600, stale-if-error=86400"
    response.headers["X-SwimTemp-Cache"] = "miss"
    return result


# ── /api/warmest ──────────────────────────────────────────────────────────────
@app.get("/api/warmest")
def api_warmest(
    region: str = Query("all"),
    limit: int = Query(20, ge=1, le=20),
):
    """
    Return warmest beaches for a region from the warmest_beaches table.
    Populated nightly by app/scripts/update_warmest.py.
    """
    conn = sqlite3.connect(sst_db_path())
    conn.row_factory = sqlite3.Row

    try:
        if region.lower() == "all":
            rows = conn.execute(
                """
                SELECT name, lat, lon, temp_c, date, region
                FROM warmest_beaches
                WHERE temp_c IS NOT NULL
                ORDER BY temp_c DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT name, lat, lon, temp_c, date, region
                FROM warmest_beaches
                WHERE region = ?
                  AND temp_c IS NOT NULL
                ORDER BY temp_c DESC
                LIMIT ?
                """,
                (region.lower(), limit),
            ).fetchall()

        beaches = [
            {
                "name": r["name"],
                "lat": r["lat"],
                "lon": r["lon"],
                "temp_c": r["temp_c"],
                "date": r["date"],
                "region": r["region"],
            }
            for r in rows
        ]

        meta = conn.execute(
            "SELECT MAX(updated_at) as last_updated FROM warmest_beaches"
        ).fetchone()

        return {
            "region": region,
            "beaches": beaches,
            "last_updated": meta["last_updated"] if meta else None,
            "count": len(beaches),
        }

    except sqlite3.OperationalError:
        return {
            "region": region,
            "beaches": [],
            "message": "Run update_warmest to populate.",
            "count": 0,
        }
    finally:
        conn.close()


# ── /api/admin/update-warmest ─────────────────────────────────────────────────
@app.post("/api/admin/update-warmest")
def admin_update_warmest(authorization: str = Header(None)):
    """Trigger nightly warmest beaches update. Protected by PREWARM_SECRET."""
    secret = os.environ.get("PREWARM_SECRET", "")
    if not secret or authorization != f"Bearer {secret}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    import threading
    from app.scripts.update_warmest import run as run_update

    def _run():
        try:
            run_update()
        except Exception as e:
            log.error("update_warmest failed: %s", e)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {
        "status": "started",
        "message": "Warmest beaches update running in background",
    }



app.include_router(beach_search_router)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
