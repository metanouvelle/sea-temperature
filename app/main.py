"""this is the main entry point for the sea temperature project"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import init_db
from app.logger import get_logger
from app.middleware import TimingMiddleware
from app.scripts.prewarm_tiles import prewarm, yesterday_utc
from app.services.optimized_query import query_points_in_bbox_optimized
from app.services.sst_cache import (
    ensure_tile,
    login_copernicus,
    point_temperature,
    query_points_in_bbox,
    tile_exists,
    tile_id_for,
    yesterday_utc,
)
from fastapi.responses import FileResponse
from app.data.beaches import BEACHES, BEACHES_BY_SLUG

log = get_logger(__name__)

PREWARM_SECRET = os.getenv("PREWARM_SECRET", "")


_tile_executor = ThreadPoolExecutor(max_workers=8)


app = FastAPI()
templates = Jinja2Templates(directory="app/templates")


app.add_middleware(TimingMiddleware)


@app.on_event("startup")
def _startup():
    """
    init database before everything else
    """
    init_db()
    login_copernicus()
    # Don't run prewarm on app startup — it can timeout on shared CPU.
    # Rely on scheduled cron job instead (see fly.toml [[crons]])
    # threading.Thread(target=_background_prewarm, daemon=True).start()


@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    """
    render landing page
    """
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/map", response_class=HTMLResponse)
def map_page(request: Request):
    """
    render map main page
    """
    return templates.TemplateResponse("sea-temp-map.html", {"request": request})


@app.get("/about", response_class=HTMLResponse)
def about(request: Request):
    """
    render about page
    """
    return templates.TemplateResponse("about.html", {"request": request})


@app.get("/api/area")
def api_area(
    min_lat: float = Query(..., ge=-90, le=90),
    max_lat: float = Query(..., ge=-90, le=90),
    min_lon: float = Query(..., ge=-180, le=180),
    max_lon: float = Query(..., ge=-180, le=180),
):
    """
    Return cached SST grid points within a bounding box.
    Never triggers new Copernicus fetches — overlay-safe.
    """
    if min_lat >= max_lat:
        raise HTTPException(status_code=422, detail="min_lat must be less than max_lat")
    d = yesterday_utc()
    bbox = {
        "min_lat": min_lat,
        "max_lat": max_lat,
        "min_lon": min_lon,
        "max_lon": max_lon,
    }
    try:
        raw = query_points_in_bbox(d, bbox)
        return {
            "date": d,
            "points": [{"lat": p[0], "lon": p[1], "temp_c": p[2]} for p in raw],
        }
    except Exception as exc:  # pylint: disable=broad-exception-caught
        log.error("api_area error bbox=%s: %s", bbox, exc)
        raise HTTPException(
            status_code=503, detail="Service temporarily unavailable"
        ) from exc


@app.get("/api/point")
def api_point(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(10.0, gt=0, le=50),
):
    """
    get temperature by coordinates
    """
    d = yesterday_utc()
    try:
        return point_temperature(d, lat, lon, radius_km)
    except Exception as exc:
        log.error("api_point error lat=%s lon=%s: %s", lat, lon, exc)
        raise HTTPException(
            status_code=503, detail="Service temporarily unavailable"
        ) from exc


@app.get("/api/grid")
def get_grid(bbox: str, zoom: float = Query(8.0)):  # add zoom param
    """
    Return cached SST grid points for a bounding box.
    Uncached tiles are submitted to a background thread pool; cached data is
    returned immediately so the frontend can show what it has and poll for more.
    bbox format: south,west,north,east
    """
    south, west, north, east = map(float, bbox.split(","))
    date = yesterday_utc()
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
    points = query_points_in_bbox_optimized(date, bounds, zoom=zoom)  # swap this
    return {
        "points": [
            {"lat": p[0], "lon": p[1], "temp_c": round(p[2], 2)} for p in points
        ],
        "pending": pending,
    }


def _background_prewarm():
    log.info("Background prewarm starting...")
    try:
        date = yesterday_utc()
        start = time.time()
        results = prewarm(date)
        elapsed_prewarm = round((time.time() - start) / 60, 1)
        log.info(
            "Prewarm complete — %s fetched, %s skipped, %.1f min",
            results.get("ok", 0),
            results.get("skipped", 0),
            elapsed_prewarm,
        )

        # Render tiles after prewarm
        log.info("Starting tile render...")
        from app.scripts.render_tiles import render

        render(date)
        log.info("Tile render complete")

        # Write status only after BOTH prewarm and render succeed
        elapsed_total = round((time.time() - start) / 60, 1)
        Path("/data/prewarm_status.json").write_text(
            json.dumps(
                {
                    "last_run": date,
                    "elapsed_minutes": elapsed_total,
                    "fetched": results.get("ok", 0),
                    "skipped": results.get("skipped", 0),
                    "failed": results.get("failed", 0),
                    "total": results.get("total", 0),
                    "success": True,
                },
                indent=2,
            )
        )

    except Exception as e:
        log.error("Background prewarm failed: %s", e)


@app.post("/api/admin/prewarm")
def trigger_prewarm(authorization: str = Header(None)):
    if not PREWARM_SECRET or authorization != f"Bearer {PREWARM_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    threading.Thread(target=_background_prewarm, daemon=True).start()
    return {"status": "started"}


@app.get("/tiles/latest/{z}/{x}/{y}.png")
def serve_tile_latest(z: int, x: int, y: int):
    tiles_dir = Path(os.getenv("SST_TILES_DIR", "/data/tiles"))
    tile_path = tiles_dir / "latest" / str(z) / str(x) / f"{y}.png"
    if not tile_path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(
        tile_path,
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=86400",  # cache 24h in browser
        },
    )


@app.get("/api/status")
def api_status():
    import json

    status_file = Path("/data/prewarm_status.json")
    prewarm_info = {}
    if status_file.exists():
        try:
            prewarm_info = json.loads(status_file.read_text())
        except Exception:
            prewarm_info = {"error": "could not read status file"}
    return {"status": "ok", "prewarm": prewarm_info}


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
        },
    )


@app.get("/api/beaches")
def api_beaches():
    """
    Return all beaches with current SST temperature.
    Fetches temperatures in parallel for speed.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.services.sst_cache import point_temperature, yesterday_utc

    date = yesterday_utc()

    def fetch(beach):
        try:
            result = point_temperature(date, beach["lat"], beach["lon"], radius_km=25)
            temp = result["mean_c"] if result and result.get("status") == "ok" else None
        except Exception:
            temp = None
        return {**beach, "temp_c": temp, "date": date}

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(fetch, b) for b in BEACHES]
        results = [f.result() for f in as_completed(futures)]

    # Sort by temperature descending (warmest first), nulls last
    results.sort(
        key=lambda x: x["temp_c"] if x["temp_c"] is not None else -99, reverse=True
    )
    return {"beaches": results, "date": date}


@app.get("/api/beach/{slug}")
def api_beach(slug: str):
    """Return temperature for a single beach."""
    from app.services.sst_cache import point_temperature, yesterday_utc

    beach = BEACHES_BY_SLUG.get(slug)
    if not beach:
        raise HTTPException(status_code=404)

    date = yesterday_utc()
    result = point_temperature(date, beach["lat"], beach["lon"], radius_km=25)
    temp = result["mean_c"] if result and result.get("status") == "ok" else None

    return {**beach, "temp_c": temp, "date": date}
