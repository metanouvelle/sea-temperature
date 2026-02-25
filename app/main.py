"""this is the main entry point for the sea temperature project"""

import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import init_db
from app.middleware import TimingMiddleware
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

_tile_executor = ThreadPoolExecutor(max_workers=8)

log = logging.getLogger(__name__)

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
