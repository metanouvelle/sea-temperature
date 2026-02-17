"""this is the main entry point for the sea temperature project"""

import logging

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import init_db
from app.services.sst_cache import (
    login_copernicus,
    point_temperature,
    query_points_in_bbox,
    yesterday_utc,
)

log = logging.getLogger(__name__)

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")


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
    return templates.TemplateResponse("map_click.html", {"request": request})


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
