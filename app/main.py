"""this is the main entry point for the sea temperature project"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import init_db
from app.services.sst_cache import point_temperature, today_utc, login_copernicus

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


@app.get("/api/point")
def api_point(lat: float, lon: float, radius_km: float = 3.0):
    """
    get temperature by coordinates
    """
    d = today_utc()
    return point_temperature(d, lat, lon, radius_km)
