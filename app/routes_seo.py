"""SwimTemp — sitemap.xml + robots.txt for search engines.

INSTALL:
  1. Copy this file to app/routes_seo.py
  2. In app/main.py add:
         from app.routes_seo import router as seo_router
         app.include_router(seo_router)
  3. Deploy, then verify:
         https://swimtemp.com/sitemap.xml
         https://swimtemp.com/robots.txt

To add pages later (e.g. per-beach pages), append entries to PAGES —
or replace it with a DB query once beach pages exist.
"""

from datetime import date

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, Response

router = APIRouter()

BASE_URL = "https://swimtemp.com"

# (path, lastmod) — bump lastmod when a page meaningfully changes.
# Google ignores changefreq/priority these days; lastmod is what counts,
# and only if it's honest.
PAGES = [
    ("/", date(2026, 7, 6)),
    ("/map", date(2026, 7, 6)),
    ("/beaches", date(2026, 7, 6)),
    ("/about", date(2026, 6, 1)),
    ("/privacy", date(2026, 6, 1)),
    ("/embed", date(2026, 6, 1)),
]


@router.get("/sitemap.xml")
def sitemap() -> Response:
    urls = "\n".join(
        f"  <url>\n"
        f"    <loc>{BASE_URL}{path}</loc>\n"
        f"    <lastmod>{lastmod.isoformat()}</lastmod>\n"
        f"  </url>"
        for path, lastmod in PAGES
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{urls}\n"
        "</urlset>\n"
    )
    return Response(content=xml, media_type="application/xml")


@router.get("/robots.txt")
def robots() -> PlainTextResponse:
    return PlainTextResponse(
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        f"\nSitemap: {BASE_URL}/sitemap.xml\n"
    )
