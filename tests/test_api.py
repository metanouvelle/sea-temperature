"""Core API and page tests for the current SwimTemp product."""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_tmp_db = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
os.environ["SST_DB_PATH"] = _tmp_db.name
os.environ["SWIMTEMP_DB"] = _tmp_db.name
os.environ.setdefault("COPERNICUSMARINE_USERNAME", "test")
os.environ.setdefault("COPERNICUSMARINE_PASSWORD", "test")
os.environ.setdefault("PREWARM_SECRET", "test-secret")

from app.main import app

client = TestClient(app, raise_server_exceptions=False)

COASTAL_LAT = 37.502
COASTAL_LON = 15.147


def fake_point_temperature(date_str, lat, lon, radius_km):
    return {
        "date": date_str,
        "lat": lat,
        "lon": lon,
        "radius_km": radius_km,
        "status": "ok",
        "mean_c": 24.5,
        "min_c": 23.1,
        "max_c": 25.8,
        "cells_used": 12,
    }


class TestApiPoint:
    def test_returns_temperature_for_valid_coords(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(f"/api/point?lat={COASTAL_LAT}&lon={COASTAL_LON}")
        assert r.status_code == 200
        assert r.json()["mean_c"] == 24.5
        assert "max-age=900" in r.headers["cache-control"]

    @pytest.mark.parametrize(
        "url",
        [
            "/api/point?lat=999&lon=15",
            "/api/point?lat=37&lon=999",
            "/api/point",
            "/api/point?lat=37&lon=15&radius_km=999",
        ],
    )
    def test_invalid_parameters_return_422(self, url):
        assert client.get(url).status_code == 422


class TestGridValidation:
    @pytest.mark.parametrize(
        "bbox",
        ["bad", "10,20,5,30", "-91,0,5,10", "0,-181,5,10", "-80,-170,80,170"],
    )
    def test_invalid_bbox_is_rejected(self, bbox):
        r = client.get("/api/grid", params={"bbox": bbox})
        assert r.status_code == 422


class TestHistoryValidation:
    def test_requires_coordinates(self):
        assert client.get("/api/sst-history?lat=37").status_code == 422

    @pytest.mark.parametrize(
        "url",
        ["/api/sst-history?lat=999&lon=15", "/api/sst-history?lat=37&lon=999"],
    )
    def test_invalid_coordinates_are_rejected(self, url):
        assert client.get(url).status_code == 422

    def test_fresh_cache_shape(self):
        payload = {
            "lat": round(COASTAL_LAT, 1),
            "lon": round(COASTAL_LON, 1),
            "history": [{"date": "2026-08-01", "sst": 24.5}],
            "forecast": [{"date": "2026-08-23", "sst": 25.1}],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            "data": json.dumps(payload),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }[key]
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("app.main.sqlite3.connect", return_value=mock_conn):
            r = client.get(f"/api/sst-history?lat={COASTAL_LAT}&lon={COASTAL_LON}")

        assert r.status_code == 200
        assert r.json()["history"][0]["sst"] == 24.5


class TestStatus:
    def test_healthz(self):
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}

    def test_status_contract(self):
        with patch("app.main.get_latest_available_date", return_value="2026-08-21"):
            r = client.get("/api/status")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["latest_sst_date"] == "2026-08-21"
        assert "prewarm" in data


class TestPages:
    def test_home(self):
        r = client.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert r.headers["x-content-type-options"] == "nosniff"
        assert r.headers["referrer-policy"] == "strict-origin-when-cross-origin"
        assert "geolocation=(self)" in r.headers["permissions-policy"]

    @pytest.mark.parametrize("path", ["/about", "/privacy", "/embed"])
    def test_static_pages(self, path):
        assert client.get(path).status_code == 200

    def test_location_requires_coordinates(self):
        assert client.get("/location").status_code == 422

    def test_location_page(self):
        r = client.get(
            "/location",
            params={"lat": 38.7, "lon": -9.4, "name": "Test beach"},
        )
        assert r.status_code == 200
        assert "Test beach" in r.text
        assert 'name="robots" content="noindex,follow"' in r.text

    def test_legacy_discovery_routes_redirect(self):
        for path in ("/map", "/beaches"):
            r = client.get(path, follow_redirects=False)
            assert r.status_code == 308
            assert r.headers["location"] == "/"

    def test_widget_is_noindex(self):
        r = client.get("/widget?lat=38.7&lon=-9.4&name=Test")
        assert r.status_code == 200
        assert 'name="robots" content="noindex,nofollow"' in r.text

    def test_sitemap(self):
        r = client.get("/sitemap.xml")
        assert r.status_code == 200
        assert "application/xml" in r.headers["content-type"]
        assert "https://swimtemp.com/" in r.text

    def test_robots(self):
        r = client.get("/robots.txt")
        assert r.status_code == 200
        assert "Sitemap: https://swimtemp.com/sitemap.xml" in r.text


class TestRemovedLegacySurface:
    @pytest.mark.parametrize(
        "path",
        [
            "/api/auth/me",
            "/api/saves/beaches",
            "/api/beaches",
            "/api/beach/anything",
            "/beach/anything",
        ],
    )
    def test_removed_routes_are_404(self, path):
        assert client.get(path).status_code == 404
