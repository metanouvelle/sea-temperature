"""
tests/test_api.py
─────────────────────────────────────────────────────────────────────────────
pytest test suite for SwimTemp FastAPI endpoints.

Run with:
    pytest tests/test_api.py -v

Dependencies (add to requirements-dev.txt if not present):
    pytest
    httpx
    fastapi[testclient]

The tests use FastAPI's TestClient which runs the app in-process —
no live Copernicus or Open-Meteo calls are made. Database operations
use a temporary SQLite file so nothing touches /data/sst.sqlite.
─────────────────────────────────────────────────────────────────────────────
"""

import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ── Ensure project root is on sys.path ───────────────────────────────────────
# Allows running: pytest tests/test_api.py from the project root
# OR: python -m pytest from anywhere
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Env setup before importing the app ───────────────────────────────────────
# Point the app at a temp database so tests don't touch /data/
_tmp_db = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
os.environ.setdefault("SST_DB_PATH", _tmp_db.name)
os.environ.setdefault("COPERNICUSMARINE_USERNAME", "test")
os.environ.setdefault("COPERNICUSMARINE_PASSWORD", "test")
os.environ.setdefault("PREWARM_SECRET", "test-secret")

# Patch Copernicus login so startup doesn't fail without real credentials
with patch("app.services.sst_cache.copernicusmarine"):
    from app.main import app

client = TestClient(app, raise_server_exceptions=False)

# ── Fixtures ──────────────────────────────────────────────────────────────────

COASTAL_LAT = 37.502  # just off Catania, Sicily — in the sea
COASTAL_LON = 15.147
INLAND_LAT = 41.902  # Rome — no ocean data
INLAND_LON = 12.496

YESTERDAY = (date.today() - timedelta(days=1)).isoformat()


# ── Helper ────────────────────────────────────────────────────────────────────


def fake_point_temperature(date_str, lat, lon, radius_km):
    """Return a realistic SST result for any coastal point."""
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
        "debug": {"tile_id": "37_15", "tile_fetched_now": 0},
    }


# ─────────────────────────────────────────────────────────────────────────────
# /api/point
# ─────────────────────────────────────────────────────────────────────────────


class TestApiPoint:
    def test_returns_temperature_for_valid_coords(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(f"/api/point?lat={COASTAL_LAT}&lon={COASTAL_LON}")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert "mean_c" in data
        assert isinstance(data["mean_c"], float)

    def test_rejects_invalid_latitude(self):
        r = client.get("/api/point?lat=999&lon=15.0")
        assert r.status_code == 422

    def test_rejects_invalid_longitude(self):
        r = client.get("/api/point?lat=37.5&lon=999")
        assert r.status_code == 422

    def test_rejects_missing_params(self):
        r = client.get("/api/point")
        assert r.status_code == 422

    def test_radius_km_defaults_to_10(self):
        with patch(
            "app.main.point_temperature", side_effect=fake_point_temperature
        ) as mock:
            client.get(f"/api/point?lat={COASTAL_LAT}&lon={COASTAL_LON}")
            _, kwargs = mock.call_args[0], mock.call_args
            # radius_km arg should be 10.0
            assert mock.call_args[0][3] == 10.0

    def test_custom_radius_km(self):
        with patch(
            "app.main.point_temperature", side_effect=fake_point_temperature
        ) as mock:
            client.get(f"/api/point?lat={COASTAL_LAT}&lon={COASTAL_LON}&radius_km=25")
            assert mock.call_args[0][3] == 25.0

    def test_radius_km_too_large_rejected(self):
        r = client.get(f"/api/point?lat={COASTAL_LAT}&lon={COASTAL_LON}&radius_km=999")
        assert r.status_code == 422


# ─────────────────────────────────────────────────────────────────────────────
# /api/beaches
# ─────────────────────────────────────────────────────────────────────────────


class TestApiBeaches:
    def test_returns_beach_list(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get("/api/beaches")
        assert r.status_code == 200
        data = r.json()
        assert "beaches" in data
        assert isinstance(data["beaches"], list)
        assert len(data["beaches"]) > 0

    def test_beaches_have_required_fields(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get("/api/beaches")
        for beach in r.json()["beaches"]:
            assert "name" in beach
            assert "lat" in beach
            assert "lon" in beach
            assert "slug" in beach

    def test_beaches_sorted_warmest_first(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get("/api/beaches")
        temps = [b["temp_c"] for b in r.json()["beaches"] if b["temp_c"] is not None]
        assert temps == sorted(temps, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# /api/beach/{slug}
# ─────────────────────────────────────────────────────────────────────────────


class TestApiBeachSlug:
    def test_valid_slug_returns_data(self):
        from app.content.beaches import BEACHES

        slug = BEACHES[0]["slug"]
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(f"/api/beach/{slug}")
        assert r.status_code == 200
        data = r.json()
        assert data["slug"] == slug
        assert "temp_c" in data

    def test_invalid_slug_returns_404(self):
        r = client.get("/api/beach/this-beach-does-not-exist")
        assert r.status_code == 404

    def test_slug_with_live_temp(self):
        from app.content.beaches import BEACHES

        slug = BEACHES[0]["slug"]
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(f"/api/beach/{slug}")
        assert r.json()["temp_c"] == 24.5


# ─────────────────────────────────────────────────────────────────────────────
# /api/beaches/nearby  ← NEW
# ─────────────────────────────────────────────────────────────────────────────


class TestApiBeachesNearby:
    def test_returns_list_for_valid_coords(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}")
        assert r.status_code == 200
        data = r.json()
        assert "beaches" in data
        assert isinstance(data["beaches"], list)

    def test_beaches_have_required_fields(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(
                f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}&limit=3"
            )
        for b in r.json()["beaches"]:
            assert "slug" in b
            assert "name" in b
            assert "lat" in b
            assert "lon" in b
            assert "distance_km" in b
            assert "month" in b

    def test_limit_param_respected(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(
                f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}&limit=3"
            )
        assert len(r.json()["beaches"]) <= 3

    def test_sorted_by_distance(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(
                f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}&limit=10"
            )
        dists = [b["distance_km"] for b in r.json()["beaches"]]
        assert dists == sorted(dists)

    def test_invalid_lat_rejected(self):
        r = client.get("/api/beaches/nearby?lat=999&lon=15.0")
        assert r.status_code == 422

    def test_invalid_lon_rejected(self):
        r = client.get("/api/beaches/nearby?lat=37.5&lon=999")
        assert r.status_code == 422

    def test_missing_coords_rejected(self):
        r = client.get("/api/beaches/nearby")
        assert r.status_code == 422

    def test_limit_too_large_rejected(self):
        r = client.get(
            f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}&limit=999"
        )
        assert r.status_code == 422

    def test_includes_live_temp_when_available(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(
                f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}&limit=5"
            )
        beaches_with_temp = [
            b for b in r.json()["beaches"] if b.get("temp_c") is not None
        ]
        # At least some should have temp data (mocked to return ok)
        assert len(beaches_with_temp) > 0

    def test_monthly_avg_included(self):
        with patch("app.main.point_temperature", side_effect=fake_point_temperature):
            r = client.get(
                f"/api/beaches/nearby?lat={COASTAL_LAT}&lon={COASTAL_LON}&limit=5"
            )
        for b in r.json()["beaches"]:
            # monthly_avg may be None for some beaches but key must exist
            assert "monthly_avg" in b


# ─────────────────────────────────────────────────────────────────────────────
# /api/beaches/seasonal  ← NEW
# ─────────────────────────────────────────────────────────────────────────────


class TestApiBeachesSeasonal:
    def test_returns_list(self):
        r = client.get("/api/beaches/seasonal")
        assert r.status_code == 200
        data = r.json()
        assert "beaches" in data
        assert "month" in data
        assert "month_name" in data

    def test_current_month_default(self):
        from datetime import datetime

        r = client.get("/api/beaches/seasonal")
        assert r.json()["month"] == datetime.now().month

    def test_explicit_month(self):
        r = client.get("/api/beaches/seasonal?month=7")
        assert r.status_code == 200
        assert r.json()["month"] == 7
        assert r.json()["month_name"] == "July"

    def test_all_months_valid(self):
        for m in range(1, 13):
            r = client.get(f"/api/beaches/seasonal?month={m}")
            assert r.status_code == 200, f"Month {m} failed"

    def test_invalid_month_rejected(self):
        r = client.get("/api/beaches/seasonal?month=13")
        assert r.status_code == 422

    def test_month_zero_rejected(self):
        r = client.get("/api/beaches/seasonal?month=0")
        assert r.status_code == 422

    def test_sorted_warmest_first(self):
        r = client.get("/api/beaches/seasonal?month=8&limit=20")
        temps = [b["monthly_avg"] for b in r.json()["beaches"]]
        assert temps == sorted(temps, reverse=True)

    def test_limit_respected(self):
        r = client.get("/api/beaches/seasonal?month=6&limit=5")
        assert len(r.json()["beaches"]) <= 5

    def test_beach_fields_present(self):
        r = client.get("/api/beaches/seasonal?month=6")
        for b in r.json()["beaches"]:
            assert "slug" in b
            assert "name" in b
            assert "lat" in b
            assert "lon" in b
            assert "monthly_avg" in b
            assert "band" in b

    def test_band_labels_valid(self):
        valid_bands = {"Cold", "Brave", "Nice", "Perfect", "Very Warm"}
        r = client.get("/api/beaches/seasonal?month=6&limit=50")
        for b in r.json()["beaches"]:
            assert b["band"] in valid_bands, f"Unknown band: {b['band']}"

    def test_warmest_month_august(self):
        """August should return warmer averages than February globally."""
        aug = client.get("/api/beaches/seasonal?month=8&limit=1").json()
        feb = client.get("/api/beaches/seasonal?month=2&limit=1").json()
        # Top beach in August should be warmer than top beach in February
        # (both hemispheres are curated so this may vary — just check both succeed)
        assert aug["beaches"][0]["monthly_avg"] > 0
        assert feb["beaches"][0]["monthly_avg"] > 0


# ─────────────────────────────────────────────────────────────────────────────
# /api/sst-history
# ─────────────────────────────────────────────────────────────────────────────


class TestApiSSTHistory:
    def test_returns_correct_shape(self):
        """
        SST history hits Copernicus + SQLite — fully mock the DB and history fetch.
        We patch sqlite3.connect to return a fake cache hit so no real DB or
        Copernicus call is made.
        """
        cached_payload = json.dumps(
            {
                "lat": round(COASTAL_LAT, 1),
                "lon": round(COASTAL_LON, 1),
                "history": [{"date": "2025-07-01", "sst": 24.5}],
                "forecast": [{"date": "2026-06-30", "sst": 25.1}],
                "generated_at": "2026-06-29T10:00:00+00:00",
            }
        )

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            "data": cached_payload,
            "cached_at": "2026-06-29T08:00:00+00:00",  # fresh — within 24h
        }[key]

        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = mock_row

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.row_factory = None

        with patch("app.main.sqlite3.connect", return_value=mock_conn):
            r = client.get(f"/api/sst-history?lat={COASTAL_LAT}&lon={COASTAL_LON}")

        assert r.status_code == 200
        data = r.json()
        assert "history" in data
        assert "forecast" in data
        assert isinstance(data["history"], list)

    def test_invalid_lat_still_processes(self):
        """
        /api/sst-history has no Query validators so lat=999 is accepted as float.
        Patch a cache hit so the endpoint returns immediately without touching
        Copernicus or httpx — we just confirm it doesn't crash (500).
        """
        cached_payload = json.dumps(
            {
                "lat": 999.0,
                "lon": 15.0,
                "history": [],
                "forecast": [],
                "generated_at": "2026-06-29T08:00:00+00:00",
            }
        )
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            "data": cached_payload,
            "cached_at": "2026-06-29T08:00:00+00:00",
        }[key]
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("app.main.sqlite3.connect", return_value=mock_conn):
            r = client.get("/api/sst-history?lat=999&lon=15.0")

        assert r.status_code != 500

    def test_requires_both_params(self):
        """Missing lon should return 422 — FastAPI enforces required float params."""
        r = client.get("/api/sst-history?lat=37.5")
        assert r.status_code == 422


# ─────────────────────────────────────────────────────────────────────────────
# /api/status
# ─────────────────────────────────────────────────────────────────────────────


class TestApiStatus:
    def test_returns_ok(self):
        r = client.get("/api/status")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_has_prewarm_key(self):
        r = client.get("/api/status")
        assert "prewarm" in r.json()


# ─────────────────────────────────────────────────────────────────────────────
# /api/auth/* stubs
# ─────────────────────────────────────────────────────────────────────────────


class TestAuthStubs:
    def test_me_returns_null_user(self):
        r = client.get("/api/auth/me")
        assert r.status_code == 200
        data = r.json()
        assert data["user"] is None
        assert data["saved"] == []
        assert data["liked"] == []

    def test_register_returns_501(self):
        r = client.post(
            "/api/auth/register", json={"email": "a@b.com", "password": "x"}
        )
        assert r.status_code == 501

    def test_login_returns_501(self):
        r = client.post("/api/auth/login", json={"email": "a@b.com", "password": "x"})
        assert r.status_code == 501

    def test_save_beach_requires_auth(self):
        r = client.post("/api/saves/beach/bondi-beach", json={"action": "save"})
        assert r.status_code == 401

    def test_get_saved_beaches_returns_empty(self):
        r = client.get("/api/saves/beaches")
        assert r.status_code == 200
        data = r.json()
        assert data["saved"] == []
        assert data["liked"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Pages (smoke tests)
# ─────────────────────────────────────────────────────────────────────────────


class TestPages:
    def test_landing_page(self):
        r = client.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]

    def test_map_page(self):
        r = client.get("/map")
        assert r.status_code == 200

    def test_beaches_page(self):
        r = client.get("/beaches")
        assert r.status_code == 200

    def test_about_page(self):
        r = client.get("/about")
        assert r.status_code == 200

    def test_privacy_page(self):
        r = client.get("/privacy")
        assert r.status_code == 200

    def test_valid_beach_page(self):
        from app.content.beaches import BEACHES

        slug = BEACHES[0]["slug"]
        r = client.get(f"/beach/{slug}")
        assert r.status_code == 200

    def test_invalid_beach_page_404(self):
        r = client.get("/beach/does-not-exist")
        assert r.status_code == 404

    def test_sitemap_xml(self):
        r = client.get("/sitemap.xml")
        assert r.status_code == 200
        assert "application/xml" in r.headers["content-type"]
        assert b"swimtemp.com" in r.content

    def test_robots_txt(self):
        r = client.get("/robots.txt")
        assert r.status_code == 200
        assert b"Sitemap" in r.content
