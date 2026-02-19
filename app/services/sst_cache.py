"""this is to fetch data and store locally"""

from __future__ import annotations

import logging
import math
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Iterable

import copernicusmarine
import numpy as np
from dotenv import load_dotenv

from app.database import connect
from app.services.geo import (
    bbox_for_radius_km,
    haversine_km,
    wrap_lon_180,
    wrap_lon_360,
)

load_dotenv()

log = logging.getLogger(__name__)

DATASET_ID = "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
TILE_DEG = 2.0  # 2° x 2° tiles (small enough to fetch fast)


def login_copernicus():
    """Validate that Copernicus credentials are present.

    The SDK reads COPERNICUSMARINE_USERNAME / COPERNICUSMARINE_PASSWORD from
    the environment automatically on each API call, so no explicit login() is
    needed — we just fail fast here if the vars are missing.
    """
    username = os.getenv("COPERNICUSMARINE_USERNAME")
    password = os.getenv("COPERNICUSMARINE_PASSWORD")

    if not username or not password:
        raise RuntimeError("Copernicus credentials not set")


def yesterday_utc() -> str:
    """Return yesterday's UTC date (ISO string).

    Copernicus SST data has ~1-2 day latency; the fallback in
    _open_sst_dataset handles the case where yesterday is not yet available.
    """
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def tile_origin(value: float, step: float) -> float:
    """
    get original points
    """
    return math.floor(value / step) * step


def tile_id_for(lat: float, lon: float) -> str:
    """
    get id
    """
    a = tile_origin(lat, TILE_DEG)
    o = tile_origin(lon, TILE_DEG)
    return f"{a:.0f}_{o:.0f}"


def tile_bbox(tile_id: str) -> dict:
    """
    get bbox
    """
    a_str, o_str = tile_id.split("_")
    a = float(a_str)
    o = float(o_str)
    return {
        "min_lat": a,
        "max_lat": a + TILE_DEG,
        "min_lon": o,
        "max_lon": o + TILE_DEG,
    }


def _pick_sst_var(ds) -> str:
    """
    get variables
    """
    # Common names
    for cand in ["analysed_sst", "sea_surface_temperature", "sst"]:
        if cand in ds.data_vars:
            return cand
    # fallback
    return list(ds.data_vars)[0]


def _to_celsius(values: np.ndarray, units: str | None) -> np.ndarray:
    """
    convert to celsius
    """
    if not units:
        return values
    u = units.lower()
    if "k" in u or "kelvin" in u:
        return values - 273.15
    return values


def tile_exists(date: str, tile_id: str) -> bool:
    """
    fetch sst_tile
    """
    conn = connect()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT 1 FROM sst_tile WHERE date=? AND tile_id=? LIMIT 1", (date, tile_id)
    ).fetchone()
    conn.close()
    return row is not None


def store_tile(
    date: str, tile_id: str, points: Iterable[tuple[float, float, float]]
) -> int:
    """
    save sst_grid
    """
    conn = connect()
    cur = conn.cursor()

    # idempotent: clear tile points then insert fresh
    cur.execute("DELETE FROM sst_grid WHERE date=? AND tile_id=?", (date, tile_id))
    n = 0
    for lat, lon, temp_c in points:
        cur.execute(
            "INSERT OR REPLACE INTO sst_grid(date, tile_id, lat, lon, temp_c) VALUES (?,?,?,?,?)",
            (date, tile_id, float(lat), float(lon), float(temp_c)),
        )
        n += 1

    cur.execute(
        "INSERT OR REPLACE INTO sst_tile(date, tile_id, fetched_at) VALUES (?,?,?)",
        (date, tile_id, datetime.now(timezone.utc).isoformat()),
    )

    conn.commit()
    conn.close()
    return n


def _open_sst_dataset(
    tile_id: str, date: str, min_lon: float, max_lon: float, bbox: dict
):
    """Open a Copernicus SST dataset, falling back to the previous day on any error."""
    try:
        return copernicusmarine.open_dataset(
            dataset_id=DATASET_ID,
            minimum_longitude=min_lon,
            maximum_longitude=max_lon,
            minimum_latitude=bbox["min_lat"],
            maximum_latitude=bbox["max_lat"],
            start_datetime=f"{date}T00:00:00Z",
            end_datetime=f"{date}T00:00:00Z",
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        fallback_date = (
            (datetime.fromisoformat(date) - timedelta(days=1)).date().isoformat()
        )
        log.warning(
            "Copernicus fetch failed for tile=%s date=%s (%s); retrying with %s",
            tile_id,
            date,
            exc,
            fallback_date,
        )
        try:
            ds = copernicusmarine.open_dataset(
                dataset_id=DATASET_ID,
                minimum_longitude=min_lon,
                maximum_longitude=max_lon,
                minimum_latitude=bbox["min_lat"],
                maximum_latitude=bbox["max_lat"],
                start_datetime=f"{fallback_date}T00:00:00Z",
                end_datetime=f"{fallback_date}T00:00:00Z",
            )
            if ds is None:
                raise RuntimeError("open_dataset returned None on fallback")
            return ds
        except Exception as exc2:  # pylint: disable=broad-exception-caught
            log.error(
                "Copernicus fallback also failed for tile=%s fallback_date=%s: %s",
                tile_id,
                fallback_date,
                exc2,
            )
            raise


def fetch_tile_from_copernicus(tile_id: str, date: str):
    """Fetch all SST grid points for a 2°×2° tile from Copernicus."""
    bbox = tile_bbox(tile_id)
    min_lon = wrap_lon_360(bbox["min_lon"])
    max_lon = wrap_lon_360(bbox["max_lon"])
    # Tiles straddling the 0°/360° boundary need max extended to avoid an
    # empty range (e.g. lon -2→0 becomes 358→0 in [0,360] space).
    if max_lon < min_lon:
        max_lon += 360

    ds = _open_sst_dataset(tile_id, date, min_lon, max_lon, bbox)
    if ds is None:
        raise RuntimeError("failed to download SST data")
    ds = ds.load()

    sst = ds[_pick_sst_var(ds)].isel(time=0)
    values = _to_celsius(np.array(sst.values), sst.attrs.get("units"))
    lats = np.array(sst.latitude.values)
    lons = np.array(sst.longitude.values)

    # values shape: [lat, lon]; NaN = land / masked cells
    points = []
    for (i, j), v in np.ndenumerate(values):
        if not np.isnan(v):
            points.append((float(lats[i]), wrap_lon_180(float(lons[j])), float(v)))
    return points


_TILE_LOCKS: dict[str, threading.Lock] = {}
_TILE_LOCKS_GUARD = threading.Lock()


def _tile_lock(key: str) -> threading.Lock:
    with _TILE_LOCKS_GUARD:
        if key not in _TILE_LOCKS:
            _TILE_LOCKS[key] = threading.Lock()
        return _TILE_LOCKS[key]


def ensure_tile(date: str, tile_id: str) -> int:
    """
    validate tile, fetching from Copernicus if not cached.
    Per-tile lock prevents duplicate concurrent fetches.
    """
    with _tile_lock(f"{date}:{tile_id}"):
        if tile_exists(date, tile_id):
            return 0
        points = fetch_tile_from_copernicus(tile_id, date)
        return store_tile(date, tile_id, points)


def query_points_in_bbox(date: str, bbox: dict) -> list[tuple[float, float, float]]:
    """
    Fetch SST points inside bounding box.
    Handles dateline crossing correctly.
    """

    conn = connect()
    cur = conn.cursor()

    min_lat = bbox["min_lat"]
    max_lat = bbox["max_lat"]
    min_lon = bbox["min_lon"]
    max_lon = bbox["max_lon"]

    # Normal case (no dateline crossing)
    if min_lon <= max_lon:
        rows = cur.execute(
            """
            SELECT lat, lon, temp_c
            FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND lon BETWEEN ? AND ?
            """,
            (date, min_lat, max_lat, min_lon, max_lon),
        ).fetchall()

    # Dateline crossing case
    else:
        rows = cur.execute(
            """
            SELECT lat, lon, temp_c
            FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND (
                    lon BETWEEN ? AND 180
                 OR lon BETWEEN -180 AND ?
              )
            """,
            (date, min_lat, max_lat, min_lon, max_lon),
        ).fetchall()

    conn.close()

    return [(float(a), float(b), float(c)) for a, b, c in rows]


def point_temperature(date: str, lat: float, lon: float, radius_km: float) -> dict:

    lon = wrap_lon_180(lon)

    t_id = tile_id_for(lat, lon)
    fetched = ensure_tile(date, t_id)

    bb = bbox_for_radius_km(lat, lon, radius_km)

    bb["min_lon"] = wrap_lon_180(bb["min_lon"])
    bb["max_lon"] = wrap_lon_180(bb["max_lon"])

    conn = connect()
    cur = conn.cursor()

    # Normal case
    if bb["min_lon"] <= bb["max_lon"]:
        rows = cur.execute(
            """
            SELECT lat, lon, temp_c
            FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND lon BETWEEN ? AND ?
        """,
            (date, bb["min_lat"], bb["max_lat"], bb["min_lon"], bb["max_lon"]),
        ).fetchall()

    # Dateline crossing case
    else:
        rows = cur.execute(
            """
            SELECT lat, lon, temp_c
            FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND (
                    lon BETWEEN ? AND 180
                 OR lon BETWEEN -180 AND ?
              )
        """,
            (date, bb["min_lat"], bb["max_lat"], bb["min_lon"], bb["max_lon"]),
        ).fetchall()

    conn.close()

    temps = []

    for la, lo, tc in rows:
        if haversine_km(lat, lon, la, lo) <= radius_km:
            temps.append(tc)

    if not temps:
        return {
            "date": date,
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km,
            "status": "unavailable",
            "debug": {"tile_id": t_id, "tile_fetched_now": fetched},
        }

    return {
        "date": date,
        "lat": lat,
        "lon": lon,
        "radius_km": radius_km,
        "status": "ok",
        "mean_c": round(sum(temps) / len(temps), 2),
        "min_c": round(min(temps), 2),
        "max_c": round(max(temps), 2),
        "cells_used": len(temps),
        "debug": {"tile_id": t_id, "tile_fetched_now": fetched},
    }
