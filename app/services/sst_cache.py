"""this is to fetch data and store locally"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Iterable

import copernicusmarine
import numpy as np

from app.database import connect
from app.services.geo import bbox_for_radius_km, haversine_km


DATASET_ID = "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
TILE_DEG = 5.0  # 5° x 5° tiles (small enough to fetch fast)


def today_utc() -> str:
    """
    get today utc
    """
    return datetime.now(timezone.utc).date().isoformat()


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


def fetch_tile_from_copernicus(tile_id: str):
    """
    fetch tile
    """
    bbox = tile_bbox(tile_id)

    ds = copernicusmarine.open_dataset(
        dataset_id=DATASET_ID,
        minimum_longitude=bbox["min_lon"],
        maximum_longitude=bbox["max_lon"],
        minimum_latitude=bbox["min_lat"],
        maximum_latitude=bbox["max_lat"],
    ).load()

    var = _pick_sst_var(ds)
    sst = ds[var].isel(time=0)

    units = sst.attrs.get("units")
    values = np.array(sst.values)
    values = _to_celsius(values, units)

    lats = np.array(sst.latitude.values)
    lons = np.array(sst.longitude.values)

    # values shape: [lat, lon]
    points = []
    # [values[i, j] for i in range(len(lats)) for j ]
    for (i, j), v in np.ndenumerate(values):
        if not np.isnan(v):
            points.append((float(lats[i]), float(lons[j]), float(v)))
    return points


def ensure_tile(date: str, tile_id: str) -> int:
    """
    validate tile
    """
    if tile_exists(date, tile_id):
        return 0
    points = fetch_tile_from_copernicus(tile_id)
    return store_tile(date, tile_id, points)


def query_points_in_bbox(date: str, bbox: dict) -> list[tuple[float, float, float]]:
    """
    fetch bbox
    """
    conn = connect()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT lat, lon, temp_c
        FROM sst_grid
        WHERE date=?
          AND lat BETWEEN ? AND ?
          AND lon BETWEEN ? AND ?
        """,
        (date, bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"]),
    ).fetchall()
    conn.close()
    return [(float(a), float(b), float(c)) for a, b, c in rows]


def point_temperature(date: str, lat: float, lon: float, radius_km: float) -> dict:
    """
    get point temperature
    """
    # Ensure current tile exists
    t_id = tile_id_for(lat, lon)
    fetched = ensure_tile(date, t_id)

    # Query locally within radius bbox
    bb = bbox_for_radius_km(lat, lon, radius_km)
    candidates = query_points_in_bbox(date, bb)

    temps = []
    for la, lo, tc in candidates:
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

def download_subset(bbox: dict, date: str):
    """
    Download SST subset for bounding box and date.
    Returns xarray Dataset.
    """
    ds = copernicusmarine.open_dataset(
        dataset_id=DATASET_ID,
        minimum_longitude=bbox["min_lon"],
        maximum_longitude=bbox["max_lon"],
        minimum_latitude=bbox["min_lat"],
        maximum_latitude=bbox["max_lat"],
        start_datetime=date,
        end_datetime=date,
    )
    return ds


def iterate_dataset(ds):
    """
    Yield (lat, lon, temp_c) from dataset.
    """
    sst = ds["analysed_sst"]  # check variable name if needed

    lats = sst.latitude.values
    lons = sst.longitude.values
    values = sst.values  # shape: [time, lat, lon]

    # assuming single time index
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            temp_kelvin = values[0][i][j]
            temp_c = temp_kelvin - 273.15
            yield float(lat), float(lon), float(temp_c)


def preload_tile(lat_deg: int, lon_deg: int, date: str):
    tile_id = f"{lat_deg}_{lon_deg}"

    # Check if already exists
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sst_tile WHERE date = ? AND tile_id = ?",
        (date, tile_id),
    )
    if cur.fetchone():
        conn.close()
        return

    # Download bbox
    bbox = {
        "min_lat": lat_deg,
        "max_lat": lat_deg + 1,
        "min_lon": lon_deg,
        "max_lon": lon_deg + 1,
    }

    ds = download_subset(bbox)  # your copernicus call

    # Save grid points
    for lat, lon, temp_c in iterate_dataset(ds):
        cur.execute(
            """
            INSERT OR IGNORE INTO sst_grid
            (date, tile_id, lat, lon, temp_c)
            VALUES (?, ?, ?, ?, ?)
            """,
            (date, tile_id, lat, lon, temp_c),
        )

    cur.execute(
        """
        INSERT OR REPLACE INTO sst_tile
        (date, tile_id, fetched_at)
        VALUES (?, ?, ?)
        """,
        (date, tile_id, datetime.utcnow().isoformat()),
    )

    conn.commit()
    conn.close()