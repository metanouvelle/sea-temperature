from datetime import datetime, timezone

from app.data.beaches import BEACHES
from app.data.regions import REGIONS
from app.database import connect, init_db
from app.services.mur_erddap import bbox_for_radius_km, haversine_km, stream_bbox_points

RADIUS_KM = 3.0


def utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def upsert_region_grid(date_str: str, region_id: str, bbox: dict, stride: int) -> int:
    conn = connect()
    cur = conn.cursor()

    # Delete existing (idempotent daily refresh)
    cur.execute("DELETE FROM sst_grid WHERE date=? AND region_id=?", (date_str, region_id))

    n = 0
    for lat, lon, temp_c in stream_bbox_points(bbox=bbox, stride=stride):
        cur.execute(
            "INSERT OR REPLACE INTO sst_grid(date, region_id, lat, lon, temp_c) VALUES (?,?,?,?,?)",
            (date_str, region_id, lat, lon, temp_c),
        )
        n += 1

    conn.commit()
    conn.close()
    return n


def compute_beach_daily(
    date_str: str, beach_id: str, lat0: float, lon0: float, radius_km: float
) -> dict:
    bbox = bbox_for_radius_km(lat0, lon0, radius_km)

    temps = []
    # stride=1 for max resolution around a beach
    for lat, lon, temp_c in stream_bbox_points(bbox=bbox, stride=1):
        if haversine_km(lat0, lon0, lat, lon) <= radius_km:
            temps.append(temp_c)

    if not temps:
        raise RuntimeError(
            f"No SST cells found for beach={beach_id} (likely land contamination / nearshore gap)."
        )

    return {
        "mean_c": sum(temps) / len(temps),
        "min_c": min(temps),
        "max_c": max(temps),
        "cells_used": len(temps),
    }


def upsert_beaches(date_str: str) -> int:
    conn = connect()
    cur = conn.cursor()

    n = 0
    for beach_id, b in BEACHES.items():
        stats = compute_beach_daily(date_str, beach_id, b["lat"], b["lon"], RADIUS_KM)
        cur.execute(
            """
            INSERT OR REPLACE INTO beach_daily(date, beach_id, radius_km, mean_c, min_c, max_c, cells_used)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                date_str,
                beach_id,
                RADIUS_KM,
                round(stats["mean_c"], 2),
                round(stats["min_c"], 2),
                round(stats["max_c"], 2),
                stats["cells_used"],
            ),
        )
        n += 1

    conn.commit()
    conn.close()
    return n


def main():
    init_db()
    d = utc_day()

    # Regions: downsample for map (stride=2 => ~2 km). Tweak later per performance.
    for region_id, r in REGIONS.items():
        count = upsert_region_grid(d, region_id, r["bbox"], stride=2)
        print(f"[{d}] region={region_id} grid_points={count}")

    bcount = upsert_beaches(d)
    print(f"[{d}] beaches_updated={bcount}")


if __name__ == "__main__":
    main()
