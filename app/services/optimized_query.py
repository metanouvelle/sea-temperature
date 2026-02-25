"""
Drop-in replacement for query_points_in_bbox in sst_cache.py.

Key optimization: zoom-aware downsampling.
At low zoom levels the map circles are huge — no need to send every 0.05° point.
We subsample by snapping lat/lon to a grid and taking one point per cell.

Zoom → step size → approx points for Mediterranean viewport
  5  →  0.50°   →  ~400  points   (was ~40,000)
  6  →  0.25°   →  ~1,600 points  (was ~40,000)
  7  →  0.10°   →  ~4,000 points  (was ~40,000)
  8+ →  0.05°   →  full resolution

This reduces mobile data usage by 10-100x at typical zoom levels.
"""


def _step_for_zoom(zoom: float) -> float:
    """Return the grid step size in degrees for a given zoom level."""
    if zoom < 5:
        return 1.00
    if zoom < 6:
        return 0.50
    if zoom < 7:
        return 0.25
    if zoom < 8:
        return 0.10
    return 0.05  # full resolution at zoom 8+


def query_points_in_bbox_optimized(
    date: str,
    bbox: dict,
    zoom: float = 8.0,
) -> list[tuple[float, float, float]]:
    """
    Fetch SST points inside bounding box with zoom-aware downsampling.
    Handles dateline crossing correctly.

    Args:
        date:  ISO date string e.g. "2026-02-19"
        bbox:  dict with min_lat, max_lat, min_lon, max_lon
        zoom:  current map zoom level (default 8 = full resolution)
    """
    from app.database import connect

    step = _step_for_zoom(zoom)
    conn = connect()
    cur = conn.cursor()

    min_lat = bbox["min_lat"]
    max_lat = bbox["max_lat"]
    min_lon = bbox["min_lon"]
    max_lon = bbox["max_lon"]

    if step <= 0.05:
        # Full resolution — use original query, no downsampling needed
        rows = _query_full(cur, date, min_lat, max_lat, min_lon, max_lon)
    else:
        # Downsampled — snap to grid and take one representative point per cell
        # We use ROUND(lat / step) * step to bucket points into grid cells,
        # then AVG(temp_c) to get a representative temperature for each cell.
        rows = _query_downsampled(cur, date, min_lat, max_lat, min_lon, max_lon, step)

    conn.close()
    return [(float(a), float(b), float(c)) for a, b, c in rows]


def _query_full(cur, date, min_lat, max_lat, min_lon, max_lon):
    if min_lon <= max_lon:
        return cur.execute(
            """
            SELECT lat, lon, temp_c FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND lon BETWEEN ? AND ?
            """,
            (date, min_lat, max_lat, min_lon, max_lon),
        ).fetchall()
    else:
        return cur.execute(
            """
            SELECT lat, lon, temp_c FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND (lon BETWEEN ? AND 180 OR lon BETWEEN -180 AND ?)
            """,
            (date, min_lat, max_lat, min_lon, max_lon),
        ).fetchall()


def _query_downsampled(cur, date, min_lat, max_lat, min_lon, max_lon, step):
    """
    Group points into step-degree cells, return the average temp per cell.
    ROUND(x / step) * step buckets each coordinate into the nearest grid point.
    """
    if min_lon <= max_lon:
        return cur.execute(
            """
            SELECT
                ROUND(lat / ?) * ? AS cell_lat,
                ROUND(lon / ?) * ? AS cell_lon,
                AVG(temp_c)        AS temp_c
            FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND lon BETWEEN ? AND ?
            GROUP BY cell_lat, cell_lon
            """,
            (step, step, step, step, date, min_lat, max_lat, min_lon, max_lon),
        ).fetchall()
    else:
        return cur.execute(
            """
            SELECT
                ROUND(lat / ?) * ? AS cell_lat,
                ROUND(lon / ?) * ? AS cell_lon,
                AVG(temp_c)        AS temp_c
            FROM sst_grid
            WHERE date=?
              AND lat BETWEEN ? AND ?
              AND (lon BETWEEN ? AND 180 OR lon BETWEEN -180 AND ?)
            GROUP BY cell_lat, cell_lon
            """,
            (step, step, step, step, date, min_lat, max_lat, min_lon, max_lon),
        ).fetchall()
