"""this file contains geo help function"""

import math


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    get haversine in km
    """
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def bbox_for_radius_km(lat: float, lon: float, radius_km: float) -> dict:
    """
    get bbox for radius
    """
    # Rough bounding box (good enough for small radii)
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * math.cos(math.radians(lat)))
    return {
        "min_lat": lat - dlat,
        "max_lat": lat + dlat,
        "min_lon": lon - dlon,
        "max_lon": lon + dlon,
    }


def wrap_lon_180(lon: float) -> float:
    """Return longitude in [-180, 180)."""
    return ((lon + 180) % 360) - 180


def wrap_lon_360(lon: float) -> float:
    """Return longitude in [0, 360)."""
    return lon % 360
