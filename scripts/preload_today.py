"""this file defines scale of preload data """
from datetime import datetime
from app.db import init_db
from app.services.sst_cache import preload_tile

# Define coverage region (Mediterranean example)
MIN_LAT, MAX_LAT = 30, 46
MIN_LON, MAX_LON = -10, 40

TILE_SIZE = 1


def generate_tiles():
    for lat in range(MIN_LAT, MAX_LAT):
        for lon in range(MIN_LON, MAX_LON):
            yield lat, lon


def main():
    init_db()
    today = datetime.utcnow().date().isoformat()

    for lat, lon in generate_tiles():
        print(f"Preloading tile {lat}_{lon}")
        preload_tile(lat, lon, today)


if __name__ == "__main__":
    main()