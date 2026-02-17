"""Daily refresh – preloads recently-used SST tiles for yesterday's date.

Run once a day (e.g. via cron or a Fly.io scheduled machine) to keep the
cache warm so the first user request of the day isn't slow.

Usage:
    python -m scripts.daily_refresh
"""

import logging
import sys

from app.database import connect, init_db
from app.services.sst_cache import ensure_tile, yesterday_utc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def all_known_tile_ids() -> list[str]:
    """Return every distinct tile_id that has ever been cached."""
    conn = connect()
    cur = conn.cursor()
    rows = cur.execute("SELECT DISTINCT tile_id FROM sst_tile").fetchall()
    conn.close()
    return [r[0] for r in rows]


def main() -> None:
    init_db()
    date = yesterday_utc()
    tile_ids = all_known_tile_ids()

    if not tile_ids:
        log.info("No tiles to refresh.")
        return

    log.info("Refreshing %d tile(s) for %s", len(tile_ids), date)
    errors = 0
    for tile_id in tile_ids:
        try:
            n = ensure_tile(date, tile_id)
            if n:
                log.info("  fetched  tile=%s  points=%d", tile_id, n)
            else:
                log.info("  cached   tile=%s", tile_id)
        except Exception as exc:
            log.error("  FAILED   tile=%s  error=%s", tile_id, exc)
            errors += 1

    if errors:
        log.error("Refresh completed with %d error(s).", errors)
        sys.exit(1)

    log.info("Refresh complete – all tiles up to date.")


if __name__ == "__main__":
    main()
