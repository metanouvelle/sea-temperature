import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

def _default_db_path() -> Path:
    explicit = os.getenv("SST_DB_PATH") or os.getenv("SWIMTEMP_DB")
    if explicit:
        return Path(explicit).expanduser().resolve()
    fly_data = Path("/data")
    if fly_data.exists() and os.access(fly_data, os.W_OK):
        return fly_data / "sst.sqlite"
    return Path("data/sst.sqlite").resolve()


DB_PATH = _default_db_path()


def connect() -> sqlite3.Connection:
    """Create connection to local SQLite DB."""
    # Ensure parent directory exists (helps on fresh machines where /data
    # may not exist yet). Creating the directory is safe whether it's a
    # mounted volume or the container filesystem.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=10000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db() -> None:
    """Initialize DB schema."""
    conn = connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sst_tile (
            date TEXT NOT NULL,
            tile_id TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (date, tile_id)
        );
        """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sst_grid (
            date TEXT NOT NULL,
            tile_id TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            temp_c REAL NOT NULL,
            PRIMARY KEY (date, tile_id, lat, lon)
        );
        """)

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_sst_grid_date_tile ON sst_grid(date, tile_id);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_sst_grid_date_latlon ON sst_grid(date, lat, lon);"
    )

    # ── Warmest beaches cache (populated by nightly cron) ─────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS warmest_beaches (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            region     TEXT    NOT NULL,
            name       TEXT    NOT NULL,
            lat        REAL    NOT NULL,
            lon        REAL    NOT NULL,
            temp_c     REAL,
            date       TEXT,
            updated_at TEXT    NOT NULL
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_warmest_region ON warmest_beaches(region, temp_c DESC);"
    )

    conn.commit()
    conn.close()
