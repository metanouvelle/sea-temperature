import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(os.getenv("SST_DB_PATH", "/data/sst.sqlite")).resolve()


def connect() -> sqlite3.Connection:
    """Create connection to local SQLite DB."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    """Initialize DB schema."""
    conn = connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sst_tile (
            date TEXT NOT NULL,
            tile_id TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (date, tile_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sst_grid (
            date TEXT NOT NULL,
            tile_id TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            temp_c REAL NOT NULL,
            PRIMARY KEY (date, tile_id, lat, lon)
        );
        """
    )

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_sst_grid_date_tile ON sst_grid(date, tile_id);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_sst_grid_date_latlon ON sst_grid(date, lat, lon);"
    )

    conn.commit()
    conn.close()
