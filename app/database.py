import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(os.getenv("SST_DB_PATH", "/data/sst.sqlite")).resolve()


def connect() -> sqlite3.Connection:
    """Create connection to local SQLite DB."""
    # Ensure parent directory exists (helps on fresh machines where /data
    # may not exist yet). Creating the directory is safe whether it's a
    # mounted volume or the container filesystem.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
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

    # ── User auth + saves tables ──────────────────────────────────────────
    # All IF NOT EXISTS — safe to run on every startup, no-op if already present.
    # Full implementation: see auth_saves_stubs.py and migration_user_saves.sql

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            TEXT PRIMARY KEY,
            email         TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            display_name  TEXT,
            tier          TEXT NOT NULL DEFAULT 'free',
            created_at    TEXT NOT NULL,
            last_login_at TEXT,
            verified      INTEGER NOT NULL DEFAULT 0
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS auth_tokens (
            token      TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            purpose    TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used       INTEGER NOT NULL DEFAULT 0
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            user_agent TEXT,
            ip         TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_saved_beaches (
            user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            beach_slug TEXT NOT NULL,
            saved      INTEGER NOT NULL DEFAULT 0,
            liked      INTEGER NOT NULL DEFAULT 0,
            saved_at   TEXT,
            liked_at   TEXT,
            PRIMARY KEY (user_id, beach_slug)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_saved_points (
            id         TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            lat        REAL NOT NULL,
            lon        REAL NOT NULL,
            label      TEXT,
            saved_at   TEXT NOT NULL
        );
    """)

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_saved_beaches_user ON user_saved_beaches(user_id);"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_auth_tokens_user ON auth_tokens(user_id);"
    )

    conn.commit()
    conn.close()
