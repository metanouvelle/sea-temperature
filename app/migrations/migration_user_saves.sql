-- ─────────────────────────────────────────────────────────────────────────────
-- migration_user_saves.sql
-- Run once against /data/sst.sqlite when auth feature is implemented.
-- Safe to run now in dev — nothing in the current app reads these tables.
-- ─────────────────────────────────────────────────────────────────────────────

-- Users: email is the identity, tier controls paywall access
CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,          -- UUID, generated server-side
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT,                      -- NULL if OAuth-only account
    display_name  TEXT,
    tier          TEXT NOT NULL DEFAULT 'free',  -- 'free' | 'pro'
    created_at    TEXT NOT NULL,
    last_login_at TEXT,
    verified      INTEGER NOT NULL DEFAULT 0     -- 0=unverified, 1=verified
);

-- Email verification tokens (also reused for password reset)
CREATE TABLE IF NOT EXISTS auth_tokens (
    token      TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    purpose    TEXT NOT NULL,               -- 'verify_email' | 'reset_password'
    expires_at TEXT NOT NULL,
    used       INTEGER NOT NULL DEFAULT 0
);

-- Saved / liked beaches
-- One row per user+beach. liked and saved are independent toggles.
CREATE TABLE IF NOT EXISTS user_saved_beaches (
    user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    beach_slug TEXT NOT NULL,
    saved      INTEGER NOT NULL DEFAULT 0,  -- bookmarked for later
    liked      INTEGER NOT NULL DEFAULT 0,  -- hearted / loved
    saved_at   TEXT,                        -- when saved was last toggled on
    liked_at   TEXT,                        -- when liked was last toggled on
    PRIMARY KEY (user_id, beach_slug)
);

-- Saved map points (arbitrary lat/lon, not just curated beaches)
-- Lets users pin spots on the map they want to revisit
CREATE TABLE IF NOT EXISTS user_saved_points (
    id         TEXT PRIMARY KEY,            -- UUID
    user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    lat        REAL NOT NULL,
    lon        REAL NOT NULL,
    label      TEXT,                        -- user-supplied name, e.g. "My cove"
    saved_at   TEXT NOT NULL
);

-- Sessions (server-side session store — simpler than JWT for this scale)
CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT PRIMARY KEY,            -- 64-char random hex
    user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    user_agent TEXT,
    ip         TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_user_saved_beaches_user ON user_saved_beaches(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_auth_tokens_user ON auth_tokens(user_id);
