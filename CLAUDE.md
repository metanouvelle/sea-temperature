# Sea Temperature — Project Context for Claude

## What this app does
Satellite-based sea surface temperature map for swimmers and travelers.
Data from Copernicus Marine (METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2, 0.05° resolution).

## Tech stack
- Backend: FastAPI, SQLite, Copernicus Marine API
- Frontend: MapLibre GL JS 4.7.1, vanilla JS
- Fonts: Cormorant Garamond, DM Mono
- Hosting: Fly.io (app: sea-temperature), Frankfurt region
- Volume: vol_vx2qxjw0dlqnz6wr mounted at /data

## Project structure
app/
  main.py          — FastAPI app, endpoints
  database.py      — SQLite connection, schema, indexes
  middleware.py    — Timing middleware (X-Response-Time header)
  services/
    sst_cache.py   — Copernicus fetch, tile caching, query_points_in_bbox
    optimized_query.py — zoom-aware downsampled queries
  scripts/
    prewarm_tiles.py — pre-warms 35 coastal regions, writes /data/prewarm_status.json
  templates/
    landing.html   — landing page (dark navy, Fraunces, animated ocean canvas)
    sea-temp-map.html — MapLibre map page
    about.html     — about page, matches landing aesthetic
    
## API endpoints
GET /          → landing.html
GET /map       → sea-temp-map.html
GET /about     → about.html
GET /api/grid?bbox=s,w,n,e&zoom=z  → cached SST points, triggers bg fetch if uncached
GET /api/point?lat=&lon=&radius_km= → temperature for a clicked location
GET /api/status → prewarm status, db stats

## Design system
- Dark navy background: #060f1a
- Accent: #38bdf8
- Temperature bands: Cold(<17°C) Brave(17-20) Nice(20-23) Perfect(23-26) VeryWarm(>26)
- Colors: #2563eb #0ea5e9 #22c55e #f59e0b #ef4444

## Current state (update this each session)
- Pre-warming: 35 coastal regions cached, runs on app startup in background thread
- Map overlay: always visible at 0.5 opacity, no legend interaction
- Reverse geocoding: Nominatim, fires in parallel with temperature fetch
- Performance: zoom-aware downsampling implemented, middleware logging added
- Mobile: not optimized yet — planned after beach pages

## Next up
1. Beach pages (/beaches + /beach/slug) — SEO + affiliate revenue
2. Mobile experience (after beach pages)
3. Affiliate links (last, once product feels complete)

## Known issues / decisions made
- Volume can only attach to one machine — no separate cron machine possible
- Prewarm runs on startup via background thread instead
- Legend highlight feature removed — was unreliable and not useful enough