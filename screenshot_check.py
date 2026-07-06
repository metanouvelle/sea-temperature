"""SwimTemp — cross-device screenshot matrix.

Setup (once, inside your poetry/venv):
    pip install playwright
    playwright install chromium webkit

Run:
    python screenshot_check.py                        # against https://swimtemp.com
    python screenshot_check.py http://localhost:8000  # against local dev

Output: screenshots/<page>--<device>.png — open the folder, eyeball the grid.
"""

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = sys.argv[1] if len(sys.argv) > 1 else "https://swimtemp.com"

PAGES = ["/", "/map", "/beaches", "/about"]

IOS_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)

# engine "webkit" ≈ iOS Safari — catches dvh/safe-area/zoom quirks Chrome won't.
DEVICES = [
    {"name": "iphone",           "width": 390,  "height": 844,  "touch": True,  "engine": "webkit"},
    {"name": "iphone-landscape", "width": 844,  "height": 390,  "touch": True,  "engine": "webkit"},
    {"name": "ipad-portrait",    "width": 834,  "height": 1194, "touch": True,  "engine": "webkit"},
    {"name": "ipad-landscape",   "width": 1194, "height": 834,  "touch": True,  "engine": "webkit"},
    {"name": "ipad-mini",        "width": 768,  "height": 1024, "touch": True,  "engine": "webkit"},  # sits ON your breakpoint
    {"name": "laptop",           "width": 1440, "height": 900,  "touch": False, "engine": "chromium"},
    {"name": "desktop",          "width": 1920, "height": 1080, "touch": False, "engine": "chromium"},
]

Path("screenshots").mkdir(exist_ok=True)

with sync_playwright() as p:
    for dev in DEVICES:
        browser = getattr(p, dev["engine"]).launch()
        context = browser.new_context(
            viewport={"width": dev["width"], "height": dev["height"]},
            has_touch=dev["touch"],
            device_scale_factor=2,
            user_agent=IOS_UA if dev["touch"] else None,
        )

        for path in PAGES:
            page = context.new_page()
            errors: list[str] = []
            page.on("pageerror", lambda e, errs=errors: errs.append(str(e)))
            page.on(
                "console",
                lambda m, errs=errors: errs.append(m.text) if m.type == "error" else None,
            )
            slug = "landing" if path == "/" else path.strip("/")
            try:
                page.goto(BASE + path, wait_until="networkidle", timeout=45_000)
                page.wait_for_timeout(2_500)  # let map tiles / live panels settle
                page.screenshot(path=f"screenshots/{slug}--{dev['name']}.png")
                flag = f"  ⚠ {len(errors)} JS error(s): {errors[0]}" if errors else ""
                print(f"✓ {slug:<10} @ {dev['name']}{flag}")
            except Exception as err:  # noqa: BLE001 — report and continue
                print(f"✗ {slug} @ {dev['name']} — {str(err).splitlines()[0]}")
            finally:
                page.close()

        browser.close()

print("\nDone → open the screenshots/ folder and scan the grid.")
