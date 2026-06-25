"""
Validation tests for beaches.py
Run with: python3 tests/test_beaches.py
"""

import sys

sys.path.insert(0, ".")

from app.content.beaches import BEACHES, BEACHES_BY_SLUG, BEACH_MONTHLY_AVG

errors = []
warnings = []

print(f"Checking {len(BEACHES)} beaches...\n")

for beach in BEACHES:
    slug = beach.get("slug", "MISSING_SLUG")

    # ── Required fields ──────────────────────────────────────────────────────
    for field in ["slug", "name", "city", "country", "lat", "lon"]:
        if field not in beach or beach[field] is None:
            errors.append(f"❌ {slug} — missing required field: {field}")

    # ── Slug in lookup dict ──────────────────────────────────────────────────
    if slug not in BEACHES_BY_SLUG:
        errors.append(f"❌ {slug} — not found in BEACHES_BY_SLUG lookup")

    # ── Valid coordinates ────────────────────────────────────────────────────
    try:
        lat = float(beach.get("lat", 0))
        lon = float(beach.get("lon", 0))
        if not (-90 <= lat <= 90):
            errors.append(f"❌ {slug} — invalid lat: {lat}")
        if not (-180 <= lon <= 180):
            errors.append(f"❌ {slug} — invalid lon: {lon}")
    except (TypeError, ValueError):
        errors.append(f"❌ {slug} — lat/lon not numeric")

    # ── Monthly averages ─────────────────────────────────────────────────────
    avg = BEACH_MONTHLY_AVG.get(slug, [])

    if not avg:
        warnings.append(f"⚠️  {slug} — no monthly_avg data")
    elif len(avg) != 12:
        errors.append(f"❌ {slug} — monthly_avg has {len(avg)} items, expected 12")
    elif all(v is None for v in avg):
        errors.append(f"❌ {slug} — monthly_avg is all None (will crash template)")
    else:
        # Check for unrealistic temperatures
        valid = [v for v in avg if v is not None]
        if max(valid) > 40:
            warnings.append(f"⚠️  {slug} — suspiciously high temp: {max(valid)}°C")
        if min(valid) < 0:
            warnings.append(f"⚠️  {slug} — suspiciously low temp: {min(valid)}°C")

    # ── Slug format ──────────────────────────────────────────────────────────
    if slug != slug.lower():
        errors.append(f"❌ {slug} — slug should be lowercase")
    if " " in slug:
        errors.append(f"❌ {slug} — slug contains spaces")

# ── Summary ──────────────────────────────────────────────────────────────────
print("=" * 50)

if warnings:
    print(f"\n{len(warnings)} warnings:")
    for w in warnings:
        print(f"  {w}")

if errors:
    print(f"\n{len(errors)} errors:")
    for e in errors:
        print(f"  {e}")
    print(f"\n❌ FAILED — fix errors before deploying")
    sys.exit(1)
else:
    print(f"\n✅ All {len(BEACHES)} beaches passed validation!")
    if warnings:
        print(f"   ({len(warnings)} warnings to review)")
    sys.exit(0)
