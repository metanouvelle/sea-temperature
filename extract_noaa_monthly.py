"""
One-time script to extract monthly SST averages for all beaches
from NOAA World Ocean Atlas 2023.

Usage:
    1. Download the data file:
       wget https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav/0.25/woa23_decav_t00_04.nc

    2. Run this script:
       python extract_noaa_monthly.py

    3. Copy the output into app/content/beaches.py as BEACH_MONTHLY_AVG

Requirements:
    pip install netCDF4 numpy
"""

import json
import numpy as np

# Monthly WOA23 file URLs (0.25° resolution, surface layer)
# Each file = one month (01=Jan ... 12=Dec)
MONTHLY_URLS = [
    f"https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav/0.25/woa23_decav_t{m:02d}_04.nc"
    for m in range(1, 13)
]

BEACHES = [
    {"slug": "barcelona-barceloneta", "lat": 41.378, "lon": 2.194},
    {"slug": "nice-promenade", "lat": 43.695, "lon": 7.266},
    {"slug": "mykonos-super-paradise", "lat": 37.394, "lon": 25.361},
    {"slug": "santorini-perissa", "lat": 36.352, "lon": 25.479},
    {"slug": "amalfi-coast", "lat": 40.634, "lon": 14.602},
    {"slug": "dubrovnik-banje", "lat": 42.648, "lon": 18.122},
    {"slug": "tenerife-las-teresitas", "lat": 28.514, "lon": -16.186},
    {"slug": "mallorca-es-trenc", "lat": 39.347, "lon": 2.987},
    {"slug": "ibiza-ses-salines", "lat": 38.872, "lon": 1.412},
    {"slug": "positano-beach", "lat": 40.628, "lon": 14.485},
    {"slug": "corfu-paleokastritsa", "lat": 39.659, "lon": 19.714},
    {"slug": "malta-golden-bay", "lat": 35.920, "lon": 14.338},
    {"slug": "algarve-praia-da-marinha", "lat": 37.090, "lon": -8.407},
    {"slug": "lisbon-cascais", "lat": 38.697, "lon": -9.421},
    {"slug": "costa-rica-manuel-antonio", "lat": 9.389, "lon": -84.136},
    {"slug": "cancun-playa-delfines", "lat": 21.022, "lon": -86.852},
    {"slug": "barbados-crane-beach", "lat": 13.083, "lon": -59.444},
    {"slug": "jamaica-seven-mile", "lat": 18.268, "lon": -78.352},
    {"slug": "bahamas-pink-sands", "lat": 25.506, "lon": -76.638},
    {"slug": "turks-grace-bay", "lat": 21.813, "lon": -72.213},
    {"slug": "st-lucia-anse-chastanet", "lat": 13.854, "lon": -61.073},
    {"slug": "bali-kuta", "lat": -8.718, "lon": 115.168},
    {"slug": "bali-seminyak", "lat": -8.690, "lon": 115.155},
    {"slug": "koh-samui-chaweng", "lat": 9.535, "lon": 100.061},
    {"slug": "phuket-patong", "lat": 7.897, "lon": 98.296},
    {"slug": "phuket-kata", "lat": 7.820, "lon": 98.299},
    {"slug": "krabi-railay", "lat": 8.012, "lon": 98.836},
    {"slug": "maldives-veligandu", "lat": 5.557, "lon": 73.423},
    {"slug": "langkawi-cenang", "lat": 6.296, "lon": 99.725},
    {"slug": "boracay-white-beach", "lat": 11.966, "lon": 121.924},
    {"slug": "el-nido-nacpan", "lat": 11.241, "lon": 119.411},
    {"slug": "zanzibar-nungwi", "lat": -5.726, "lon": 39.297},
    {"slug": "mauritius-belle-mare", "lat": -20.179, "lon": 57.776},
    {"slug": "seychelles-anse-source", "lat": -4.369, "lon": 55.836},
    {"slug": "reunion-boucan-canot", "lat": -21.052, "lon": 55.214},
    {"slug": "sydney-bondi", "lat": -33.891, "lon": 151.277},
    {"slug": "gold-coast-surfers", "lat": -27.999, "lon": 153.431},
    {"slug": "whitsundays-whitehaven", "lat": -20.276, "lon": 149.032},
    {"slug": "bora-bora-matira", "lat": -16.533, "lon": -151.741},
    {"slug": "hawaii-waikiki", "lat": 21.277, "lon": -157.829},
    {"slug": "hawaii-lanikai", "lat": 21.392, "lon": -157.718},
    {"slug": "rio-copacabana", "lat": -22.971, "lon": -43.182},
    {"slug": "rio-ipanema", "lat": -22.985, "lon": -43.202},
    {"slug": "miami-south-beach", "lat": 25.781, "lon": -80.130},
    {"slug": "tulum-beach", "lat": 20.208, "lon": -87.465},
    {"slug": "dubai-jumeirah", "lat": 25.187, "lon": 55.228},
    {"slug": "aqaba-beach", "lat": 29.516, "lon": 35.006},
    {"slug": "cape-town-camps-bay", "lat": -33.950, "lon": 18.376},
    {"slug": "diani-beach", "lat": -4.318, "lon": 39.567},
]


def extract_monthly_avgs():
    try:
        from netCDF4 import Dataset
    except ImportError:
        print("Install netCDF4: pip install netCDF4")
        return

    results = {b["slug"]: [] for b in BEACHES}

    for month_idx, url in enumerate(MONTHLY_URLS, 1):
        filename = url.split("/")[-1]
        print(f"Processing month {month_idx:02d} from {filename}...")

        try:
            ds = Dataset(filename)
        except FileNotFoundError:
            print(f"  File not found: {filename}")
            print(f"  Download with: wget {url}")
            continue

        # WOA23 variable name for temperature
        temp_var = ds.variables.get("t_an") or ds.variables.get("t_mn")
        lats = np.array(ds.variables["lat"][:])
        lons = np.array(ds.variables["lon"][:])

        # Surface layer only (depth index 0)
        sst = np.array(temp_var[0, 0, :, :])  # [time, depth, lat, lon]

        for beach in BEACHES:
            # Find nearest grid point
            lat_idx = np.argmin(np.abs(lats - beach["lat"]))
            lon_idx = np.argmin(np.abs(lons - beach["lon"]))
            val = float(sst[lat_idx, lon_idx])

            if np.isnan(val) or val < -2 or val > 40:
                found = False
                for offset in [1, 2, 3]:
                    for dlat, dlon in [
                        (0, offset),
                        (0, -offset),
                        (offset, 0),
                        (-offset, 0),
                    ]:
                        li = lat_idx + dlat
                        lj = lon_idx + dlon
                        if 0 <= li < len(lats) and 0 <= lj < len(lons):
                            v2 = float(sst[li, lj])
                            if not np.isnan(v2) and -2 <= v2 <= 40:
                                val = v2
                                found = True
                                break
                    if found:
                        break
                if not found:
                    val = None

            results[beach["slug"]].append(round(val, 1) if val is not None else None)

        ds.close()  # ← move OUTSIDE the beach loop, one indent level up

    # Output as Python dict for beaches.py
    print("\n\n# Paste this into app/content/beaches.py")
    print("BEACH_MONTHLY_AVG = {")
    for slug, months in results.items():
        print(f'    "{slug}": {months},')
    print("}")

    # Also save as JSON
    with open("beach_monthly_avg.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print("\nAlso saved to beach_monthly_avg.json")


if __name__ == "__main__":
    extract_monthly_avgs()
