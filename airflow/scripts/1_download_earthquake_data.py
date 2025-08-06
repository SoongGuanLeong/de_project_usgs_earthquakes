# download script written by ChatGPT

import requests
import json
import time
from calendar import monthrange

def fetch_monthly_data(year, month, min_mag=2.0, out_dir="/opt/airflow/data"):
    start = f"{year}-{month:02d}-01"
    end_day = monthrange(year, month)[1]
    end = f"{year}-{month:02d}-{end_day}"
    
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "minmagnitude": min_mag,
        "limit": 20000,
        "orderby": "time"
    }

    print(f"Fetching {start} to {end}...")
    r = requests.get(url, params=params)
    
    if r.ok:
        data = r.json()
        filename = f"{out_dir}/quakes_{year}_{month:02d}.json"
        with open(filename, "w") as f:
            json.dump(data, f)
        print(f"{year}-{month:02d}: {len(data['features'])} records")
        return len(data['features'])
    else:
        print(f"Error {r.status_code} for {year}-{month:02d}")
        return 0

total = 0
for year in range(1990, 2025):
    for month in range(1, 13):
        total += fetch_monthly_data(year, month)
        time.sleep(0.5)

print("✅ Total records:", total)
