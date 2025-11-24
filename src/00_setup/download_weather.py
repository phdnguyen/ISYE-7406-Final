import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import pytz
from meteostat import Point, Hourly

# ---------------------------------------------------------
# 1. Paths
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WEATHER_PATH = OUTPUT_DIR / "seattle_hourly_weather.parquet"

# ---------------------------------------------------------
# 2. Time Range (from Spark output)
# ---------------------------------------------------------
start = datetime(2010, 1, 1)
end   = datetime(2025, 10, 20)   

# ---------------------------------------------------------
# 3. Seattle Point
# ---------------------------------------------------------
seattle = Point(47.6062, -122.3321, 50)  # lat, lon, elevation

# ---------------------------------------------------------
# 4. Fetch hourly Meteostat data (UTC)
# ---------------------------------------------------------
print(f"Fetching weather data from {start} to {end} ...")

hourly = Hourly(seattle, start, end)
df = hourly.fetch()

# ---------------------------------------------------------
# 5. Reset index (timestamp column)
# ---------------------------------------------------------
df = df.reset_index().rename(columns={"time": "utc_time"})

# ---------------------------------------------------------
# 6. Convert UTC → Pacific Time (PST/PDT)
# ---------------------------------------------------------
utc = pytz.timezone("UTC")
pst = pytz.timezone("America/Los_Angeles")

df["pst_time"] = (
    df["utc_time"]
    .dt.tz_localize(utc)          # mark UTC
    .dt.tz_convert(pst)           # convert to PST/PDT
    .dt.tz_localize(None)         # drop timezone for Spark
)

# ---------------------------------------------------------
# 7. Derive date/time attributes
# ---------------------------------------------------------
df["year"] = df["pst_time"].dt.year
df["month"] = df["pst_time"].dt.month
df["day"] = df["pst_time"].dt.day
df["hour"] = df["pst_time"].dt.hour

# ---------------------------------------------------------
# 8. Select and rename required weather columns
# ---------------------------------------------------------
df = df.rename(columns={
    "temp": "TEMP",
    "prcp": "PRCP",
    "snow": "SNOW",
    "coco": "COCO",
})

final_cols = [
    "year", "month", "day", "hour",
    "TEMP", "PRCP", "SNOW", "COCO"
]

df = df[final_cols]

# ---------------------------------------------------------
# 9. Save as parquet
# ---------------------------------------------------------
df.to_parquet(WEATHER_PATH, index=False)
print("\nDone! Saved to:")
print(WEATHER_PATH)
print("\nPreview:")
print(df.head())