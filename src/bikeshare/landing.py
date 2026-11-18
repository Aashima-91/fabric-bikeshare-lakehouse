"""LANDING (staging) zone: land every source as-is under Files/landing/<source>/ before anything is parsed.

In Microsoft Fabric the first two steps are normally Data Factory *Copy activities* (SFTP/Blob -> Files, and
Azure SQL -> Parquet); here they are plain Python so the same logic runs locally and in a notebook.
Landing is immutable: files are never modified, only added.

| source   | system type | pattern                                                        |
|----------|-------------|----------------------------------------------------------------|
| trips    | file drop   | copy daily CSVs that are not yet in landing                    |
| stations | SQL DB      | incremental extract WHERE modified_at > watermark -> Parquet   |
| weather  | REST API    | date-window pull (watermark + lookback) -> raw JSON            |
| events   | Excel       | copy workbook when its content hash is new                     |
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from datetime import date, timedelta

from . import control
from .config import Config
from .sources.weather_client import fetch_open_meteo
from .sources.world import BikeShareWorld

_STATION_SCHEMA = ("station_id string, station_name string, suburb string, zone string, latitude double, "
                   "longitude double, capacity int, station_status string, installed_date string, modified_at string")


def land_trip_files(cfg: Config, world: BikeShareWorld) -> list[str]:
    dst = cfg.landing_posix("trips")
    os.makedirs(dst, exist_ok=True)
    copied = []
    for f in sorted(os.listdir(world.trips_dir)):
        if f.endswith(".csv") and not os.path.exists(os.path.join(dst, f)):
            shutil.copy2(os.path.join(world.trips_dir, f), os.path.join(dst, f))
            copied.append(f)
    return copied


def extract_stations(spark, cfg: Config, world: BikeShareWorld) -> dict:
    """Incremental pull from the OLTP table. The new watermark is only *promoted* after Bronze commits."""
    wm = control.get_watermark(spark, "stations.modified_at") or "1900-01-01 00:00:00"
    con = sqlite3.connect(world.db_path)
    rows = con.execute("SELECT station_id, station_name, suburb, zone, latitude, longitude, capacity, station_status, "
                       "installed_date, modified_at FROM stations WHERE modified_at > ? ORDER BY modified_at", (wm,)).fetchall()
    con.close()
    if not rows:
        return {"rows": 0}
    out = f"{cfg.landing_spark('stations')}/batch_id={cfg.batch_id}"
    spark.createDataFrame(rows, _STATION_SCHEMA).coalesce(1).write.mode("overwrite").parquet(out)
    control.set_watermark(spark, cfg, "stations.modified_at@pending", max(r[-1] for r in rows))
    return {"rows": len(rows), "path": out}


def land_weather(spark, cfg: Config, world: BikeShareWorld, as_of: date) -> dict:
    last = control.get_watermark(spark, "weather.last_date")
    start = (date.fromisoformat(last) + timedelta(days=1 - cfg.weather_lookback_days)) if last else world.start
    if start > as_of:
        return {"rows": 0}
    payload = fetch_open_meteo(cfg.lat, cfg.lon, start, as_of, cfg.tz) if cfg.weather_live else world.weather_payload(start, as_of)
    folder = os.path.join(cfg.landing_posix("weather"), f"batch_id={cfg.batch_id}")
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, f"weather_{start}_{as_of}.json"), "w") as f:
        json.dump(payload, f)
    control.set_watermark(spark, cfg, "weather.last_date@pending", as_of.isoformat())
    return {"rows": len(payload["hourly"]["time"]), "start": str(start), "end": str(as_of)}


def land_events(cfg: Config, world: BikeShareWorld) -> list[str]:
    if not os.path.exists(world.events_path):
        return []
    digest = hashlib.sha256(open(world.events_path, "rb").read()).hexdigest()[:10]
    dst_dir = cfg.landing_posix("events")
    os.makedirs(dst_dir, exist_ok=True)
    name = f"events_{digest}.xlsx"
    if os.path.exists(os.path.join(dst_dir, name)):
        return []
    shutil.copy2(world.events_path, os.path.join(dst_dir, name))
    return [name]
