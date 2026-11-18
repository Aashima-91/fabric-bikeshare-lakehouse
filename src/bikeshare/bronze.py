"""BRONZE: raw, append-only Delta tables. Nothing is cleaned or dropped here - only audit columns are added.

* trips    - all columns kept as STRING (schema-on-read safety); new columns are absorbed via mergeSchema
* stations - typed Parquet written by the SQL extract
* weather  - raw API payload (arrays of hourly values) kept exactly as returned
* events   - Excel read with pandas, every cell kept as STRING

Idempotency: file-based sources use the file ledger, batch-based sources read only this batch's landing folder.
"""
from __future__ import annotations

import os
from functools import reduce

from pyspark.sql import functions as F

from . import control, lakehouse
from .audit import RunLogger, add_bronze_audit
from .config import Config

_EVENT_COLS = ["event_name", "venue", "category", "start_date", "end_date", "expected_attendance", "venue_lat", "venue_lon", "status"]


def load_trips(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("bronze", "trips") as st:
        folder = cfg.landing_posix("trips")
        files = sorted(f for f in os.listdir(folder) if f.endswith(".csv")) if os.path.isdir(folder) else []
        new = control.unregistered_files(spark, "trips", files)
        if not new:
            st.message = "no new files"
            return
        parts = []
        for f in new:  # per-file read so files with different headers (schema drift) can coexist
            df = spark.read.option("header", "true").option("inferSchema", "false").csv(f"{cfg.landing_spark('trips')}/{f}")
            parts.append(add_bronze_audit(df, cfg, "trips_sftp", F.lit(f"landing/trips/{f}")))
        out = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts).cache()
        st.rows_read = st.rows_written = out.count()
        lakehouse.append(spark, out, "bronze.trips")
        out.unpersist()
        control.register_files(spark, cfg, "trips", new)
        st.message = f"{len(new)} file(s)"


def load_stations(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("bronze", "stations") as st:
        path = f"{cfg.landing_posix('stations')}/batch_id={cfg.batch_id}"
        if not os.path.isdir(path):
            st.message = "no changed stations"
            return
        df = add_bronze_audit(spark.read.parquet(f"{cfg.landing_spark('stations')}/batch_id={cfg.batch_id}"),
                              cfg, "stations_sqldb", F.lit(f"landing/stations/batch_id={cfg.batch_id}"))
        df = df.cache()
        st.rows_read = st.rows_written = df.count()
        lakehouse.append(spark, df, "bronze.stations")
        df.unpersist()
        control.set_watermark(spark, cfg, "stations.modified_at", control.get_watermark(spark, "stations.modified_at@pending"))


def load_weather(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("bronze", "weather") as st:
        path = f"{cfg.landing_posix('weather')}/batch_id={cfg.batch_id}"
        if not os.path.isdir(path):
            st.message = "no weather pull"
            return
        files = sorted(os.listdir(path))
        parts = []
        for f in files:
            df = spark.read.option("multiLine", "true").json(f"{cfg.landing_spark('weather')}/batch_id={cfg.batch_id}/{f}")
            parts.append(add_bronze_audit(df, cfg, "weather_api", F.lit(f"landing/weather/batch_id={cfg.batch_id}/{f}")))
        out = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts).cache()
        st.rows_read = st.rows_written = out.count()   # one row per API response
        lakehouse.append(spark, out, "bronze.weather")
        out.unpersist()
        control.set_watermark(spark, cfg, "weather.last_date", control.get_watermark(spark, "weather.last_date@pending"))


def load_events(spark, cfg: Config, log: RunLogger) -> None:
    import pandas as pd

    with log.step("bronze", "events") as st:
        folder = cfg.landing_posix("events")
        files = sorted(f for f in os.listdir(folder) if f.endswith(".xlsx")) if os.path.isdir(folder) else []
        new = control.unregistered_files(spark, "events", files)
        if not new:
            st.message = "no new workbook"
            return
        parts = []
        for f in new:
            pdf = pd.read_excel(os.path.join(folder, f), sheet_name="Events", header=3, dtype=str, engine="openpyxl")
            pdf.columns = _EVENT_COLS
            rows = [tuple(None if (v is None or v is pd.NA or (isinstance(v, float) and v != v)) else str(v) for v in r)
                    for r in pdf.itertuples(index=False, name=None)]
            df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in _EVENT_COLS))
            parts.append(add_bronze_audit(df, cfg, "events_excel", F.lit(f"landing/events/{f}")))
        out = reduce(lambda a, b: a.unionByName(b), parts).cache()
        st.rows_read = st.rows_written = out.count()
        lakehouse.append(spark, out, "bronze.events")
        out.unpersist()
        control.register_files(spark, cfg, "events", new)


def run(spark, cfg: Config, log: RunLogger) -> None:
    load_trips(spark, cfg, log)
    load_stations(spark, cfg, log)
    load_weather(spark, cfg, log)
    load_events(spark, cfg, log)
