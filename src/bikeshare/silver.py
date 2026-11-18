"""SILVER: cleaned, typed, de-duplicated, validated and conformed tables.

Every table is loaded incrementally: we only read Bronze rows whose `_ingested_at` is newer than the watermark
stored after the last successful run, so a failed run is simply retried from the same point.

Row-level data quality is enforced here (see dq.py). Rejected rows go to a quarantine table together with the
names of the rules they broke - nothing is silently dropped.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from . import control, lakehouse
from .audit import RunLogger, add_silver_audit
from .config import Config
from .dq import Rule, apply_rules

TS_FORMATS = ("yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm")

# ------------------------------------------------------------------------------------ helpers
def _ts(col: str, formats=TS_FORMATS):
    return F.coalesce(*[F.expr(f"try_to_timestamp(`{col}`, '{f}')") for f in formats])


def _new_bronze(spark, table: str, wm_name: str) -> tuple[DataFrame, str | None]:
    df = spark.table(table)
    wm = control.get_watermark(spark, wm_name)
    if wm:
        df = df.where(F.col("_ingested_at") > F.lit(wm).cast("timestamp"))
    return df, wm


def _max_ingested(df: DataFrame) -> str | None:
    return df.agg(F.date_format(F.max("_ingested_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()[0]


def _latest(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _quarantine(spark, cfg: Config, bad: DataFrame, table: str) -> int:
    bad = bad.withColumn("_quarantined_batch_id", F.lit(cfg.batch_id)).withColumn("_quarantined_at", F.current_timestamp()).cache()
    n = bad.count()
    if n:
        lakehouse.append(spark, bad, table)
    bad.unpersist()
    return n


# ------------------------------------------------------------------------------------ trips
TRIP_RULES = [
    Rule("trip_id_not_null", "trip_id IS NOT NULL", "error"),
    Rule("started_at_parseable", "started_at IS NOT NULL", "error"),
    Rule("ended_at_parseable", "ended_at IS NOT NULL", "error"),
    Rule("end_not_before_start", "ended_at >= started_at", "error"),
    Rule("duration_le_24h", "duration_sec <= 86400", "error", "bike not returned / clock error"),
    Rule("start_station_present", "start_station_id IS NOT NULL", "error"),
    Rule("rider_type_valid", "rider_type IN ('member','casual')", "error"),
    Rule("fare_non_negative", "fare_aud >= 0", "error"),
    Rule("not_future_dated", "started_at <= current_timestamp() + INTERVAL 1 DAY", "error", "clock error / bad export"),
    Rule("start_station_known", "start_station_known", "warn", "orphan FK -> mapped to Unknown member in gold"),
    Rule("duration_ge_60s", "duration_sec >= 60", "warn", "false start / dock fault"),
    Rule("end_station_present", "end_station_id IS NOT NULL", "warn", "bike not docked"),
]
TRIP_HASH_COLS = ["trip_id", "bike_id", "rideable_type", "started_at", "ended_at", "start_station_id",
                  "end_station_id", "rider_type", "fare_aud", "battery_pct"]


def transform_trips(b: DataFrame) -> DataFrame:
    """Pure DataFrame -> DataFrame (unit-testable): type casting, normalisation, derived columns."""
    battery = F.col("battery_pct").cast("int") if "battery_pct" in b.columns else F.lit(None).cast("int")
    t = (b.select(
        F.trim("trip_id").alias("trip_id"),
        F.trim("bike_id").alias("bike_id"),
        F.when(F.lower(F.col("rideable_type")).contains("electric"), "electric")
         .when(F.col("rideable_type").isNotNull(), "classic").alias("rideable_type"),
        _ts("started_at").alias("started_at"),
        _ts("ended_at").alias("ended_at"),
        F.upper(F.trim("start_station_id")).alias("start_station_id"),
        F.upper(F.trim("end_station_id")).alias("end_station_id"),
        F.lower(F.trim("rider_type")).alias("rider_type"),
        F.col("fare_aud").cast("decimal(10,2)").alias("fare_aud"),
        battery.alias("battery_pct"),
        F.col("started_at").alias("_raw_started_at"), F.col("ended_at").alias("_raw_ended_at"),
        "_source_file", "_ingested_at"))
    return (t.withColumn("duration_sec", F.unix_timestamp("ended_at") - F.unix_timestamp("started_at"))
             .withColumn("trip_date", F.to_date("started_at"))
             .withColumn("start_hour", F.hour("started_at")))


def load_trips(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("silver", "trips") as st:
        new, _ = _new_bronze(spark, "bronze.trips", "silver.trips@bronze_ingested_at")
        new = new.cache()
        st.rows_read = new.count()
        if st.rows_read == 0:
            st.message = "nothing new in bronze"
            new.unpersist()
            return
        wm_next = _max_ingested(new)
        known = (spark.table("silver.station_versions").select(F.col("station_id").alias("_known_id")).distinct()
                 .withColumn("start_station_known", F.lit(True)))
        t = (transform_trips(new).join(F.broadcast(known), F.col("start_station_id") == F.col("_known_id"), "left")
             .drop("_known_id").withColumn("start_station_known", F.coalesce("start_station_known", F.lit(False))))
        key = F.coalesce(F.col("trip_id"), F.concat(F.lit("NULL-"), F.monotonically_increasing_id().cast("string")))
        deduped = _latest(t.withColumn("_dedupe_key", key), ["_dedupe_key"]).drop("_dedupe_key")
        valid, bad, n_unique = apply_rules(spark, cfg, deduped, TRIP_RULES, "silver.trips")
        st.rows_rejected = _quarantine(spark, cfg, bad, "silver.quarantine_trips")
        good = add_silver_audit(valid.drop("_raw_started_at", "_raw_ended_at", "start_station_known"), cfg, TRIP_HASH_COLS)
        good = lakehouse.only_new_or_changed(spark, good, "silver.trips", ["trip_id"]).cache()
        st.rows_written = good.count()
        if st.rows_written:
            lakehouse.upsert(spark, good, "silver.trips", ["trip_id"])
        good.unpersist()
        new.unpersist()
        control.set_watermark(spark, cfg, "silver.trips@bronze_ingested_at", wm_next)
        st.message = f"{st.rows_read - n_unique} duplicate row(s) collapsed"


# ------------------------------------------------------------------------------------ stations (all versions)
STATION_RULES = [
    Rule("station_id_not_null", "station_id IS NOT NULL", "error"),
    Rule("station_name_not_null", "station_name IS NOT NULL", "error"),
    Rule("capacity_positive", "capacity > 0", "error"),
    Rule("lat_lon_in_melbourne", "latitude BETWEEN -39 AND -36 AND longitude BETWEEN 143 AND 146", "error"),
    Rule("status_valid", "station_status IN ('active','maintenance','decommissioned')", "error"),
    Rule("suburb_present", "suburb IS NOT NULL", "warn"),
]
STATION_HASH_COLS = ["station_id", "station_name", "suburb", "zone", "latitude", "longitude", "capacity",
                     "station_status", "installed_date", "modified_at"]


def transform_stations(b: DataFrame) -> DataFrame:
    return b.select(
        F.upper(F.trim("station_id")).alias("station_id"),
        F.trim("station_name").alias("station_name"),
        F.when(F.trim("suburb") != "", F.trim("suburb")).alias("suburb"),
        F.trim("zone").alias("zone"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("capacity").cast("int").alias("capacity"),
        F.lower(F.trim("station_status")).alias("station_status"),
        F.to_date("installed_date").alias("installed_date"),
        F.col("modified_at").cast("timestamp").alias("modified_at"),
        "_source_file", "_ingested_at")


def load_stations(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("silver", "station_versions") as st:
        new, _ = _new_bronze(spark, "bronze.stations", "silver.station_versions@bronze_ingested_at")
        new = new.cache()
        st.rows_read = new.count()
        if st.rows_read == 0:
            st.message = "nothing new in bronze"
            new.unpersist()
            return
        wm_next = _max_ingested(new)
        t = _latest(transform_stations(new), ["station_id", "modified_at"])
        valid, bad, _ = apply_rules(spark, cfg, t, STATION_RULES, "silver.station_versions")
        st.rows_rejected = _quarantine(spark, cfg, bad, "silver.quarantine_stations")
        good = add_silver_audit(valid, cfg, STATION_HASH_COLS)
        good = lakehouse.only_new_or_changed(spark, good, "silver.station_versions", ["station_id", "modified_at"]).cache()
        st.rows_written = good.count()
        if st.rows_written:
            lakehouse.upsert(spark, good, "silver.station_versions", ["station_id", "modified_at"])
        good.unpersist()
        new.unpersist()
        control.set_watermark(spark, cfg, "silver.station_versions@bronze_ingested_at", wm_next)


# ------------------------------------------------------------------------------------ weather
WEATHER_RULES = [
    Rule("obs_ts_not_null", "obs_ts IS NOT NULL", "error"),
    Rule("temp_plausible", "temperature_c BETWEEN -10 AND 50", "error"),
    Rule("precip_non_negative", "precipitation_mm >= 0", "error"),
    Rule("wind_non_negative", "wind_kmh >= 0", "error"),
    Rule("humidity_0_100", "humidity_pct BETWEEN 0 AND 100", "warn"),
]
WEATHER_HASH_COLS = ["obs_ts", "temperature_c", "precipitation_mm", "wind_kmh", "humidity_pct", "weather_code"]
WMO = {0: "Clear", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast", 61: "Light rain", 63: "Moderate rain", 65: "Heavy rain"}


def transform_weather(b: DataFrame) -> DataFrame:
    """Explode the API's column-oriented arrays into one row per hour."""
    p = b.select("_source_file", "_ingested_at", F.posexplode("hourly.time").alias("pos", "time"), "hourly")
    idx = F.col("pos")
    wmo = F.create_map(*[x for k, v in WMO.items() for x in (F.lit(k), F.lit(v))])
    out = p.select(
        F.expr("try_to_timestamp(time, \"yyyy-MM-dd'T'HH:mm\")").alias("obs_ts"),
        F.col("hourly.temperature_2m")[idx].cast("double").alias("temperature_c"),
        F.col("hourly.precipitation")[idx].cast("double").alias("precipitation_mm"),
        F.col("hourly.wind_speed_10m")[idx].cast("double").alias("wind_kmh"),
        F.col("hourly.relative_humidity_2m")[idx].cast("double").alias("humidity_pct"),
        F.col("hourly.weather_code")[idx].cast("int").alias("weather_code"),
        "_source_file", "_ingested_at")
    return (out.withColumn("weather_desc", F.coalesce(wmo[F.col("weather_code")], F.lit("Other")))
               .withColumn("is_raining", F.col("precipitation_mm") >= 0.1)
               .withColumn("rain_intensity", F.when(F.col("precipitation_mm") >= 2.5, "Heavy")
                           .when(F.col("precipitation_mm") >= 0.5, "Moderate")
                           .when(F.col("precipitation_mm") >= 0.1, "Light").otherwise("None"))
               .withColumn("temp_band", F.when(F.col("temperature_c") < 8, "Cold (<8C)")
                           .when(F.col("temperature_c") < 14, "Cool (8-14C)")
                           .when(F.col("temperature_c") < 20, "Mild (14-20C)").otherwise("Warm (20C+)")))


def load_weather(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("silver", "weather_hourly") as st:
        new, _ = _new_bronze(spark, "bronze.weather", "silver.weather_hourly@bronze_ingested_at")
        new = new.cache()
        n_payloads = new.count()
        if n_payloads == 0:
            st.message = "nothing new in bronze"
            new.unpersist()
            return
        wm_next = _max_ingested(new)
        t = _latest(transform_weather(new), ["obs_ts"])  # overlapping pulls: newest observation wins
        st.rows_read = t.count()
        valid, bad, _ = apply_rules(spark, cfg, t, WEATHER_RULES, "silver.weather_hourly")
        st.rows_rejected = _quarantine(spark, cfg, bad, "silver.quarantine_weather")
        good = add_silver_audit(valid, cfg, WEATHER_HASH_COLS)
        good = lakehouse.only_new_or_changed(spark, good, "silver.weather_hourly", ["obs_ts"]).cache()
        st.rows_written = good.count()
        if st.rows_written:
            lakehouse.upsert(spark, good, "silver.weather_hourly", ["obs_ts"])
        good.unpersist()
        new.unpersist()
        control.set_watermark(spark, cfg, "silver.weather_hourly@bronze_ingested_at", wm_next)


# ------------------------------------------------------------------------------------ events
EVENT_RULES = [
    Rule("event_name_not_null", "event_name IS NOT NULL", "error"),
    Rule("dates_valid", "start_date IS NOT NULL AND end_date >= start_date", "error"),
    Rule("attendance_non_negative", "expected_attendance >= 0", "error"),
    Rule("status_valid", "status IN ('confirmed','cancelled')", "error"),
    Rule("venue_near_a_station", "nearest_station_km <= 2.0", "warn"),
]
EVENT_HASH_COLS = ["event_id", "event_name", "venue", "category", "start_date", "end_date", "expected_attendance",
                   "status", "nearest_station_id"]


def _haversine_expr(lat1, lon1, lat2, lon2):
    r = 6371.0088
    p1, p2 = F.radians(lat1), F.radians(lat2)
    a = F.sin((p2 - p1) / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin((F.radians(lon2) - F.radians(lon1)) / 2) ** 2
    return 2 * r * F.asin(F.sqrt(a))


def transform_events(b: DataFrame, stations_latest: DataFrame) -> DataFrame:
    d = lambda c: F.coalesce(F.to_date(F.expr(f"try_to_timestamp(`{c}`, 'yyyy-MM-dd HH:mm:ss')")),
                             F.to_date(F.expr(f"try_to_timestamp(`{c}`, 'dd/MM/yyyy')")))
    e = b.select(
        F.trim("event_name").alias("event_name"), F.trim("venue").alias("venue"), F.trim("category").alias("category"),
        d("start_date").alias("start_date"), d("end_date").alias("end_date"),
        F.regexp_replace("expected_attendance", ",", "").cast("int").alias("expected_attendance"),
        F.col("venue_lat").cast("double").alias("venue_lat"), F.col("venue_lon").cast("double").alias("venue_lon"),
        F.lower(F.trim("status")).alias("status"), "_source_file", "_ingested_at")
    e = e.withColumn("event_id", F.substring(F.sha2(F.concat_ws("|", F.lower("event_name"), F.lower("venue"), F.col("start_date").cast("string")), 256), 1, 16))
    e = _latest(e, ["event_id"])
    st = stations_latest.select(F.col("station_id").alias("nearest_station_id"), F.col("latitude").alias("_slat"), F.col("longitude").alias("_slon"))
    j = e.crossJoin(F.broadcast(st)).withColumn("nearest_station_km", _haversine_expr(F.col("venue_lat"), F.col("venue_lon"), F.col("_slat"), F.col("_slon")))
    w = Window.partitionBy("event_id").orderBy("nearest_station_km")
    return (j.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_slat", "_slon")
             .withColumn("nearest_station_km", F.round("nearest_station_km", 3)))


def load_events(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("silver", "events") as st:
        new, _ = _new_bronze(spark, "bronze.events", "silver.events@bronze_ingested_at")
        new = new.cache()
        st.rows_read = new.count()
        if st.rows_read == 0:
            st.message = "nothing new in bronze"
            new.unpersist()
            return
        wm_next = _max_ingested(new)
        w = Window.partitionBy("station_id").orderBy(F.col("modified_at").desc())
        latest = (spark.table("silver.station_versions").withColumn("_rn", F.row_number().over(w))
                  .where("_rn = 1 AND station_status != 'decommissioned'"))
        t = transform_events(new, latest)
        valid, bad, _ = apply_rules(spark, cfg, t, EVENT_RULES, "silver.events")
        st.rows_rejected = _quarantine(spark, cfg, bad, "silver.quarantine_events")
        good = add_silver_audit(valid, cfg, EVENT_HASH_COLS)
        good = lakehouse.only_new_or_changed(spark, good, "silver.events", ["event_id"]).cache()
        st.rows_written = good.count()
        if st.rows_written:
            lakehouse.upsert(spark, good, "silver.events", ["event_id"])
        good.unpersist()
        new.unpersist()
        control.set_watermark(spark, cfg, "silver.events@bronze_ingested_at", wm_next)


def run(spark, cfg: Config, log: RunLogger) -> None:
    load_stations(spark, cfg, log)   # stations first: events resolve their nearest station from it
    load_trips(spark, cfg, log)
    load_weather(spark, cfg, log)
    load_events(spark, cfg, log)
