"""GOLD: dimensional model (star schema) + analytics marts, built for Power BI Direct Lake.

Dimensions : dim_date, dim_station (SCD2), dim_weather_hour, dim_event, dim_rider_type, dim_bike_type
Fact       : fact_trip           (grain = one trip; station keys resolved *as of the trip time*)
Marts      : mart_daily_kpis, mart_station_day, mart_hourly_demand   (re-computed only for affected dates)

Every table carries `_batch_id` / `_loaded_at`; SCD2 tables also `_valid_from/_valid_to/_is_current/_row_hash`.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from . import control, lakehouse
from .audit import RunLogger, add_gold_audit
from .config import Config
from .dq import log_check
from .scd2 import HIGH_DATE, apply_scd2, scd2_violations

STATION_TRACKED = ["station_name", "suburb", "zone", "latitude", "longitude", "capacity", "station_status"]
STATION_COLS = ["station_id", "station_name", "suburb", "zone", "latitude", "longitude", "capacity",
                "station_status", "installed_date", "modified_at"]
UNKNOWN_SK = -1


def _hav(lat1, lon1, lat2, lon2):
    r = 6371.0088
    p1, p2 = F.radians(lat1), F.radians(lat2)
    a = F.sin((p2 - p1) / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin((F.radians(lon2) - F.radians(lon1)) / 2) ** 2
    return 2 * r * F.asin(F.sqrt(a))


def _max_str(df: DataFrame, col: str) -> str | None:
    return df.agg(F.date_format(F.max(col), "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()[0]


# ------------------------------------------------------------------------------------ small dimensions
def build_dim_date(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("gold", "dim_date") as st:
        if lakehouse.exists(spark, "gold.dim_date"):
            st.message = "already built"
            return
        d = spark.sql("SELECT explode(sequence(to_date('2026-01-01'), to_date('2027-12-31'), interval 1 day)) AS date")
        d = (d.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
              .withColumn("year", F.year("date")).withColumn("quarter", F.quarter("date"))
              .withColumn("month", F.month("date")).withColumn("month_name", F.date_format("date", "MMMM"))
              .withColumn("year_month", F.date_format("date", "yyyy-MM"))
              .withColumn("day", F.dayofmonth("date"))
              .withColumn("day_of_week", F.weekday("date") + 1)           # 1 = Monday
              .withColumn("day_name", F.date_format("date", "EEEE"))
              .withColumn("week_start", F.date_sub(F.col("date"), F.weekday("date")))
              .withColumn("is_weekend", F.weekday("date") >= 5))
        d = add_gold_audit(d, cfg).select("date_key", "date", "year", "quarter", "month", "month_name", "year_month", "day",
                                          "day_of_week", "day_name", "week_start", "is_weekend", "_batch_id", "_loaded_at")
        lakehouse.overwrite(spark, d, "gold.dim_date")
        st.rows_written = d.count()


def build_static_dims(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("gold", "dim_rider_type + dim_bike_type") as st:
        r = spark.createDataFrame([(1, "member", "Annual / monthly subscriber"), (2, "casual", "Pay-per-ride")],
                                  "rider_type_key int, rider_type string, description string")
        b = spark.createDataFrame([(1, "classic", "Pedal bike"), (2, "electric", "Pedal-assist e-bike")],
                                  "bike_type_key int, bike_type string, description string")
        lakehouse.overwrite(spark, add_gold_audit(r, cfg), "gold.dim_rider_type")
        lakehouse.overwrite(spark, add_gold_audit(b, cfg), "gold.dim_bike_type")
        st.rows_written = 4


# ------------------------------------------------------------------------------------ dim_station (SCD2)
def build_dim_station(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("gold", "dim_station (SCD2)") as st:
        wm = control.get_watermark(spark, "gold.dim_station@modified_at")
        feed = spark.table("silver.station_versions")
        if wm:
            feed = feed.where(F.col("modified_at") > F.lit(wm).cast("timestamp"))
        feed = add_gold_audit(feed.select(*STATION_COLS), cfg).cache()
        st.rows_read = feed.count()
        if st.rows_read == 0:
            st.message = "no new station versions"
            feed.unpersist()
            return
        wm_next = _max_str(feed, "modified_at")
        stats = apply_scd2(spark, feed, "gold.dim_station", key="station_id", tracked=STATION_TRACKED,
                           effective="modified_at", sk_name="station_sk", first_valid_from="installed_date")
        feed.unpersist()
        st.rows_written = stats["versions_inserted"]
        st.message = str(stats)
        control.set_watermark(spark, cfg, "gold.dim_station@modified_at", wm_next)

        if spark.table("gold.dim_station").where(f"station_sk = {UNKNOWN_SK}").count() == 0:  # inferred/unknown member
            row = spark.sql(f"""SELECT CAST({UNKNOWN_SK} AS BIGINT) AS station_sk, 'UNKNOWN' AS station_id, 'Unknown station' AS station_name,
                CAST(NULL AS STRING) AS suburb, 'Unknown' AS zone, CAST(NULL AS DOUBLE) AS latitude, CAST(NULL AS DOUBLE) AS longitude,
                CAST(NULL AS INT) AS capacity, 'unknown' AS station_status, CAST(NULL AS DATE) AS installed_date,
                to_timestamp('1900-01-01') AS modified_at, '{cfg.batch_id}' AS _batch_id, current_timestamp() AS _loaded_at,
                to_timestamp('1900-01-01') AS _valid_from, to_timestamp('{HIGH_DATE}') AS _valid_to, true AS _is_current,
                'unknown' AS _row_hash""")
            cols = spark.table("gold.dim_station").columns
            lakehouse.append(spark, row.select(*cols), "gold.dim_station")


def build_dim_station_current(spark, cfg: Config, log: RunLogger) -> None:
    """Type-1 view of the SCD2 dimension (one row per station) - the dimension that station-grain marts relate to."""
    with log.step("gold", "dim_station_current") as st:
        cur = (spark.table("gold.dim_station").where(f"_is_current AND station_sk != {UNKNOWN_SK}")
               .select("station_id", "station_name", "suburb", "zone", "latitude", "longitude", "capacity", "station_status", "installed_date"))
        out = add_gold_audit(cur, cfg).cache()
        st.rows_written = out.count()
        lakehouse.overwrite(spark, out, "gold.dim_station_current")
        out.unpersist()


# ------------------------------------------------------------------------------------ weather / events
def build_dim_weather(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("gold", "dim_weather_hour") as st:
        s = spark.table("silver.weather_hourly")
        d = (s.select(
            F.date_format("obs_ts", "yyyyMMddHH").cast("int").alias("weather_hour_key"),
            F.date_format("obs_ts", "yyyyMMdd").cast("int").alias("date_key"),
            F.hour("obs_ts").alias("hour"), F.col("obs_ts").alias("hour_start"),
            "temperature_c", "precipitation_mm", "wind_kmh", "humidity_pct", "weather_desc",
            "temp_band", "rain_intensity", "is_raining"))
        d = add_gold_audit(d, cfg).cache()
        st.rows_written = d.count()
        lakehouse.overwrite(spark, d, "gold.dim_weather_hour")
        d.unpersist()


def build_dim_event(spark, cfg: Config, log: RunLogger) -> None:
    with log.step("gold", "dim_event") as st:
        e = spark.table("silver.events").select(
            F.xxhash64("event_id").alias("event_sk"), "event_id", "event_name", "venue", "category", "start_date",
            "end_date", "expected_attendance", "nearest_station_id", "nearest_station_km", "status")
        none = spark.sql("""SELECT CAST(-1 AS BIGINT) AS event_sk, 'NONE' AS event_id, 'No event' AS event_name, CAST(NULL AS STRING) AS venue,
                            'None' AS category, CAST(NULL AS DATE) AS start_date, CAST(NULL AS DATE) AS end_date, CAST(0 AS INT) AS expected_attendance,
                            CAST(NULL AS STRING) AS nearest_station_id, CAST(NULL AS DOUBLE) AS nearest_station_km, 'n/a' AS status""")
        d = add_gold_audit(e.unionByName(none), cfg).cache()
        st.rows_written = d.count()
        lakehouse.overwrite(spark, d, "gold.dim_event")
        d.unpersist()


# ------------------------------------------------------------------------------------ fact_trip
def build_fact_trip(spark, cfg: Config, log: RunLogger) -> tuple[int, int] | None:
    with log.step("gold", "fact_trip") as st:
        wm = control.get_watermark(spark, "gold.fact_trip@silver_processed_at")
        t = spark.table("silver.trips")
        if wm:
            t = t.where(F.col("_processed_at") > F.lit(wm).cast("timestamp"))
        t = t.cache()
        st.rows_read = t.count()
        if st.rows_read == 0:
            st.message = "no new silver trips"
            t.unpersist()
            return None
        wm_next = _max_str(t, "_processed_at")

        dim = spark.table("gold.dim_station").where(f"station_sk != {UNKNOWN_SK}")
        s = dim.select(F.col("station_id").alias("s_id"), F.col("station_sk").alias("s_sk"), F.col("latitude").alias("s_lat"),
                       F.col("longitude").alias("s_lon"), F.col("_valid_from").alias("s_from"), F.col("_valid_to").alias("s_to"))
        e = dim.select(F.col("station_id").alias("e_id"), F.col("station_sk").alias("e_sk"), F.col("latitude").alias("e_lat"),
                       F.col("longitude").alias("e_lon"), F.col("_valid_from").alias("e_from"), F.col("_valid_to").alias("e_to"))
        # point-in-time (as-of) join against the SCD2 history
        j = (t.join(F.broadcast(s), (t.start_station_id == s.s_id) & (t.started_at >= s.s_from) & (t.started_at < s.s_to), "left")
              .join(F.broadcast(e), (t.end_station_id == e.e_id) & (t.ended_at >= e.e_from) & (t.ended_at < e.e_to), "left"))

        ev = (spark.table("silver.events").where("status = 'confirmed'")
              .select("event_id", "start_date", "end_date", "nearest_station_id", "expected_attendance"))
        j = j.join(F.broadcast(ev), (j.trip_date.between(ev.start_date, ev.end_date)) &
                   ((j.start_station_id == ev.nearest_station_id) | (j.end_station_id == ev.nearest_station_id)), "left")
        w = Window.partitionBy("trip_id").orderBy(F.col("expected_attendance").desc_nulls_last())
        j = j.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")

        fact = j.select(
            "trip_id",
            F.date_format("trip_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.date_format("started_at", "yyyyMMddHH").cast("int").alias("weather_hour_key"),
            F.col("start_hour"),
            F.coalesce("s_sk", F.lit(UNKNOWN_SK)).alias("start_station_sk"),
            F.coalesce("e_sk", F.lit(UNKNOWN_SK)).alias("end_station_sk"),
            F.when(F.col("rider_type") == "member", 1).otherwise(2).alias("rider_type_key"),
            F.when(F.col("rideable_type") == "electric", 2).otherwise(1).alias("bike_type_key"),
            F.when(F.col("event_id").isNotNull(), F.xxhash64("event_id")).otherwise(F.lit(-1).cast("bigint")).alias("event_sk"),  # xxhash64(NULL) is NOT NULL!
            "bike_id", "started_at", "ended_at", "duration_sec",
            F.round(F.col("duration_sec") / 60.0, 2).alias("duration_min"),
            F.round(_hav(F.col("s_lat"), F.col("s_lon"), F.col("e_lat"), F.col("e_lon")), 3).alias("distance_km"),
            F.col("fare_aud"), F.col("battery_pct"),
            (F.col("duration_sec") < 60).alias("is_short_trip"),
            (F.col("start_station_id") == F.col("end_station_id")).alias("is_round_trip"),
            (F.size("dq_warnings") > 0).alias("has_dq_warning"))
        fact = add_gold_audit(fact, cfg).cache()
        st.rows_written = fact.count()
        bounds = fact.agg(F.min("date_key"), F.max("date_key")).first()
        lakehouse.upsert(spark, fact, "gold.fact_trip", ["trip_id"])
        fact.unpersist()
        t.unpersist()
        control.set_watermark(spark, cfg, "gold.fact_trip@silver_processed_at", wm_next)
        return int(bounds[0]), int(bounds[1])


# ------------------------------------------------------------------------------------ marts
def build_marts(spark, cfg: Config, log: RunLogger, bounds: tuple[int, int] | None) -> None:
    if bounds is None and lakehouse.exists(spark, "gold.mart_daily_kpis"):
        with log.step("gold", "marts") as st:
            st.message = "no changed dates"
        return
    lo, hi = bounds if bounds else (0, 99999999)
    where = f"date_key BETWEEN {lo} AND {hi}"
    fact = spark.table("gold.fact_trip").where(where)
    wx = spark.table("gold.dim_weather_hour")
    dim = spark.table("gold.dim_station").select("station_sk", "station_id", "capacity")

    with log.step("gold", "mart_daily_kpis") as st:
        k = fact.groupBy("date_key").agg(
            F.count("*").alias("trips"),
            F.sum((F.col("rider_type_key") == 1).cast("int")).alias("member_trips"),
            F.sum((F.col("rider_type_key") == 2).cast("int")).alias("casual_trips"),
            F.sum((F.col("bike_type_key") == 2).cast("int")).alias("electric_trips"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
            F.round(F.sum("fare_aud"), 2).alias("revenue_aud"),
            F.countDistinct("start_station_sk").alias("active_stations"),
            F.sum((F.col("event_sk") != -1).cast("int")).alias("event_trips"))
        d = wx.where(where).groupBy("date_key").agg(
            F.round(F.avg("temperature_c"), 1).alias("avg_temp_c"), F.round(F.sum("precipitation_mm"), 1).alias("total_rain_mm"),
            F.sum(F.col("is_raining").cast("int")).alias("rain_hours"))
        out = add_gold_audit(k.join(d, "date_key", "left"), cfg)
        lakehouse.overwrite(spark, out, "gold.mart_daily_kpis", replace_where=where)
        st.rows_written = out.count()

    with log.step("gold", "mart_station_day") as st:
        starts = (fact.groupBy("date_key", "start_station_sk").agg(F.count("*").alias("trips_started"))
                  .join(dim, F.col("start_station_sk") == dim.station_sk).select("date_key", "station_id", "trips_started", F.col("capacity").alias("cap_s")))
        ends = (fact.groupBy("date_key", "end_station_sk").agg(F.count("*").alias("trips_ended"))
                .join(dim, F.col("end_station_sk") == dim.station_sk).select("date_key", "station_id", "trips_ended", F.col("capacity").alias("cap_e")))
        m = starts.join(ends, ["date_key", "station_id"], "full_outer")
        m = (m.withColumn("trips_started", F.coalesce("trips_started", F.lit(0))).withColumn("trips_ended", F.coalesce("trips_ended", F.lit(0)))
              .withColumn("capacity", F.coalesce("cap_s", "cap_e")).drop("cap_s", "cap_e")
              .where("station_id != 'UNKNOWN'")
              .withColumn("net_flow", F.col("trips_ended") - F.col("trips_started"))
              .withColumn("turnover_per_dock", F.round((F.col("trips_started") + F.col("trips_ended")) / F.col("capacity"), 3)))
        out = add_gold_audit(m, cfg)
        lakehouse.overwrite(spark, out, "gold.mart_station_day", replace_where=where)
        st.rows_written = out.count()

    with log.step("gold", "mart_hourly_demand") as st:
        h = fact.groupBy("date_key", "weather_hour_key", "start_hour").agg(
            F.count("*").alias("trips"), F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.sum((F.col("rider_type_key") == 1).cast("int")).alias("member_trips"))
        wcols = wx.select("weather_hour_key", "temperature_c", "precipitation_mm", "wind_kmh", "temp_band", "rain_intensity", "is_raining")
        out = add_gold_audit(h.join(wcols, "weather_hour_key", "left"), cfg)
        lakehouse.overwrite(spark, out, "gold.mart_hourly_demand", replace_where=where)
        st.rows_written = out.count()


def build_quarantine_summary(spark, cfg: Config, log: RunLogger) -> None:
    """audit.quarantine_summary: one row per (batch, table, broken rule) - feeds the Power BI data-quality page."""
    with log.step("gold", "audit.quarantine_summary") as st:
        parts = []
        for t in ("trips", "stations", "weather", "events"):
            name = f"silver.quarantine_{t}"
            if lakehouse.exists(spark, name):
                parts.append(spark.table(name).select(F.lit(f"silver.{t}").alias("source_table"),
                                                      F.col("_quarantined_batch_id").alias("batch_id"),
                                                      F.explode("dq_errors").alias("rule_name")))
        if not parts:
            st.message = "nothing quarantined"
            return
        from functools import reduce
        out = (reduce(lambda a, b: a.unionByName(b), parts).groupBy("source_table", "batch_id", "rule_name")
               .agg(F.count("*").alias("rows_quarantined")))
        lakehouse.overwrite(spark, out, "audit.quarantine_summary")
        st.rows_written = out.count()


# ------------------------------------------------------------------------------------ gold-level data quality
def run_checks(spark, cfg: Config, log: RunLogger) -> list[str]:
    """Reconciliation + integrity checks. Results are logged to audit.dq_results; returns the failed check names."""
    failed = []
    with log.step("gold", "dq_checks") as st:
        silver_n = spark.table("silver.trips").count()
        fact = spark.table("gold.fact_trip")
        fact_n = fact.count()
        def chk(name, checked, bad, sev="error"):
            if not log_check(spark, cfg, "gold", name.split(":")[0], name.split(":")[1], checked, bad, sev):
                if sev == "error":
                    failed.append(name)
        chk("fact_trip:rowcount_matches_silver", silver_n, abs(silver_n - fact_n))
        s_rev = spark.table("silver.trips").agg(F.coalesce(F.sum("fare_aud"), F.lit(0))).first()[0]
        f_rev = fact.agg(F.coalesce(F.sum("fare_aud"), F.lit(0))).first()[0]
        chk("fact_trip:revenue_matches_silver", 1, 0 if s_rev == f_rev else 1)
        chk("fact_trip:trip_id_unique", fact_n, fact_n - fact.select("trip_id").distinct().count())
        chk("fact_trip:date_key_in_dim_date", fact_n, fact.join(spark.table("gold.dim_date"), "date_key", "left_anti").count())
        chk("fact_trip:weather_key_in_dim_weather", fact_n,
            fact.join(spark.table("gold.dim_weather_hour"), "weather_hour_key", "left_anti").count(), "warn")
        chk("fact_trip:start_station_resolved", fact_n, fact.where("start_station_sk = -1").count(), "warn")
        chk("fact_trip:end_station_resolved", fact_n, fact.where("end_station_sk = -1").count(), "warn")
        v = scd2_violations(spark, "gold.dim_station", "station_id")
        n_keys = spark.table("gold.dim_station").select("station_id").distinct().count()
        chk("dim_station:one_current_row_per_key", n_keys, v["multi_current"] + v["no_current"])
        chk("dim_station:no_overlap_or_gap", n_keys, v["overlap_or_gap"])
        st.rows_read = silver_n
        st.rows_rejected = len(failed)
        st.message = f"failed: {failed}" if failed else "all critical checks passed"
    return failed


def run(spark, cfg: Config, log: RunLogger) -> list[str]:
    build_dim_date(spark, cfg, log)
    build_static_dims(spark, cfg, log)
    build_dim_station(spark, cfg, log)
    build_dim_station_current(spark, cfg, log)
    build_dim_weather(spark, cfg, log)
    build_dim_event(spark, cfg, log)
    bounds = build_fact_trip(spark, cfg, log)
    build_marts(spark, cfg, log, bounds)
    build_quarantine_summary(spark, cfg, log)
    return run_checks(spark, cfg, log)
