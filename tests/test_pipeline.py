"""End-to-end tests on the full 4-batch incremental load (data is built once per session)."""
import csv
from collections import Counter

import pytest
from pyspark.sql import functions as F

from bikeshare.control import get_watermark
from bikeshare.pipeline import run_incremental
from bikeshare.scd2 import scd2_violations


def _ground_truth(root):
    gt = Counter()
    with open(f"{root}/Files/source_world/injected_errors.csv") as f:
        for r in csv.DictReader(f):
            gt[r["error_type"]] += int(r["count"])
    return gt


def test_every_run_step_succeeded(spark, built):
    status = {r.status: r["count"] for r in spark.table("audit.pipeline_run_log").groupBy("status").count().collect()}
    assert status == {"SUCCESS": status["SUCCESS"]}
    assert spark.table("audit.pipeline_run_log").select("batch_id").distinct().count() == 4


def test_injected_errors_are_caught_quarantined_or_flagged(spark, built, root):
    """Ground-truth reconciliation: the simulator logs each defect it injects; the pipeline must account for all of them."""
    gt = _ground_truth(root)
    q = spark.table("silver.quarantine_trips").select(F.explode("dq_errors").alias("r")).groupBy("r").count()
    quarantined = {r.r: r["count"] for r in q.collect()}
    w = spark.table("silver.trips").select(F.explode("dq_warnings").alias("w")).groupBy("w").count()
    warned = {r.w: r["count"] for r in w.collect()}
    expected = [  # injected defect              -> where the pipeline must have caught it
        ("end_before_start", quarantined, "end_not_before_start"),
        ("not_returned_over_24h", quarantined, "duration_le_24h"),
        ("negative_fare", quarantined, "fare_non_negative"),
        ("null_trip_id", quarantined, "trip_id_not_null"),
        ("invalid_rider_type", quarantined, "rider_type_valid"),
        ("future_dated", quarantined, "not_future_dated"),
        ("false_start_under_60s", warned, "duration_ge_60s"),
        ("missing_end_station", warned, "end_station_present"),
        ("orphan_start_station", warned, "start_station_known"),
    ]
    print("\n%-26s %9s %9s" % ("injected defect", "injected", "caught"))
    for defect, bucket, rule in expected:
        caught = bucket.get(rule, 0)
        print("%-26s %9d %9d  (%s)" % (defect, gt[defect], caught, rule))
        # tolerance: exact duplicates/replays of a bad row are collapsed (or, for NULL keys, not collapsible)
        assert 0.85 * gt[defect] <= caught <= 1.3 * gt[defect], f"{defect}: injected {gt[defect]}, caught {caught}"


def test_cosmetic_defects_are_normalised_not_rejected(spark, built):
    s = spark.table("silver.trips")
    assert {r[0] for r in s.select("rider_type").distinct().collect()} == {"member", "casual"}
    assert {r[0] for r in s.select("rideable_type").distinct().collect()} == {"classic", "electric"}
    reasons = {r[0] for r in spark.table("silver.quarantine_trips").select(F.explode("dq_errors")).distinct().collect()}
    assert "started_at_parseable" not in reasons      # dd/MM/yyyy timestamps were parsed, not rejected


def test_no_bad_row_leaks_into_silver_or_gold(spark, built):
    s = spark.table("silver.trips")
    assert s.where("trip_id IS NULL OR ended_at < started_at OR fare_aud < 0 OR duration_sec > 86400 "
                   "OR started_at > current_timestamp() OR rider_type NOT IN ('member','casual')").count() == 0
    assert s.count() == s.select("trip_id").distinct().count(), "duplicates survived dedupe"
    q = spark.table("silver.quarantine_trips")
    assert q.where("size(dq_errors) = 0").count() == 0 and q.where("_quarantined_batch_id IS NULL").count() == 0


def test_row_conservation_bronze_to_silver(spark, built):
    bronze = spark.table("bronze.trips").count()
    silver, quarantined = spark.table("silver.trips").count(), spark.table("silver.quarantine_trips").count()
    dups = bronze - silver - quarantined
    assert dups > 0 and dups / bronze < 0.02          # only exact duplicates / replays account for the difference


def test_bronze_has_audit_columns_and_absorbed_schema_drift(spark, built):
    b = spark.table("bronze.trips")
    for c in ["_batch_id", "_source_system", "_source_file", "_ingested_at", "_ingest_date"]:
        assert c in b.columns and b.where(F.col(c).isNull()).count() == 0
    assert "battery_pct" in b.columns                                  # appeared mid-stream (day 46)
    assert b.where("battery_pct IS NOT NULL").count() > 0 and b.where("battery_pct IS NULL").count() > 0
    assert b.select("_batch_id").distinct().count() == 4


def test_scd2_station_history(spark, built):
    d = spark.table("gold.dim_station")
    cap = lambda sid: [r.capacity for r in d.where(f"station_id='{sid}'").orderBy("_valid_from").collect()]
    assert cap("S001") == [30, 40, 44]
    assert [r.station_status for r in d.where("station_id='S004'").orderBy("_valid_from").collect()] == ["active", "maintenance", "active"]
    assert d.where("station_id='S033' AND _is_current").first().station_status == "decommissioned"
    assert d.where("station_id='S041'").count() == 1
    assert d.where("station_id='S012' AND _is_current").first().station_name == "Birrarung Marr Riverbank"
    assert scd2_violations(spark, "gold.dim_station", "station_id") == {"multi_current": 0, "no_current": 0, "overlap_or_gap": 0}


def test_fact_uses_the_station_version_valid_at_trip_time(spark, built):
    f, d = spark.table("gold.fact_trip"), spark.table("gold.dim_station")
    j = f.where("start_station_sk != -1").join(d, f.start_station_sk == d.station_sk)
    assert j.where("NOT (started_at >= _valid_from AND started_at < _valid_to)").count() == 0
    def cap_on(day):
        return {r.capacity for r in j.where(f"station_id='S001' AND to_date(started_at)='{day}'").select("capacity").distinct().collect()}
    assert cap_on("2026-08-05") == {30} and cap_on("2026-09-01") == {40} and cap_on("2026-10-20") == {44}


def test_gold_reconciliation_checks_passed(spark, built):
    bad = spark.table("audit.dq_results").where("layer='gold' AND status='FAIL'").count()
    assert bad == 0
    assert spark.table("gold.fact_trip").count() == spark.table("silver.trips").count()


def test_events_link_only_to_trips_near_event_venues(spark, built):
    f = spark.table("gold.fact_trip")
    n_event, n = f.where("event_sk != -1").count(), f.count()
    assert 0 < n_event < 0.2 * n
    k = spark.table("gold.mart_daily_kpis")
    assert k.agg(F.sum("event_trips")).first()[0] == n_event
    assert k.agg(F.sum("trips")).first()[0] == n


def test_weather_has_expected_effect_on_demand(spark, built):
    h = spark.table("gold.mart_hourly_demand").where("start_hour BETWEEN 8 AND 18")
    rain = h.where("is_raining").agg(F.avg("trips")).first()[0]
    dry = h.where("NOT is_raining").agg(F.avg("trips")).first()[0]
    assert rain < 0.8 * dry


def test_rerun_is_idempotent_and_watermarks_advanced(spark, built, root):
    tables = ["bronze.trips", "silver.trips", "silver.quarantine_trips", "silver.station_versions", "gold.fact_trip",
              "gold.dim_station", "gold.mart_daily_kpis", "gold.mart_station_day"]
    before = {t: spark.table(t).count() for t in tables}
    run_incremental(spark, root, [89])                                  # same as_of again, brand new batch id
    after = {t: spark.table(t).count() for t in tables}
    assert after == before
    assert get_watermark(spark, "weather.last_date") == "2026-10-29"
    assert get_watermark(spark, "stations.modified_at") == "2026-10-15 03:00:00"
