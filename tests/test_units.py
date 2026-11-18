"""Fast unit tests of the reusable building blocks (SCD2 engine, DQ engine, transforms)."""
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from bikeshare import control
from bikeshare.dq import DataQualityError, Rule, apply_rules
from bikeshare.scd2 import apply_scd2, scd2_violations
from bikeshare.silver import transform_trips

SCHEMA = "station_id string, name string, capacity int, installed_date date, modified_at timestamp"
D = lambda *a: datetime(*a)


@pytest.fixture()
def dim(spark, cfg):
    control.ensure_control_plane(spark)
    t = "gold.t_dim_unit"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    yield t
    spark.sql(f"DROP TABLE IF EXISTS {t}")


def _apply(spark, dim, rows):
    return apply_scd2(spark, spark.createDataFrame(rows, SCHEMA), dim, "station_id", ["name", "capacity"], "modified_at",
                      "sk", first_valid_from="installed_date")


def _versions(spark, dim, key="A"):
    return [(r.capacity, r._valid_from, r._valid_to, r._is_current) for r in
            spark.table(dim).where(f"station_id='{key}'").orderBy("_valid_from").collect()]


def test_scd2_initial_load_starts_at_install_date(spark, dim):
    _apply(spark, dim, [("A", "x", 10, date(2025, 1, 1), D(2026, 8, 1))])
    (cap, vf, vt, cur), = _versions(spark, dim)
    assert (cap, cur) == (10, True) and vf == D(2025, 1, 1) and vt.year == 9999


def test_scd2_multiple_versions_in_one_batch_are_chained(spark, dim):
    _apply(spark, dim, [("A", "x", 10, date(2025, 1, 1), D(2026, 8, 1))])
    stats = _apply(spark, dim, [("A", "x", 20, date(2025, 1, 1), D(2026, 8, 5)), ("A", "x", 30, date(2025, 1, 1), D(2026, 8, 9))])
    v = _versions(spark, dim)
    assert [x[0] for x in v] == [10, 20, 30]
    assert v[0][2] == D(2026, 8, 5) and v[1][1] == D(2026, 8, 5) and v[1][2] == D(2026, 8, 9)
    assert [x[3] for x in v] == [False, False, True]
    assert stats["versions_inserted"] == 2 and stats["versions_expired"] == 1
    assert scd2_violations(spark, dim, "station_id") == {"multi_current": 0, "no_current": 0, "overlap_or_gap": 0}


def test_scd2_is_idempotent_and_ignores_unchanged_and_late_rows(spark, dim):
    _apply(spark, dim, [("A", "x", 10, date(2025, 1, 1), D(2026, 8, 1))])
    _apply(spark, dim, [("A", "x", 20, date(2025, 1, 1), D(2026, 8, 5))])
    n = spark.table(dim).count()
    _apply(spark, dim, [("A", "x", 20, date(2025, 1, 1), D(2026, 8, 5))])                 # exact replay
    _apply(spark, dim, [("A", "x", 20, date(2025, 1, 1), D(2026, 8, 7))])                 # newer but nothing changed
    stats = _apply(spark, dim, [("A", "x", 99, date(2025, 1, 1), D(2026, 7, 1))])         # late-arriving, older than current
    assert spark.table(dim).count() == n
    assert stats["late_ignored"] == 1


def test_scd2_new_key_in_later_batch(spark, dim):
    _apply(spark, dim, [("A", "x", 10, date(2025, 1, 1), D(2026, 8, 1))])
    _apply(spark, dim, [("B", "y", 5, date(2026, 8, 10), D(2026, 8, 10, 3))])
    assert _versions(spark, dim, "B")[0][1] == D(2026, 8, 10)     # least(installed_date, modified_at)


def test_dq_engine_splits_rows_logs_rules_and_trips_circuit_breaker(spark, cfg):
    control.ensure_control_plane(spark)
    df = spark.createDataFrame([(1, 5), (2, -1), (3, None), (4, 7)], "id int, v int")
    rules = [Rule("v_not_null", "v IS NOT NULL", "error"), Rule("v_positive", "v > 0", "warn")]
    valid, bad, total = apply_rules(spark, cfg, df, rules, "unit.t", max_reject_ratio=0.5)
    assert total == 4 and valid.count() == 3 and bad.count() == 1
    assert bad.select(F.explode("dq_errors")).first()[0] == "v_not_null"
    assert valid.where("size(dq_warnings) > 0").count() == 1          # the -1 row is kept but flagged
    res = {r.rule_name: (r.rows_failed, r.status) for r in
           spark.table("audit.dq_results").where(f"batch_id='{cfg.batch_id}' AND table_name='unit.t'").collect()}
    assert res["v_not_null"] == (1, "FAIL") and res["v_positive"][1] == "WARN"
    with pytest.raises(DataQualityError):
        apply_rules(spark, cfg, df, rules, "unit.t", max_reject_ratio=0.1)


def test_trip_transform_normalises_dirty_values(spark):
    cols = "trip_id string,bike_id string,rideable_type string,started_at string,ended_at string,start_station_id string," \
           "end_station_id string,rider_type string,fare_aud string,_source_file string,_ingested_at timestamp"
    now = datetime(2026, 8, 1)
    df = spark.createDataFrame([
        (" T1 ", "B1", "Electric", "2026-08-01 08:00:00", "2026-08-01 08:10:00", "s001", "S002", " CASUAL ", "3.50", "f", now),
        ("T2", "B1", "Classic Bike", "01/08/2026 08:00", "01/08/2026 08:30", "S001", None, "Member", "0.0", "f", now),
        ("T3", "B1", "classic_bike", "garbage", "2026-08-01 08:30:00", "S001", "S002", "member", "0", "f", now)], cols)
    r = {x.trip_id: x for x in transform_trips(df).collect()}
    assert r["T1"].rider_type == "casual" and r["T1"].rideable_type == "electric" and r["T1"].start_station_id == "S001"
    assert r["T1"].duration_sec == 600 and r["T1"].start_hour == 8
    assert r["T2"].started_at == datetime(2026, 8, 1, 8, 0) and r["T2"].rideable_type == "classic"   # alt date format parsed
    assert r["T3"].started_at is None                                                              # unparseable -> NULL -> rejected by DQ
