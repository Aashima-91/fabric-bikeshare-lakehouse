"""Control-plane tables: schemas, watermarks (incremental extraction) and the file ledger (idempotent file ingestion)."""
from __future__ import annotations

from typing import Optional

from pyspark.sql import functions as F

from . import lakehouse
from .config import SCHEMAS, Config

_DDL = [
    """CREATE TABLE IF NOT EXISTS audit.watermarks (
           name STRING, value STRING, batch_id STRING, updated_at TIMESTAMP) USING {fmt}""",
    """CREATE TABLE IF NOT EXISTS audit.file_ledger (
           source STRING, file_name STRING, batch_id STRING, registered_at TIMESTAMP) USING {fmt}""",
    """CREATE TABLE IF NOT EXISTS audit.pipeline_run_log (
           batch_id STRING, layer STRING, step STRING, status STRING, rows_read BIGINT, rows_written BIGINT,
           rows_rejected BIGINT, started_at TIMESTAMP, ended_at TIMESTAMP, duration_sec DOUBLE, message STRING) USING {fmt}""",
    """CREATE TABLE IF NOT EXISTS audit.dq_results (
           batch_id STRING, layer STRING, table_name STRING, rule_name STRING, severity STRING, check_type STRING,
           rows_checked BIGINT, rows_failed BIGINT, pass_rate DOUBLE, status STRING, checked_at TIMESTAMP) USING {fmt}""",
]


def ensure_control_plane(spark) -> None:
    for s in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
    for ddl in _DDL:
        spark.sql(ddl.replace("{fmt}", lakehouse.table_ddl_using()))


# ---------------------------------------------------------------- watermarks
def get_watermark(spark, name: str) -> Optional[str]:
    rows = spark.table("audit.watermarks").where(F.col("name") == name).select("value").collect()
    return rows[0]["value"] if rows else None


def set_watermark(spark, cfg: Config, name: str, value) -> None:
    src = (spark.createDataFrame([(name, str(value), cfg.batch_id)], "name string, value string, batch_id string")
           .withColumn("updated_at", F.current_timestamp()))
    lakehouse.upsert(spark, src, "audit.watermarks", ["name"])


# ---------------------------------------------------------------- file ledger
def unregistered_files(spark, source: str, file_names: list[str]) -> list[str]:
    seen = {r["file_name"] for r in spark.table("audit.file_ledger").where(F.col("source") == source).select("file_name").collect()}
    return [f for f in file_names if f not in seen]


def register_files(spark, cfg: Config, source: str, file_names: list[str]) -> None:
    if not file_names:
        return
    df = (spark.createDataFrame([(source, f, cfg.batch_id) for f in file_names], "source string, file_name string, batch_id string")
          .withColumn("registered_at", F.current_timestamp()))
    lakehouse.append(spark, df, "audit.file_ledger")
