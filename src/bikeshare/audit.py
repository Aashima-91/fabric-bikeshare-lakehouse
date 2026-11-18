"""Audit columns (one standard per layer) and the pipeline run logger.

Bronze  : _batch_id, _source_system, _source_file, _ingested_at, _ingest_date
Silver  : inherits _source_file/_ingested_at (lineage) + _batch_id, _processed_at, _row_hash
Gold    : _batch_id, _loaded_at   (+ _valid_from/_valid_to/_is_current/_row_hash on SCD2 dimensions)
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, functions as F

from . import lakehouse
from .config import Config

_SEP = "\x1f"


def row_hash(cols: list[str]) -> Column:
    """Deterministic SHA-256 over business columns - used for change detection and idempotent merges."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\x00")) for c in cols]
    return F.sha2(F.concat_ws(_SEP, *parts), 256)


def add_bronze_audit(df: DataFrame, cfg: Config, source_system: str, source_file: Column) -> DataFrame:
    return (df.withColumn("_batch_id", F.lit(cfg.batch_id))
              .withColumn("_source_system", F.lit(source_system))
              .withColumn("_source_file", source_file)
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_ingest_date", F.current_date()))


def add_silver_audit(df: DataFrame, cfg: Config, hash_cols: list[str]) -> DataFrame:
    return (df.withColumn("_row_hash", row_hash(hash_cols))
              .withColumn("_batch_id", F.lit(cfg.batch_id))
              .withColumn("_processed_at", F.current_timestamp()))


def add_gold_audit(df: DataFrame, cfg: Config) -> DataFrame:
    return df.withColumn("_batch_id", F.lit(cfg.batch_id)).withColumn("_loaded_at", F.current_timestamp())


class StepContext:
    """Mutable counters a pipeline step fills in; flushed to audit.pipeline_run_log on exit."""

    def __init__(self):
        self.rows_read = 0
        self.rows_written = 0
        self.rows_rejected = 0
        self.message = ""


class RunLogger:
    SCHEMA = ("batch_id string, layer string, step string, status string, rows_read bigint, rows_written bigint, "
              "rows_rejected bigint, started_at timestamp, ended_at timestamp, duration_sec double, message string")

    def __init__(self, spark, cfg: Config):
        self.spark, self.cfg = spark, cfg

    @contextmanager
    def step(self, layer: str, name: str):
        ctx, t0, started = StepContext(), time.time(), datetime.now(timezone.utc).replace(tzinfo=None)
        status = "SUCCESS"
        try:
            yield ctx
        except Exception as exc:  # noqa: BLE001 - we log then re-raise
            status, ctx.message = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
            raise
        finally:
            ended = datetime.now(timezone.utc).replace(tzinfo=None)
            row = (self.cfg.batch_id, layer, name, status, int(ctx.rows_read), int(ctx.rows_written),
                   int(ctx.rows_rejected), started, ended, round(time.time() - t0, 2), ctx.message)
            lakehouse.append(self.spark, self.spark.createDataFrame([row], self.SCHEMA), "audit.pipeline_run_log")
            print(f"  [{layer:<7}] {name:<28} {status:<8} read={ctx.rows_read:<7} written={ctx.rows_written:<7} "
                  f"rejected={ctx.rows_rejected:<5} {time.time() - t0:5.1f}s")
