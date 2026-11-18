"""Table I/O layer - the ONLY module that knows how tables are physically stored.

* ``BIKESHARE_FORMAT=delta`` (default) - Delta Lake tables. This is what runs in Microsoft Fabric.
* ``BIKESHARE_FORMAT=parquet``         - a Parquet-backed test double with the same semantics, used where the
  Delta jars cannot be downloaded (offline sandboxes). It lets the whole pipeline logic be exercised anywhere.

Everything else in the project calls ``append / overwrite / upsert / scd2_merge`` and never touches
``DeltaTable`` directly, so the medallion logic is identical in both modes.
"""
from __future__ import annotations

import os
from functools import reduce

from pyspark.sql import DataFrame, functions as F


def fmt() -> str:
    return os.environ.get("BIKESHARE_FORMAT", "delta").lower()


def is_delta() -> bool:
    return fmt() == "delta"


def exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


# =============================================================================================
# Parquet test double
# =============================================================================================
def _pq_replace(spark, df: DataFrame, table: str) -> None:
    """Atomic-enough swap: materialise to a staging table first (df may be derived from `table`)."""
    stg = f"{table}__stg"
    spark.sql(f"DROP TABLE IF EXISTS {stg}")
    df.write.format("parquet").mode("overwrite").saveAsTable(stg)
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"ALTER TABLE {stg} RENAME TO {table}")


def _pq_append(spark, df: DataFrame, table: str) -> None:
    if not exists(spark, table):
        df.write.format("parquet").mode("overwrite").saveAsTable(table)
        return
    cur = spark.table(table)
    if set(df.columns) == set(cur.columns):
        df.select(*cur.columns).write.mode("append").insertInto(table)
    else:  # schema evolution
        _pq_replace(spark, cur.unionByName(df, allowMissingColumns=True), table)


def _pq_overwrite(spark, df: DataFrame, table: str, replace_where: str | None) -> None:
    if replace_where and exists(spark, table):
        keep = spark.table(table).where(f"NOT ({replace_where})")
        _pq_replace(spark, keep.unionByName(df, allowMissingColumns=True), table)
    else:
        _pq_replace(spark, df, table) if exists(spark, table) else df.write.format("parquet").mode("overwrite").saveAsTable(table)


def _pq_upsert(spark, df: DataFrame, table: str, keys: list[str]) -> None:
    if not exists(spark, table):
        df.write.format("parquet").mode("overwrite").saveAsTable(table)
        return
    cur = spark.table(table)
    kdf = df.select(*[F.col(k).alias(f"_k_{k}") for k in keys]).distinct()
    cond = reduce(lambda a, b: a & b, [cur[k] == kdf[f"_k_{k}"] for k in keys])
    survivors = cur.join(kdf, cond, "left_anti")
    _pq_replace(spark, survivors.unionByName(df, allowMissingColumns=True), table)


def _pq_scd2_merge(spark, staged: DataFrame, table: str, key: str, dim_cols: list[str]) -> None:
    cur = spark.table(table)
    exp = (staged.where(F.col("_merge_key").isNotNull())
           .select(F.col("_merge_key").alias("_k"), F.col("_valid_from").alias("_new_to")))
    joined = cur.join(exp, (cur[key] == exp["_k"]) & cur["_is_current"], "left")
    closed = (joined.withColumn("_valid_to", F.coalesce(F.col("_new_to"), F.col("_valid_to")))
              .withColumn("_is_current", F.when(F.col("_new_to").isNotNull(), F.lit(False)).otherwise(F.col("_is_current")))
              .select(*cur.columns))
    _pq_replace(spark, closed.unionByName(staged.where(F.col("_merge_key").isNull()).select(*dim_cols)), table)


# =============================================================================================
# Delta Lake implementation (Fabric)
# =============================================================================================
def _delta_table(spark, table: str):
    from delta.tables import DeltaTable
    return DeltaTable.forName(spark, table)


def _delta_overwrite(df: DataFrame, table: str, replace_where: str | None) -> None:
    w = df.write.format("delta").mode("overwrite")
    if replace_where:
        w = w.option("replaceWhere", replace_where)
    else:
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(table)


def _delta_upsert(spark, df: DataFrame, table: str, keys: list[str]) -> None:
    if not exists(spark, table):
        df.write.format("delta").mode("overwrite").saveAsTable(table)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (_delta_table(spark, table).alias("t").merge(df.alias("s"), cond)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def _delta_scd2_merge(spark, staged: DataFrame, table: str, key: str, dim_cols: list[str]) -> None:
    (_delta_table(spark, table).alias("t")
     .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t._is_current = true")
     .whenMatchedUpdate(set={"_is_current": F.lit(False), "_valid_to": F.col("s._valid_from")})
     .whenNotMatchedInsert(values={c: F.col(f"s.{c}") for c in dim_cols})
     .execute())


# =============================================================================================
# Public API
# =============================================================================================
def append(spark, df: DataFrame, table: str) -> None:
    if is_delta():
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    else:
        _pq_append(spark, df, table)


def overwrite(spark, df: DataFrame, table: str, replace_where: str | None = None) -> None:
    """Full overwrite, or partial overwrite of the slice matching `replace_where` (Delta `replaceWhere`)."""
    if replace_where and not exists(spark, table):
        replace_where = None
    _delta_overwrite(df, table, replace_where) if is_delta() else _pq_overwrite(spark, df, table, replace_where)


def upsert(spark, df: DataFrame, table: str, keys: list[str]) -> None:
    """MERGE: update matching rows, insert new ones (callers pre-filter unchanged rows by _row_hash)."""
    _delta_upsert(spark, df, table, keys) if is_delta() else _pq_upsert(spark, df, table, keys)


def scd2_merge(spark, staged: DataFrame, table: str, key: str, dim_cols: list[str]) -> None:
    _delta_scd2_merge(spark, staged, table, key, dim_cols) if is_delta() else _pq_scd2_merge(spark, staged, table, key, dim_cols)


def only_new_or_changed(spark, df: DataFrame, table: str, keys: list[str], hash_col: str = "_row_hash") -> DataFrame:
    """Drop incoming rows that already exist unchanged in `table` (same key + same hash) -> no-op rewrites avoided."""
    if not exists(spark, table):
        return df
    cur = spark.table(table).select(*[F.col(k).alias(f"_c_{k}") for k in keys], F.col(hash_col).alias("_c_hash"))
    cond = reduce(lambda a, b: a & b, [df[k] == cur[f"_c_{k}"] for k in keys]) & (df[hash_col] == cur["_c_hash"])
    return df.join(cur, cond, "left_anti")


def table_ddl_using() -> str:
    return "DELTA" if is_delta() else "PARQUET"


def maintain(spark, tables: list[str], retain_hours: int = 168) -> None:
    """Delta housekeeping (compaction + old-file cleanup). No-op for the Parquet double."""
    if not is_delta():
        return
    for t in tables:
        spark.sql(f"OPTIMIZE {t}")
        spark.sql(f"VACUUM {t} RETAIN {retain_hours} HOURS")
