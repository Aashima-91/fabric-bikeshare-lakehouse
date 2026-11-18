"""Slowly Changing Dimension Type 2 on Delta Lake, using a single atomic MERGE.

Design
------
* Input is a *change feed*: one row per (business key, effective timestamp). It may contain several
  versions of the same key in one batch - they are chained with LEAD() so history is never lost.
* Versions identical to the previous one (by hash of tracked columns) are dropped, so re-running the same
  input is a no-op (idempotent).
* Changes older than the key's current version are treated as late-arriving and ignored (counted + returned).
* Surrogate key = xxhash64(business_key, valid_from): deterministic, so a rebuild yields the same keys.
* Classic staged-merge trick: each first-change row appears twice - once with the real key (to expire the
  current row) and once with a NULL merge key (so it is inserted as the new current row).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from . import lakehouse
from .audit import row_hash

HIGH_DATE = "9999-12-31 00:00:00"
SCD_COLS = ["_valid_from", "_valid_to", "_is_current", "_row_hash"]


def apply_scd2(spark, source: DataFrame, target: str, key: str, tracked: list[str], effective: str,
               sk_name: str, first_valid_from: str | None = None) -> dict:
    """Upsert `source` into SCD2 table `target`. Returns counters for the run log."""
    high = F.lit(HIGH_DATE).cast("timestamp")
    src = (source.withColumn("_row_hash", row_hash(tracked))
           .dropDuplicates([key, effective]))
    exists = spark.catalog.tableExists(target)

    if exists:
        cur = spark.table(target).where("_is_current").select(
            key, F.col("_row_hash").alias("_cur_hash"), F.col("_valid_from").alias("_cur_from"))
        stale = src.join(cur, key).where(F.col(effective) <= F.col("_cur_from")).count()
        src = (src.join(cur, key, "left")
               .where(F.col("_cur_from").isNull() | (F.col(effective) > F.col("_cur_from")))
               .drop("_cur_from", "_cur_hash"))
        seed = cur.select(key, F.col("_cur_hash").alias("_row_hash"), F.col("_cur_from").alias(effective),
                          F.lit(0).alias("_ord"))
        has_cur = cur.select(key, F.lit(True).alias("_has_current"))
    else:
        stale, seed, has_cur = 0, None, None

    batch_rows = src.select(key, "_row_hash", effective).withColumn("_ord", F.lit(1))
    combined = batch_rows if seed is None else seed.unionByName(batch_rows)
    w_prev = Window.partitionBy(key).orderBy(F.col(effective), F.col("_ord"))
    flagged = (combined.withColumn("_prev_hash", F.lag("_row_hash").over(w_prev))
               .where((F.col("_ord") == 1) & (F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash"))))
               .select(key, effective))

    changed = src.join(flagged, [key, effective], "inner")
    w_chain = Window.partitionBy(key).orderBy(effective)
    changed = (changed.withColumn("_next_eff", F.lead(effective).over(w_chain))
               .withColumn("_rn", F.row_number().over(w_chain)))
    if has_cur is not None:
        changed = changed.join(has_cur, key, "left")
    else:
        changed = changed.withColumn("_has_current", F.lit(None).cast("boolean"))

    vf = F.col(effective)
    if first_valid_from:
        vf = F.when((F.col("_rn") == 1) & F.col("_has_current").isNull(), F.least(F.col(first_valid_from).cast("timestamp"), F.col(effective))
                    ).otherwise(F.col(effective))
    changed = (changed.withColumn("_valid_from", vf)
               .withColumn("_valid_to", F.coalesce(F.col("_next_eff"), high))
               .withColumn("_is_current", F.col("_next_eff").isNull())
               .withColumn(sk_name, F.xxhash64(F.col(key), F.col("_valid_from"))))

    attr_cols = [c for c in source.columns]
    dim_cols = [sk_name] + attr_cols + SCD_COLS
    new_versions = changed.select(*dim_cols, "_rn", "_has_current").persist()
    n_new = new_versions.count()

    if not exists:
        lakehouse.overwrite(spark, new_versions.select(*dim_cols), target)
        new_versions.unpersist()
        return {"versions_inserted": n_new, "versions_expired": 0, "late_ignored": 0}

    expire = (new_versions.where((F.col("_rn") == 1) & F.col("_has_current").isNotNull())
              .select(*dim_cols).withColumn("_merge_key", F.col(key)))
    n_expire = expire.count()
    insert = new_versions.select(*dim_cols).withColumn("_merge_key", F.lit(None).cast(src.schema[key].dataType))
    staged = expire.unionByName(insert)

    lakehouse.scd2_merge(spark, staged, target, key, dim_cols)
    new_versions.unpersist()
    return {"versions_inserted": n_new, "versions_expired": n_expire, "late_ignored": stale}


def scd2_violations(spark, table: str, key: str) -> dict:
    """Integrity checks: exactly one current row per key, no overlaps, no gaps."""
    df = spark.table(table).where(f"{key} IS NOT NULL AND _valid_from > '1900-01-01'")
    multi_current = (df.where("_is_current").groupBy(key).count().where("count > 1").count())
    no_current = (df.groupBy(key).agg(F.sum(F.col("_is_current").cast("int")).alias("c")).where("c = 0").count())
    w = Window.partitionBy(key).orderBy("_valid_from")
    nxt = df.withColumn("_next_from", F.lead("_valid_from").over(w)).where("_next_from IS NOT NULL")
    overlaps_or_gaps = nxt.where(F.col("_valid_to") != F.col("_next_from")).count()
    return {"multi_current": multi_current, "no_current": no_current, "overlap_or_gap": overlaps_or_gaps}
