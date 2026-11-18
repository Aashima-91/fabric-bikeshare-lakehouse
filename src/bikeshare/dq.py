"""Declarative data-quality rules.

* A rule is a SQL boolean expression that must be TRUE for a row to be valid (NULL counts as a failure).
* severity="error"  -> row is quarantined (never reaches the next layer)
* severity="warn"   -> row is kept, but the rule name is recorded in `dq_warnings`
* every rule's pass/fail counts are written to audit.dq_results, so DQ is itself reportable in Power BI
* a circuit breaker aborts the run if too many rows are rejected (guards against a broken upstream file)
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from . import lakehouse
from .config import Config


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Rule:
    name: str
    expr: str
    severity: str = "error"   # "error" | "warn"
    description: str = ""


def _passes(rule: Rule):
    return F.coalesce(F.expr(rule.expr), F.lit(False))


def _flag_array(df: DataFrame, rules: list[Rule]):
    if not rules:
        return F.array().cast("array<string>")
    return F.filter(F.array(*[F.when(~_passes(r), F.lit(r.name)) for r in rules]), lambda x: x.isNotNull())


def write_dq_rows(spark, cfg: Config, rows: list[tuple]) -> None:
    if rows:
        schema = ("batch_id string, layer string, table_name string, rule_name string, severity string, check_type string, "
                  "rows_checked bigint, rows_failed bigint, pass_rate double, status string")
        df = spark.createDataFrame(rows, schema).withColumn("checked_at", F.current_timestamp())
        lakehouse.append(spark, df, "audit.dq_results")


def log_check(spark, cfg: Config, layer: str, table: str, name: str, checked: int, failed: int,
              severity: str = "error", check_type: str = "reconciliation") -> bool:
    """Record a non-row-level check (reconciliation, referential integrity, SCD2 integrity...). Returns True if passed."""
    passed = failed == 0
    status = "PASS" if passed else ("FAIL" if severity == "error" else "WARN")
    write_dq_rows(spark, cfg, [(cfg.batch_id, layer, table, name, severity, check_type, int(checked), int(failed),
                                1.0 if checked == 0 else round(1 - failed / checked, 6), status)])
    return passed


def apply_rules(spark, cfg: Config, df: DataFrame, rules: list[Rule], table: str, layer: str = "silver",
                max_reject_ratio: float = 0.05):
    """Return (valid_df, rejected_df, total_rows). `valid_df` has `dq_warnings`; `rejected_df` has `dq_errors`."""
    errs = [r for r in rules if r.severity == "error"]
    warns = [r for r in rules if r.severity == "warn"]
    flagged = (df.withColumn("dq_errors", _flag_array(df, errs)).withColumn("dq_warnings", _flag_array(df, warns)))

    aggs = [F.count(F.lit(1)).alias("n")] + [
        F.sum(F.when(~_passes(r), 1).otherwise(0)).alias(f"f{i}") for i, r in enumerate(rules)
    ] + [F.sum(F.when(F.size("dq_errors") > 0, 1).otherwise(0)).alias("rejected")]
    res = flagged.agg(*aggs).first()
    total, rejected = int(res["n"]), int(res["rejected"] or 0)

    rows = []
    for i, r in enumerate(rules):
        failed = int(res[f"f{i}"] or 0)
        status = "PASS" if failed == 0 else ("FAIL" if r.severity == "error" else "WARN")
        rows.append((cfg.batch_id, layer, table, r.name, r.severity, "row_rule", total, failed,
                     1.0 if total == 0 else round(1 - failed / total, 6), status))
    write_dq_rows(spark, cfg, rows)

    if total and rejected / total > max_reject_ratio:
        raise DataQualityError(f"{table}: {rejected}/{total} rows rejected ({rejected / total:.1%}) "
                               f"exceeds circuit-breaker threshold {max_reject_ratio:.0%}")

    valid = flagged.where(F.size("dq_errors") == 0).drop("dq_errors")
    bad = flagged.where(F.size("dq_errors") > 0).drop("dq_warnings")
    return valid, bad, total
