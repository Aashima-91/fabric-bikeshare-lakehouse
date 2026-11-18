"""Local Spark + Delta session. In Fabric the `spark` object already exists - never call this there."""
from __future__ import annotations

import os


def get_local_spark(root: str, app: str = "bikeshare-local"):
    from pyspark.sql import SparkSession

    from .lakehouse import is_delta

    root = os.path.abspath(root)
    os.makedirs(root, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app)
        .master("local[2]")
        .config("spark.sql.warehouse.dir", os.path.join(root, "warehouse"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={os.path.join(root, 'metastore_db')};create=true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "3g")
        # Fabric Runtime 1.3 (Spark 3.5) defaults: ANSI off, UTC session time zone
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .enableHiveSupport()
    )
    if is_delta():
        from delta import configure_spark_with_delta_pip
        builder = (builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:  # Parquet test double (see lakehouse.py)
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
