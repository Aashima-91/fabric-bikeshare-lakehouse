"""Run the whole medallion pipeline locally in incremental batches (simulating days passing).

    BIKESHARE_FORMAT=delta   python run_local.py      # Delta Lake (needs internet once, for the Delta jars)
    BIKESHARE_FORMAT=parquet python run_local.py      # offline test double

Default batches end on day offsets 4, 34, 64, 89 so every station change lands in a different batch.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from bikeshare.pipeline import run_incremental  # noqa: E402
from bikeshare.spark_session import get_local_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="./lakehouse_local")
    ap.add_argument("--reset", action="store_true", help="wipe the local lakehouse first")
    ap.add_argument("--batch-ends", default="4,34,64,89", help="comma-separated day offsets (0 = first day)")
    ap.add_argument("--live-weather", action="store_true", help="call the real Open-Meteo API instead of the mock")
    a = ap.parse_args()
    if a.reset and os.path.isdir(a.root):
        shutil.rmtree(a.root)
    spark = get_local_spark(a.root)
    run_incremental(spark, a.root, [int(x) for x in a.batch_ends.split(",")], a.live_weather)
    print("\nDone. Tables are in", os.path.join(a.root, "warehouse"))


if __name__ == "__main__":
    main()
