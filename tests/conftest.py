import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ["TZ"] = "UTC"            # Spark session tz is UTC; keep Python datetimes consistent with it
time.tzset()
os.environ.setdefault("BIKESHARE_FORMAT", "parquet")   # CI / your laptop: set BIKESHARE_FORMAT=delta


@pytest.fixture(scope="session")
def root(tmp_path_factory):
    return os.environ.get("BIKESHARE_TEST_ROOT") or str(tmp_path_factory.mktemp("lakehouse"))


@pytest.fixture(scope="session")
def spark(root):
    from bikeshare.spark_session import get_local_spark
    s = get_local_spark(root, "bikeshare-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def built(spark, root):
    """Run the full 4-batch incremental load once (or reuse BIKESHARE_TEST_ROOT if it is already built)."""
    from bikeshare.control import ensure_control_plane
    ensure_control_plane(spark)
    if spark.catalog.tableExists("gold.fact_trip"):
        return root
    from bikeshare.pipeline import run_incremental
    run_incremental(spark, root, [4, 34, 64, 89])
    return root


@pytest.fixture()
def cfg(root):
    from bikeshare.config import make_config
    return make_config(env="local", root=root)
