"""End-to-end orchestration of one batch (the Data Factory pipeline calls the same steps as separate activities)."""
from __future__ import annotations

from datetime import date

from . import bronze, control, gold, landing, silver
from .audit import RunLogger
from .config import Config
from .sources.world import BikeShareWorld


def run_landing(spark, cfg: Config, log: RunLogger, world: BikeShareWorld, as_of: date) -> None:
    with log.step("landing", "trip_files (file drop -> Files)") as st:
        st.rows_written = len(landing.land_trip_files(cfg, world))
    with log.step("landing", "stations (SQL DB -> Parquet)") as st:
        r = landing.extract_stations(spark, cfg, world)
        st.rows_written = r["rows"]
    with log.step("landing", "weather (REST API -> JSON)") as st:
        r = landing.land_weather(spark, cfg, world, as_of)
        st.rows_written = r["rows"]
        st.message = "live Open-Meteo" if cfg.weather_live else "mock API"
    with log.step("landing", "events (Excel -> Files)") as st:
        st.rows_written = len(landing.land_events(cfg, world))


def run_batch(spark, cfg: Config, world: BikeShareWorld, as_of: date) -> list[str]:
    control.ensure_control_plane(spark)
    log = RunLogger(spark, cfg)
    print(f"\n=== batch {cfg.batch_id}  as_of={as_of} ===")
    run_landing(spark, cfg, log, world, as_of)
    bronze.run(spark, cfg, log)
    silver.run(spark, cfg, log)
    failed = gold.run(spark, cfg, log)
    if failed:
        raise RuntimeError(f"critical data-quality checks failed: {failed}")
    return failed


def run_incremental(spark, root: str, day_offsets: list[int], weather_live: bool = False) -> None:
    """Simulate days passing: advance the source world, then run one pipeline batch per offset."""
    from datetime import timedelta

    from .config import make_config

    for off in day_offsets:
        cfg = make_config(env="local", root=root, weather_live=weather_live)
        world = BikeShareWorld(cfg.source_world)
        as_of = world.start + timedelta(days=off)
        world.advance(as_of)
        run_batch(spark, cfg, world, as_of)
