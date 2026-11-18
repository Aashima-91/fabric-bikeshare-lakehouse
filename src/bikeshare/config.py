"""Runtime configuration shared by the Fabric notebooks and the local runner.

The same code runs in two environments:

* ``fabric`` - default lakehouse mounted at /lakehouse/default, Spark paths are relative ("Files/...")
* ``local``  - a folder on disk, Spark paths are file:// URIs (used for development, tests and CI)
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

SCHEMAS = ("bronze", "silver", "gold", "audit")


@dataclass(frozen=True)
class Config:
    env: str                 # "fabric" | "local"
    files_posix: str         # path visible to plain Python (open, pandas, sqlite3)
    files_spark: str         # path visible to Spark readers/writers
    batch_id: str            # one id per pipeline run (use @pipeline().RunId from Data Factory)
    run_ts: datetime
    weather_live: bool = False   # True -> call the real Open-Meteo archive API
    lat: float = -37.8136
    lon: float = 144.9631
    tz: str = "Australia/Melbourne"
    weather_lookback_days: int = 1   # re-pull the last N days to pick up provider revisions

    # ---- path helpers --------------------------------------------------------------------
    def landing_posix(self, source: str) -> str:
        return os.path.join(self.files_posix, "landing", source)

    def landing_spark(self, source: str) -> str:
        return f"{self.files_spark}/landing/{source}"

    @property
    def source_world(self) -> str:
        """Where the simulated upstream systems live (stand-in for SFTP, Azure SQL, ...)."""
        return os.path.join(self.files_posix, "source_world")


def make_config(env: str | None = None, root: str | None = None, batch_id: str | None = None,
                weather_live: bool = False) -> Config:
    env = env or os.environ.get("BIKESHARE_ENV") or ("fabric" if os.path.isdir("/lakehouse/default/Files") else "local")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    batch_id = batch_id or f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    if env == "fabric":
        return Config("fabric", "/lakehouse/default/Files", "Files", batch_id, now, weather_live)
    root = os.path.abspath(root or os.environ.get("BIKESHARE_ROOT", "./lakehouse_local"))
    files = os.path.join(root, "Files")
    os.makedirs(files, exist_ok=True)
    return Config("local", files, "file://" + files, batch_id, now, weather_live)
