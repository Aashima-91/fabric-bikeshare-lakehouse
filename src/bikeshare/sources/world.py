"""Source-system simulator for the fictional "YarraRide" bike-share scheme.

It plays the role of the *outside world* that a data platform has to ingest from, and it
deliberately exposes four different kinds of source:

  1. File drop      -> daily trip CSVs (with dirty rows, duplicates, replays and schema drift)
  2. Relational DB  -> SQLite `stations.db` (an OLTP table that is UPDATEd in place over time)
  3. REST API       -> Open-Meteo-shaped hourly weather JSON (mock of the real API)
  4. Spreadsheet    -> an Excel events calendar with a banner, mixed date formats, text numbers

Everything is deterministic (seeded per day) so that incremental generation gives the same data
as a one-shot generation. All data is synthetic. Station coordinates are approximate.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
import math
import os
import sqlite3
from datetime import date, datetime, timedelta

import numpy as np

SEED = 42
START_DATE = date(2026, 8, 1)
TOTAL_DAYS = 90
LAT0, LON0 = -37.8136, 144.9631
FLEET = 600
SCHEMA_DRIFT_DAY = 46  # trips files gain a `battery_pct` column from this day offset onwards

# station_id, name, suburb, zone, lat, lon, capacity
_STATIONS = [
    ("S001", "Flinders Street Station", "Melbourne", "CBD", -37.8183, 144.9671, 30),
    ("S002", "Southern Cross Station", "Docklands", "CBD", -37.8183, 144.9525, 32),
    ("S003", "Federation Square", "Melbourne", "CBD", -37.8179, 144.9690, 26),
    ("S004", "Queen Victoria Market", "Melbourne", "CBD", -37.8076, 144.9568, 24),
    ("S005", "Melbourne Central", "Melbourne", "CBD", -37.8102, 144.9628, 28),
    ("S006", "State Library Victoria", "Melbourne", "CBD", -37.8099, 144.9653, 22),
    ("S007", "Parliament Station", "East Melbourne", "CBD", -37.8110, 144.9730, 20),
    ("S008", "Flagstaff Gardens", "West Melbourne", "CBD", -37.8120, 144.9547, 18),
    ("S009", "Melbourne Museum", "Carlton", "Inner North", -37.8033, 144.9717, 20),
    ("S010", "Carlton Gardens South", "Carlton", "Inner North", -37.8050, 144.9710, 16),
    ("S011", "Fitzroy Gardens", "East Melbourne", "Inner East", -37.8130, 144.9800, 18),
    ("S012", "Birrarung Marr", "Melbourne", "CBD", -37.8197, 144.9760, 24),
    ("S013", "Arts Centre Melbourne", "Southbank", "Southbank", -37.8215, 144.9690, 24),
    ("S014", "Southbank Promenade", "Southbank", "Southbank", -37.8200, 144.9650, 26),
    ("S015", "Crown Entertainment Complex", "Southbank", "Southbank", -37.8225, 144.9580, 20),
    ("S016", "Marvel Stadium", "Docklands", "Docklands", -37.8165, 144.9475, 28),
    ("S017", "Victoria Harbour", "Docklands", "Docklands", -37.8135, 144.9440, 22),
    ("S018", "Royal Botanic Gardens", "South Yarra", "Inner South", -37.8304, 144.9796, 24),
    ("S019", "Shrine of Remembrance", "Melbourne", "Inner South", -37.8305, 144.9734, 20),
    ("S020", "Melbourne Cricket Ground", "Yarra Park", "Inner East", -37.8200, 144.9834, 28),
    ("S021", "Rod Laver Arena", "Melbourne", "Inner East", -37.8216, 144.9785, 24),
    ("S022", "Richmond Station", "Richmond", "Inner East", -37.8240, 144.9900, 22),
    ("S023", "Collingwood Yards", "Collingwood", "Inner North", -37.8040, 144.9860, 18),
    ("S024", "Brunswick Street", "Fitzroy", "Inner North", -37.7990, 144.9780, 20),
    ("S025", "Smith Street", "Collingwood", "Inner North", -37.8010, 144.9830, 18),
    ("S026", "Lygon Street", "Carlton", "Inner North", -37.7970, 144.9660, 20),
    ("S027", "University of Melbourne", "Parkville", "Inner North", -37.7963, 144.9614, 30),
    ("S028", "RMIT City Campus", "Melbourne", "CBD", -37.8080, 144.9640, 24),
    ("S029", "North Melbourne Station", "North Melbourne", "West", -37.8070, 144.9420, 20),
    ("S030", "Kensington Station", "Kensington", "West", -37.7930, 144.9300, 18),
    ("S031", "South Yarra Station", "South Yarra", "Inner South", -37.8380, 144.9930, 22),
    ("S032", "Prahran Market", "South Yarra", "Inner South", -37.8500, 144.9910, 16),
    ("S033", "St Kilda Junction", "St Kilda", "Inner South", -37.8600, 144.9810, 18),
    ("S034", "Albert Park Lake", "Albert Park", "Inner South", -37.8420, 144.9680, 22),
    ("S035", "Port Melbourne Beach", "Port Melbourne", "West", -37.8370, 144.9420, 20),
    ("S036", "Cremorne", "Cremorne", "Inner East", -37.8300, 144.9930, 16),
    ("S037", "Abbotsford Convent", "Abbotsford", "Inner East", -37.8010, 144.9990, 16),
    ("S038", "Brunswick Station", "Brunswick", "Inner North", -37.7660, 144.9610, 18),
    ("S039", "Footscray Station", "Footscray", "West", -37.8010, 144.9030, 20),
    ("S040", "Yarraville Station", "Yarraville", "West", -37.8160, 144.8890, 16),
]
_NEW_STATION = ("S041", "Docklands NewQuay", "Docklands", "Docklands", -37.8110, 144.9460, 24)
_NEW_STATION_DAY = 35

# (day_offset, station_id, {column: new value}) -> applied as in-place OLTP UPDATEs at 03:00
_CHANGES = [
    (12, "S001", {"capacity": 40}),
    (20, "S004", {"station_status": "maintenance"}),
    (33, "S012", {"station_name": "Birrarung Marr Riverbank"}),
    (40, "S018", {"capacity": 32}),
    (44, "S004", {"station_status": "active"}),
    (55, "S020", {"capacity": 36}),
    (62, "S033", {"station_status": "decommissioned"}),
    (70, "S001", {"capacity": 44}),
    (75, "S027", {"station_name": "University of Melbourne (Grattan St)", "capacity": 34}),
]

_ZONE_W = {  # cbd, residential, leisure attractiveness
    "CBD": (1.00, 0.20, 0.50),
    "Southbank": (0.70, 0.30, 0.90),
    "Docklands": (0.65, 0.30, 0.70),
    "Inner North": (0.40, 1.00, 0.40),
    "Inner East": (0.40, 0.90, 0.60),
    "Inner South": (0.30, 0.90, 0.90),
    "West": (0.20, 1.00, 0.25),
}
_LEISURE_WORDS = ("Gardens", "Beach", "Lake", "Botanic", "Birrarung", "Promenade", "Convent", "Shrine")

# name, venue (station name used for coordinates), category, start offset, end offset, attendance
_EVENTS = [
    ("Winter Film Festival", "Federation Square", "Festival", 6, 15, 42000),
    ("Arena Concert - Night 1", "Rod Laver Arena", "Concert", 13, 13, 14000),
    ("Arena Concert - Night 2", "Rod Laver Arena", "Concert", 14, 14, 14000),
    ("Night Market Series", "Queen Victoria Market", "Market", 18, 18, 9000),
    ("Spring Footy Round", "Melbourne Cricket Ground", "Sport", 20, 20, 52000),
    ("Docklands Food Fest", "Victoria Harbour", "Festival", 26, 27, 18000),
    ("Marathon Expo", "Marvel Stadium", "Expo", 31, 32, 12000),
    ("City Fun Run", "Birrarung Marr", "Community", 34, 34, 7500),
    ("Spring Footy Round", "Melbourne Cricket Ground", "Sport", 41, 41, 48000),
    ("Botanic Spring Picnic", "Royal Botanic Gardens", "Community", 43, 44, 11000),
    ("Arena Concert - Night 1", "Rod Laver Arena", "Concert", 48, 48, 15000),
    ("Finals Series - Qualifying", "Melbourne Cricket Ground", "Sport", 52, 52, 85000),
    ("Uni Open Day", "University of Melbourne", "Education", 55, 55, 8000),
    ("Finals Series - Preliminary", "Melbourne Cricket Ground", "Sport", 59, 59, 88000),
    ("Grand Final Festival", "Federation Square", "Festival", 62, 63, 36000),
    ("Wine & Cheese Weekend", "Southbank Promenade", "Festival", 69, 70, 16000),
    ("Arena Concert - Night 1", "Rod Laver Arena", "Concert", 76, 76, 14500),
    ("Spring Racing Carnival Fan Zone", "Southern Cross Station", "Festival", 83, 84, 20000),
    ("Night Market Series", "Queen Victoria Market", "Market", 85, 85, 9500),
    ("Cancelled Street Party", "Smith Street", "Festival", 38, 38, 5000),
]


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0088
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = p2 - p1
    dl = np.radians(lon2) - np.radians(lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def _profile(parts, base=0.02):
    h = np.arange(24)
    p = np.full(24, base, dtype=float)
    for w, c, s in parts:
        p += w * np.exp(-0.5 * ((h - c) / s) ** 2)
    return p / p.sum()


_PROFILES = {
    ("member", False): _profile([(0.35, 8.3, 1.2), (0.42, 17.5, 1.5), (0.15, 12.5, 2.0)]),
    ("member", True): _profile([(0.45, 11.0, 3.0), (0.45, 15.5, 3.0)], base=0.03),
    ("casual", False): _profile([(0.60, 13.0, 3.5), (0.30, 17.5, 2.0)], base=0.03),
    ("casual", True): _profile([(0.90, 14.0, 3.2)], base=0.03),
}
_BASE_TRIPS = {("member", False): 840, ("member", True): 400, ("casual", False): 280, ("casual", True): 610}


class BikeShareWorld:
    def __init__(self, root: str, seed: int = SEED, start: date = START_DATE, total_days: int = TOTAL_DAYS):
        self.root = root
        self.seed = seed
        self.start = start
        self.total_days = total_days
        self.trips_dir = os.path.join(root, "trips")
        self.db_path = os.path.join(root, "stations.db")
        self.events_path = os.path.join(root, "events", "city_events_calendar.xlsx")
        self.state_path = os.path.join(root, "world_state.json")
        self.stations = list(_STATIONS) + [_NEW_STATION]
        self._idx = {s[0]: i for i, s in enumerate(self.stations)}
        lat = np.array([s[4] for s in self.stations])
        lon = np.array([s[5] for s in self.stations])
        self._dist = haversine_km(lat[:, None], lon[:, None], lat[None, :], lon[None, :])
        w = np.array([_ZONE_W[s[3]] for s in self.stations], dtype=float)
        leis_boost = np.array([1.8 if any(k in s[1] for k in _LEISURE_WORDS) else 1.0 for s in self.stations])
        self._w_cbd, self._w_res, self._w_leis = w[:, 0], w[:, 1], w[:, 2] * leis_boost
        self._events = self._build_events()

    # ------------------------------------------------------------------ events
    def _build_events(self):
        out = []
        for name, venue, cat, s, e, att in _EVENTS:
            st = next(x for x in self.stations if x[1] == venue)
            out.append(dict(name=name, venue=venue, category=cat, start=self.start + timedelta(days=s),
                            end=self.start + timedelta(days=e), attendance=att, lat=st[4], lon=st[5],
                            station_idx=self._idx[st[0]], status="Cancelled" if name.startswith("Cancelled") else "Confirmed"))
        return out

    def _events_on(self, d: date):
        return [(e["station_idx"], e["attendance"] / max((e["end"] - e["start"]).days + 1, 1))
                for e in self._events if e["start"] <= d <= e["end"] and e["status"] == "Confirmed"]

    # ------------------------------------------------------------------ station availability
    def _active_mask(self, d: date) -> np.ndarray:
        n = len(self.stations)
        active = np.ones(n, dtype=bool)
        day = (d - self.start).days
        if day < _NEW_STATION_DAY:
            active[self._idx[_NEW_STATION[0]]] = False
        for off, sid, ch in _CHANGES:
            if off <= day and "station_status" in ch:
                active[self._idx[sid]] = ch["station_status"] == "active"
        return active

    # ------------------------------------------------------------------ weather (mock of the Open-Meteo API)
    def _weather_day(self, d: date) -> dict:
        rng = np.random.default_rng([self.seed, d.toordinal(), 7])
        frac = (d - self.start).days / max(self.total_days, 1)
        base = 9.5 + 4.5 * frac + rng.normal(0, 3.0)
        h = np.arange(24)
        temp = base + 4.5 * np.sin(2 * np.pi * (h - 9) / 24) + rng.normal(0, 0.3, 24)
        precip = np.zeros(24)
        if rng.random() < 0.42:
            start_h = int(rng.integers(0, 20))
            length = int(rng.integers(2, 11))
            for i in range(start_h, min(24, start_h + length)):
                if rng.random() < 0.8:
                    precip[i] = rng.gamma(1.1, 0.9)
        wind = rng.gamma(4.0, 5.0, 24) + (3.0 if precip.sum() > 0 else 0.0)
        rh = np.clip(78 - 1.2 * (temp - base) + 12 * (precip > 0) + rng.normal(0, 4, 24), 30, 100)
        code = np.where(precip > 2.5, 65, np.where(precip > 0.5, 63, np.where(precip > 0.05, 61,
                        rng.choice([0, 1, 2, 3], 24, p=[0.3, 0.2, 0.25, 0.25]))))
        return dict(temperature_2m=np.round(temp, 1), precipitation=np.round(precip, 1),
                    wind_speed_10m=np.round(wind, 1), relative_humidity_2m=np.round(rh, 0), weather_code=code)

    def weather_payload(self, start: date, end: date) -> dict:
        times, cols = [], {k: [] for k in ("temperature_2m", "precipitation", "wind_speed_10m", "relative_humidity_2m", "weather_code")}
        d = start
        while d <= end:
            wd = self._weather_day(d)
            for hh in range(24):
                times.append(f"{d.isoformat()}T{hh:02d}:00")
            for k in cols:
                cols[k].extend(wd[k].tolist())
            d += timedelta(days=1)
        return {
            "latitude": -37.8, "longitude": 144.96875, "generationtime_ms": 0.9, "utc_offset_seconds": 36000,
            "timezone": "Australia/Melbourne", "timezone_abbreviation": "GMT+10", "elevation": 35.0,
            "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "precipitation": "mm", "wind_speed_10m": "km/h",
                             "relative_humidity_2m": "%", "weather_code": "wmo code"},
            "hourly": {"time": times, **cols},
        }

    # ------------------------------------------------------------------ trips
    def _gen_day(self, d: date, with_replay: bool = True, ctr=None) -> list[dict]:
        rng = np.random.default_rng([self.seed, d.toordinal()])
        wd = self._weather_day(d)
        day = (d - self.start).days
        weekend = d.weekday() >= 5
        active = self._active_mask(d)
        events = self._events_on(d)
        growth = 1 + 0.004 * day
        ev_boost = 1 + sum(a for _, a in events) / 90000
        n_st = len(self.stations)
        ev_share = np.zeros(n_st)
        for si, a in events:
            ev_share[si] += a / 40000
        rows, seq = [], 0
        for h in range(24):
            temp, rain, wind = wd["temperature_2m"][h], wd["precipitation"][h], wd["wind_speed_10m"][h]
            wm = float(np.clip((temp - 3) / 12, 0.4, 1.15)) * (1 / (1 + 1.3 * rain)) * (0.85 if wind > 35 else 1.0)
            wm = max(wm, 0.12)
            for rider in ("member", "casual"):
                lam = _BASE_TRIPS[(rider, weekend)] * _PROFILES[(rider, weekend)][h] * wm * growth * ev_boost
                n = int(rng.poisson(lam))
                if n == 0:
                    continue
                if rider == "member" and not weekend and 6 <= h <= 9:
                    ps, pe = self._w_res, self._w_cbd
                elif rider == "member" and not weekend and 16 <= h <= 19:
                    ps, pe = self._w_cbd, self._w_res
                elif rider == "member":
                    ps = pe = (self._w_cbd + self._w_res) / 2
                else:
                    ps = pe = self._w_leis
                boost = 1 + 6 * ev_share if h >= 11 else np.ones(n_st)
                ps = ps * boost * active
                pe = pe * boost * active
                s_idx = rng.choice(n_st, n, p=ps / ps.sum())
                e_idx = rng.choice(n_st, n, p=pe / pe.sum())
                if rider == "casual":
                    rt = rng.random(n) < 0.35
                    e_idx = np.where(rt, s_idx, e_idx)
                electric = rng.random(n) < 0.32
                dist = self._dist[s_idx, e_idx] * 1.3
                same = s_idx == e_idx
                dist = np.where(same, rng.uniform(1.0, 6.0, n) if rider == "casual" else rng.uniform(0.6, 2.0, n), dist)
                speed = np.where(electric, 19.0, 14.5) * np.clip(rng.normal(1, 0.12, n), 0.7, 1.3)
                dwell = rng.exponential(8 * 60 if rider == "casual" else 90, n)
                dur = np.maximum((dist / speed * 3600 + dwell + 60).astype(int), 90)
                sec = rng.integers(0, 3600, n)
                bike = rng.integers(1, FLEET + 1, n)
                mins = np.ceil(dur / 60)
                if rider == "casual":
                    fare = 1.5 + mins * np.where(electric, 0.45, 0.20)
                else:
                    fare = mins * np.where(electric, 0.18, 0.0)
                for i in range(n):
                    seq += 1
                    t0 = datetime(d.year, d.month, d.day) + timedelta(hours=h, seconds=int(sec[i]))
                    rows.append(dict(
                        trip_id=f"T{d:%y%m%d}{seq:05d}", bike_id=f"B{int(bike[i]):04d}",
                        rideable_type="electric_bike" if electric[i] else "classic_bike",
                        started_at=t0, ended_at=t0 + timedelta(seconds=int(dur[i])),
                        start_station_id=self.stations[s_idx[i]][0], end_station_id=self.stations[e_idx[i]][0],
                        rider_type=rider, fare_aud=round(float(fare[i]), 2),
                        battery_pct=str(int(rng.integers(8, 100))) if electric[i] else ""))
        rows = self._make_dirty(rows, rng, day, ctr)
        if with_replay and day > 0:
            prev = self._gen_day(d - timedelta(days=1), with_replay=False)
            k = max(1, int(len(prev) * 0.0015))
            for j in rng.choice(len(prev), k, replace=False):
                rows.append(dict(prev[int(j)]))
                if ctr is not None:
                    ctr.update(["replayed_from_previous_day"])
        return rows

    def _make_dirty(self, rows, rng, day, ctr=None):
        """Inject production-style defects. `ctr` (a Counter) records ground truth for each defect type."""
        bump = (lambda k: ctr.update([k])) if ctr is not None else (lambda k: None)
        for r in rows:
            u = rng.random()
            if u < 0.005:
                r["end_station_id"] = ""; bump("missing_end_station")
            elif u < 0.0065:
                r["started_at"], r["ended_at"] = r["ended_at"], r["started_at"]; bump("end_before_start")
            elif u < 0.0145:
                r["ended_at"] = r["started_at"] + timedelta(seconds=int(rng.integers(20, 50)))
                r["end_station_id"] = r["start_station_id"]; bump("false_start_under_60s")
            elif u < 0.0150:
                r["ended_at"] = r["started_at"] + timedelta(hours=26, minutes=int(rng.integers(0, 600))); bump("not_returned_over_24h")
            elif u < 0.0155:
                r["fare_aud"] = -abs(r["fare_aud"]) or -1.5; bump("negative_fare")
            elif u < 0.0165:
                r["trip_id"] = ""; bump("null_trip_id")
            elif u < 0.0175:
                r["rider_type"] = "unknown"; bump("invalid_rider_type")
            elif u < 0.0180:
                r["started_at"] = r["started_at"] + timedelta(days=365); r["ended_at"] = r["ended_at"] + timedelta(days=365); bump("future_dated")
            elif u < 0.0190:
                r["start_station_id"] = "S999"; bump("orphan_start_station")
            if r["rider_type"] in ("member", "casual") and rng.random() < 0.08:
                r["rider_type"] = rng.choice(["Member", "MEMBER"]) if r["rider_type"] == "member" else rng.choice(["Casual", " casual ", "CASUAL"])
                bump("inconsistent_casing")
            if rng.random() < 0.05:
                r["rideable_type"] = "Electric" if r["rideable_type"] == "electric_bike" else "Classic Bike"; bump("inconsistent_bike_type_label")
        k = int(len(rows) * 0.004)  # in-file exact duplicates
        if rows and k:
            for j in rng.choice(len(rows), k, replace=False):
                rows.append(dict(rows[int(j)])); bump("exact_duplicate_in_file")
        return rows

    @staticmethod
    def _fmt_ts(ts, alt: bool) -> str:
        return ts.strftime("%d/%m/%Y %H:%M") if alt else ts.strftime("%Y-%m-%d %H:%M:%S")

    def _write_trips_file(self, d: date):
        ctr = Counter()
        rows = self._gen_day(d, ctr=ctr)
        rng = np.random.default_rng([self.seed, d.toordinal(), 99])
        drift = (d - self.start).days >= SCHEMA_DRIFT_DAY
        cols = ["trip_id", "bike_id", "rideable_type", "started_at", "ended_at", "start_station_id",
                "end_station_id", "rider_type", "fare_aud"] + (["battery_pct"] if drift else [])
        path = os.path.join(self.trips_dir, f"trips_{d.isoformat()}.csv")
        tmp = path + ".tmp"
        with open(tmp, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                alt = rng.random() < 0.003
                if alt:
                    ctr.update(["alt_timestamp_format"])
                rec = dict(r, started_at=self._fmt_ts(r["started_at"], alt), ended_at=self._fmt_ts(r["ended_at"], alt))
                w.writerow([rec[c] for c in cols])
        os.replace(tmp, path)  # atomic publish, like a real SFTP drop
        ledger = os.path.join(self.root, "injected_errors.csv")  # ground truth, NOT visible to the pipeline
        new = not os.path.exists(ledger)
        with open(ledger, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["file_date", "error_type", "count"])
            for k, v in sorted(ctr.items()):
                w.writerow([d.isoformat(), k, v])

    # ------------------------------------------------------------------ stations DB
    def _init_db(self):
        os.makedirs(self.root, exist_ok=True)
        if os.path.exists(self.db_path):
            return
        con = sqlite3.connect(self.db_path)
        con.execute("""CREATE TABLE stations(station_id TEXT PRIMARY KEY, station_name TEXT, suburb TEXT, zone TEXT,
                       latitude REAL, longitude REAL, capacity INTEGER, station_status TEXT, installed_date TEXT, modified_at TEXT)""")
        for i, s in enumerate(_STATIONS):
            inst = (date(2025, 3, 1) + timedelta(days=i)).isoformat()
            name = s[1] + (" " if s[0] == "S007" else "")  # trailing whitespace on purpose
            suburb = None if s[0] == "S039" else s[2]  # missing suburb on purpose
            status = {"S014": "ACTIVE", "S022": "Active"}.get(s[0], "active")  # inconsistent casing on purpose
            con.execute("INSERT INTO stations VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (s[0], name, suburb, s[3], s[4], s[5], s[6], status, inst, f"{inst} 00:00:00"))
        con.commit()
        con.close()

    def _apply_changes(self, through: date, state: dict):
        con = sqlite3.connect(self.db_path)
        applied = set(state.get("changes_applied", []))
        for n, (off, sid, ch) in enumerate(_CHANGES):
            when = self.start + timedelta(days=off)
            if when <= through and n not in applied:
                sets = ", ".join(f"{k}=?" for k in ch) + ", modified_at=?"
                con.execute(f"UPDATE stations SET {sets} WHERE station_id=?", [*ch.values(), f"{when} 03:00:00", sid])
                applied.add(n)
        when = self.start + timedelta(days=_NEW_STATION_DAY)
        if when <= through and "new_station" not in state:
            s = _NEW_STATION
            con.execute("INSERT INTO stations VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (s[0], s[1], s[2], s[3], s[4], s[5], s[6], "active", when.isoformat(), f"{when} 03:00:00"))
            state["new_station"] = True
        con.commit()
        con.close()
        state["changes_applied"] = sorted(applied)

    # ------------------------------------------------------------------ events workbook
    def _init_events(self):
        if os.path.exists(self.events_path):
            return
        from openpyxl import Workbook
        os.makedirs(os.path.dirname(self.events_path), exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = "Events"
        ws["A1"] = "City of Melbourne - Major Events Calendar (synthetic)"
        ws["A2"] = "Exported manually by Events Team. Dates may be text or date cells."
        ws.append([])
        ws.append(["Event Name", "Venue", "Category", "Start Date", "End Date", "Expected Attendance", "Venue Lat", "Venue Lon", "Status"])
        for i, e in enumerate(self._events):
            as_text = i % 4 == 3
            sd = e["start"].strftime("%d/%m/%Y") if as_text else e["start"]
            ed = e["end"].strftime("%d/%m/%Y") if as_text else e["end"]
            att = f"{e['attendance']:,}" if i % 3 == 0 else e["attendance"]
            ws.append([e["name"], e["venue"], e["category"], sd, ed, att, e["lat"], e["lon"], e["status"]])
        wb.save(self.events_path)

    # ------------------------------------------------------------------ public API
    def init_sources(self):
        os.makedirs(self.trips_dir, exist_ok=True)
        self._init_db()
        self._init_events()

    def _load_state(self) -> dict:
        if os.path.exists(self.state_path):
            with open(self.state_path) as f:
                return json.load(f)
        return {}

    def advance(self, through: date) -> dict:
        """Move the simulated world forward so that data exists up to and including `through`."""
        self.init_sources()
        through = min(through, self.start + timedelta(days=self.total_days - 1))
        written = 0
        d = self.start
        while d <= through:
            if not os.path.exists(os.path.join(self.trips_dir, f"trips_{d.isoformat()}.csv")):
                self._write_trips_file(d)
                written += 1
            d += timedelta(days=1)
        state = self._load_state()
        self._apply_changes(through, state)
        state["through"] = through.isoformat()
        with open(self.state_path, "w") as f:
            json.dump(state, f)
        return {"through": through.isoformat(), "trip_files_written": written}
