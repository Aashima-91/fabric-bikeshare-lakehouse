# Architecture

```mermaid
flowchart LR
  subgraph SRC[Heterogeneous sources]
    A1[Trip CSVs<br/>SFTP drop, daily]
    A2[(Stations DB<br/>Azure SQL / SQLite)]
    A3[Weather REST API<br/>Open-Meteo JSON]
    A4[Events calendar<br/>Excel]
  end
  subgraph LH[Fabric Lakehouse  lh_yarraride]
    L[Files/landing<br/>immutable raw files]
    B[(bronze<br/>raw Delta + audit cols)]
    S[(silver<br/>typed, deduped, validated)]
    Q[(silver.quarantine_*<br/>rejected rows + reasons)]
    G[(gold<br/>star schema + marts)]
    AU[(audit<br/>run log, DQ results,<br/>watermarks, file ledger)]
  end
  PBI[Power BI<br/>Direct Lake semantic model + report]
  A1 -->|copy new files| L
  A2 -->|incremental extract<br/>modified_at watermark| L
  A3 -->|date-window pull<br/>+ lookback| L
  A4 -->|copy on new hash| L
  L --> B --> S --> G --> PBI
  S -.rule failures.-> Q
  B -.-> AU
  S -.-> AU
  G -.-> AU
  AU --> PBI
  DF[Data Factory pipeline<br/>pl_bikeshare_master] -.orchestrates.-> L
```

## Layer contracts

| Layer | Contents | Load pattern | Audit columns |
|---|---|---|---|
| Landing (`Files/landing`) | files exactly as received | add-only, never modified | file name / folder = lineage |
| Bronze | all strings (trips, events), typed Parquet (stations), raw JSON arrays (weather) | append; file ledger / batch folder = idempotent | `_batch_id _source_system _source_file _ingested_at _ingest_date` |
| Silver | typed, conformed, deduped, validated | incremental by Bronze `_ingested_at` watermark; `MERGE` upsert on business key, unchanged rows skipped via `_row_hash` | `_batch_id _processed_at _row_hash` + inherited `_source_file _ingested_at` |
| Gold | dimensional model + marts | fact: watermark on Silver `_processed_at` + `MERGE`; SCD2 dimension: staged `MERGE`; marts: `replaceWhere` only the affected dates | `_batch_id _loaded_at` (+ `_valid_from _valid_to _is_current _row_hash`) |

## Dimensional model

```mermaid
erDiagram
  fact_trip }o--|| dim_date : date_key
  fact_trip }o--|| dim_station : "start_station_sk (active)"
  fact_trip }o--|| dim_station : "end_station_sk (inactive)"
  fact_trip }o--|| dim_weather_hour : weather_hour_key
  fact_trip }o--|| dim_event : event_sk
  fact_trip }o--|| dim_rider_type : rider_type_key
  fact_trip }o--|| dim_bike_type : bike_type_key
  mart_station_day }o--|| dim_station_current : station_id
  fact_trip {
    string trip_id
    int date_key
    int weather_hour_key
    bigint start_station_sk
    bigint end_station_sk
    bigint event_sk
    double duration_min
    double distance_km
    decimal fare_aud
  }
  dim_station {
    bigint station_sk
    string station_id
    int capacity
    string station_status
    timestamp _valid_from
    timestamp _valid_to
    boolean _is_current
  }
```

Grain of `fact_trip`: **one trip**. Measures are additive (`duration_min`, `distance_km`, `fare_aud`) except flags
(`is_round_trip`, `is_short_trip`, `has_dq_warning`) which are summed as counts.

## Key design decisions 

1. **Landing before Bronze.** Raw files are kept so Bronze can be rebuilt without going back to the source (replay/backfill).
2. **Watermarks are promoted only after the next layer commits** (`stations.modified_at@pending` -> `stations.modified_at`),
   so a failed run is retried from the same point instead of skipping data. Delivery is at-least-once; Silver/Gold are idempotent.
3. **Idempotent by construction.** `MERGE` on business keys + `_row_hash` change detection means re-running a batch changes nothing
   (asserted in `test_rerun_is_idempotent_and_watermarks_advanced`).
4. **SCD2 with deterministic surrogate keys** (`xxhash64(station_id, _valid_from)`) and a point-in-time join in the fact, so history is
   never rewritten and a Gold rebuild yields identical keys. An *Unknown* member (`-1`) absorbs orphan keys instead of dropping facts.
5. **Snapshot-extraction limitation.** The OLTP table only exposes its current state, so SCD2 captures one version per extract; changes
   between two extracts are collapsed. The SCD2 engine itself handles several versions per key in one batch (unit-tested) - with CDC
   (Fabric mirroring / change tracking) nothing would be lost.
6. **Timestamps are Melbourne wall-clock time stored in a UTC session** (no DST conversion). Simple and consistent, but a production
   system would store UTC + a time zone.
7. **Pipeline fails loudly:** circuit breaker on rejects, failed reconciliation checks fail the run, `Fail` activity in Data Factory.
