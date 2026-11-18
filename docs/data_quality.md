# Data quality: injected defects and how the pipeline handles them

The source simulator deliberately injects production-style defects and writes a **ground-truth ledger**
(`Files/source_world/injected_errors.csv`) that the pipeline never reads. `tests/test_pipeline.py::test_injected_errors_are_caught_quarantined_or_flagged`
reconciles the ledger against what Silver quarantined/flagged, so detection is *proven*, not assumed.

## Three-tier policy

| Tier | Severity | What happens | Why |
|---|---|---|---|
| Reject | `error` | row is **quarantined** in `silver.quarantine_*` with `dq_errors` (array of rule names), batch id and timestamp | the row cannot be trusted or used (wrong, impossible, or unkeyed) |
| Keep + flag | `warn` | row continues to Silver/Gold with the rule recorded in `dq_warnings`; Gold maps missing FKs to an *Unknown* member | the row is still analytically valuable (a trip with no end dock is still a trip) |
| Repair | n/a | trimmed, case-normalised, re-parsed with fallback formats, de-duplicated | cosmetic problems should not cost us data |

Plus a **circuit breaker**: if more than 5% of a batch is rejected the run fails (a broken upstream export should stop the
pipeline, not quietly quarantine the whole day). Every rule result - pass or fail - is written to `audit.dq_results`, and
`audit.quarantine_summary` is rebuilt for the Power BI page.

## Catalogue

### Trip files (CSV)
| Injected defect | ~Rate | Rule / handling | Outcome |
|---|---|---|---|
| `ended_at` before `started_at` | 0.15% | `end_not_before_start` | quarantine |
| trip longer than 24h (bike never returned) | 0.05% | `duration_le_24h` | quarantine |
| negative fare | 0.05% | `fare_non_negative` | quarantine |
| missing `trip_id` | 0.10% | `trip_id_not_null` | quarantine |
| rider type `unknown` | 0.10% | `rider_type_valid` | quarantine |
| start time one year in the future | 0.05% | `not_future_dated` | quarantine |
| duration < 60s ("false start") | 0.80% | `duration_ge_60s` (warn) | kept, `is_short_trip = true` |
| missing end station | 0.50% | `end_station_present` (warn) | kept, end station = Unknown |
| start station not in master data (`S999`) | 0.10% | `start_station_known` (warn) | kept, start station = Unknown |
| `Member`, `MEMBER`, ` casual `, `CASUAL` | 8% | `lower(trim())` | repaired |
| `Electric`, `Classic Bike` labels | 5% | normalised to `electric` / `classic` | repaired |
| `dd/MM/yyyy HH:mm` timestamps | 0.3% | fallback format in `coalesce(try_to_timestamp...)` | repaired |
| exact duplicate rows in a file | 0.4% | de-dupe on `trip_id`, newest `_ingested_at` wins | collapsed |
| yesterday's trips re-sent today | 0.15% | same de-dupe + `only_new_or_changed` (row hash) | collapsed |
| new column `battery_pct` from day 46 (schema drift) | - | bronze `mergeSchema`, silver always projects the full schema | absorbed |

### Stations (SQL database)
Trailing whitespace in a name, `ACTIVE` / `Active` casing, a NULL suburb (`suburb_present`, warn), plus in-place `UPDATE`s that
become SCD2 history (capacity, status, rename, new station, decommissioning).

### Events (Excel)
Banner rows above the header, numbers stored as text (`"42,000"`), dates stored as text (`dd/MM/yyyy`) or real dates, and a
cancelled event (kept in Silver with `status = cancelled`, excluded from the Gold event link).

### Weather (REST API)
Overlapping date windows (1-day look-back) re-deliver recent hours; the newest observation wins, so provider revisions
are absorbed and re-runs are idempotent.

## Gold-level checks (reconciliation, `audit.dq_results.check_type = 'reconciliation'`)
Row count and revenue of `fact_trip` equal Silver; `trip_id` unique; every `date_key` exists in `dim_date`; weather and
station keys resolve (warn); `dim_station` has exactly one current row per station and no overlaps or gaps in validity ranges.
A failed critical check fails the pipeline.

## What I would add next
A *replay-from-quarantine* job (fix the root cause, re-submit quarantined rows through Silver) and Great Expectations / Soda
style profiling on Bronze for anomaly detection (volume, distribution drift).
