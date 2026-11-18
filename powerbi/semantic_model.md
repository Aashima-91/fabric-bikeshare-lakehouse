# Semantic model: `sm_yarraride`

Direct Lake semantic model over the Gold and audit tables in `lh_yarraride`. No data is imported or copied. Visuals read the Delta tables directly.

## Tables

| Group | Table | Grain | Notes |
|---|---|---|---|
| Fact | `fact_trip` | one row per trip | Keys: `start_station_sk`, `end_station_sk`, `date_key`, `weather_hour_key`, `event_sk`, `bike_type_key`, `rider_type_key` (key columns hidden) |
| Dimension | `dim_date` | one row per day | Marked as the date table; `date` is the key |
| Dimension | `dim_station` | one row per station **version** (SCD2) | `_valid_from`, `_valid_to`, `_is_current`; `station_sk = -1` is the Unknown member |
| Dimension | `dim_station_current` | one row per station, current state only | Used for slicers, maps and the station marts |
| Dimension | `dim_weather_hour` | one row per hour | `is_raining`, `precipitation_mm`, `rain_intensity` |
| Dimension | `dim_event` | one row per city event | `event_sk = -1` is "No event" |
| Dimension | `dim_bike_type`, `dim_rider_type` | small lookups | Hidden surrogate keys |
| Mart | `mart_daily_kpis` | one row per day | Pre-aggregated daily KPIs |
| Mart | `mart_hourly_demand` | one row per hour | Used for the rain vs demand scatter |
| Mart | `mart_station_day` | one row per station per day | Net flow, turnover per dock, rebalancing |
| Audit | `pipeline_run_log` | one row per pipeline step per run | Rows read, written and rejected; duration |
| Audit | `dq_results` | one row per DQ rule per run | Severity: error or warn |
| Audit | `quarantine_summary` | one row per rule per source table | Rows quarantined |

## Relationships

Star schema, single-direction filtering from dimension to fact.

| From (many) | To (one) | Active | Purpose |
|---|---|---|---|
| `fact_trip[start_station_sk]` | `dim_station[station_sk]` | yes | Trips by the station where they started |
| `fact_trip[end_station_sk]` | `dim_station[station_sk]` | **no** | Activated by `USERELATIONSHIP` in `[Trips Ended]` |
| `fact_trip[date_key]` | `dim_date[date_key]` | yes | |
| `fact_trip[weather_hour_key]` | `dim_weather_hour[weather_hour_key]` | yes | Rain vs dry analysis |
| `fact_trip[event_sk]` | `dim_event[event_sk]` | yes | Event impact |
| `fact_trip[bike_type_key]` | `dim_bike_type[bike_type_key]` | yes | |
| `fact_trip[rider_type_key]` | `dim_rider_type[rider_type_key]` | yes | |
| `mart_daily_kpis[date_key]` | `dim_date[date_key]` | yes | |
| `mart_hourly_demand[date_key]` | `dim_date[date_key]` | yes | |
| `mart_station_day[date_key]` | `dim_date[date_key]` | yes | |
| `mart_station_day[station_id]` | `dim_station_current[station_id]` | yes | Marts use the current station, not the SCD2 version |

The three audit tables have no relationships. Each is used on its own visuals.

## Design decisions

- **Inactive end-station relationship.** A trip has two stations but a dimension can only have one active relationship to a fact. Start is active, and `[Trips Ended]` switches to the end station with `USERELATIONSHIP`. `[Net Flow]` is the difference.
- **SCD2 dimension plus a current view.** `fact_trip` joins to the station version that was valid when the trip happened (as-of join in Gold), so history stays correct after renames or capacity changes. Station-level marts and slicers use `dim_station_current` so each station appears once.
- **Unknown members.** `station_sk = -1` and `event_sk = -1` keep late or unmatched rows in the fact instead of dropping them. `[Active Stations]` excludes the Unknown station.
- **Marts only relate to `dim_date` and `dim_station_current`.** A visual mixing mart columns with `fact_trip` measures or other dimensions will show "can't determine relationships". Use mart columns with mart measures.
- **Hidden columns.** All surrogate and join keys are hidden so report authors work with business columns.
- **Audit columns** (`_batch_id`, `_loaded_at`, `_row_hash`, SCD2 validity) remain in the model for lineage and are not used in visuals, except the SCD2 history table on the Stations page.

## Report pages

1. **Executive overview**: trips, revenue, duration, e-bike share, 7-day average
2. **Weather & events**: rain impact, trips per hour
3. **Stations**: net flow, rebalancing priority, SCD2 history for the selected station
4. **Pipeline & data quality**: step success rate, critical checks passed, quarantine rate, slowest steps

Measures are in [`measures.dax`](measures.dax).
