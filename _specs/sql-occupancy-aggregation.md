# Spec for sql-occupancy-aggregation

branch: claude/feature/boards-occupancy-by-time-period

## Summary

The boards occupancy by time period report currently performs its entire aggregation in Python: it iterates over every reservation, distributes slots into per-hour buckets, and then groups by period and location. This is slow for large date ranges. The goal is to move as much of this computation as possible into SQL (BigQuery), and keep only the irreducible Python-side logic (e.g. anything that depends on local lookup tables like `historical_location_hours_availability` or `LAST_HOURS_AVAILABILITY` overrides that are not stored in the database).

## Functional Requirements

- The final chart and export output must remain identical to the current implementation.
- SQL should perform: slot summation per location per hour, date/time bucketing by granularity (hour, day, week, month), filtering by street, attraction group, and visit type.
- Python should only perform: capacity lookup (boards available per hour from `historical_location_hours_availability` / `historical_location_boards_availability` and `LAST_HOURS_AVAILABILITY`), occupancy ratio calculation, and any aggregation that requires local lookup data not available in BigQuery.
- The `@st.cache_data` caching on both the query and the computation should be preserved so repeated loads with the same parameters remain fast.
- Query parameters (streets, attraction groups, visit types, start date, end date) must continue to be passed as BigQuery parameterized query parameters.

## Possible Edge Cases

- Reservations that span midnight (start hour near end of day) must be bucketed to the correct date/hour in SQL.
- The `LAST_HOURS_AVAILABILITY` override table is Python-only; capacity values for recent hours cannot be pushed to SQL and must remain in Python.
- `historical_location_hours_availability` and `historical_location_boards_availability` are not in BigQuery; any join against them must stay in Python.
- Attraction group condition uses a dynamic `IN` vs `IS NOT NULL` branch (`format_array_for_query`) — this must be preserved in the new query.
- Visit types filtered to an empty list should produce no results, not an error.

## Acceptance Criteria

- Page load time for a full-year, all-locations query is measurably faster than the current Python loop implementation.
- Chart output for an identical parameter set matches the current implementation exactly (same occupancy percentages).
- All four granularities (Godzina, Dzien, Tydzien, Miesiac) continue to render correct labels and sorted order.
- Download export produces the same per-street sheets with the same column values.
- Caching still works: a second load with unchanged parameters does not re-run the BigQuery query or recompute occupancy.

## Open Questions

- Which parts of the slot-distribution logic are truly impossible to express in SQL given the current schema, and which can be moved with schema additions? decide 
- Should `historical_location_hours_availability` and `historical_location_boards_availability` be loaded into BigQuery as reference tables to enable a full SQL join? if it will make computing faster yes
- Is it acceptable to round hour buckets in SQL (e.g. `EXTRACT(HOUR FROM start_date)`) or must fractional-hour slot distribution be preserved? yes
