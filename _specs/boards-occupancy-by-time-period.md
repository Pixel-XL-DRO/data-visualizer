# Spec for Boards Occupancy by Time Period

branch: claude/feature/boards-occupancy-by-time-period

## Summary

Add a report page that shows boards occupancy aggregated by a user-selected time period (hour, day, week, month). The user can filter by location(s) and visit/attraction types, choose a date range, and download the result as `.xlsx`. The page visualises the data as a bar chart (mirroring `reservations_by_time_period.py`) and calculates occupancy the same way `safi_view.py` does — distributing `slots_taken` into time-unit buckets and dividing by `total_boards` available for that slot.

## Functional Requirements

- **Filters (sidebar)**
  - Multi-select: locations (cities/streets)
  - Multi-select: attraction groups
  - Multi-select: visit type groups
  - Date range: start date / end date
  - Grouping period: Hour, Day, Week, Month (same options as `reservations_by_time_period.py`)

- **Data fetching**
  - Query reservations for all selected streets and visit types within the chosen date range, including `slots_taken`, `time_taken`, `reservation_system`, and `start_date` — analogous to `boards_occupancy_queries.get_reservations_data` but multi-location.
  - Also fetch `get_slots_occupancy` for plan4u normalisation (same as `safi_view.py`).
  - Use `get_historical_location_hours_availability` and `get_historical_location_boards_availability` for capacity lookups (already loaded as cached data).

- **Occupancy calculation**
  - Reuse the slot-distribution logic from `safi_view.py`: iterate reservations, distribute `slots_taken` into hourly/sub-hourly `hours_map` buckets, look up `total_boards` per date from `historical_location_boards_availability`, apply `LAST_HOURS_AVAILABILITY` overrides.
  - Result per bucket: `boards_occupancy = slots_taken / total_boards * 100`.
  - Occupancy is **not** capped at 100 %.
  - `Arena` visit type is always excluded.

- **Aggregation by time period**
  - After building per-slot occupancy, group by the selected period (Hour / Day / Week / Month) using the same period logic as `reservations_by_time_period_queries`.
  - Aggregate: sum `slots_taken` and sum `total_boards` per period, then compute `avg_occupancy = sum(slots_taken) / sum(total_boards) * 100` (weighted, not a mean of percentages).
  - Produce one row per period with: `period`, `avg_occupancy`, `total_slots_taken`, `total_boards`.

- **Visualisation**
  - Bar chart (Altair, `create_bar_chart`) with period on X-axis and average occupancy (%) on Y-axis, mirroring the style of `reservations_by_time_period.py`.
  - Highlight the current (incomplete) period if applicable.

- **Download**
  - `.xlsx` export via `utils.download_button`.
  - One sheet per selected location.
  - Columns: `okres` (period label), `wszystkie_maty` (total boards capacity), `zajete_maty` (taken), `zajętość (%)`.
  - Filename includes selected streets, attraction groups, and date range.
  - Download button wrapped in `@st.fragment` to avoid full page reload.

- **UI / UX**
  - Spinner during reservation fetch and during occupancy calculation.
  - If no data for selected range, show Polish info message and `st.stop()`.
  - UI labels in Polish.

## Possible Edge Cases

- A location may have no `historical_location_boards_availability` row for a given date — skip those dates gracefully.
- A location may have no `historical_location_hours_availability` row matching the weekday/date — skip those dates.
- plan4u reservations with `time_taken == 0` must be skipped (division by zero).
- Selecting all locations with very long date ranges may be slow — spinner with `show_time=True` should be shown.
- Incomplete current period should be labelled or excluded, consistent with `reservations_by_time_period.py`.

## Acceptance Criteria

- Selecting a single location and a single week produces the same per-slot occupancy values as the existing `safi_view.py` heatmap for the same location/week.
- Selecting multiple locations aggregates correctly using weighted occupancy (sum slots / sum boards), not a simple average of percentages.
- All four grouping periods (Hour, Day, Week, Month) produce sensibly-labelled charts and export rows.
- `.xlsx` download contains one sheet per location with the correct columns and values.
- Changing any filter re-runs the query and recalculates without errors.
- Empty date range shows the Polish info message.

## Open Questions

- Should the bar chart stack locations (one colour per location) or always show a single aggregated series? The existing occupancy report heatmap aggregates; this spec follows the same approach (single series), but stacked could be useful. yes, stack
- Should incomplete current periods be excluded from the chart (as in `reservations_by_time_period.py`) or shown with a visual indicator? there should be checkbox for that, so when click it will include them in charts
