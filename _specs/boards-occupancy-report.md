# Spec for Boards Occupancy Report

branch: claude/feature/boards-occupancy-report

## Summary

Add a new report page under `navigation_pages/reports/` that shows boards (mat) occupancy across a flexible date range and multiple locations. Unlike the existing `boards_occupancy` page (which shows a single location's week-by-week heatmap), this report lets the user select multiple locations and visit types, aggregate by configurable time periods (hour, day, week, month), visualise the result as a heatmap, and download the underlying data as an `.xlsx` file.

The data fetching and occupancy calculation logic mirrors what is already implemented in `boards_occupancy_queries.py` and `safi_view.py`, adapted to work across multiple locations and a user-defined date range instead of a fixed week window.

## Functional Requirements

- **Sidebar filters**
  - Multi-select for locations (cities/streets) – defaults to all locations permitted by the user's role via `auth.filter_locations`.
  - Multi-select for attraction groups / visit types – sourced from `df_initial` (same as the existing boards occupancy page).
  - Date range picker: start date and end date.
  - Period granularity selector: Hour, Day, Week, Month — controls how occupancy slots are bucketed/aggregated in the output.

- **Data loading**
  - Fetch `get_initial_data`, `get_locations_data`, `get_historical_location_hours_availability`, `get_historical_location_boards_availability` in parallel with `utils.run_in_parallel`, exactly as in `boards_occupancy.py`.
  - Fetch reservation data for all selected locations and the selected date range (extend the existing `boards_occupancy_queries.get_reservations_data` or write a new query variant that accepts a list of streets instead of a single street).
  - Fetch `queries.get_slots_occupancy` for the selected date range (same as `safi_view.py`).

- **Occupancy calculation**
  - Apply the same slot-mapping logic as `safi_view.py`: iterate over every slot in the date range, look up `historical_location_hours_availability` for the correct starting hour and number of hours, then distribute `slots_taken` across time buckets.
  - Apply the `LAST_HOURS_AVAILABILITY` override for the last slot of each day (matching `safi_view.py`).
  - Compute `boards_occupancy` as `(slots_taken / total_boards * 100)` per bucket, capped at 100 %.
  - When multiple locations are selected, calculate occupancy per location separately, then aggregate (average) across locations for the combined view.
  - Aggregate the per-slot data up to the chosen granularity period before rendering.

- **Visualisation**
  - Render an Altair heatmap of occupancy (%) with:
    - X-axis: date labels at the chosen granularity.
    - Y-axis: time-of-day bucket (hour slot) — collapsed/averaged when granularity is Day, Week, or Month.
    - Color scale: `redyellowgreen` (0–100 %).
    - Text overlay showing the occupancy percentage.
    - Daily/period average row appended at the bottom of the heatmap (as in `safi_view.py`).
  - When multiple locations are selected, show a location selector or tabs so the user can switch between individual location views and an aggregated view.

- **Download**
  - Provide a download button using `utils.download_button` that exports the aggregated occupancy data table to an `.xlsx` file.
  - The sheet should contain: date, period bucket, location, slots taken, total boards, occupancy (%).

- **Access control**
  - Page available to `super-admin` only (add to the `Raporty` section in `main.py`).
  - Apply `auth.filter_locations` before showing location options.

## Possible Edge Cases

- A selected date range may include dates before the location opened (no availability data) — show a user-friendly warning and skip those dates.
- The `historical_location_hours_availability` lookup must pick the correct historical row for dates in the past, not just the current row (same sorted lookup as `safi_view.py`).
- `get_slots_occupancy` is parameterised by a date range; very wide ranges (months) may be slow — consider a `ttl` appropriate for a report context.
- Some locations may not appear in `LAST_HOURS_AVAILABILITY`; the code must handle the missing-key case gracefully (already handled via `.get()`).
- If no reservations exist in the selected range the heatmap will be empty — display an informative message rather than a broken chart.
- The `Arena` visit type is excluded in the existing query; this exclusion should be preserved in the report query.
- When granularity is Hour, the Y-axis can become very long for wide date ranges — consider limiting the visible range or adding a scroll.

## Acceptance Criteria

- [ ] Report page appears in the `Raporty` nav section for `super-admin` role.
- [ ] Sidebar allows selecting multiple locations, multiple attraction groups/visit types, a date range, and a period granularity.
- [ ] Occupancy heatmap renders correctly for a single location using the same values as the existing boards occupancy page for the same week.
- [ ] Occupancy heatmap renders correctly when multiple locations are selected (aggregated view).
- [ ] Period granularity changes (Hour / Day / Week / Month) correctly re-aggregate and re-render the heatmap.
- [ ] Download button exports a valid `.xlsx` file containing the occupancy data for the current filter state.
- [ ] Locations not permitted for the user's role do not appear in the sidebar.
- [ ] No data in range shows a clear Polish-language message instead of a broken chart.

## Open Questions

- Should the report show each location in a separate tab, or only an aggregated view, or both? report as data in xlsx yes, the visuals should be aggregated
- Should the occupancy percentage be capped at 100 % in the report (the existing `safi_view.py` has a TODO comment noting the cap was reverted)? no
- Is `Arena` always excluded, or should it be a sidebar toggle for the report? arena is always excluded
- What is the intended max date range for the report (to manage query cost/speed)? like a year
