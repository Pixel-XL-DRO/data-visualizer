# Plan: Boards Occupancy by Time Period Report

## Context

This is a **new** report page (distinct from the already-implemented `occupancy_report.py` heatmap). It visualises boards occupancy as a **stacked bar chart by location** over a user-chosen time period grouping (Hour / Day / Week / Month), with filters for locations, attraction groups, and visit types. The occupancy calculation reuses the exact same Python-side slot-distribution logic from `safi_view.py` and `occupancy_report.py`. Open questions from the spec are resolved: chart is stacked by location; incomplete current period is excluded by default with an opt-in checkbox.

---

## Files to create / modify

| File | Action |
|---|---|
| `shared/sidebars/boards_occupancy_time_period_sidebar.py` | **Create** — new sidebar with all filters |
| `shared/queries/report_queries/occupancy_report_queries.py` | **Extend** — add `get_reservations_with_visit_type()` |
| `navigation_pages/reports/boards_occupancy_time_period_report.py` | **Create** — new report page |
| `main.py` | **Modify** — register new page for `super-admin` |

---

## Step-by-step implementation

### 1. Sidebar (`shared/sidebars/boards_occupancy_time_period_sidebar.py`)

New file. Modelled after `occupancy_report_sidebar.py` with additions:

```
filter_data(df, df_locations) → (attraction_groups, visit_types, selected_streets, start_date, end_date, granularity, show_unended_period)
```

Controls:
- `show_unended_period = st.checkbox('Pokazuj niepełny okres')` — outside expander, at top of sidebar (same as `reservations_by_time_period_sidebar.py`)
- Inside `st.expander("Filtry", expanded=True)`:
  - `attraction_groups` multiselect — from `df['attraction_group'].unique()`
  - `visit_types` multiselect — from `df['visit_type'].unique()`, default all
  - `filtered_locations` multiselect — display labels, reverse-mapped to streets before return
  - `start_date` / `end_date` date inputs — default last 30 days
  - `granularity` selectbox — `["Godzina", "Dzień", "Tydzień", "Miesiąc"]`

---

### 2. Query addition (`shared/queries/report_queries/occupancy_report_queries.py`)

Add new function `get_reservations_with_visit_type(streets, attraction_groups, visit_types, start_date, end_date)`:

- Same base query as existing `get_reservations_data` (multi-street via `IN UNNEST(@streets)`, `loc.street AS street`, Arena excluded)
- Add `AND dvt.name IN UNNEST(@visit_types)` filter
- Add `ArrayQueryParameter("visit_types", "STRING", visit_types)` to job_config

The existing `get_reservations_data` is left unchanged (still used by `occupancy_report.py`).

---

### 3. Report page (`navigation_pages/reports/boards_occupancy_time_period_report.py`)

#### Path setup
```python
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("shared/queries")
sys.path.append("shared/queries/report_queries")
sys.path.append("navigation_pages/boards_occupancy")
```

#### Initial data + sidebar
- Load 4 cached datasets in parallel (same as `occupancy_report.py`), apply `auth.filter_locations`
- Call `boards_occupancy_time_period_sidebar.filter_data(df_initial, df_locations)`

#### Data fetch (with spinner)
In parallel:
- `occupancy_report_queries.get_reservations_with_visit_type(selected_streets, attraction_groups, visit_types, start_datetime, end_datetime)`
- `queries.get_slots_occupancy(start_datetime, end_datetime)`

#### Occupancy calculation (with spinner)
**Identical to `occupancy_report.py` lines 53–167**: per-street `hours_map` loop using `historical_location_hours_availability` and `historical_location_boards_availability`, plan4u branching via `df_slots_occupancy`, `LAST_HOURS_AVAILABILITY` override (imported from `safi_view.py`). Produces flat `df_all` with columns `street`, `date`, `hour_key`, `slots_taken`, `total_boards`, `boards_occupancy`.

#### Incomplete period filtering
After building `df_all`, if `not show_unended_period`, drop rows in the current incomplete period:

| Granularity | Drop condition |
|---|---|
| Godzina | `date == today AND parsed hour == current hour` |
| Dzień | `date == today` |
| Tydzień | current ISO week + year |
| Miesiąc | current month + year |

#### Granularity aggregation (per location)
Add period label columns to `df_all` (same logic as `occupancy_report.py`):
- Godzina → `display_label = DD.MM, day_name`, group by (display_label, sort_key, street)
- Dzień → `display_label = DD.MM`
- Tydzień → `display_label = W{wk} YYYY`
- Miesiąc → `display_label = MM.YYYY`

Aggregate per `(sort_key, display_label, street)`:
```
boards_occupancy = sum(slots_taken) / sum(total_boards) * 100
```
Add `location_name = street_to_location[street]`.

#### Stacked bar chart (Altair, inline)
`create_bar_chart` does not support stacking so build inline:
```python
alt.Chart(agg_df).mark_bar().encode(
    x=alt.X('display_label:O', sort=sort_field, title='', axis=alt.Axis(labelAngle=-45)),
    y=alt.Y('boards_occupancy:Q', title='Zajętość mat (%)'),
    color=alt.Color('location_name:N', title='Lokacja'),
    tooltip=[display_label, location_name, boards_occupancy]
).properties(width=800, title='Zajętość mat w czasie')
```

#### Download
- `@st.fragment` wrapper
- One sheet per street, columns `okres`, `wszystkie_maty`, `zajete_maty`, `zajętość (%)`
- Same `build_export_sheet` logic as `occupancy_report.py`
- Filename: `raport_zajętości_mat_po_okresie_{streets}_{groups}_{start}_{end}`

#### Empty data guard
`st.info("Brak danych dla wybranego zakresu dat.")` + `st.stop()`

---

### 4. `main.py`

Add page declaration and register under `super-admin` Raporty list (after `occupancy_report`):
```python
boards_occupancy_time_period_report = st.Page(
    "navigation_pages/reports/boards_occupancy_time_period_report.py",
    title="Zajętość mat — okresy",
    icon=":material/bar_chart:"
)
```

---

## Key reused code

| What | Source |
|---|---|
| Occupancy calculation loop | `navigation_pages/reports/occupancy_report.py` lines 53–167 |
| `LAST_HOURS_AVAILABILITY` | `navigation_pages/boards_occupancy/safi_view.py` (import) |
| `get_slots_occupancy` | `shared/queries.py` |
| `run_in_parallel`, `street_to_location`, `parse_hour`, `get_day_of_week_string_shortcut`, `download_button` | `shared/utils.py` |
| `auth.filter_locations` | `shared/auth.py` |
| Period label + sort_key logic | `navigation_pages/reports/occupancy_report.py` granularity block |
| Export sheet builder | `navigation_pages/reports/occupancy_report.py` `build_export_sheet` function |

---

## Verification

1. Run `streamlit run main.py`, log in as `super-admin`.
2. Navigate to **Raporty → Zajętość mat — okresy**.
3. Select one location, one week → confirm occupancy values match `safi_view.py` / `occupancy_report.py` for the same location/week.
4. Select multiple locations → confirm stacked bars appear, one colour per location.
5. Switch all four granularities → confirm axis labels change correctly.
6. Uncheck "Pokazuj niepełny okres" → confirm current period bar disappears.
7. Check it → current period reappears.
8. Download `.xlsx` → verify one sheet per location, correct columns.
9. Select a date range with no data → confirm Polish info message.
