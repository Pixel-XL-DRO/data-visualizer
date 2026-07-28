# Spec for Reservations Daily Report

branch: reservation-report-DRO-383

## Summary

Add a new downloadable `.xlsx` report under `navigation_pages/reports/` that presents day-by-day reservation numbers and expected revenue in a tabular format, broken down by city. The report is filterable by date type (creation vs. visit date), one or more cities, one or more attraction groups, and a date range (either a whole month of a given year, or a single specific day). Results are further split into tabs by booking source (Sumarycznie / Online / Host / CC), mirroring the role-based breakdown already used in `bok_income_report.py`. An optional "Rozdziel rodzaje atrakcji" checkbox adds an extra column that splits each day's row by attraction/visit type, matching the existing "Rozdziel ..." checkbox pattern used in `reservations_sidebar.py`.

Data is sourced from a new dedicated SAFI endpoint, `GET /api/reservations_report`, rather than from BigQuery directly. Given `from_dt`/`to_dt` (ISO 8601 UTC), it returns the current period's reservations (`current`), the same period shifted back one year (`previous_year`, computed server-side), and per-location mat/board counts (`mats_by_location`) — all in a single call, so the frontend does not need a separate query for year-over-year data.

## Functional Requirements

- **Filters**
  - Rodzaj daty: single choice between "Data stworzenia" and "Data odbycia" (creation date vs. visit date). The endpoint returns both `created_at` and `start_at` per reservation, so this choice determines which field the day-by-day rows are bucketed by; see the Data fetching note below on how this interacts with the range filter.
  - Miasta: multi-select, one or more cities, filtered to the locations permitted by the user's role (`auth.filter_locations`). The endpoint's `location_id` corresponds to the `safi_id` values in the static `safi_locations` list already defined in `financial_report.py` (label ↔ safi_id) — reuse that mapping to group reservations by city instead of introducing a new one.
  - Grupy atrakcji: multi-select, one or more attraction groups. Per confirmation, this operates directly on the distinct `visit_name` values returned by the endpoint — no separate attraction-group mapping/lookup is used.
  - Zakres daty: user chooses one of two modes:
    - a specific month within a specific year, or
    - a single specific day.
    - The range may cross a calendar-year boundary (e.g. December into January).
  - Checkbox "Rozdziel rodzaje atrakcji": when enabled, adds an "Rodzaj atrakcji" column/breakdown to each city table so values are shown per attraction type (i.e. per `visit_name`) rather than only aggregated per city.

- **Data fetching**
  - Convert the selected date range (month, or single day) to `from_dt`/`to_dt` in ISO 8601 UTC, following the same conversion pattern already used in `reservations_report.py` (`to_utc_range`).
  - Call `GET /api/reservations_report` with `from_dt`/`to_dt` (same auth pattern as `bok_income_report.py`: bearer token from `st.secrets["safi"]`).
  - The response provides three pieces of data in one round trip:
    - `current` — one row per reservation created in the range, with `id`, `location_id`, `location_name`, `visit_name`, `status`, `total_price_cents` (expected revenue in cents — divide by 100 for PLN), `created_at`, `start_at` (visit date/time), and `role_name` (the role of whoever set the reservation's status to `NEW`, i.e. its creator; `NULL` when there's no matching status/user/role row). The payload also includes `brutto_cents`/`netto_cents` (settled amounts net of discounts/vouchers/products/VAT) — these are not used by this report, which needs the pre-discount expected total (`total_price_cents`) for columns D/F.
    - `previous_year` — the same shape, for the range shifted back one year (computed server-side).
    - `mats_by_location` — total mat count per `location_id` (`SUM(mats_count)` from the `areas` table), a current snapshot rather than a historical value.
  - Note: the `WHERE` filter on this endpoint is always based on `r.created_at`, regardless of which "Rodzaj daty" is selected in the UI. When "Data odbycia" is selected, rows are bucketed by each reservation's `start_at` day, but the underlying set of reservations fetched is still bounded by their creation date falling in `from_dt`/`to_dt` — see Possible Edge Cases.
  - City, attraction group, and Online/Host/CC source filtering/splitting are all done client-side over the raw reservation lists in `current`/`previous_year`:
    - Source classification reuses the `bok_income_report.py` logic directly against `role_name`: no `role_name` → Online, `role_name == "Worker"` → Host, otherwise → CC. No separate call to `get_bok_income_reservations` is needed — `role_name` is already present on this endpoint's payload.
    - Cities are derived from `location_id` via the `safi_locations` mapping in `financial_report.py`; attraction groups/breakdown use `visit_name` directly, unmapped.

- **Report structure / layout**
  - The report is organized into tabs (Excel sheets) by booking source: default tab "Sumarycznie" (all sources combined), plus "Online", "Host", "CC" — using the same role-based classification already implemented in `bok_income_report.py`, applied to the `role_name` field returned directly by `/api/reservations_report` (role_name absent → Online, role_name "Worker" → Host, otherwise → CC).
  - Within each tab, data is grouped into stacked tables, one per selected city, each preceded by a heading row with the city name (same visual pattern as the existing annual attraction-group report in `reservations_report.py`, but with cities as the grouping headings instead of attraction groups).
  - If only one city is selected, only that single city heading/table appears.
  - A final "Sumarycznie" heading and table appears at the bottom of each tab, totalling all selected cities together.
  - Each city's table has one row per day within the selected range, plus a final "Suma" row totalling all columns for that city over the whole range.

- **Table columns** (per day, per city)
  - A: Data — the day, formatted like `14.07`.
  - B: Liczba rezerwacji tego dnia, including cancelled reservations.
  - C: Liczba rezerwacji tego dnia, które zostały anulowane (`status == "CANCELLED"`).
  - D: Przychody spodziewane (`total_price_cents / 100`) — not-yet-paid/expected revenue, summed across all reservations that day including cancelled ones.
  - E: Przychody na matę — column D divided by the number of mats available for that city (via `mats_by_location`, summed across all locations mapped to that city).
  - F: Przychody z rezerwacji anulowanych — same `total_price_cents / 100` basis, but summed only over cancelled reservations.
  - G: Przychody z rezerwacji anulowanych na matę — column F divided by the number of boards/mats.
  - Zmiana liczbowa r/r dla B i C — the difference vs. the same day in the previous year.
  - Zmiana procentowa r/r dla B i C — the percentage change vs. the same day in the previous year.

- **Export**
  - Report downloads as a single `.xlsx` file via `utils.download_button`, with one sheet per source tab (Sumarycznie / Online / Host / CC).
  - Formatting (city heading rows, "Suma" row, sheet layout) should follow the visual conventions of the existing `reservations_report.py` annual report.

- **Access control**
  - Page restricted to the `super-admin` role only; no per-viewer restriction on the Online/Host/CC tabs is needed.
  - Apply `auth.filter_locations` before showing city options in the sidebar, consistent with all other reports.

## Possible Edge Cases

- A selected city may have no reservations at all in the chosen range — its table should still render with zero-filled rows rather than being skipped.
- `mats_by_location` is confirmed to be a current, non-historical snapshot (`SUM(mats_count)` grouped by `location_id` from the `areas` table) — if a location's mat count changed within the selected range, or differs between the current range and the previous-year range, the per-mat columns (E, G) will use today's count rather than the count that was actually valid on that day.
- `mats_by_location` is keyed by `location_id`, not by city — a city with multiple locations needs its mat counts summed across all of its locations before computing columns E/G.
- Year-over-year comparison for a day near Feb 29 in a leap year has no exact equivalent day in a non-leap previous year — per the answer above, show nothing (blank) for that day's comparison rather than a computed fallback.
- If the previous year has zero reservations for a given day, the percentage-change columns would divide by zero — must show blank/N/A instead of an error.
- A single-day range still needs a heading per city and a "Suma" row, even though there's only one data row.
- "Rozdziel rodzaje atrakcji" combined with multiple cities and multiple attraction groups could produce a very large sheet — layout must remain readable (e.g. attraction type as a sub-grouping within each city's daily rows). Since the endpoint returns `visit_name` directly, this breakdown column can use it as-is without needing an attraction-group mapping.
- `role_name` is `NULL` whenever there's no `status = 'NEW'` row for a reservation, or no matching user/role (the query uses `LEFT JOIN`s throughout) — this is the intended signal for classifying a reservation as "Online" and must not be treated as missing/bad data.
- Cancelled reservations should still count toward columns B and D (sum everything, cancelled and not), which is the opposite of the "exclude cancelled" filtering used in most existing reports — must make sure cancelled records are not filtered out when processing the `current`/`previous_year` payloads.
- When "Data odbycia" is selected, reservations are bucketed by `start_at` day, but the endpoint only filters by `created_at` range — a reservation created well before `from_dt` for a visit that falls inside the selected range would be missed, and one created inside the range for a visit outside it would need to be excluded client-side. The request's `from_dt`/`to_dt` (based on `created_at`) may need to be widened beyond the displayed range to reliably capture all reservations whose `start_at` falls within it.
- `total_price_cents`, `brutto_cents`, and `netto_cents` are all integer cents — must divide by 100 consistently; only `total_price_cents` is relevant to this report's D/F columns.

## Acceptance Criteria

- [ ] Report page appears under the `Raporty` navigation section.
- [ ] Sidebar allows: date type (creation/visit) selection, multi-select cities, multi-select attraction groups, and a date range chosen either as a month+year or a single day.
- [ ] "Rozdziel rodzaje atrakcje" checkbox toggles an attraction-type breakdown within each city table.
- [ ] Downloaded `.xlsx` contains one sheet per source tab: Sumarycznie, Online, Host, CC.
- [ ] Each sheet shows one table per selected city (with a city heading), plus a final "Sumarycznie" heading/table totalling all cities.
- [ ] Each city table has columns A–G as specified, a row per day, and a final "Suma" row.
- [ ] Columns B and C show both a numeric and a percentage year-over-year change.
- [ ] Locations not permitted for the user's role never appear as filter options.
- [ ] Cities/tables with zero reservations in range render with zero values rather than being omitted or erroring.

## Open Questions

- Should the `from_dt`/`to_dt` sent to `/api/reservations_report` be widened automatically when "Data odbycia" is selected (to catch reservations created outside the displayed range but visited within it), and if so by how much (e.g. a fixed lookback based on typical booking lead time)? use endpoint reservations_report_start_at or reservations_report_created_at based on this
