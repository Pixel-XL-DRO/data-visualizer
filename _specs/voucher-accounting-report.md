# Spec for voucher-accounting-report

branch: vouchers-for-finances (existing branch — no new branch created per user request)

## Summary

Build a single new report screen for the accounting department ("Raport voucherów – księgowość") under `navigation_pages/reports/`. The user picks one shared date range (styled like the range picker in `financial_report.py` / `voucher_report.py`) and, on submit, the app independently computes and displays **four** voucher sub-reports — Purchased, Cancelled, Redeemed, Expired — each in its own section with a row count, sum of gross amount, and its own "Pobierz .xlsx" download button (pattern from `utils.download_button`, as used in `voucher_report.py` and `financial_report.py`).

Data for Purchased, Redeemed, and Expired comes from three Safi-API accounting endpoints already planned in `work/md-docs/safi-api/plans/voucher-accounting-reports.md` (repo: `eparagony-safi-api`). Cancelled has **no existing endpoint** — a new one must be added to that same backend, reusing the purchased-report query with a cancellation-date filter. All four endpoints are called the same way `voucher_report.py` calls `/api/promo_codes_report` and `financial_report.py` calls `/api/receipts` — via `requests.get`, `Authorization: Bearer {st.secrets["safi"].get("auth_token")}`, base URL `https://safi-api.pixel-xl.tech:9999`.

Each sub-report uses its own reference date field for range filtering (purchase date, cancellation date, reservation start date, expiry date, respectively) — all four queries share the same user-selected `[start_date, end_date)` range, converted to UTC ISO 8601 exactly like `financial_report.py` (`Europe/Warsaw` local range → UTC `Z`-suffixed strings).

## Functional Requirements

- New page module, e.g. `navigation_pages/reports/voucher_accounting_report.py`, added to `main.py`'s `st.Page` list and role-based nav (at least for whichever roles currently see `voucher_report_page` / accounting-relevant reports — confirm with user which roles: `super-admin`, `admin`, `manager`?).
- One date-range UI, modeled on `financial_report.py`'s "Zakres" mode (`st.date_input` "Od kiedy" / "Do kiedy", `min_date`, Warsaw-timezone → UTC conversion) or the simpler two-`date_input` layout in `voucher_report.py`. No "Miesiąc" mode is required unless the user wants parity with `financial_report.py` — flag as an open question.
- A single "Generuj raport" button (or `st.fragment`, matching `voucher_report.py`'s `@st.fragment` + button pattern) triggers all four API calls, run in parallel via `utils.run_in_parallel` (per `shared/utils.py` convention for independent fetches).
- Four independent result sections, each showing:
  - Section title (Polish): "Vouchery zakupione", "Vouchery anulowane", "Vouchery zrealizowane", "Vouchery, które straciły ważność"
  - Row count and sum of `kwota brutto` (gross) — matching the "Suma NETTO ..." summary pattern used in `financial_report.py`/`voucher_report.py`
  - A preview table (`st.write(pd.DataFrame(...))` or `st.dataframe`)
  - A dedicated `utils.download_button({...}, file_name, label=...)` producing one `.xlsx` per sub-report (not one workbook with 4 sheets, per the "pobrać każdy osobno" requirement)
- Locations: confirm whether this report is scoped to all Safi locations (like `voucher_report.py`, which passes every location the user is authorized for) or needs its own location filter — default to reusing `auth.filter_locations` + the same `SAFI_LOCATIONS` mapping pattern already in `main.py`/`voucher_report.py`/`financial_report.py`.

### Field mapping per sub-report

**1. Vouchery zakupione** — endpoint `GET /api/get-vouchers-purchased` (per the existing plan doc, needs the receipt-field change below), filtered on `vouchers_orders.created_at` (purchase date) in range:

| Report column | Source |
|---|---|
| numer kodu vouchera | `vouchers_codes.code` → `voucher_code` |
| kwota netto | computed from `vouchers_codes.price` + VAT rate → `net_amount` |
| podatek VAT | computed → `vat_tax` |
| stawka VAT | `vat_rates.vat_rate` → `vat_rate` |
| kwota brutto | `vouchers_codes.price` → `gross_amount` |
| data zakupu | `vouchers_orders.created_at` → `purchase_date` |
| data anulowania | `vouchers_orders_statuses` (status=CANCELLED) → `cancellation_date` |
| nr paragonu | **iteration 1: e-paragon URL only** (see note below) |
| nr faktury | not stored anywhere yet → `invoice_number` (NULL) |
| nr zamówienia Safi | `vouchers_orders.number` → `safi_order_number` |
| email / imię / nazwisko | `vouchers_orders.customer_email/name/surname` |

**2. Vouchery anulowane** — **no existing endpoint**; needs a new one, e.g. `GET /api/get-vouchers-cancelled`, filtered on the cancellation-status timestamp (`vouchers_orders_statuses.created_at WHERE status='CANCELLED'`) in range. Field set proposed as the same shape as "zakupione" (voucher code, net/VAT/gross, purchase date, cancellation date, receipt/invoice/Safi order numbers, buyer email/name/surname), since the plan doc has no separate schema for this — **exact final field list, and whether a cancellation reason/status field exists in Safi, is an open question** (see below).

**3. Vouchery zrealizowane** — endpoint `GET /api/get-vouchers-redeemed` (per plan doc), filtered on `reservations.start_at` (reservation date) in range, only `reservations_vouchers.voucher_blocked = 0`:

| Report column | Source |
|---|---|
| numer kodu vouchera | `vouchers_codes.code` |
| kwota netto / podatek VAT / stawka VAT / kwota brutto | computed from `reservations_vouchers.discount_amount` + VAT rate |
| numer rezerwacji | `reservations.number` |
| data odbycia się rezerwacji | `reservations.start_at` |
| nr zamówienia Safi | `vouchers_orders.number` |
| email / imię / nazwisko | **voucher buyer** (`vouchers_orders.customer_*`), not the reservation's own customer — plan doc already confirms this |

**4. Vouchery, które straciły ważność** — endpoint `GET /api/get-vouchers-expired` (per plan doc), filtered on `vouchers_codes.valid_to` (expiry date) in range, only `status='PAID'` with remaining balance > 0:

| Report column | Source |
|---|---|
| numer kodu vouchera | `vouchers_codes.code` |
| kwota netto / podatek VAT / stawka VAT / kwota brutto | `vouchers_codes.amount − sum(unblocked discount_amount)` + VAT rate (the unused remainder that expired) |
| data wygaśnięcia | `vouchers_codes.valid_to` |
| nr paragonu | iteration 1: e-paragon URL only (same note as report 1) |
| nr faktury | NULL for now |
| nr zamówienia | NULL for now (`order_number`, distinct from Safi order number — plan doc lists both `order_number` and `safi_order_number` as separate columns for this report only) |
| nr zamówienia Safi | `vouchers_orders.number` |
| email / imię / nazwisko | `vouchers_orders.customer_*` |

**Receipt field note (iteration 1 constraint):** the plan doc's SQL currently sources "nr paragonu" from the local `receipts.number` column. Per the user's instruction, iteration 1 must use **only the e-paragon URL** (the same `document_url` field `financial_report.py` already reads from the separate `/api/receipts` Safi endpoint), not a receipt number — Safi does not yet reliably populate the number. The backend query/column should be renamed/added as `receipt_url` (or exposed alongside a currently-unused `receipt_number` placeholder) so a future iteration can add the number without breaking the report's shape. The frontend should render this column as a clickable link.

### Date filtering logic (per sub-report)

All four share one user-selected range `[start_date, end_date)`, converted Warsaw-local → UTC like `financial_report.py`. Each endpoint call passes `from_dt`/`to_dt` but applies it to a different backend column:

| Sub-report | Reference date column |
|---|---|
| Zakupione | `vouchers_orders.created_at` |
| Anulowane | `vouchers_orders_statuses.created_at` where `status = 'CANCELLED'` |
| Zrealizowane | `reservations.start_at` |
| Wygasłe | `vouchers_codes.valid_to` |

### XLSX export

- One `.xlsx` per sub-report (4 separate download buttons), via `utils.download_button({sheet_name: df}, file_name)` — single-sheet workbooks, consistent with `voucher_report.py`'s per-visit-type export.
- Suggested file naming, consistent with existing reports' `raport_..._{start_date}-{end_date}` convention:
  - `raport_voucherow_zakupione_{start_date}-{end_date}.xlsx`
  - `raport_voucherow_anulowane_{start_date}-{end_date}.xlsx`
  - `raport_voucherow_zrealizowane_{start_date}-{end_date}.xlsx`
  - `raport_voucherow_wygasle_{start_date}-{end_date}.xlsx`
- Column headers in the exported file should be the Polish labels listed under "Zakres" above (not the English backend field names), matching how `voucher_report.py`/`financial_report.py` build their export DataFrames with Polish keys.

### Backend changes required (in `eparagony-safi-api`, not this repo)

1. Implement the three endpoints already sketched in `voucher-accounting-reports.md` (`get-vouchers-purchased`, `get-vouchers-redeemed`, `get-vouchers-expired`) if not yet done.
2. Update the "purchased" and "expired" SQL/response to swap `receipt_number` for a `receipt_url` field (or add it alongside), per the receipt-field note above.
3. Add a **new fourth endpoint** for cancelled vouchers (not in the current plan doc) — same shape as purchased, filtered by cancellation timestamp instead of purchase timestamp.

### Frontend changes required (in this repo, `data-visualizer`)

1. New page file under `navigation_pages/reports/` with the date-range UI + 4 result sections + 4 download buttons, following the `voucher_report.py` fetch/render/export pattern (Safi API call via `requests`, not BigQuery).
2. Register the page in `main.py`'s page list and appropriate role-based navigation groups.
3. No new `shared/sidebars/` module is needed if the date-range picker is simple enough to live inline in the page (as in `voucher_report.py`), but flag as an open question if a shared sidebar-style filter is preferred instead.

## Possible Edge Cases

- Empty result set for any of the 4 sub-reports within the selected range (show "Brak danych w tym okresie" per existing convention, and skip/disable that section's download button).
- Voucher purchased and cancelled in the same selected range should appear in both "Zakupione" (with `cancellation_date` populated) and "Anulowane" — this is expected, not a duplicate bug.
- Voucher redeemed by someone other than the buyer — confirm buyer identity (not redeemer) is always available even when the reservation was made by a different customer account.
- Vouchers expiring with zero remaining balance must be excluded from "Wygasłe" (per plan doc: `gross_amount > 0` filter) — worth surfacing in UI copy so accounting doesn't expect fully-used vouchers to show up.
- Large date ranges — no pagination is defined in the plan doc's endpoints; confirm expected volume doesn't require it.
- Timezone edge at range boundaries (midnight Warsaw vs UTC) — reuse `financial_report.py`'s exact conversion logic to avoid off-by-one-day discrepancies.

## Acceptance Criteria

- One date range selector controls all four sub-reports.
- Four sub-reports render independently, each with row count + gross-amount sum + preview table + its own XLSX download button.
- Each sub-report filters by its own correct reference date (purchase / cancellation / reservation / expiry), not a single shared date column.
- "Vouchery zrealizowane" shows the voucher **buyer's** email/name/surname, never the reservation customer's, when they differ.
- "Nr paragonu" is populated from the e-paragon URL only; no receipt-number field is shown in iteration 1.
- Expired vouchers with zero remaining balance are excluded.
- Exported XLSX files use Polish column headers and the `raport_..._{start}-{end}.xlsx` naming convention, one file per sub-report.

## Open Questions

- Exact final field list for "Vouchery anulowane" — does Safi track a cancellation reason/status beyond the timestamp? Needs confirmation from whoever owns the `eparagony-safi-api` schema.
- Which roles should see this new report page (all of `super-admin`/`admin`/`manager`, or a narrower "accounting" audience)?
- Should the date-range UI offer a "Miesiąc" (month) mode like `financial_report.py`, or is a plain from/to range (like `voucher_report.py`) sufficient?
- Is a shared `shared/sidebars/` module wanted for consistency, or is an inline date picker (as in `voucher_report.py`) acceptable given there's only one filter?
- Should all four sub-reports also be combinable into one multi-sheet XLSX in addition to the four separate downloads, or strictly four separate files as specified?
- Timing/ownership: who implements the new "cancelled" endpoint and the receipt_url change in `eparagony-safi-api` — is that in scope for this ticket or a separate backend ticket to coordinate?
- Confirm whether "kwota netto"/"podatek VAT" should be computed the same way across all four reports (net = gross / (1 + rate), consistent with the plan doc's SQL) or if accounting expects a different net/VAT derivation for any sub-report.
