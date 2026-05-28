# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the app

```bash
streamlit run main.py
```

Dependencies are in `requirements.txt`. Install with `pip install -r requirements.txt`.

## Architecture

**Data Visualizer** is a Streamlit multi-page app that visualizes business data (reservations, income, POS, reviews, vouchers). The UI is in Polish.

### Entry point

`main.py` defines all pages and routes them by user role (`super-admin`, `admin`, `manager`, `marketing`). Pages live under `navigation_pages/` and reports under `navigation_pages/reports/`.

### Shared modules (`shared/`)

All pages prepend `shared/` to `sys.path` to import:

- **`queries.py`** — BigQuery clients and core data fetching functions. Has four clients: `client` (main), `reviews_client`, `performance_reviews_client`, `sandbox_client`. Functions use `@st.cache_data(ttl=28800)` for caching.
- **`utils.py`** — Chart creation (`create_chart_new` with Plotly, `create_chart`/`create_bar_chart` with Altair), `download_button`, `run_in_parallel` (ThreadPoolExecutor), date/time helpers, `street_to_location` mapping, and `lazy_load_initials`.
- **`auth.py`** — Firebase-based auth. `authorize(roles)` checks login and role. `filter_locations(df)` filters a DataFrame to the user's permitted locations (expects `city` and `street` columns).
- **`shared/queries/`** — Per-feature query modules (e.g. `reservations_queries.py`, `income_queries/`).
- **`shared/sidebars/`** — Per-feature sidebar modules. Each exports a `filter_data(...)` function that renders sidebar widgets and returns filter state.

### Page pattern

Each navigation page follows this structure:
1. `sys.path.append("shared")` + other path appends as needed
2. Load initial/filter data (typically with `utils.run_in_parallel(...)`)
3. Apply `auth.filter_locations(df)` for location-based access control
4. Call `sidebar.filter_data(df)` to render sidebar and get filter values
5. Load detailed data based on filters (with `run_in_parallel` where possible)
6. Render charts with `utils.create_chart_new` (Plotly) or `utils.create_chart`/`create_bar_chart` (Altair)

### Data sources

- **BigQuery** (`pixelxl-database-dev` project): datasets `reservation_data`, `POS_system_data`, `vouchers_data`, `performance_data`, `reviews`
- **Firebase Firestore**: user roles and locations stored in `streamlitUserRoles` collection
- Credentials come from `st.secrets` (`gcp_service_account`, `gcp_reviews_account`, `gcp_performance_reviews_account`, `gcp_sandbox_account`, `firebase`)

### Key conventions

- Pages under `navigation_pages/reports/` import their sidebar from `shared/sidebars/` and queries from `shared/queries/report_queries/`
- Use `utils.run_in_parallel()` when fetching multiple independent datasets
- BigQuery query functions should use `@st.cache_data(ttl=28800)` unless the data changes frequently
- Location filtering (`city + "-" + street`) must be applied after fetching data via `auth.filter_locations(df)`
- The `download_button` utility in `utils.py` supports multi-sheet `.xlsx` and `.csv` export
