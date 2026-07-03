import sys
sys.path.append("shared/queries/report_queries")

import io
import streamlit as st
import reservations_report_queries
import pandas as pd
import utils
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")
min_date = date(2023, 1, 1)
QUARTER_LABELS = ["Q1", "Q2", "Q3", "Q4"]
SECTION_LABEL_ROWS = 1
ROWS_BETWEEN_TABLES = 2
FIXED_ROWS_PER_SECTION = 6
GROUP_ORDER = ["indywidualnie", "urodziny", "szkoły", "integracja firmowa", "imprezy zorganizowane"]

def to_utc_range(start, end):
  utc_start = (
    datetime.combine(start, datetime.min.time(), tzinfo=USER_TZ)
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )
  utc_end = (
    datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=USER_TZ)
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )
  return utc_start, utc_end


def view():
  today = date.today()
  years_possible = list(range(min_date.year, today.year + 1))
  selected_year = st.selectbox("Rok", years_possible, index=len(years_possible) - 1, key="report_year")

  utc_start, utc_end = to_utc_range(date(selected_year, 1, 1), date(selected_year, 12, 31))
  utc_start_prev, utc_end_prev = to_utc_range(date(selected_year - 1, 1, 1), date(selected_year - 1, 12, 31))

  if st.button("Generuj"):
    with st.spinner("Generowanie raportu...", show_time=True):

      data, data_prev, city_order = utils.run_in_parallel(
        (reservations_report_queries.get_reservations_count, (utc_start, utc_end)),
        (reservations_report_queries.get_reservations_count, (utc_start_prev, utc_end_prev)),
        (reservations_report_queries.get_city_opening_order, ())
      )

    df = (
      data.merge(data_prev, on=["month", "attraction_group", "city"], how="outer", suffixes=("", "_prev"))
      .fillna(0)
      .assign(
        month=lambda d: d["month"].astype(int),
        count=lambda d: d["count"].astype(int),
        count_prev=lambda d: d["count_prev"].astype(int),
      )
      .sort_values(["month", "attraction_group", "city"])
      .reset_index(drop=True)
    )

    present_cities = set(df["city"].dropna().unique())
    city_cols = [c for c in city_order if c in present_cities]
    city_cols += sorted(present_cities - set(city_cols))
    present_groups = set(df["attraction_group"].dropna().unique())
    groups = [g for g in GROUP_ORDER if g in present_groups]
    groups += sorted(present_groups - set(groups))
    tables = [(g, df[df["attraction_group"] == g]) for g in groups] + [("Razem", df)]

    def add_quarter(d):
      return d.assign(quarter=((d["month"] - 1) // 3 + 1).map(lambda q: f"Q{q}"))

    def count_pivot(d):
      p = d.groupby(["month", "city"])["count"].sum().unstack("city").reindex(columns=city_cols).fillna(0).astype(int)
      p.index.name = "miesiac"
      return p

    def count_quarter_pivot(d):
      p = (add_quarter(d).groupby(["quarter", "city"])["count"].sum()
           .unstack("city").reindex(index=QUARTER_LABELS, columns=city_cols).fillna(0).astype(int))
      p.index.name = "kwartal"
      return p

    def pct_pivot(d):
      g = d.groupby(["month", "city"])[["count", "count_prev"]].sum()
      p = (((g["count"] - g["count_prev"]) / g["count_prev"] * 100)
           .where(g["count_prev"] != 0).round(1).unstack("city").reindex(columns=city_cols))
      p.index.name = "miesiac"
      return p

    def pct_quarter_pivot(d):
      g = add_quarter(d).groupby(["quarter", "city"])[["count", "count_prev"]].sum()
      p = (((g["count"] - g["count_prev"]) / g["count_prev"] * 100)
           .where(g["count_prev"] != 0).round(1).unstack("city").reindex(index=QUARTER_LABELS, columns=city_cols))
      p.index.name = "kwartal"
      return p

    buf = io.BytesIO()
    last_col = len(city_cols)
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
      label_fmt = writer.book.add_format({"bold": True, "align": "center"})
      for sheet_name, monthly_fn, quarterly_fn in [
        ("Liczba", count_pivot, count_quarter_pivot),
        ("Zmiana %", pct_pivot, pct_quarter_pivot),
      ]:
        current_row = 0
        for label, d in tables:
          monthly = monthly_fn(d)
          quarterly = quarterly_fn(d)
          monthly.to_excel(writer, sheet_name=sheet_name, startrow=current_row + SECTION_LABEL_ROWS)
          quarterly.to_excel(writer, sheet_name=sheet_name, startrow=current_row + SECTION_LABEL_ROWS + len(monthly) + ROWS_BETWEEN_TABLES)
          ws = writer.sheets[sheet_name]
          ws.merge_range(current_row, 0, current_row, last_col, label, label_fmt)
          current_row += len(monthly) + len(quarterly) + FIXED_ROWS_PER_SECTION

    st.download_button(
      label="Pobierz plik .xlsx",
      data=buf.getvalue(),
      icon="⬇️",
      file_name=f"raport_rezerwacje_{selected_year}.xlsx",
      mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

view()
