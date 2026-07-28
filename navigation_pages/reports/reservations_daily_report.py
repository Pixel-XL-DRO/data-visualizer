import sys
sys.path.append("shared")
sys.path.append("shared/queries/report_queries")

import io
import calendar
import pandas as pd
import streamlit as st
import reservations_daily_report_queries as rdrq
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")
min_date = date(2025, 1, 1)

POLISH_MONTHS = [
  "styczeń", "luty", "marzec", "kwiecień", "maj", "czerwiec",
  "lipiec", "sierpień", "wrzesień", "październik", "listopad", "grudzień"
]

SAFI_LOCATIONS = {
  "krakow-lubicz": ("Kraków", "01976898-679c-70e0-9b4f-dc2a14131e3d"),
  "lodz-ogrodowa": ("Łódź", "01988093-0fa0-731f-9ca0-b864decd2e94"),
  "warszawa-kijowska": ("Warszawa", "019a1050-b96b-7032-baee-8a69101d49d4"),
  "poznan-swietego-marcina": ("Poznań", "019a39f1-045f-713a-834d-a66fb85287c5"),
  "katowice-sokolska": ("Katowice", "019ae347-fb95-73cd-84a3-5b2101273631"),
  "gdansk-grunwaldzka": ("Gdańsk", "019b3130-6834-7373-8b4b-c22d2b8b086a"),
  "warszawa-arkadia": ("Warszawa Box", "019bc67a-793e-705f-99db-3ee07379f1e1"),
  "bydgoszcz-szajnochy": ("Bydgoszcz", "019c6612-ff2e-711a-9646-78e9d3054c68"),
  "wroclaw-swidnicka": ("Wrocław", "019c32dd-e660-7073-8969-b350de2f45c9"),
}

LOCATION_ID_TO_CITY = {safi_id: city for city, safi_id in SAFI_LOCATIONS.values()}
ALL_CITIES = sorted({city for city, _ in SAFI_LOCATIONS.values()})
ALL_SAFI_IDS = {safi_id for _, safi_id in SAFI_LOCATIONS.values()}

VISIT_NAME_MAP = {
  "Integracja firmowa": "Integracje",
  "Wycieczki szkolne": "Szkoły",
  "Wycieczki szkolne / półkolonie": "Szkoły",
}


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


def build_day_range(start_date, end_date):
  days = []
  d = start_date
  while d <= end_date:
    days.append(d)
    d += timedelta(days=1)
  return days


def classify_role(role_name):
  return "Online" if not role_name else ("Host" if role_name == "Worker" else "CC")


def rows_to_df(reservations, date_field):
  columns = ["location_id", "city", "day", "source", "is_cancelled", "price", "visit_name"]
  records = []

  for r in reservations:
    location_id = r.get("location_id")
    city = LOCATION_ID_TO_CITY.get(location_id)
    if city is None:
      continue

    raw_dt = r.get(date_field)
    if not raw_dt:
      continue
    day = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).astimezone(USER_TZ).date()

    visit_name = r.get("visit_name")
    visit_name = VISIT_NAME_MAP.get(visit_name, visit_name)

    records.append({
      "location_id": location_id,
      "city": city,
      "day": day,
      "source": classify_role(r.get("role_name")),
      "is_cancelled": r.get("status") in ("CANCELLED", "CANCELLED_TO_RETURN"),
      "price": float(r.get("total_price_cents") or 0) / 100,
      "visit_name": visit_name,
    })

  if not records:
    return pd.DataFrame(columns=columns)

  return pd.DataFrame(records)


def mats_by_city(mats_by_location, allowed_safi_ids):
  totals = {}
  for entry in mats_by_location or []:
    location_id = entry.get("location_id")
    if location_id not in allowed_safi_ids:
      continue
    city = LOCATION_ID_TO_CITY.get(location_id)
    if city is None:
      continue
    mats = entry.get("mats_count")
    if mats is None:
      mats = entry.get("sum", 0)
    totals[city] = totals.get(city, 0) + float(mats or 0)
  return totals


def compute_metrics(df):
  count = len(df)
  cancelled = int(df["is_cancelled"].sum()) if count else 0
  price = df["price"].sum() if count else 0.0
  price_cancelled = df.loc[df["is_cancelled"], "price"].sum() if count else 0.0
  return count, cancelled, float(price), float(price_cancelled)


def previous_year_day(day):
  try:
    return day.replace(year=day.year - 1)
  except ValueError:
    return None


def yoy(count, cancelled, prev_count, prev_cancelled):
  diff_b = count - prev_count
  pct_b = round(diff_b / prev_count * 100, 1) if prev_count else None
  diff_c = cancelled - prev_cancelled
  pct_c = round(diff_c / prev_cancelled * 100, 1) if prev_cancelled else None
  return diff_b, pct_b, diff_c, pct_c


def build_row(label, count, cancelled, price, price_cancelled, mats_count, diff_b, pct_b, diff_c, pct_c, visit_name=None):
  row = {
    "Data": label,
    "Liczba rezerwacji": count,
    "Liczba anulowanych": cancelled,
    "Przychody spodziewane": round(price, 2),
    "Przychody na matę": round(price / mats_count, 2) if mats_count else None,
    "Przychody anulowane": round(price_cancelled, 2),
    "Przychody anulowane na matę": round(price_cancelled / mats_count, 2) if mats_count else None,
    "Zmiana liczbowa rezerwacji r/r": diff_b,
    "Zmiana % rezerwacji r/r": pct_b,
    "Zmiana liczbowa anulowanych r/r": diff_c,
    "Zmiana % anulowanych r/r": pct_c,
  }
  if visit_name is not None:
    row = {"Data": label, "Rodzaj atrakcji": visit_name, **{k: v for k, v in row.items() if k != "Data"}}
  return row


def build_city_rows(current_df, previous_df, days, mats_count, visit_names=None):
  rows = []
  groups = visit_names if visit_names else [None]

  for day in days:
    day_current_all = current_df[current_df["day"] == day]
    prev_day = previous_year_day(day)
    day_prev_all = previous_df[previous_df["day"] == prev_day] if prev_day is not None else previous_df.iloc[0:0]

    for visit_name in groups:
      day_current = day_current_all if visit_name is None else day_current_all[day_current_all["visit_name"] == visit_name]
      day_prev = day_prev_all if visit_name is None else day_prev_all[day_prev_all["visit_name"] == visit_name]

      count, cancelled, price, price_cancelled = compute_metrics(day_current)

      if prev_day is not None:
        prev_count, prev_cancelled, _, _ = compute_metrics(day_prev)
        diff_b, pct_b, diff_c, pct_c = yoy(count, cancelled, prev_count, prev_cancelled)
      else:
        diff_b = pct_b = diff_c = pct_c = None

      rows.append(build_row(
        day.strftime("%d.%m"), count, cancelled, price, price_cancelled,
        mats_count, diff_b, pct_b, diff_c, pct_c, visit_name,
      ))

  return rows


def build_suma_row(current_df, previous_df, mats_count, with_visit_col):
  count, cancelled, price, price_cancelled = compute_metrics(current_df)
  prev_count, prev_cancelled, _, _ = compute_metrics(previous_df)
  diff_b, pct_b, diff_c, pct_c = yoy(count, cancelled, prev_count, prev_cancelled)

  return build_row(
    "Suma", count, cancelled, price, price_cancelled,
    mats_count, diff_b, pct_b, diff_c, pct_c, "" if with_visit_col else None,
  )


def build_city_table(current_df_city, previous_df_city, mats_count, days, visit_names=None):
  rows = build_city_rows(current_df_city, previous_df_city, days, mats_count, visit_names)
  rows.append(build_suma_row(current_df_city, previous_df_city, mats_count, with_visit_col=visit_names is not None))
  return pd.DataFrame(rows)


def build_source_tables(current_df, previous_df, selected_cities, mats_map, days, breakdown_visit_names):
  tables = []

  for city in selected_cities:
    cur_c = current_df[current_df["city"] == city]
    prev_c = previous_df[previous_df["city"] == city]
    mats_count = mats_map.get(city, 0)
    tables.append((city, build_city_table(cur_c, prev_c, mats_count, days, breakdown_visit_names)))

  total_mats = sum(mats_map.get(c, 0) for c in selected_cities)
  cur_all = current_df[current_df["city"].isin(selected_cities)]
  prev_all = previous_df[previous_df["city"].isin(selected_cities)]
  tables.append(("Sumarycznie", build_city_table(cur_all, prev_all, total_mats, days, breakdown_visit_names)))

  return tables


def write_report(current_df, previous_df, selected_cities, mats_map, days, breakdown_visit_names):
  buf = io.BytesIO()

  sources = [("Sumarycznie", None), ("Online", "Online"), ("Host", "Host"), ("CC", "CC")]

  with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
    label_fmt = writer.book.add_format({"bold": True, "align": "center"})

    for sheet_name, source_filter in sources:
      cur_src = current_df if source_filter is None else current_df[current_df["source"] == source_filter]
      prev_src = previous_df if source_filter is None else previous_df[previous_df["source"] == source_filter]

      tables = build_source_tables(cur_src, prev_src, selected_cities, mats_map, days, breakdown_visit_names)
      last_col = len(tables[0][1].columns) - 1 if tables else 0

      current_row = 0
      for label, df in tables:
        df.to_excel(writer, sheet_name=sheet_name, startrow=current_row + 1, index=False)
        ws = writer.sheets[sheet_name]
        ws.merge_range(current_row, 0, current_row, last_col, label, label_fmt)
        current_row += len(df) + 3

  return buf.getvalue()


@st.fragment
def render_results(current_raw, previous_raw, mats_by_location, days, use_start_date, allowed_cities, allowed_safi_ids, start_date, end_date):
  date_field = "start_at" if use_start_date else "created_at"

  current_df = rows_to_df(current_raw, date_field)
  previous_df = rows_to_df(previous_raw, date_field)

  current_df = current_df[current_df["location_id"].isin(allowed_safi_ids)]
  previous_df = previous_df[previous_df["location_id"].isin(allowed_safi_ids)]

  current_df = current_df[(current_df["day"] >= days[0]) & (current_df["day"] <= days[-1])]

  mats_map = mats_by_city(mats_by_location, allowed_safi_ids)

  all_visit_names = sorted(current_df["visit_name"].dropna().unique().tolist())

  selected_cities = st.multiselect("Miasta", allowed_cities, default=allowed_cities, key="daily_report_cities")
  selected_visit_names = st.multiselect("Grupy atrakcji", all_visit_names, default=all_visit_names, key="daily_report_visit_names")
  breakdown = st.checkbox("Rozdziel rodzaje atrakcji", key="daily_report_breakdown")

  if not selected_cities:
    st.warning("Wybierz co najmniej jedno miasto!")
    return
  if not selected_visit_names:
    st.warning("Wybierz co najmniej jedną grupę atrakcji!")
    return

  current_df = current_df[current_df["visit_name"].isin(selected_visit_names)]
  previous_df = previous_df[previous_df["visit_name"].isin(selected_visit_names)]

  breakdown_visit_names = selected_visit_names if breakdown else None

  xlsx_bytes = write_report(current_df, previous_df, selected_cities, mats_map, days, breakdown_visit_names)

  st.download_button(
    label="Pobierz plik .xlsx",
    data=xlsx_bytes,
    icon="⬇️",
    file_name=f"raport_dzienny_rezerwacji_{start_date}_{end_date}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="daily_report_download",
  )


def view():
  today = date.today()
  current_year = today.year

  mode_col, type_col = st.columns(2)
  with mode_col:
    mode = st.selectbox("Tryb", ["Miesiąc", "Dzień"], key="daily_report_mode")
  with type_col:
    date_type = st.selectbox("Rodzaj daty", ["Data stworzenia", "Data odbycia"], key="daily_report_date_type")
  use_start_date = date_type == "Data odbycia"

  if mode == "Miesiąc":
    year_col, month_col = st.columns(2)
    with year_col:
      year = st.selectbox("Rok", list(range(min_date.year, current_year + 1)), index=current_year - min_date.year, key="daily_report_year")
    with month_col:
      month = st.selectbox(
        "Miesiąc", list(range(1, 13)),
        format_func=lambda m: POLISH_MONTHS[m - 1],
        index=today.month - 1,
        key="daily_report_month",
      )
    start_date = date(year, month, 1)
    end_date = date(year, month, calendar.monthrange(year, month)[1])
  else:
    day = st.date_input("Dzień", value=today - timedelta(days=1), min_value=min_date, key="daily_report_day")
    start_date = end_date = day

  current_params = {"use_start_date": use_start_date, "start_date": start_date, "end_date": end_date}

  if st.button("Generuj", key="daily_report_generate"):
    with st.spinner("Generowanie raportu...", show_time=True):
      utc_start, utc_end = to_utc_range(start_date, end_date)
      data = rdrq.get_reservations_report(utc_start, utc_end, use_start_date)
      st.session_state["daily_report_payload"] = {
        "current": data.get("current", []),
        "previous_year": data.get("previous_year", []),
        "mats_by_location": data.get("mats_by_location", []),
        "days": build_day_range(start_date, end_date),
        "use_start_date": use_start_date,
        "start_date": start_date,
        "end_date": end_date,
        "params": current_params,
      }

  payload = st.session_state.get("daily_report_payload")
  if payload and payload["params"] == current_params:
    render_results(
      payload["current"], payload["previous_year"], payload["mats_by_location"],
      payload["days"], payload["use_start_date"], ALL_CITIES, ALL_SAFI_IDS,
      payload["start_date"], payload["end_date"],
    )


view()
