import sys

sys.path.append("shared")
sys.path.append("shared/queries")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import requests
import streamlit as st
import pandas as pd
import queries
import utils
import auth
from datetime import date, timedelta, datetime, timezone

from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")

min_date = date(2025, 1, 1)

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_initial_data()
  df = auth.filter_locations(df)

def get_data(start, end, use_start_date):
  url = "https://safi-api.pixel-xl.tech:9999/api/get-products-online"
  params = {
    "date_from": start,
    "date_to": end,
    "use_start_date": use_start_date
  }
  safi_auth_token = st.secrets["safi"].get("auth_token")
  headers = {
    "Authorization": f"Bearer {safi_auth_token}"
  }
  response = requests.get(url, params=params, headers=headers)
  data = response.json()
  response.raise_for_status()
  return pd.DataFrame(data)


def parse_data(df, cities=None):

  df = df.copy()
  df["brutto"] = df["brutto"].astype(float)
  df["netto"] = df["netto"].astype(float)

  df_sums = pd.DataFrame({
    "sum_online_brutto": [
      df.loc[df["payment_status"] == "Paid", "brutto"].sum()
    ],
    "sum_online_netto": [
      df.loc[df["payment_status"] == "Paid", "netto"].sum()
    ],
    "sum_onsite_brutto": [
      df.loc[df["payment_status"] != "Paid", "brutto"].sum()
    ],
    "sum_onsite_netto": [
      df.loc[df["payment_status"] != "Paid", "netto"].sum()
    ]
  })

  df_sums["sum_total_brutto"] = df_sums["sum_online_brutto"] + df_sums["sum_onsite_brutto"]
  df_sums["sum_total_netto"] = df_sums["sum_online_netto"] + df_sums["sum_onsite_netto"]

  df_by_location = (
    df.groupby("location_name")[["brutto", "netto", "total_reservations"]]
    .agg({"brutto": "sum", "netto": "sum", "total_reservations": lambda x: x.drop_duplicates().sum()})
    .reset_index()
    .rename(columns={"brutto": "sum_brutto", "netto": "sum_netto"})
  )

  df_reservations_paid = (
    df[df["payment_status"] == "Paid"]
    .groupby("location_name")["reservations_with_product"]
    .sum()
    .reset_index()
    .rename(columns={"reservations_with_product": "total_reservations_paid"})
  )

  df_reservations_not_paid = (
    df[df["payment_status"] != "Paid"]
    .groupby("location_name")["reservations_with_product"]
    .sum()
    .reset_index()
    .rename(columns={"reservations_with_product": "total_reservations_not_paid"})
  )

  df_reservations_by_location = (
    df_reservations_paid
    .merge(df_reservations_not_paid, on="location_name", how="outer")
    .merge(df_by_location[["location_name", "total_reservations"]], on="location_name", how="left")
  )
  df_reservations_by_location[["total_reservations_paid", "total_reservations_not_paid"]] = (
    df_reservations_by_location[["total_reservations_paid", "total_reservations_not_paid"]].fillna(0)
  )
  df_reservations_by_location["pct_addon_paid"] = (df_reservations_by_location["total_reservations_paid"] / df_reservations_by_location["total_reservations"] * 100).round(2)
  df_reservations_by_location["pct_addon_not_paid"] = (df_reservations_by_location["total_reservations_not_paid"] / df_reservations_by_location["total_reservations"] * 100).round(2)
  df_reservations_by_location["pct_addon_total"] = ((df_reservations_by_location["total_reservations_paid"] + df_reservations_by_location["total_reservations_not_paid"]) / df_reservations_by_location["total_reservations"] * 100).round(2)

  return df_sums, df_reservations_by_location


def product_stats(df, names, scope):

  if scope == "paid":
    df_scope = df[df["payment_status"] == "Paid"]
  elif scope == "not_paid":
    df_scope = df[df["payment_status"] != "Paid"]
  else:
    df_scope = df

  df_product = df_scope[df_scope["name"].isin(names)]
  reservations_with = df_product["reservations_with_product"].sum()
  brutto = df_product["brutto"].astype(float).sum()
  netto = df_product["netto"].astype(float).sum()

  total = (
    df.groupby("location_name")["total_reservations"]
    .first()
    .sum()
  )

  return int(reservations_with), int(total), brutto, netto


def product_by_visit_type(df, names, scope):
  if scope == "paid":
    df_scope = df[df["payment_status"] == "Paid"]
  elif scope == "not_paid":
    df_scope = df[df["payment_status"] != "Paid"]
  else:
    df_scope = df

  df_product = df_scope[df_scope["name"].isin(names)]

  df_by_visit = (
    df_product.groupby("visit_name")["reservations_with_product"]
    .sum()
    .reset_index()
    .rename(columns={"reservations_with_product": "count"})
    .sort_values("count", ascending=False)
  )
  return df_by_visit


def product_by_location(df, names, scope):
  if scope == "paid":
    df_scope = df[df["payment_status"] == "Paid"]
  elif scope == "not_paid":
    df_scope = df[df["payment_status"] != "Paid"]
  else:
    df_scope = df

  df_product = df_scope[df_scope["name"].isin(names)]

  df_by_loc = (
    df_product.groupby("location_name")["reservations_with_product"]
    .sum()
    .reset_index()
    .rename(columns={"reservations_with_product": "count"})
    .sort_values("count", ascending=False)
  )
  return df_by_loc


@st.fragment
def show_results():
  data = st.session_state["products_online_data"].copy()
  data["visit_name"] = data["visit_name"].replace({
    "Integracja firmowe": "Integracje",
    "Wycieczki szkolne": "Szkoły",
  })

  selected_locations = st.multiselect(
    "Lokalizacje",
    options=data["location_name"].unique(),
    default=data["location_name"].unique(),
  )
  if len(selected_locations) == 0:
    st.warning("Wybierz lokacje")
    st.stop()


  data = data[data["location_name"].isin(selected_locations)]

  selected_visit_types = st.multiselect(
    "Typ wizyty",
    options=data["visit_name"].unique(),
    default=data["visit_name"].unique(),
  )

  if len(selected_visit_types) == 0:
    st.warning("Wybierz typy wizyt")
    st.stop()

  data = data[data["visit_name"].isin(selected_visit_types)]

  df_sums, df_reservations_by_location = parse_data(data)

  total_paid = df_reservations_by_location["total_reservations_paid"].sum()
  total_not_paid = df_reservations_by_location["total_reservations_not_paid"].sum()
  total_res = df_reservations_by_location["total_reservations"].sum()
  pct_total = round((total_paid + total_not_paid) / total_res * 100, 2) if total_res else 0

  all_products = sorted(data["name"].dropna().unique().tolist())
  selected_products = st.multiselect("Wybierz produkt(y)", options=all_products, default=[])

  if len(selected_products) == 0:
    st.warning("Wybierz produkty")
    st.stop()

  def fmt(val):
    return f"{val:,.2f} PLN".replace(",", " ")

  if selected_products:
    paid_with = int(data[
      (data["payment_status"] == "Paid") & (data["name"].isin(selected_products))
    ]["reservations_with_product"].sum())
    not_paid_with = int(data[
      (data["payment_status"] != "Paid") & (data["name"].isin(selected_products))
    ]["reservations_with_product"].sum())
    pct_selected = round((paid_with + not_paid_with) / total_res * 100, 2) if total_res else 0
    st.metric(label="% Wizyt z produktem(ami)", value=f"{pct_selected}%")

  t1, t2, t3 = st.tabs(["Wszystkie", "Online", "Onsite"])

  def render_tab(scope, default_brutto, default_netto):
    if selected_products:
      _, _, brutto_p, netto_p = product_stats(data, selected_products, scope)
      col1, col2 = st.columns(2)
      with col1:
        st.metric(label="Brutto", value=fmt(brutto_p))
      with col2:
        st.metric(label="Netto", value=fmt(netto_p))
    else:
      col1, col2 = st.columns(2)
      with col1:
        st.metric(label="Brutto", value=fmt(default_brutto))
      with col2:
        st.metric(label="Netto", value=fmt(default_netto))

    if selected_products:
      st.divider()
      with_p, _, _, _ = product_stats(data, selected_products, scope)
      st.metric(label="Wizyty z produktem", value=f"{with_p}")

      df_by_visit = product_by_visit_type(data, selected_products, scope)
      df_by_loc = product_by_location(data, selected_products, scope)

      if df_by_visit.empty and df_by_loc.empty:
        st.info("Brak danych dla wybranych produktów w tym zakresie.")
        return

      if not df_by_loc.empty:
        st.markdown("**Sprzedaż wg lokalizacji**")
        st.altair_chart(utils.create_bar_chart(df_by_loc, "location_name", "Lokalizacja", "count", "Liczba rezerwacji", None), use_container_width=True)

      if not df_by_visit.empty:
        st.markdown("**Sprzedaż wg typu wizyty**")
        st.altair_chart(utils.create_bar_chart(df_by_visit, "visit_name", "Typ wizyty", "count", "Liczba rezerwacji", None), use_container_width=True)


  with t1:
    render_tab("all", df_sums["sum_total_brutto"].loc[0], df_sums["sum_total_netto"].loc[0])

  with t2:
    render_tab("paid", df_sums["sum_online_brutto"].loc[0], df_sums["sum_online_netto"].loc[0])

  with t3:
    render_tab("not_paid", df_sums["sum_onsite_brutto"].loc[0], df_sums["sum_onsite_netto"].loc[0])


@st.fragment
def view():
  today = date.today()
  col1, col2 = st.columns(2)

  with col1:
    start_date = st.date_input(
      "Od kiedy",
      value=today - timedelta(days=7),
      min_value=min_date,
      key="range_start"
    )

  with col2:
    end_date = st.date_input(
      "Do kiedy (włącznie)",
      value=today,
      min_value=min_date + timedelta(days=1),
      key="range_end"
    )

  dt_start = datetime.combine(start_date, datetime.min.time(), tzinfo=USER_TZ)
  utc_start = (
    dt_start
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )

  dt_end = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=USER_TZ)
  utc_end = (
    dt_end
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )

  date_type = st.selectbox("Wybierz rodzaj daty", ["Data stworzenia", "Data rozpoczecia"], key="date_type")
  use_start_date = True if date_type == "Data rozpoczecia" else False

  if st.button("Generuj"):
    with st.spinner("Ładowanie...", show_time=True):

      st.session_state["products_online_data"] = get_data(utc_start, utc_end, use_start_date)

  if "products_online_data" in st.session_state:
    show_results()


view()

utils.lazy_load_initials()