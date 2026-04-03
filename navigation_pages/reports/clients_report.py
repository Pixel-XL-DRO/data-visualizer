import sys
import utils
import streamlit as st
from datetime import datetime, timedelta, timezone
import queries
from zoneinfo import ZoneInfo
import requests
import plan4u_dump_queries

sys.path.append("shared/queries")
USER_TZ = ZoneInfo("Europe/Warsaw")

attraction_map = {
  "Indywidualne": "Indywidualnie",
  "Indywidualnie": "Indywidualnie",
  "Integracje": "Integracja Firmowa",
  "Integracja firmowa": "Integracja Firmowa",
  "Integracja Firmowa": "Integracja Firmowa",
  "Wycieczki szkolne / półkolonie": "Szkoły",
  "Wycieczki szkolne": "Szkoły",
  "Szkoły": "Szkoły",
  "Urodziny": "Urodziny",
  "Blokada": "Blokada"
};

city_names_map = {
  "krakow lubicz": "Kraków",
  "lodz ogrodowa": "Łódź",
  "katowice": "Katowice",
  "poznan": "Poznań",
}

def get_clients(iso_start, iso_end, attractions, clients_from_p4u):

  if len(attractions) == 0:
    st.error("Wybierz atrakcje")
    return

  safi_auth_token = st.secrets["safi"].get("auth_token")

  url = "https://safi-api.pixel-xl.tech:9999/api/get_clients_from_period_without_reservation_ahead"

  params = {
    "from_dt": iso_start,
    "to_dt": iso_end,
  }

  headers = {
    "Authorization": f"Bearer {safi_auth_token}"
  }

  response = requests.get(url, params=params, headers=headers)

  data = response.json()
  response.raise_for_status()

  data.extend(clients_from_p4u)

  parsed = {
    "Wszystkie": []
  }

  for client in data:

    visit_name = client["visit_name"]
    city = client["location_name"]

    client_attraction_name = attraction_map[visit_name]

    if client_attraction_name not in attractions:
      continue

    if city in city_names_map:
      city = city_names_map[city]

    if city not in parsed:
      parsed[city] = []

    parsed[city].append(client)
    parsed["Wszystkie"].append(client)

  return parsed


def view():

  with st.spinner("Ładowanie danych...", show_time=True):
    init = queries.get_initial_data()
    attractions = init["attraction_group"].unique()

  st.info("Klienci, którzy mieli rezerwacje w danym okresie, a nie mają w przyszłości")

  date_col1, date_col2 = st.columns(2)

  now = datetime.now()
  with date_col1:
    start_date = st.date_input("Podaj date poczatku", now - timedelta(days=7), key="start_date", max_value=now - timedelta(days=1))

  with date_col2:
    end_date = st.date_input("Podaj date końca", now, key="end_date")

  dt_start_date = datetime.combine(start_date, datetime.min.time(), tzinfo=USER_TZ)
  dt_end_date = datetime.combine(end_date, datetime.min.time(), tzinfo=USER_TZ)

  utc_start = (
    dt_start_date
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )

  dt_end = datetime.combine(dt_end_date + timedelta(days=1), datetime.min.time(), tzinfo=USER_TZ)

  utc_end = (
    dt_end
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )

  clients_from_p4u = plan4u_dump_queries.get_clients(utc_start, utc_end)

  selected_attractions = st.multiselect("Wybierz atrakcje", attractions, default=attractions[:1])

  if st.button("Generuj raport"):
    with st.spinner("Ładowanie danych...", show_time=True):
      data = get_clients(utc_start, utc_end, selected_attractions, clients_from_p4u)

      if not data:
        return

      utils.download_button(data, f"klienci_{start_date}-{end_date}_bez_rezerwacji_w_przyszlosci")

view()
