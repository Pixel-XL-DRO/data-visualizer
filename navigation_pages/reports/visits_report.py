
import streamlit as st
import pandas as pd
import utils
import requests
from datetime import date, datetime, timedelta, timezone

from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")
min_date = date(2025, 1, 1)

def get_safi_data(start, end, use_start_date):

  url = "https://safi-api.pixel-xl.tech:9999/api/get_reservations_for_invoice_report"

  parsed_data = []

  invoice_types = {
    "COMPANY": "Na firmę",
    "PERSON": "Na osobę fizyczną"
  }  

  headers = {
    "Authorization": f"Bearer {st.secrets['safi']['auth_token']}"
  }

  all_reservations = []
  page = 0
  should_fetch = True

  while (should_fetch):

    params = {
      "from_dt": start,
      "to_dt": end,
      "page": page,
      "use_start_date": use_start_date
    }
    
    response = requests.get(f"{url}", params=params, headers=headers)

    data = response.json()

    if len(data) == 0 or "error" in data:
      should_fetch = False  
      break  
    
    all_reservations.extend(data)

    page += 1

  for reservation in all_reservations:

    reservation_status = reservation["status"]
    reservation_date = (
      pd.to_datetime(reservation["start_at"])
        .tz_convert("Europe/Warsaw")
        .date()
        .strftime("%Y-%m-%d")
    )
    if "CANCELLED" in reservation_status:
      reservation_status = "ANULOWANA"
    else:
      reservation_status = "NIEANULOWANA"

    parsed_data.append({
      "Data wizyty": reservation_date,
      "Numer rezerwacji": reservation["number"],
      "Status": reservation_status,
      "Atrakcja": reservation["visit_name"],
      "Typ faktury": invoice_types[reservation["invoice_type"]] if reservation["invoice_type"] else None,
      "NIP": reservation["invoice_tax_number"]
    })

  return pd.DataFrame(parsed_data)

def view():

  today = date.today()
  col1, col2 = st.columns(2)

  with col1:
    start_date = st.date_input(
      "Od kiedy",
      value=today - timedelta(days=30),
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

  dt_start = datetime.combine(
    start_date,
    datetime.min.time(),
    tzinfo=USER_TZ
  )

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

  use_start_date = st.checkbox("Czy użyć daty odbycia rezerwacji? (w przeciwnym razie data stworzenia)")

  if st.button("Generuj"):
    with st.spinner("Ładowanie...", show_time=True):
      data = get_safi_data(utc_start, utc_end, use_start_date)
      utils.download_button(data, f"Rezerwacje w przedziale {start_date}-{end_date}", label="Pobierz raport .xlxs")

view()