
import streamlit as st
import pandas as pd
import utils
import requests
from datetime import date, datetime

def get_safi_data(year):

  url = "https://safi-api.pixel-xl.tech:9999/api/get_reservations_for_invoice_report"

  start_date = datetime(year, 1, 1).date()
  end_date = datetime(year, 12, 31).date()

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
      "start_at_from": start_date.isoformat(),
      "start_at_to": end_date.isoformat(),
      "page": page
    }

    response = requests.get(f"{url}", params=params, headers=headers)

    data = response.json()

    if len(data) == 0:
      should_fetch = False  
         
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
  CURRENT_YEAR = today.year
  MIN_YEAR = 2025

  year = st.selectbox(
    "Rok",
    list(range(MIN_YEAR, CURRENT_YEAR + 1)),
    index=CURRENT_YEAR - MIN_YEAR
  )

  if st.button("Generuj"):
    with st.spinner("Ładowanie...", show_time=True):
      data = get_safi_data(year)
      utils.download_button(data, f"Odbyte rezerwacje w roku {year}", label="Pobierz raport .xlxs")

view()