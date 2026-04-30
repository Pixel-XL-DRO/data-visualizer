import sys
sys.path.append("shared/queries/report_queries")

import streamlit as st
import utils
import requests
from datetime import date, datetime, timedelta, timezone
import json
from zoneinfo import ZoneInfo
import not_started_reservation_queries as nsrq

USER_TZ = ZoneInfo("Europe/Warsaw")
min_date = date(2025, 1, 1)

def get_safi_data(start, end):

  url = "https://safi-api.pixel-xl.tech:9999/api/get-not-started-reservations"

  params = {
    "start_date_from": start,
    "start_date_to": end,
  }
  safi_auth_token = st.secrets["safi"].get("auth_token")

  headers = { 
    "Authorization": f"Bearer {safi_auth_token}"
  }

  response = requests.get(url, params=params, headers=headers)
  
  data = response.json()
  response.raise_for_status()

  not_started_reservation = []
  not_started_reservation_ids = []

  for reservation in data:

    if(reservation["customer_present"] == 1):
      continue

    not_started_reservation_ids.append(reservation["reservation_id"])

    if reservation.get("request_data"):
      request_data_parsed = json.loads(reservation["request_data"])
      lines = request_data_parsed["eReceipt"]["lines"]
      tax_rates = request_data_parsed["eReceipt"]["metadata"]["taxRates"]

      total_brutto = 0
      total_net = 0

      for line in lines:
        line_brutto = line["totalLineValue"] / 100
        tax_rate = int(tax_rates[line["taxRate"]]) if tax_rates.get(line["taxRate"], "").isdigit() else None

        is_reservation = "bilet" in line["productOrServiceName"].lower() 
        
        discounts = line.get("rebatesMarkups", [])
        discount_value = sum(d["value"] / 100 for d in discounts) if discounts and is_reservation else 0

        effective_brutto = line_brutto + discount_value
        effective_net = effective_brutto / (1 + tax_rate / 100) if tax_rate is not None else effective_brutto

        total_brutto += effective_brutto
        total_net += effective_net
    else:
      total_brutto = 0
      total_net = 0

    visit_dt = datetime.fromisoformat(reservation["start_at"].replace("Z", "+00:00")).astimezone(USER_TZ)
    reservation_dt = datetime.fromisoformat(reservation["created_at"].replace("Z", "+00:00")).astimezone(USER_TZ)

    if reservation.get("invoice_required"):
      payment_info = "rezerwacja na fakturę"
    else:
      payment_info = reservation.get("przelewy24_token") or "brak tokenu"

    no_show_entry = {
      "numer rezerwacji": reservation["number"],
      "data wizyty": visit_dt.strftime("%Y-%m-%d %H:%M"),
      "data rezerwacji": reservation_dt.strftime("%Y-%m-%d %H:%M"),
      "lokalizacja": reservation["location_name"],
      "kwota brutto": round(total_brutto, 2),
      "kwota netto": round(total_net, 2),
      "link do e-paragonu": reservation.get("document_url"),
      "potwierdzenie płatności": payment_info,
      "id": reservation["reservation_id"]
    }

    not_started_reservation.append(no_show_entry)

  started_but_unchecked = nsrq.get_started_reservation_percent_without_mark_as_started(not_started_reservation_ids)

  return not_started_reservation, len(data), started_but_unchecked


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
      "Do kiedy",
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

  if st.button("Generuj"):
    with st.spinner("Ładowanie...", show_time=True):
      data, all_data_length, started_but_unchecked_df = get_safi_data(utc_start, utc_end)
      utils.download_button({"Rezerwacje": data}, f"Rezerwacje w przedziale {start_date}-{end_date}", label="Pobierz raport .xlxs")
      utils.download_button({"Rezerwacje": data}, f"Rezerwacje w przedziale {start_date}-{end_date}", label="Pobierz raport .csv", format="csv")
      started_but_unchecked = started_but_unchecked_df["started_but_unchecked"][0]
      not_started_len = len(data)

    st.info(f"{round(((all_data_length - not_started_len)/all_data_length * 100), 1)}% wizyt zostało oznaczone jako odbyte")  
    st.info(f"Z pośród wizyt oznaczonych jako nieodbyte {round(((started_but_unchecked)/not_started_len * 100), 1)}% zostało wystartowane ({started_but_unchecked} wizyty)")  

  
view()