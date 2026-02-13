import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import utils

from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")

SAFI_URL = "https://rezerwacje.pixel-xl.pl/api/integration";
AUTH_ENDPOINT = "/auth/login";
RESERVATIONS_ENDPOINT = "/reservations";

def get_promo_codes_reports(start_date, end_date, use_start_at):

  url = "https://safi-api.pixel-xl.tech:9999/api/promo_codes_report"

  params = {
      "from_dt": start_date,
      "to_dt": end_date,
      "use_start_date": use_start_at
  }
  
  auth_token = st.secrets["safi"].get("auth_token")

  if not auth_token:
    st.error("Brak autoryzacji")
    return

  headers = {
      "Authorization": f"Bearer {auth_token}"
  }

  response = requests.get(url, params=params, headers=headers)
  
  data = response.json()
  return data

@st.fragment
def view(): 

  now = datetime.now()
  start_date = st.date_input("Podaj date poczatku", now - timedelta(days=1), key="start_date", max_value=now - timedelta(days=1))
  end_date = st.date_input("Podaj date końca", now, key="end_date", max_value=now)
  use_start_at = st.checkbox("Pokazuj tylko odbyte wizyty", key="should_count_cancelled", value=False)
  
  dt_start_date = datetime.combine(start_date, datetime.min.time(), tzinfo=USER_TZ)
  dt_end_date = datetime.combine(end_date, datetime.min.time(), tzinfo=USER_TZ)

  utc_start = (
    dt_start_date
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )

  utc_end = (
    dt_end_date
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
  )
  
  if st.button("Generuj raport"):
    with st.spinner("Ładowanie danych...", show_time=True):
      data = get_promo_codes_reports(utc_start, utc_end, use_start_at)
      st.info("Sumaryczne dane, rozdzielone typy wizyt dostępne w pliku do pobrania")
      for visit_name in data:
        
        data[visit_name] = dict(
          sorted(
              data[visit_name].items(),
              key=lambda item: item[1]["Liczba"],
              reverse=True
          )
        )
        
      st.write(pd.DataFrame(data["Sumaryczne"]).T)
      visit_type = "odbyte" if use_start_at else "stworzone"
      utils.download_button(data, f"raport_kody_promocyjne_{start_date}-{end_date}_wizyty_{visit_type}", transpose=True)

view()