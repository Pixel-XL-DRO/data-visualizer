import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import utils

from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")

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

  col1, col2 = st.columns(2)

  now = datetime.now()
  with col1:
    start_date = st.date_input("Podaj date poczatku", now - timedelta(days=1), key="start_date", max_value=now - timedelta(days=1))
  
  with col2:
    end_date = st.date_input("Podaj date końca", now, key="end_date")
  
  date_type = st.selectbox("Wybierz rodzaj daty", ["Data stworzenia", "Data rozpoczecia"], key="date_type")
  use_start_date = True if date_type == "Data rozpoczecia" else False


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
      data = get_promo_codes_reports(utc_start, utc_end, use_start_date)
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
      visit_type = "odbyte" if use_start_date else "stworzone"
      utils.download_button(data, f"raport_kody_promocyjne_{start_date}-{end_date}_wizyty_{visit_type}", transpose=True)

view()