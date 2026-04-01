import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import utils
import auth
import queries
from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")

SAFI_LOCATIONS = {
  "krakow-lubicz": "01976898-679c-70e0-9b4f-dc2a14131e3d",
  "lodz-ogrodowa": "01988093-0fa0-731f-9ca0-b864decd2e94",
  "warszawa-kijowska": "019a1050-b96b-7032-baee-8a69101d49d4",
  "poznan-swietego-marcina": "019a39f1-045f-713a-834d-a66fb85287c5",
  "katowice-sokolska": "019ae347-fb95-73cd-84a3-5b2101273631",
  "gdansk-grunwaldzka": "019b3130-6834-7373-8b4b-c22d2b8b086a",
  "warszawa-arkadia": "019bc67a-793e-705f-99db-3ee07379f1e1",
  "wroclaw-swidnicka": "019c32dd-e660-7073-8969-b350de2f45c9",
  "bydgoszcz-szajnochy": "019c6612-ff2e-711a-9646-78e9d3054c68"
}

def get_promo_codes_reports(start_date, end_date, use_start_at, locations):

  safi_locations_ids = [SAFI_LOCATIONS[location] for location in locations]
  safi_locations_ids = ",".join(safi_locations_ids)

  url = "https://safi-api.pixel-xl.tech:9999/api/promo_codes_report"

  params = {
      "from_dt": start_date,
      "to_dt": end_date,
      "use_start_date": use_start_at,
      "location_ids": safi_locations_ids
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

  with st.spinner(""):
    df = queries.get_initial_data()
    df = auth.filter_locations(df)
    locations = list(set([f"{city}-{street}" for city, street in df[["city", "street"]].values]))
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
      data = get_promo_codes_reports(utc_start, utc_end, use_start_date, locations)
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