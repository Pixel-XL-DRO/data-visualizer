import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO

SAFI_URL = "https://rezerwacje.pixel-xl.pl/api/integration";
AUTH_ENDPOINT = "/auth/login";
RESERVATIONS_ENDPOINT = "/reservations";

def auth_safi():
  try: 
    r = requests.post(f"{SAFI_URL}{AUTH_ENDPOINT}?email={st.secrets["safi"].get("safi_email")}&password={st.secrets["safi"].get("safi_password")}")
    data = r.json()
    return data.get("data").get("token")  
  except: 
    st.error("Brak autoryzacji")
    return None

def get_reservations(start_date, end_date):

  token = auth_safi()
  if not token:
    st.error("Brak autoryzacji") 
    return
  try:
    headers = {
    "Authorization": f"Bearer {token}"
    }

    should_fetch = True
    page = 1
    reservations = []

    while should_fetch:

      r = requests.get(f"{SAFI_URL}{RESERVATIONS_ENDPOINT}?where[start_at_from]={start_date}&where[start_at_to]={end_date}&page={page}&per_page=1000", headers=headers)
      data = r.json()
      reservations.extend(data.get("data"))
      page += 1
      should_fetch = True if data.get("links").get("next") else False
    return reservations
  except: 
    st.error("Błąd podczas pobierania danych")
    return []

def parse_data(start_date, end_date, visit_types):

  if len(visit_types) == 0:
    st.error("Wybierz typ wizyty!")
    return 
  reservations = get_reservations(start_date, end_date)

  raport = {
    "Sumaryczne": {},
  }
  
  for reservation in reservations:
    
    city = reservation.get("location_name")
    is_cancelled = "CANCELLED" in reservation.get("status")
    visit_name = reservation.get("visit_name")

    if is_cancelled or not reservation.get("voucher") or visit_name not in visit_types:
      continue

    if visit_name not in raport:  
      raport[visit_name] = {}

    voucher_code = reservation.get("voucher").get("code")  
    price = float(reservation.get("total_price"))

    if voucher_code not in raport["Sumaryczne"]:
      raport["Sumaryczne"][voucher_code] = {
        "Liczba": 0,
        "Wartosc": 0
      }      
    raport["Sumaryczne"][voucher_code]["Liczba"] += 1
    raport["Sumaryczne"][voucher_code]["Wartosc"] += price

    if f"{city} liczba" not in raport["Sumaryczne"][voucher_code]:
      raport["Sumaryczne"][voucher_code][f"{city} liczba"] = 0
    raport["Sumaryczne"][voucher_code][f"{city} liczba"] += 1

    if f"{city} wartosc" not in raport["Sumaryczne"][voucher_code]:
      raport["Sumaryczne"][voucher_code][f"{city} wartosc"] = 0
    raport["Sumaryczne"][voucher_code][f"{city} wartosc"] += price

    if voucher_code not in raport[visit_name]:
      raport[visit_name][voucher_code] = {
        "Liczba": 0,
        "Wartosc": 0
      }      
    raport[visit_name][voucher_code]["Liczba"] += 1
    raport[visit_name][voucher_code]["Wartosc"] += price

    if f"{city} liczba" not in raport[visit_name][voucher_code]:
      raport[visit_name][voucher_code][f"{city} liczba"] = 0
    raport[visit_name][voucher_code][f"{city} liczba"] += 1

    if f"{city} wartosc" not in raport[visit_name][voucher_code]:
      raport[visit_name][voucher_code][f"{city} wartosc"] = 0
    raport[visit_name][voucher_code][f"{city} wartosc"] += price
  
  return raport
@st.fragment
def view(): 

  now = datetime.now()
  start_date = st.date_input("Podaj date poczatku", now - timedelta(days=1), key="start_date", max_value=now - timedelta(days=1))
  end_date = st.date_input("Podaj date końca", now, key="end_date", max_value=now)
  
  dt_start_date = datetime.combine(start_date, datetime.min.time())
  dt_end_date = datetime.combine(end_date, datetime.min.time())

  visits = ['Indywidualne', 'Urodziny', 'Integracje', 'Wycieczki szkolne']
  visit_types = st.multiselect('Wybierz typy wizyt', visits, default=visits)

  if st.button("Generuj raport"):
    with st.spinner("Ładowanie danych...", show_time=True):
      data = parse_data(dt_start_date, dt_end_date, visit_types)
      st.info("Sumaryczne dane, dokładniejsze dostępne w pliku do pobrania")
      st.write(pd.DataFrame(data["Sumaryczne"]).T)
      output = BytesIO()
      with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for visit in data:
          df = pd.DataFrame(data[visit]).T
          df.sort_values(by=["Liczba"], ascending=False, inplace=True)
          df.to_excel(writer, sheet_name=visit)
        writer.close()
        processed_data = output.getvalue()

        return (
          st.download_button(
          label="Pobierz plik .xlxs",
          data=processed_data,
          icon="⬇️",
          file_name=f"raport vouchery {start_date}-{end_date}.xlsx",
          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ))

view()

