import requests
import streamlit as st


def get_reservations_report(utc_start, utc_end, use_start_date):
  safi_auth_token = st.secrets["safi"].get("auth_token")
  headers = {"Authorization": f"Bearer {safi_auth_token}"}
  params = {"use_start_date": use_start_date, "from_dt": utc_start, "to_dt": utc_end}

  res = requests.get(
    "https://safi-api.pixel-xl.tech:9999/api/reservations_report",
    headers=headers,
    params=params,
  )

  data = res.json()
  res.raise_for_status()

  return data
