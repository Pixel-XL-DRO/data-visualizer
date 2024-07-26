import pandas as pd
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
  st.secrets["gcp_service_account"]
)

client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def run_query(query):
  query_job = client.query(query)
  rows_raw = query_job.result()
  # dict cause of caching
  rows = [dict(row) for row in rows_raw]
  return rows

def get_reservation_data():
  query = """
    SELECT
      res.id,
      res.start_date_id,
      res.booked_date_id,
      res.location_id,
      res.client_id,
      res.is_payed,
      res.is_cancelled,
      res.no_of_people,
      res.whole_cost_with_voucher,
      res.attraction_group,
      res.visit_type as visit_type,
      start_date.date AS start_date,
      booked_date.date AS booked_date,
      start_date.day_of_week as day_of_week,
      location.city AS city,
      client.language AS language,
      client.id AS client_id
    FROM
      `pixelxl-database-dev.reservation_data.event_create_reservation` res
    JOIN
      `pixelxl-database-dev.reservation_data.dim_date` start_date
    ON
      res.start_date_id = start_date.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_date` booked_date
    ON
      res.booked_date_id = booked_date.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_location` location
    ON
      res.location_id = location.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_client` client
    ON
      res.client_id = client.id
  """

  rows = run_query(query)
  df = pd.DataFrame(rows)

  new_values = df.apply(
    lambda row: mock_price_and_people(
      row['day_of_week'], row['visit_type'],
      row['whole_cost_with_voucher'], row['no_of_people']
    ), axis=1
  )

  df['whole_cost_with_voucher'] = new_values.apply(lambda x: x[0])
  df['no_of_people'] = new_values.apply(lambda x: x[1])

  return df


def mock_price_and_people(day_of_week, visit_type, current_price, current_number_of_people):
  if visit_type == "urodziny - standard":
    return 549, 6 if day_of_week > 0 and day_of_week < 5 else 649, 6
  if visit_type == "urodziny Pixel":
    return 549, 6 if day_of_week > 0 and day_of_week < 5 else 649, 6
  if visit_type == "urodziny - XL":
    return 899, 12 if day_of_week > 0 and day_of_week < 5 else 999, 12
  if visit_type == "urodziny - XXL":
    return 2299, 50 if day_of_week > 0 and day_of_week < 5 else 2399, 50
  if visit_type == "szkoła do 24 osób":
    # 18 people * 28pln
    return 504, 18
  if visit_type == "szkoła do 36 osób":
    # 30 people * 28 pln
    return 840, 30
  if visit_type == "szkoła do 48 osób":
    # 42 people * 28 pln
    return 1176, 42
  if visit_type == "szkoła od 48 osób":
    # 54 people * 28 pln
    return 1512, 54
  if visit_type == "integracja - L":
    return 699, 10
  if visit_type == "integracja - L+":
    return 999, 16
  if visit_type == "integracja - XL":
    return 1299, 25
  if visit_type == "integracja - XL+":
    return 1899, 31
  if visit_type == "integracja - XXL":
    return 2899, 50
  return current_price, current_number_of_people
