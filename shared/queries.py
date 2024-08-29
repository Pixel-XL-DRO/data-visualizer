import pandas as pd
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
  st.secrets["gcp_service_account"]
)

client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=6000)
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
      res.additional_items_cost,
      res.attraction_group,
      res.visit_type AS visit_type,
      start_date.date AS start_date,
      booked_date.date AS booked_date,
      start_date.hour AS start_date_hour,
      start_date.day_of_month AS start_date_day_of_month,
      start_date.day_of_week AS start_date_day_of_week,
      start_date.week_of_month AS start_date_week_of_month,
      start_date.week AS start_date_week_of_year,
      start_date.month AS start_date_month,
      start_date.year AS start_date_year,
      booked_date.hour AS booked_date_hour,
      booked_date.day_of_month AS booked_date_day_of_month,
      booked_date.day_of_week AS booked_date_day_of_week,
      booked_date.week_of_month AS booked_date_week_of_month,
      booked_date.week AS booked_date_week_of_year,
      booked_date.month AS booked_date_month,
      booked_date.year AS booked_date_year,
      location.city AS city,
      client.language AS language,
      client.id AS client_id,
      client.email AS email
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
      row['start_date_day_of_week'], row['visit_type'],
      row['city'], row['additional_items_cost'],
      row['whole_cost_with_voucher'], row['no_of_people']
    ), axis=1
  )

  df['whole_cost_with_voucher'] = new_values.apply(lambda x: x[0])
  df['no_of_people'] = new_values.apply(lambda x: x[1])

  return df

def mock_price_and_people(day_of_week, visit_type, city, additional_items_cost, current_price, current_number_of_people):
  if visit_type == "urodziny - standard":
    return (max((549 + additional_items_cost), current_price), 6) if day_of_week > 0 and day_of_week < 5 else (max((649 + additional_items_cost), current_price), 6)
  if visit_type == "urodziny Pixel":
    return (max((549 + additional_items_cost), current_price), 6) if day_of_week > 0 and day_of_week < 5 else (max((649 + additional_items_cost), current_price), 6)
  if visit_type == "urodziny - L":
    return (max((899 + additional_items_cost), current_price), 12) if day_of_week > 0 and day_of_week < 5 else (max((999 + additional_items_cost), current_price), 12)
  if visit_type == "urodziny - XL":
    if city == "wroclaw":
      return (max((899 + additional_items_cost), current_price), 12) if day_of_week > 0 and day_of_week < 5 else (max((999 + additional_items_cost), current_price), 12)
    return (1349 + additional_items_cost, 18) if day_of_week > 0 and day_of_week < 5 else (1449 + additional_items_cost, 18)
  if visit_type == "urodziny - XXL":
    if city == "wroclaw":
      return (max((2299 + additional_items_cost), current_price), 50) if day_of_week > 0 and day_of_week < 5 else (max((2399 + additional_items_cost), current_price), 50)
    return (max((1799 + additional_items_cost), current_price), 24) if day_of_week > 0 and day_of_week < 5 else (max((1889 + additional_items_cost), current_price), 24)
  if visit_type == "szkoła do 24 osób":
    # 18 people * 28pln
    return (max((504 + additional_items_cost), current_price), 18)
  if visit_type == "szkoła do 36 osób":
    # 30 people * 28 pln
    return (max((840 + additional_items_cost), current_price), 30)
  if visit_type == "szkoła do 48 osób":
    # 42 people * 28 pln
    return (max((1176 + additional_items_cost), current_price), 42)
  if visit_type == "szkoła od 48 osób":
    # 54 people * 28 pln
    return (max((1512 + additional_items_cost), current_price), 54)
  if visit_type == "integracja - L":
    if city == "wroclaw":
      return (max((699 + additional_items_cost), current_price), 10)
    return (max((699 + additional_items_cost), current_price), 8)
  if visit_type == "integracja - L+":
    if city == "wroclaw":
      return (max((999 + additional_items_cost), current_price), 16)
    return (max((1299 + additional_items_cost), current_price), 16)
  if visit_type == "integracja - XL":
    if city == "wroclaw":
      return (max((1299 + additional_items_cost), current_price), 25)
    return (max((1899 + additional_items_cost), current_price), 24)
  if visit_type == "integracja - XL+":
    if city == "wroclaw":
      return (max((1899 + additional_items_cost), current_price), 31)
    return (max((2449 + additional_items_cost), current_price), 32)
  if visit_type == "integracja - XXL":
    return (max((2899 + additional_items_cost), current_price), 50)
  return (current_price, current_number_of_people)
