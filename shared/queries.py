import pandas as pd
import streamlit as st
import uuid
import datetime

from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
  st.secrets["gcp_service_account"]
)

reviews_credentials = service_account.Credentials.from_service_account_info(
  st.secrets["gcp_reviews_account"]
)

performance_reviews_credentials = service_account.Credentials.from_service_account_info(
  st.secrets["gcp_performance_reviews_account"]
)

client = bigquery.Client(credentials=credentials)
reviews_client = bigquery.Client(credentials=reviews_credentials)
performance_reviews_client = bigquery.Client(credentials=performance_reviews_credentials)

def run_query(query, job_config=None):

  query_job = client.query(query, job_config)
  rows_raw = query_job.result()
  # dict cause of caching
  rows = [dict(row) for row in rows_raw]
  return rows

def run_reviews_query(query, job_config=None):

  query_job = reviews_client.query(query, job_config)
  rows_raw = query_job.result()
  # dict cause of caching
  rows = [dict(row) for row in rows_raw]
  return rows

def run_performance_review_query(query, job_config=None):

  query_job = performance_reviews_client.query(query, job_config)
  rows_raw = query_job.result()
  # dict cause of caching
  rows = [dict(row) for row in rows_raw]
  return rows


@st.cache_data(ttl=6000)
def get_reviews():
  query = """
    SELECT
      ratings.location_id AS location_id,
      ratings.value AS rating,
      ratings.create_time AS create_time,
      dim_location.address AS address,
      dim_location.locality AS city,
    FROM
      reviews.star_rating ratings
    JOIN
      reviews.dim_location dim_location
      ON ratings.location_id = dim_location.name
  """
  rows = run_reviews_query(query)
  df = pd.DataFrame(rows, columns=['location_id', 'rating', 'create_time', 'address', 'city'])

  return df

@st.cache_data(ttl=6000)
def get_locations_data():
  query = """
    SELECT
      dim_location.id,
      dim_location.street AS street,
      dim_location.city AS city,
      dim_location.country AS country,
    FROM
      reservation_data.dim_location dim_location
  """
  rows = run_query(query)

  df = pd.DataFrame(rows, columns=['id', 'street', 'city', 'country'])

  return df

@st.cache_data(ttl=6000)
def get_historical_location_hours_availability():
  query = """
    SELECT
      dim_location_id AS hours_availability_dim_location_id,
      since_when AS hours_availability_since_when,
      until_when AS hours_availability_until_when,
      day_of_week AS hours_availability_day_of_week,
      number_of_hours AS hours_availability_number_of_hours,
      starting_hour AS hours_availability_starting_hour
    FROM
      reservation_data.historical_location_hours_availability
  """
  rows = run_query(query)
  df = pd.DataFrame(rows, columns=['hours_availability_dim_location_id', 'hours_availability_since_when', 'hours_availability_until_when', 'hours_availability_day_of_week', 'hours_availability_number_of_hours', 'hours_availability_starting_hour'])

  return df

@st.cache_data(ttl=6000)
def get_historical_location_boards_availability():
  query = """
    SELECT
      dim_location_id AS boards_availability_dim_location_id,
      since_when AS boards_availability_since_when,
      until_when AS boards_availability_until_when,
      number_of_boards AS boards_availability_number_of_boards,
      time_unit_in_hours AS boards_availability_time_unit_in_hours
    FROM
      reservation_data.historical_location_boards_availability
  """
  rows = run_query(query)
  df = pd.DataFrame(rows, columns=['boards_availability_dim_location_id', 'boards_availability_since_when', 'boards_availability_until_when', 'boards_availability_number_of_boards', 'boards_availability_time_unit_in_hours'])

  return df

@st.cache_data(ttl=6000)
def get_visit_types_data():
  query = """
    SELECT
      dim_visit_type.id AS visit_type_id,
      dim_visit_type.location_id  AS visit_type_dim_location_id,
      dim_visit_type.name AS name,
      dim_visit_type.attraction_group AS attraction_group
    FROM
      reservation_data.dim_visit_type dim_visit_type
  """
  rows = run_query(query)
  df = pd.DataFrame(rows, columns=['visit_type_id', 'visit_type_dim_location_id', 'name', 'attraction_group'])

  return df

@st.cache_data(ttl=6000)
def get_historical_visit_type_availability():
  query = """
    SELECT
      dim_visit_type_id AS visit_type_availability_dim_visit_type_id,
      since_when AS visit_type_availability_since_when,
      until_when AS visit_type_availability_until_when,
      number_of_boards_per_time_unit AS visit_type_availability_number_of_boards_per_time_unit,
      duration_in_time_units AS visit_type_availability_duration_in_time_units
    FROM
      reservation_data.historical_visit_type_availability
  """
  rows = run_query(query)

  df = pd.DataFrame(rows, columns=['visit_type_availability_dim_visit_type_id', 'visit_type_availability_since_when', 'visit_type_availability_until_when', 'visit_type_availability_number_of_boards_per_time_unit', 'visit_type_availability_duration_in_time_units'])

  return df

@st.cache_data(ttl=6000)
def get_slots_occupancy():
  query = """
    SELECT
      reservation_id AS slots_occupancy_reservation_id,
      slots_taken AS slots_occupancy_slots_taken,
      time_taken AS slots_occupancy_time_taken,
      datetime_slot AS slots_occupancy_datetime_slot
    FROM
      reservation_data.reservation_slots_occupancy
  """
  rows = run_query(query)

  df = pd.DataFrame(rows, columns=['slots_occupancy_reservation_id', 'slots_occupancy_slots_taken', 'slots_occupancy_time_taken', 'slots_occupancy_datetime_slot'])

  return df

def refresh_data_editor_data():
  get_locations_data.clear()
  get_visit_types_data.clear()
  get_historical_location_hours_availability.clear()
  get_historical_location_boards_availability.clear()
  get_historical_visit_type_availability.clear()
  get_notes.clear()

def add_historical_location_hours_availability(location_id, since_when, day_of_week, number_of_hours, starting_hour):
  query = """
      UPDATE
        reservation_data.historical_location_hours_availability
      SET
        until_when = @until_when
      WHERE
        until_when IS NULL
      AND
        dim_location_id = @location_id
      AND
        day_of_week = @day_of_week
    """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("until_when", "TIMESTAMP", since_when - datetime.timedelta(days=1)),
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
        bigquery.ScalarQueryParameter("day_of_week", "INTEGER", day_of_week),
    ]
  )
  run_query(query, job_config)

  query = """
      INSERT INTO
        reservation_data.historical_location_hours_availability (dim_location_id, since_when, day_of_week, number_of_hours, starting_hour)
      VALUES
        (@location_id, @since_when, @day_of_week, @number_of_hours, @starting_hour)
    """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("day_of_week", "INTEGER", day_of_week),
        bigquery.ScalarQueryParameter("number_of_hours", "FLOAT", number_of_hours),
        bigquery.ScalarQueryParameter("starting_hour", "FLOAT", starting_hour),
    ]
  )
  run_query(query, job_config)
  get_historical_location_hours_availability.clear()

def add_historical_location_boards_availability(location_id, since_when, number_of_boards, time_unit_in_hours):
  query = """
      UPDATE
        reservation_data.historical_location_boards_availability
      SET
        until_when = @until_when
      WHERE
        until_when IS NULL
      AND
        dim_location_id = @location_id
    """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("until_when", "TIMESTAMP", since_when - datetime.timedelta(days=1)),
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
    ]
  )
  run_query(query, job_config)

  query = """
      INSERT INTO
        reservation_data.historical_location_boards_availability (dim_location_id, since_when, number_of_boards, time_unit_in_hours)
      VALUES
        (@location_id, @since_when, @number_of_boards, @time_unit_in_hours)
    """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("number_of_boards", "INTEGER", number_of_boards),
        bigquery.ScalarQueryParameter("time_unit_in_hours", "FLOAT", time_unit_in_hours),
    ]
  )
  run_query(query, job_config)
  get_historical_location_boards_availability.clear()

def add_historical_visit_type_availability(visit_type_id, since_when, number_of_boards_per_time_unit, duration_in_time_units):
  query = """
      UPDATE
        reservation_data.historical_visit_type_availability
      SET
        until_when = @until_when
      WHERE
        until_when IS NULL
      AND
        dim_visit_type_id = @visit_type_id
    """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("until_when", "TIMESTAMP", since_when - datetime.timedelta(days=1)),
        bigquery.ScalarQueryParameter("visit_type_id", "STRING", visit_type_id),
    ]
  )
  run_query(query, job_config)

  query = """
      INSERT INTO
        reservation_data.historical_visit_type_availability (dim_visit_type_id, since_when, number_of_boards_per_time_unit, duration_in_time_units)
      VALUES
        (@visit_type_id, @since_when, @number_of_boards_per_time_unit, @duration_in_time_units)
    """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("visit_type_id", "STRING", visit_type_id),
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("number_of_boards_per_time_unit", "INTEGER", number_of_boards_per_time_unit),
        bigquery.ScalarQueryParameter("duration_in_time_units", "FLOAT", duration_in_time_units),
    ]
  )
  run_query(query, job_config)
  get_historical_visit_type_availability.clear()

@st.cache_data(ttl=6000)
def get_reservation_data():
  query = """
    SELECT
      res.id,
      ANY_VALUE(res.start_date_id) AS start_date_id,
      ANY_VALUE(res.booked_date_id) AS booked_date_id,
      ANY_VALUE(res.location_id) AS location_id,
      ANY_VALUE(res.client_id) AS client_id,
      ANY_VALUE(res.is_payed) AS is_payed,
      ANY_VALUE(res.is_cancelled) AS is_cancelled,
      ANY_VALUE(res.no_of_people) AS no_of_people,
      ANY_VALUE(res.whole_cost_with_voucher) AS whole_cost_with_voucher,
      ANY_VALUE(res.additional_items_cost) AS additional_items_cost,
      ANY_VALUE(res.time_taken) AS reservation_time_taken,
      ANY_VALUE(res.slots_taken) AS reservation_slots_taken,
      ANY_VALUE(start_date.date) AS start_date,
      ANY_VALUE(booked_date.date) AS booked_date,
      ANY_VALUE(start_date.hour) AS start_date_hour,
      ANY_VALUE(start_date.day_of_month) AS start_date_day_of_month,
      ANY_VALUE(start_date.day_of_week) AS start_date_day_of_week,
      ANY_VALUE(start_date.week_of_month) AS start_date_week_of_month,
      ANY_VALUE(start_date.week) AS start_date_week_of_year,
      ANY_VALUE(start_date.month) AS start_date_month,
      ANY_VALUE(start_date.year) AS start_date_year,
      ANY_VALUE(booked_date.hour) AS booked_date_hour,
      ANY_VALUE(booked_date.day_of_month) AS booked_date_day_of_month,
      ANY_VALUE(booked_date.day_of_week) AS booked_date_day_of_week,
      ANY_VALUE(booked_date.week_of_month) AS booked_date_week_of_month,
      ANY_VALUE(booked_date.week) AS booked_date_week_of_year,
      ANY_VALUE(booked_date.month) AS booked_date_month,
      ANY_VALUE(booked_date.year) AS booked_date_year,
      ANY_VALUE(location.city) AS city,
      ANY_VALUE(location.street) AS street,
      ANY_VALUE(client.language) AS language,
      ANY_VALUE(client.id) AS client_id_detailed,
      ANY_VALUE(client.email) AS email,
      ANY_VALUE(visit_type.id) AS visit_type_id,
      ANY_VALUE(visit_type.name) AS visit_type,
      ANY_VALUE(visit_type.attraction_group) AS attraction_group,
      COALESCE(SUM(reservation_slots_occupancy.slots_taken), 0) AS slots_taken,
      COALESCE(AVG(reservation_slots_occupancy.time_taken), 0) AS time_unit
    FROM
      `pixelxl-database-dev.reservation_data.event_create_reservation` res
    JOIN
      `pixelxl-database-dev.reservation_data.dim_date` start_date
      ON res.start_date_id = start_date.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_date` booked_date
      ON res.booked_date_id = booked_date.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_location` location
      ON res.location_id = location.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_client` client
      ON res.client_id = client.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_visit_type` visit_type
      ON res.visit_type_id = visit_type.id
    LEFT JOIN
      `pixelxl-database-dev.reservation_data.reservation_slots_occupancy` reservation_slots_occupancy
      ON res.id = reservation_slots_occupancy.reservation_id
    GROUP BY
      res.id;
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

  df['boardhours_taken'] = (df['slots_taken'] * df['time_unit'] / 60)

  # we stopped mocking data when we started with dotypos
  df.loc[df['booked_date'] <= '2025-02-01', ['whole_cost_with_voucher']] = new_values.apply(lambda x: x[0])
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

@st.cache_data(ttl=6000)
def get_notes():
  query = """
    SELECT
      notes.id as id,
      notes.date_id as date,
      notes.content as content,
      location.city as note_city
    FROM
      reservation_data.notes notes
    JOIN
      `pixelxl-database-dev.reservation_data.dim_location` location
    ON
      notes.dim_location_id = location.id
  """
  rows = run_query(query)
  df = pd.DataFrame(rows, columns=['id', 'date', 'content', 'note_city'])

  return df

def add_note(date_id, content, location_id):
  note_id = str(uuid.uuid4())
  query = """
    INSERT INTO
      reservation_data.notes (id, date_id, content, dim_location_id)
    VALUES
      (@id, @date_id, @content, @location_id)
  """
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", note_id),
        bigquery.ScalarQueryParameter("date_id", "STRING", date_id),
        bigquery.ScalarQueryParameter("content", "STRING", content),
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
    ]
  )
  run_query(query, job_config)
  get_notes.clear()

def delete_note(id):
  query = """
    DELETE FROM
      reservation_data.notes notes
    WHERE
      notes.id = @id
  """
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", id),
    ]
  )
  res = run_query(query, job_config)
  get_notes.clear()

@st.cache_data(ttl=6000)
def get_performance_reviews():
  query = """
    SELECT
      review.feedback as feedback,
      review.score as score,
      review.date as date,
      location.city as city,
      review.reservationId as reservationId
    FROM
      performance_data.mail_review review
    JOIN
      performance_data.dim_location location
    ON
      review.dim_location_id = location.id
  """
  rows = run_performance_review_query(query)
  df = pd.DataFrame(rows, columns=['reservationId', 'feedback', 'score', 'city', 'date'])
  return df

@st.cache_data(ttl=6000)
def get_order_items():
  query = """
    SELECT
      order_items.order_id as order_id,
      order_items.quantity as quantity,
      item.name as name,
      item.category as category,
      order_items.price_brutto as brutto,
      order_items.price_netto as netto,
      location.city as city,
      location.street as street,
      o.creation_date as creation_date,
      o.value as value,
      o.status as status,
      o.document_number as document_number
    FROM
      POS_system_data.order_items order_items
    JOIN
      POS_system_data.item item
    ON
      order_items.item_id = item.id
    JOIN
      POS_system_data.order o
    ON
      order_items.order_id = o.id
    JOIN
      POS_system_data.dim_location location
    ON
      o.dim_location_id = location.id
  """

  rows = run_query(query)
  df = pd.DataFrame(rows, columns=['order_id', 'quantity', 'name', 'category', 'brutto', 'netto', 'city', 'street', 'creation_date', 'value', 'status','document_number'])
  df['start_date'] = df['creation_date'].dt.date
  df = df[df['start_date'] >= pd.to_datetime('2025-02-01').date()]
  df_all = df
  mask = df['name'].str.contains('bilet|zadatek|voucher|integracja|uczestnik|urodzin', case=False, na=False)
  df = df[~mask]

  df_all = df_all[df_all['document_number'] != '0']
  df_all = df_all[df_all['status'] != 'canceled']

  return df, df_all

def get_voucher_data():
  query = """
    SELECT
      voucher.id as id,
      voucher.voucher_creation_date as creation_date,
      voucher.voucher_name as voucher_name,
      voucher.net_amount as net_amount,
      location.city as city,
      location.street as street
    FROM
      vouchers_data.voucher voucher
    JOIN
      vouchers_data.dim_location location
    ON
      voucher.dim_location_id = location.id
  """

  rows = run_query(query)
  df = pd.DataFrame(rows, columns=['id', 'creation_date', 'voucher_name', 'net_amount', 'city', 'street'])
  return df

