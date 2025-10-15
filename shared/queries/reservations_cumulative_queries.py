import pandas as pd
from google.cloud import bigquery
from queries import run_query
import streamlit as st
import time
from datetime import datetime
import utils

def get_reservations_cumulative(date_type, since_when, end_date, groupBy, cities, language, attraction_groups, status, visit_type_groups):

  cities_condition = format_array_for_query(cities)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_select = f", {groupBy}" if groupBy else ""

  if groupBy == 'status':
    groupBy_select = f", CASE WHEN ecr.is_cancelled = TRUE THEN 'Anulowane' WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone' ELSE 'Zrealizowane' END AS {groupBy}"
  elif groupBy == "attraction_group":
    groupBy_select = f", dvt.attraction_group AS {groupBy}"
  elif groupBy == "visit_type":
    groupBy_select = f", dvt.name AS {groupBy}"

  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
    WITH initial AS (
    SELECT
        COUNT(*) AS count,
        DATE({date_type}) AS date
        {groupBy_select}
    FROM
        reservation_data.event_create_reservation ecr
    JOIN
        reservation_data.dim_client dc
        ON ecr.client_id = dc.id
    JOIN
        reservation_data.dim_location dl
        ON ecr.location_id = dl.id
    JOIN
        reservation_data.dim_visit_type dvt
        ON ecr.visit_type_id = dvt.id
    WHERE
        ecr.deleted_at IS NULL
        AND language {language_condition}
        AND dvt.name {visit_type_condition}
        AND dvt.attraction_group {attraction_condition}
        AND city {cities_condition}
        AND CASE
            WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
            WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
            ELSE 'Zrealizowane'
        END {status_condition}
    GROUP BY
        DATE({date_type})
        {groupBy_condition}
)
SELECT
    date,
    SUM(count) OVER (
        {groupBy_partition}
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_count
    {groupBy_condition}
FROM
    initial
ORDER BY
    date
    {groupBy_condition};
  """

  rows = run_query(query)
  df = pd.DataFrame(rows)

  df['date'] = pd.to_datetime(df['date']).dt.date
  since_when = since_when.date()
  end_date = end_date.date()

  df = df[df['date'] >= since_when]
  df = df[df['date'] < end_date]

  return df

def get_reservations_boardhours_cumulative(date_type, since_when, end_date, groupBy, cities, language, attraction_groups, status, visit_type_groups):


  cities_condition = format_array_for_query(cities)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_select = f", {groupBy}" if groupBy else ""

  if groupBy == 'status':
    groupBy_select = f", CASE WHEN ecr.is_cancelled = TRUE THEN 'Anulowane' WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone' ELSE 'Zrealizowane' END AS {groupBy}"
  elif groupBy == "attraction_group":
    groupBy_select = f", dvt.attraction_group AS {groupBy}"
  elif groupBy == "visit_type":
    groupBy_select = f", dvt.name AS {groupBy}"

  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
    WITH initial AS (
    SELECT
        SUM(
        CASE
          WHEN ecr.reservation_system = "plan4u"
            THEN rso.slots_taken * rso.time_taken / 60
          ELSE ecr.slots_taken * ecr.time_taken / 60
        END
      ) AS boardhours_taken,
        DATE({date_type}) AS date
        {groupBy_select}
    FROM
        reservation_data.event_create_reservation ecr
    JOIN
        reservation_data.dim_client dc
        ON ecr.client_id = dc.id
    JOIN
        reservation_data.dim_location dl
        ON ecr.location_id = dl.id
    JOIN
        reservation_data.dim_visit_type dvt
        ON ecr.visit_type_id = dvt.id
    LEFT JOIN
        reservation_data.reservation_slots_occupancy rso
    ON
        ecr.id = rso.reservation_id
    WHERE
        ecr.deleted_at IS NULL
        AND language {language_condition}
        AND dvt.name {visit_type_condition}
        AND dvt.attraction_group {attraction_condition}
        AND city {cities_condition}
        AND CASE
            WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
            WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
            ELSE 'Zrealizowane'
        END {status_condition}
    GROUP BY
        DATE({date_type})
        {groupBy_condition}
)
SELECT
    date,
    SUM(boardhours_taken) OVER (
        {groupBy_partition}
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_boardhours_taken
    {groupBy_condition}
FROM
    initial
ORDER BY
    date
    {groupBy_condition};
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )
  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  df['date'] = pd.to_datetime(df['date']).dt.date
  since_when = since_when.date()
  end_date = end_date.date()

  df = df[df['date'] >= since_when]
  df = df[df['date'] < end_date]

  return df

def get_reservations_people_cumulative(date_type, since_when, end_date, groupBy, cities, language, attraction_groups, status, visit_type_groups):


  cities_condition = format_array_for_query(cities)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_select = f", {groupBy}" if groupBy else ""

  if groupBy == 'status':
    groupBy_select = f", CASE WHEN ecr.is_cancelled = TRUE THEN 'Anulowane' WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone' ELSE 'Zrealizowane' END AS {groupBy}"
  elif groupBy == "attraction_group":
    groupBy_select = f", dvt.attraction_group AS {groupBy}"
  elif groupBy == "visit_type":
    groupBy_select = f", dvt.name AS {groupBy}"

  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
    CREATE TEMP FUNCTION mock_people(visit_type STRING, city STRING, person_count INT64, date_val DATE) RETURNS INT64
    LANGUAGE js AS '''
    if (visit_type === "urodziny - standard" || visit_type === "urodziny Pixel") {{
      return 6;
    }}
    if (visit_type === "urodziny - L") {{
      return 12;
    }}
    if (visit_type === "urodziny - XL") {{
      return city === "wroclaw" ? 12 : 18;
    }}
    if (visit_type === "urodziny - XXL") {{
      return city === "wroclaw" ? 50 : 24;
    }}
    if (visit_type === "szkoła do 24 osób") {{
      return 18;
    }}
    if (visit_type === "szkoła do 36 osób") {{
      return 30;
    }}
    if (visit_type === "szkoła do 48 osób") {{
      return 42;
    }}
    if (visit_type === "szkoła od 48 osób") {{
      return 54;
    }}
    if (visit_type === "integracja - L") {{
      return city === "wroclaw" ? 10 : 8;
    }}
    if (visit_type === "integracja - L+") {{
      return 16;
    }}
    if (visit_type === "integracja - XL") {{
      return city === "wroclaw" ? 25 : 24;
    }}
    if (visit_type === "integracja - XL+") {{
      return city === "wroclaw" ? 31 : 32;
    }}
    if (visit_type === "integracja - XXL") {{
      return 50;
    }}
    return person_count;
    ''';
    WITH initial AS (
    SELECT
        SUM(
        mock_people(
          dvt.name,
          city,
          no_of_people,
          DATE({date_type})
        )
      ) AS person_count,
        DATE({date_type}) AS date
        {groupBy_select}
    FROM
        reservation_data.event_create_reservation ecr
    JOIN
        reservation_data.dim_client dc
        ON ecr.client_id = dc.id
    JOIN
        reservation_data.dim_location dl
        ON ecr.location_id = dl.id
    JOIN
        reservation_data.dim_visit_type dvt
        ON ecr.visit_type_id = dvt.id
    WHERE
        ecr.deleted_at IS NULL
        AND language {language_condition}
        AND dvt.name {visit_type_condition}
        AND dvt.attraction_group {attraction_condition}
        AND city {cities_condition}
        AND CASE
            WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
            WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
            ELSE 'Zrealizowane'
        END {status_condition}
    GROUP BY
        DATE({date_type})
        {groupBy_condition}
)
SELECT
    date,
    SUM(person_count) OVER (
        {groupBy_partition}
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_people_taken
    {groupBy_condition}
FROM
    initial
ORDER BY
    date
    {groupBy_condition};
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )
  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  df['date'] = pd.to_datetime(df['date']).dt.date
  since_when = since_when.date()
  end_date = end_date.date()

  df = df[df['date'] >= since_when]
  df = df[df['date'] < end_date]

  return df

def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"
