import pandas as pd
from google.cloud import bigquery
from queries import run_query
import time
import utils
from datetime import datetime
def get_reservations_count(date_type, since_when, moving_average_days, groupBy, cities, language, attraction_groups, status, visit_type_groups, notes=None):

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
  timestamp_now = datetime.fromtimestamp(time.time())

  query = f"""
  WITH initial AS(
    SELECT
      COUNT(*) AS count,
      DATE({date_type}) AS date
      {groupBy_select}
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_client dc
    ON
      ecr.client_id = dc.id
    JOIN
      reservation_data.dim_location dl
    ON
      ecr.location_id = dl.id
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      ecr.visit_type_id = dvt.id
    WHERE
      ecr.deleted_at IS NULL
      AND DATE({date_type}) > DATE(@since_when)
      AND DATE({date_type}) < DATE('{timestamp_now}')
      AND language {language_condition}
      AND dvt.name {visit_type_condition}
      AND dvt.attraction_group {attraction_condition}
      AND street {cities_condition}
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
    count,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(count) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    )
    END AS moving_avg
    {groupBy_condition}
    FROM
      INITIAL
    ORDER BY
      date
      {groupBy_condition}
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )
  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  if notes is not None:
    notes["date"] = pd.to_datetime(notes["date"], utc=True)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.merge(notes, how='left', on=['date', 'city'] if groupBy == 'city' else 'date')

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df

def get_people_count(date_type, since_when, moving_average_days, groupBy, cities, language, attraction_groups, status, visit_type_groups, notes=None):

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
  timestamp_now = datetime.fromtimestamp(time.time())

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
    ON
      ecr.client_id = dc.id
    JOIN
      reservation_data.dim_location dl
    ON
      ecr.location_id = dl.id
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      ecr.visit_type_id = dvt.id
    WHERE
      ecr.deleted_at IS NULL
      AND DATE({date_type}) > DATE(@since_when)
      AND DATE({date_type}) < DATE('{timestamp_now}')
      AND language {language_condition}
      AND dvt.name {visit_type_condition}
      AND street {cities_condition}
      AND dvt.attraction_group {attraction_condition}
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
    person_count,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(person_count) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    )
    END AS moving_avg
    {groupBy_condition}
  FROM
    initial
  ORDER BY
    date
    {groupBy_condition}
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )
  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  if notes is not None:
    notes["date"] = pd.to_datetime(notes["date"], utc=True)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.merge(notes, how='left', on=['date', 'city'] if groupBy == 'city' else 'date')

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location)

  return df

def get_boardhours(date_type, since_when, moving_average_days, groupBy, cities, language, attraction_groups, status, visit_type_groups, notes=None):

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
  timestamp_now = datetime.fromtimestamp(time.time())

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
    ON
      ecr.client_id = dc.id
    JOIN
      reservation_data.dim_location dl
    ON
      ecr.location_id = dl.id
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      ecr.visit_type_id = dvt.id
    LEFT JOIN
      reservation_data.reservation_slots_occupancy rso
    ON
      ecr.id = rso.reservation_id
    WHERE
      ecr.deleted_at IS NULL
      AND {date_type} IS NOT NULL
      AND DATE({date_type}) > DATE(@since_when)
      AND DATE({date_type}) < DATE('{timestamp_now}')
      AND language {language_condition}
      AND dvt.name {visit_type_condition}
      AND street {cities_condition}
      AND dvt.attraction_group {attraction_condition}
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
    boardhours_taken,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(boardhours_taken) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    )
    END AS moving_avg
    {groupBy_condition}
  FROM
    initial
  ORDER BY
    date
    {groupBy_condition}
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )
  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  if notes is not None:
    notes["date"] = pd.to_datetime(notes["date"], utc=True)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.merge(notes, how='left', on=['date', 'city'] if groupBy == 'city' else 'date')

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location)

  return df

def get_mean_days_ahead(date_type, since_when,cities):

  cities_condition = format_array_for_query(cities)
  timestamp_now = datetime.fromtimestamp(time.time())

  query = f"""
    SELECT
      AVG(DATE_DIFF(DATE(ecr.start_date), DATE(ecr.booked_date), day)) as days,
      dl.city,
      count(*) as count
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    WHERE
      DATE({date_type}) >= DATE(@since_when)
    AND
      DATE({date_type}) < DATE('{timestamp_now}')
    AND
      ecr.deleted_at IS NULL
    AND
      dl.street {cities_condition}
    GROUP BY
      dl.city
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  return df

def get_days_ahead_by_city(date_type, period, since_when, street):
  timestamp_now = datetime.fromtimestamp(time.time())

  query = f"""
    SELECT
      CASE
        WHEN DATE_DIFF(DATE(ecr.start_date), DATE(ecr.booked_date), DAY) < {period}
          THEN CAST(DATE_DIFF(DATE(ecr.start_date), DATE(ecr.booked_date), DAY) AS STRING)
        ELSE '{period}+'
      END AS days,
      COUNT(*) AS reservations
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    WHERE
      dl.street = @street
    AND
      DATE_DIFF(DATE(ecr.start_date), DATE(ecr.booked_date), DAY) >= 0
    AND
      DATE({date_type}) > DATE(@since_when)
    AND
      DATE({date_type}) < DATE('{timestamp_now}')
    AND
      ecr.deleted_at IS NULL
    GROUP BY
      days
    ORDER BY
      SAFE_CAST(NULLIF(REGEXP_EXTRACT(days, r'^\\d+'), '') AS INT64)
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("street", "STRING", street),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  return df

def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"

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