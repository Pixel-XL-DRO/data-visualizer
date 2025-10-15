import pandas as pd
from google.cloud import bigquery
from queries import run_query
import streamlit as st
from datetime import datetime
import utils

day_of_week_map = {
  1: "7. Niedziela",
  2: "1. Poniedzialek",
  3: "2. Wtorek",
  4: "3. Sroda",
  5: "4. Czwartek",
  6: "5. Piatek",
  7: "6. Sobota"
}

def get_reservations_by_time_period(date_type, since_when, end_when, status, cities, language, attraction_groups, visit_type_groups, grouping_period):

  cities_condition = format_array_for_query(cities)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  group_years = since_when.year != end_when.year

  if grouping_period == "HOUR":

    select_statement = f"""
      TIMESTAMP_TRUNC(ecr.{date_type}, {grouping_period}, "Europe/Warsaw") AS period,
      EXTRACT(DAY FROM ecr.{date_type}) AS day,
      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), {grouping_period}, "Europe/Warsaw") AS current_period,
    """

    later_extract = f"""
      EXTRACT({grouping_period} FROM period) AS period,
      EXTRACT({grouping_period}
        FROM current_period AT TIME ZONE "Europe/Warsaw"
      ) AS current_period
    """

    optional_groping = "day,"
    optional_where = ""

    average_select = f"""
      SUM(daily_count) / ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
        @since_when,
        @end_when,
        INTERVAL 1 DAY
      )) AS avg_count
    """

  elif grouping_period == "DAYOFWEEK" or grouping_period == "DAY":

    select_statement = f"""
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(ecr.{date_type}, DAY)) AS period,
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY )) AS current_period,
      DATE(TIMESTAMP_TRUNC(ecr.{date_type}, DAY)) AS day,
    """

    optional_groping = f"""
      day,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_where = ""
    average_select = f"""
    SUM(daily_count) /
      (
        SELECT
          COUNTIF(EXTRACT({grouping_period} FROM day) = period)
        FROM
          UNNEST(GENERATE_DATE_ARRAY(DATE(@since_when), DATE(@end_when))) AS day
      ) AS avg_count"""

  else:
    select_statement = f"""
      {f"EXTRACT(YEAR FROM ecr.{date_type}) AS year," if  group_years  else ""}
      EXTRACT({grouping_period} FROM ecr.{date_type}) AS period,
      EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP() AT TIME ZONE 'Europe/Warsaw') AS current_period,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_groping = "year," if group_years else ""

    optional_where = f"""
      AND NOT (
        EXTRACT(YEAR FROM ecr.{date_type}) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
        AND EXTRACT({grouping_period} FROM ecr.{date_type}) >= EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP())
      )
    """ if not grouping_period == "YEAR" else ""

    average_select = f"""
    AVG(daily_count) AS avg_count"""

  query = f"""

  WITH daily_counts AS (
    SELECT
      {select_statement}
      COUNT(*) AS daily_count
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    JOIN
      reservation_data.dim_client dc
    ON
      dc.id = ecr.client_id
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      dvt.id = ecr.visit_type_id
    WHERE
      ecr.deleted_at IS NULL
      AND DATE({date_type}) >= DATE(@since_when)
      AND DATE({date_type}) <= DATE(@end_when)
      AND CASE
        WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
        WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
        ELSE 'Zrealizowane'
      END {status_condition}
      AND dc.language {language_condition}
      AND dvt.name {visit_type_condition}
      AND dl.city {cities_condition}
      AND dvt.attraction_group {attraction_condition}
      {optional_where}
    GROUP BY {optional_groping} period
  )
  SELECT
    {average_select},
    {later_extract}
  FROM daily_counts
  GROUP BY period, current_period
  ORDER BY period;

  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)

  df.sort_values('period', inplace=True)
  if grouping_period == "DAYOFWEEK":
    df['period'] = df['period'].map(day_of_week_map)
    df['current_period'] = df['current_period'].map(day_of_week_map)
  elif grouping_period == "MONTH":
    df['period'] = df['period'].map(utils.get_month_from_month_number)
    df['current_period'] = df['current_period'].map(utils.get_month_from_month_number)


  return df

def get_boardhours_by_time_period(date_type, since_when, end_when, status, cities, language, attraction_groups, visit_type_groups, grouping_period):

  cities_condition = format_array_for_query(cities)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  group_years = since_when.year != end_when.year

  if grouping_period == "HOUR":

    select_statement = f"""
      TIMESTAMP_TRUNC(ecr.{date_type}, {grouping_period}, "Europe/Warsaw") AS period,
      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), {grouping_period}, "Europe/Warsaw") AS current_period,
    """

    later_extract = f"""
      EXTRACT({grouping_period} FROM period) AS period,
      EXTRACT({grouping_period}
        FROM current_period AT TIME ZONE "Europe/Warsaw"
      ) AS current_period
    """
    optional_where = ""
    optional_groping = ""

    average_select = f"""
      SUM(boardhours_taken) / ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
        @since_when,
        @end_when,
        INTERVAL 1 DAY
      )) AS avg_boardhours
    """

  elif grouping_period == "DAYOFWEEK" or grouping_period == "DAY":

    select_statement = f"""
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(ecr.{date_type}, DAY)) AS period,
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY )) AS current_period,
      DATE(TIMESTAMP_TRUNC(ecr.{date_type}, DAY)) AS day,
    """

    optional_groping = f"""
      day,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_where = ""
    average_select = f"""
    SUM(boardhours_taken) /
      (
        SELECT
          COUNTIF(EXTRACT({grouping_period} FROM day) = period)
        FROM
          UNNEST(GENERATE_DATE_ARRAY(DATE(@since_when), DATE(@end_when))) AS day
      ) AS avg_boardhours"""

  else:
    select_statement = f"""
      {f"EXTRACT(YEAR FROM ecr.{date_type}) AS year," if  group_years  else ""}
      EXTRACT({grouping_period} FROM ecr.{date_type}) AS period,
      EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP() AT TIME ZONE 'Europe/Warsaw') AS current_period,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_where = f"""
      AND NOT (
        EXTRACT(YEAR FROM ecr.{date_type}) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
        AND EXTRACT({grouping_period} FROM ecr.{date_type}) >= EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP())
      )
    """ if not grouping_period == "YEAR" else ""

    optional_groping = "year," if group_years else ""

    average_select = f"""
    AVG(boardhours_taken) AS avg_boardhours"""

  query = f"""

  WITH daily_counts AS (
    SELECT
      {select_statement}
      SUM(
        CASE
          WHEN ecr.reservation_system = "plan4u"
            THEN rso.slots_taken * rso.time_taken / 60
          ELSE ecr.slots_taken * ecr.time_taken / 60
        END
      ) AS boardhours_taken
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    JOIN
      reservation_data.dim_client dc
    ON
      dc.id = ecr.client_id
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      dvt.id = ecr.visit_type_id
    LEFT JOIN
      reservation_data.reservation_slots_occupancy rso
    ON
      ecr.id = rso.reservation_id
    WHERE
      ecr.deleted_at IS NULL
      AND DATE({date_type}) >= DATE(@since_when)
      AND DATE({date_type}) <= DATE(@end_when)
      AND CASE
        WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
        WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
        ELSE 'Zrealizowane'
      END {status_condition}
      AND dc.language {language_condition}
      AND dvt.name {visit_type_condition}
      AND dl.city {cities_condition}
      AND dvt.attraction_group {attraction_condition}
      {optional_where}
    GROUP BY {optional_groping} period
  )
  SELECT
    {average_select},
    {later_extract}
  FROM daily_counts
  GROUP BY period, current_period
  ORDER BY period;

  """
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)
  df.sort_values('period', inplace=True)

  if grouping_period == "DAYOFWEEK":
    df['period'] = df['period'].map(day_of_week_map)
    df['current_period'] = df['current_period'].map(day_of_week_map)
  elif grouping_period == "MONTH":
    df['period'] = df['period'].map(utils.get_month_from_month_number)
    df['current_period'] = df['current_period'].map(utils.get_month_from_month_number)


  return df

def get_people_by_time_period(date_type, since_when, end_when, status, cities, language, attraction_groups, visit_type_groups, grouping_period):

  cities_condition = format_array_for_query(cities)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  group_years = since_when.year != end_when.year

  if grouping_period == "HOUR":

    select_statement = f"""
      TIMESTAMP_TRUNC(ecr.{date_type}, {grouping_period}, "Europe/Warsaw") AS period,
      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), {grouping_period}, "Europe/Warsaw") AS current_period,
    """

    later_extract = f"""
      EXTRACT({grouping_period} FROM period) AS period,
      EXTRACT({grouping_period}
        FROM current_period AT TIME ZONE "Europe/Warsaw"
      ) AS current_period
    """

    optional_groping = ""
    optional_where = ""
    average_select = f"""
      SUM(person_count) / ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
        @since_when,
        @end_when,
        INTERVAL 1 DAY
      )) AS avg_people
    """

  elif grouping_period == "DAYOFWEEK" or grouping_period == "DAY":

    select_statement = f"""
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(ecr.{date_type}, DAY)) AS period,
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)) AS current_period,
      DATE(TIMESTAMP_TRUNC(ecr.{date_type}, DAY)) AS day,
    """

    optional_groping = f"""
      day,
    """

    later_extract = f"""
      period,
      current_period
    """
    optional_where = ""
    average_select = f"""
    SUM(person_count) /
      (
        SELECT
          COUNTIF(EXTRACT({grouping_period} FROM day) = period)
        FROM
          UNNEST(GENERATE_DATE_ARRAY(DATE(@since_when), DATE(@end_when))) AS day
      ) AS avg_people"""

  else:
    select_statement = f"""
      {f"EXTRACT(YEAR FROM ecr.{date_type}) AS year," if group_years else ""}
      EXTRACT({grouping_period} FROM ecr.{date_type}) AS period,
      EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP() AT TIME ZONE 'Europe/Warsaw') AS current_period,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_groping = "year," if group_years else ""

    optional_where = f"""
      AND NOT (
        EXTRACT(YEAR FROM ecr.{date_type}) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP() )
        AND EXTRACT({grouping_period} FROM ecr.{date_type}) >= EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP())
      )
    """ if not grouping_period == "YEAR" else ""

    average_select = f"""
    AVG(person_count) avg_people"""

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
  WITH mocked AS (
    SELECT
      {select_statement}
      SUM(
        mock_people(
          dvt.name,
          dl.city,
          ecr.no_of_people,
          DATE({date_type})
        )
      ) AS person_count
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    JOIN
      reservation_data.dim_client dc
    ON
      dc.id = ecr.client_id
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      dvt.id = ecr.visit_type_id
    WHERE
      ecr.deleted_at IS NULL
      AND DATE({date_type}) >= DATE(@since_when)
      AND DATE({date_type}) <= DATE(@end_when)
      AND CASE
        WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
        WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
        ELSE 'Zrealizowane'
      END {status_condition}
      AND dc.language {language_condition}
      AND dvt.name {visit_type_condition}
      AND dl.city {cities_condition}
      AND dvt.attraction_group {attraction_condition}
      {optional_where}
    GROUP BY {optional_groping} period
  )
  SELECT
    {average_select},
    {later_extract}
  FROM
    mocked
  GROUP BY period, current_period
  ORDER BY period

  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)
  df.sort_values('period', inplace=True)

  if grouping_period == "DAYOFWEEK":
    df['period'] = df['period'].map(day_of_week_map)
    df['current_period'] = df['current_period'].map(day_of_week_map)
  elif grouping_period == "MONTH":
    df['period'] = df['period'].map(utils.get_month_from_month_number)
    df['current_period'] = df['current_period'].map(utils.get_month_from_month_number)

  return df


def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"
