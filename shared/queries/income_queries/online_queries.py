import pandas as pd
from google.cloud import bigquery
from queries import run_query
import utils
from datetime import timedelta
import streamlit as st

day_of_week_map = {
  1: "7. Niedziela",
  2: "1. Poniedzialek",
  3: "2. Wtorek",
  4: "3. Sroda",
  5: "4. Czwartek",
  6: "5. Piatek",
  7: "6. Sobota"
}

mock_function = """
  CREATE TEMP FUNCTION mock_price(
    day_of_week INT64,
    visit_type STRING,
    city STRING,
    additional_items_cost FLOAT64,
    current_price FLOAT64,
    date TIMESTAMP
  ) RETURNS FLOAT64
  LANGUAGE js AS '''
  if (new Date(date) >= new Date('2025-02-01')) {{
    return current_price;
  }}

  if (visit_type === "urodziny - standard" || visit_type === "urodziny Pixel") {{
    if (day_of_week > 0 && day_of_week < 5) {{
      return Math.max(549 + additional_items_cost, current_price);
    }} else {{
      return Math.max(649 + additional_items_cost, current_price);
    }}
  }}

  if (visit_type === "urodziny - L") {{
    if (day_of_week > 0 && day_of_week < 5) {{
      return Math.max(899 + additional_items_cost, current_price);
    }} else {{
      return Math.max(999 + additional_items_cost, current_price);
    }}
  }}

  if (visit_type === "urodziny - XL") {{
    if (city === "wroclaw") {{
      if (day_of_week > 0 && day_of_week < 5) {{
        return Math.max(899 + additional_items_cost, current_price);
      }} else {{
        return Math.max(999 + additional_items_cost, current_price);
      }}
    }}
    if (day_of_week > 0 && day_of_week < 5) {{
      return 1349 + additional_items_cost;
    }} else {{
      return 1449 + additional_items_cost;
    }}
  }}

  if (visit_type === "urodziny - XXL") {{
    if (city === "wroclaw") {{
      if (day_of_week > 0 && day_of_week < 5) {{
        return Math.max(2299 + additional_items_cost, current_price);
      }} else {{
        return Math.max(2399 + additional_items_cost, current_price);
      }}
    }}
    if (day_of_week > 0 && day_of_week < 5) {{
      return Math.max(1799 + additional_items_cost, current_price);
    }} else {{
      return Math.max(1889 + additional_items_cost, current_price);
    }}
  }}

  if (visit_type === "szkoła do 24 osób") {{
    return Math.max(504 + additional_items_cost, current_price);
  }}

  if (visit_type === "szkoła do 36 osób") {{
    return Math.max(840 + additional_items_cost, current_price);
  }}

  if (visit_type === "szkoła do 48 osób") {{
    return Math.max(1176 + additional_items_cost, current_price);
  }}

  if (visit_type === "szkoła od 48 osób") {{
    return Math.max(1512 + additional_items_cost, current_price);
  }}

  if (visit_type === "integracja - L") {{
    return Math.max(699 + additional_items_cost, current_price);
  }}

  if (visit_type === "integracja - L+") {{
    if (city === "wroclaw") {{
      return Math.max(999 + additional_items_cost, current_price);
    }} else {{
      return Math.max(1299 + additional_items_cost, current_price);
    }}
  }}

  if (visit_type === "integracja - XL") {{
    if (city === "wroclaw") {{
      return Math.max(1299 + additional_items_cost, current_price);
    }} else {{
      return Math.max(1899 + additional_items_cost, current_price);
    }}
  }}

  if (visit_type === "integracja - XL+") {{
    if (city === "wroclaw") {{
      return Math.max(1899 + additional_items_cost, current_price);
    }} else {{
      return Math.max(2449 + additional_items_cost, current_price);
    }}
  }}

  if (visit_type === "integracja - XXL") {{
    return Math.max(2899 + additional_items_cost, current_price);
  }}

  return current_price;
  ''';
"""
@st.cache_data(ttl=28800)
def get_online_income(groupBy, moving_average_days, start, end, date_type, cities, languages, attractions, status, attraction_types):

  cities_condition = utils.format_array_for_query(cities)
  language_condition = utils.format_array_for_query(languages)
  attraction_condition = utils.format_array_for_query(attractions)
  status_condition = utils.format_array_for_query(status)
  visit_type_condition = utils.format_array_for_query(attraction_types)

  groupBy_select = f", {groupBy}" if groupBy else ""
  
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""

  {mock_function}

  with initial as (SELECT
    ecr.deleted_at,
    dc.language,
    dvt.name as visit_type,
    dl.street,
    dvt.attraction_group,
    DATE(ecr.{date_type}) as date,
    CASE WHEN ecr.is_cancelled = TRUE THEN 'Anulowane' WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone' ELSE 'Zrealizowane' END AS status,
    mock_price((EXTRACT(DAYOFWEEK FROM ecr.start_date) - 1), dvt.name, dl.city, ecr.additional_items_cost, ecr.whole_cost_with_voucher, ecr.booked_date) as price
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location dl
  ON
    dl.id = ecr.location_id
  JOIN
    reservation_data.dim_visit_type dvt
  ON
    dvt.id = ecr.visit_type_id
  JOIN
    reservation_data.dim_client dc
  ON
    dc.id = ecr.client_id
  WHERE
    ecr.deleted_at IS NULL
    AND DATE({date_type}) >= DATE(@since_when)
    AND DATE({date_type}) < DATE(@until_when)
    AND CASE
      WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
      WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
      ELSE 'Zrealizowane'
    END {status_condition}
    AND dc.language {language_condition}
    AND dvt.name {visit_type_condition}
    AND dl.street {cities_condition}
    AND dvt.attraction_group {attraction_condition}
  ),
  calculation AS (
    SELECT
      SUM(price) AS price,
      date
      {groupBy_select}
    FROM
      initial
    GROUP BY
      date
      {groupBy_select}
  )
  SELECT
    price,
    date,
    CASE
      WHEN COUNT(price) OVER (
          {groupBy_partition}
          ORDER BY date ASC
          ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
      ) > {moving_average_days}
      THEN AVG(price) OVER (
          {groupBy_partition}
          ORDER BY date ASC
          ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
      )
    END AS price_ma
    {groupBy_select}
  FROM
    calculation
  ORDER BY
    date ASC;

  """
  
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
      bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start),
      bigquery.ScalarQueryParameter("until_when", "TIMESTAMP", end),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df

@st.cache_data(ttl=28800)
def get_online_income_by_time_period(date_type, since_when, end_when, status, cities, language, attraction_groups, visit_type_groups, grouping_period):

  cities_condition = utils.format_array_for_query(cities)
  language_condition = utils.format_array_for_query(language)
  attraction_condition = utils.format_array_for_query(attraction_groups)
  status_condition = utils.format_array_for_query(status)
  visit_type_condition = utils.format_array_for_query(visit_type_groups)
  
  group_years = since_when.year != end_when.year

  if grouping_period == "HOUR":

    select_statement = f"""
      TIMESTAMP_TRUNC(date, {grouping_period}, "Europe/Warsaw") AS period,
      EXTRACT(DAY FROM date) AS day,
      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), {grouping_period}, "Europe/Warsaw") AS current_period,
    """

    later_extract = f"""
      EXTRACT({grouping_period} FROM period) AS period,
      EXTRACT({grouping_period}
        FROM current_period AT TIME ZONE "Europe/Warsaw"
      ) AS current_period
    """

    optional_grouping = "day,"
    optional_where = ""

    average_select = f"""
      SUM(price) / ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
        @since_when,
        @end_when,
        INTERVAL 1 DAY
      )) AS avg_count
    """

  elif grouping_period == "DAYOFWEEK" or grouping_period == "DAY":

    select_statement = f"""
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(date, DAY)) AS period,
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY )) AS current_period,
      DATE(TIMESTAMP_TRUNC(date, DAY)) AS day,
    """

    optional_grouping = f"""
      day,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_where = ""
    average_select = f"""
    SUM(price) /
      (
        SELECT
          COUNTIF(EXTRACT({grouping_period} FROM day) = period)
        FROM
          UNNEST(GENERATE_DATE_ARRAY(DATE(@since_when), DATE(@end_when))) AS day
      ) AS avg_count"""

  else:
    select_statement = f"""
      {f"EXTRACT(YEAR FROM date) AS year," if  group_years  else ""}
      EXTRACT({grouping_period} FROM date) AS period,
      EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP() AT TIME ZONE 'Europe/Warsaw') AS current_period,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_grouping = "year," if group_years else ""

    optional_where = f"""
      AND NOT (
        EXTRACT(YEAR FROM ecr.{date_type}) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
        AND EXTRACT({grouping_period} FROM ecr.{date_type}) >= EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP())
      )
    """ if not grouping_period == "YEAR" else ""

    average_select = f"""
    AVG(price) AS avg_count"""

  query = f"""

  {mock_function}

  with initial as (SELECT
    ecr.deleted_at,
    ecr.is_cancelled,
    ecr.is_payed,
    dc.language,
    dvt.name,
    dl.street,
    dvt.attraction_group,
    ecr.{date_type} as date,
    mock_price((EXTRACT(DAYOFWEEK FROM ecr.start_date) - 1), dvt.name, dl.city, ecr.additional_items_cost, ecr.whole_cost_with_voucher, ecr.booked_date) as price
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location dl
  ON
    dl.id = ecr.location_id
  JOIN
    reservation_data.dim_visit_type dvt
  ON
    dvt.id = ecr.visit_type_id
  JOIN
    reservation_data.dim_client dc
  ON
    dc.id = ecr.client_id
  WHERE
    ecr.deleted_at IS NULL
    AND DATE({date_type}) >= DATE(@since_when)
    AND DATE({date_type}) < DATE(@end_when)
    AND CASE
      WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
      WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
      ELSE 'Zrealizowane'
    END {status_condition}
    AND dc.language {language_condition}
    AND dvt.name {visit_type_condition}
    AND dl.street {cities_condition}
    AND dvt.attraction_group {attraction_condition}
    {optional_where}
  ),
  grouped as (
    SELECT
      {select_statement}
    SUM(price) AS price
    FROM initial
    GROUP BY {optional_grouping} period, current_period
  )
  SELECT
    {later_extract},
    {average_select}
  FROM grouped
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

  if df.empty:
    return df

  df.sort_values('period', inplace=True)
  if grouping_period == "DAYOFWEEK":
    df['period'] = df['period'].map(day_of_week_map)
    df['current_period'] = df['current_period'].map(day_of_week_map)
  elif grouping_period == "MONTH":
    df['period'] = df['period'].map(utils.get_month_from_month_number)
    df['current_period'] = df['current_period'].map(utils.get_month_from_month_number)

  return df
@st.cache_data(ttl=28800)
def get_online_income_cumulative(groupBy, start, end, date_type, cities, languages, attractions, status, attraction_types):
  cities_condition = utils.format_array_for_query(cities)
  language_condition = utils.format_array_for_query(languages)
  attraction_condition = utils.format_array_for_query(attractions)
  status_condition = utils.format_array_for_query(status)
  visit_type_condition = utils.format_array_for_query(attraction_types)

  groupBy_select = f", {groupBy}" if groupBy else ""

  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""
  
  query = f"""

  {mock_function}

  with initial as (SELECT
    ecr.deleted_at,
    CASE WHEN ecr.is_cancelled = TRUE THEN 'Anulowane' WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone' ELSE 'Zrealizowane' END AS status,
    dc.language,
    dvt.name as visit_type,
    dl.street,
    dvt.attraction_group,
    DATE(ecr.{date_type}) as date,
    mock_price((EXTRACT(DAYOFWEEK FROM ecr.start_date) - 1), dvt.name, dl.city, ecr.additional_items_cost, ecr.whole_cost_with_voucher, ecr.booked_date) as price
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location dl
  ON
    dl.id = ecr.location_id
  JOIN
    reservation_data.dim_visit_type dvt
  ON
    dvt.id = ecr.visit_type_id
  JOIN
    reservation_data.dim_client dc
  ON
    dc.id = ecr.client_id
  WHERE
    ecr.deleted_at IS NULL
    AND CASE
      WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
      WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
      ELSE 'Zrealizowane'
    END {status_condition}
    AND dc.language {language_condition}
    AND dvt.name {visit_type_condition}
    AND dl.street {cities_condition}
    AND dvt.attraction_group {attraction_condition}
  ),
  calculation AS (
    SELECT
      SUM(price) AS price,
      date
      {groupBy_select}
    FROM
      initial
    GROUP BY
      date
      {groupBy_select}
  )
  SELECT
    date,
    SUM(price) OVER (
        {groupBy_partition}
        ORDER BY date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS price
    {groupBy_select}
    FROM
      calculation
    ORDER BY
      date;
  """
  
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
      bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start),
      bigquery.ScalarQueryParameter("until_when", "TIMESTAMP", end),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)

  if df.empty:
    return df

  df['date'] = pd.to_datetime(df['date']).dt.date  
  start = start.date()
  end = end.date()
  
  df = df[df['date'] >= start]
  df = df[df['date'] < end]
  
  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df