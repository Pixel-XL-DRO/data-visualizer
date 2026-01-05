import pandas as pd
from google.cloud import bigquery
from queries import run_performance_review_query
from queries import run_query
import utils
from datetime import datetime, timedelta
import streamlit as st

@st.cache_data(ttl=28800)
def get_pos_income(start, end, cities, filter_checkbox, moving_average_days, groupBy):

  if start.year == 2025 and start < pd.to_datetime("2025-02-01"):
    start = datetime(year=2025, month=2, day=1, hour=0, minute=0)

  cities_condition = utils.format_array_for_query(cities)

  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
  WITH initial AS (
  SELECT 
    SUM(CAST(oi.price_brutto AS FLOAT64)) AS price, 
    DATE(o.creation_date) AS date
    {groupBy_select}
  FROM 
    POS_system_data.order o
  JOIN 
    POS_system_data.dim_location l
  ON 
    l.id = o.dim_location_id  
  JOIN 
    POS_system_data.order_items oi
  ON
    o.id = oi.order_id  
  JOIN 
    POS_system_data.item i  
  ON
    i.id = oi.item_id
  WHERE
    document_number NOT LIKE '0' 
    AND document_number IS NOT NULL 
    AND document_number != ''
    AND status NOT LIKE 'canceled'  
    AND l.street {cities_condition}
    AND CASE 
      WHEN {filter_checkbox} 
      THEN REGEXP_CONTAINS(i.name, '(?i)bilet|zadatek|voucher|integracja|uczestnik|urodzin')
      ELSE TRUE
    END
    AND DATE(o.creation_date) >= DATE(@since_when)
    AND DATE(o.creation_date) <= DATE(@until_when)
  GROUP BY
    DATE(o.creation_date)
    {groupBy_select}
  ORDER BY
    DATE(o.creation_date)
  )
  SELECT 
    date,
    price,
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
    initial
  GROUP BY
    date,
    price
    {groupBy_select}
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

  if df.empty:
    return df

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df
  

@st.cache_data(ttl=28800)
def get_pos_income_by_period(grouping_period, start_date, end_date, cities, filter_checkbox):

  if start_date.year == 2025 and start_date < pd.to_datetime("2025-02-01"):
    start_date = datetime(year=2025, month=2, day=1, hour=0, minute=0)

  cities_condition = utils.format_array_for_query(cities)

  group_years = start_date.year != end_date.year

  if grouping_period == "HOUR":

    select_statement = f"""
      TIMESTAMP_TRUNC(o.creation_date, {grouping_period}, "Europe/Warsaw") AS period,
      EXTRACT(DAY FROM o.creation_date) AS day,
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
      SUM(price) / ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
        @since_when,
        @end_when,
        INTERVAL 1 DAY
      )) AS avg_count
    """

  elif grouping_period == "DAYOFWEEK" or grouping_period == "DAY":

    select_statement = f"""
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(o.creation_date, DAY)) AS period,
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY )) AS current_period,
      DATE(TIMESTAMP_TRUNC(o.creation_date, DAY)) AS day,
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
    SUM(price) /
      (
        SELECT
          COUNTIF(EXTRACT({grouping_period} FROM day) = period)
        FROM
          UNNEST(GENERATE_DATE_ARRAY(DATE(@since_when), DATE(@end_when))) AS day
      ) AS avg_count"""

  else:
    select_statement = f"""
      {f"EXTRACT(YEAR FROM o.creation_date) AS year," if  group_years  else ""}
      EXTRACT({grouping_period} FROM o.creation_date) AS period,
      EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP() AT TIME ZONE 'Europe/Warsaw') AS current_period,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_groping = "year," if group_years else ""

    optional_where = f"""
      AND NOT (
        EXTRACT(YEAR FROM o.creation_date) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
        AND EXTRACT({grouping_period} FROM o.creation_date) >= EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP())
      )
    """ if not grouping_period == "YEAR" else ""

    average_select = f"""
    AVG(price) AS avg_count"""

  query = f"""

  WITH initial AS (
  SELECT 
    {select_statement}
    SUM(CAST(oi.price_brutto AS FLOAT64)) AS price
  FROM 
    POS_system_data.order o
  JOIN 
    POS_system_data.dim_location l
  ON 
    l.id = o.dim_location_id  
  JOIN 
    POS_system_data.order_items oi
  ON
    o.id = oi.order_id  
  JOIN 
    POS_system_data.item i  
  ON
    i.id = oi.item_id
  WHERE
    document_number NOT LIKE '0' 
    AND document_number IS NOT NULL 
    AND document_number != ''
    AND status NOT LIKE 'canceled'  
    AND l.street {cities_condition}
    AND CASE 
      WHEN {filter_checkbox} 
      THEN REGEXP_CONTAINS(i.name, '(?i)bilet|zadatek|voucher|integracja|uczestnik|urodzin')
      ELSE TRUE
    END
    AND DATE(o.creation_date) >= DATE(@since_when)
    AND DATE(o.creation_date) <= DATE(@end_when)
    {optional_where}
  GROUP BY
    {optional_groping}
    period
  )
  SELECT 
    {average_select},
    {later_extract}
  FROM 
    initial
  GROUP BY 
    period, current_period  
  ORDER BY
    period;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)
  
  if df.empty:
    return df

  df.sort_values('period', inplace=True)
  if grouping_period == "DAYOFWEEK":
    df['period'] = df['period'].map(utils.day_of_week_map)
    df['current_period'] = df['current_period'].map(utils.day_of_week_map)
  elif grouping_period == "MONTH":
    df['period'] = df['period'].map(utils.get_month_from_month_number)
    df['current_period'] = df['current_period'].map(utils.get_month_from_month_number)

  return df


@st.cache_data(ttl=28800)
def get_pos_cumulative_income(start, end, cities, filter_checkbox, groupBy):

  if start.year == 2025 and start < pd.to_datetime("2025-02-01"):
    start = datetime(year=2025, month=2, day=1, hour=0, minute=0)

  cities_condition = utils.format_array_for_query(cities)

  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
  WITH initial AS (
  SELECT 
    SUM(CAST(oi.price_brutto AS FLOAT64)) AS price, 
    DATE(o.creation_date) AS date
    {groupBy_select}
  FROM 
    POS_system_data.order o
  JOIN 
    POS_system_data.dim_location l
  ON 
    l.id = o.dim_location_id  
  JOIN 
    POS_system_data.order_items oi
  ON
    o.id = oi.order_id  
  JOIN 
    POS_system_data.item i  
  ON
    i.id = oi.item_id
  WHERE
    document_number NOT LIKE '0' 
    AND document_number IS NOT NULL 
    AND document_number != ''
    AND status NOT LIKE 'canceled'  
    AND l.street {cities_condition}
    AND CASE 
      WHEN {filter_checkbox} 
      THEN REGEXP_CONTAINS(i.name, '(?i)bilet|zadatek|voucher|integracja|uczestnik|urodzin')
      ELSE TRUE
    END
  GROUP BY
    DATE(o.creation_date)
    {groupBy_select}
  ORDER BY
    DATE(o.creation_date)
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
    initial
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
  
