import pandas as pd
from google.cloud import bigquery
from queries import run_query
import utils
from datetime import timedelta
import streamlit as st

@st.cache_data(ttl=28800)
def get_voucher_income(groupBy, moving_average_days, start_date, end_date, cities):

  cities_condition = utils.format_array_for_query(cities)

  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_grouping = f", street" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""
  
  query = f"""
  WITH daily_totals AS (
  SELECT
    DATE(v.voucher_creation_date) AS date,
    SUM(v.net_amount) AS price
    {groupBy_select}
  FROM
    vouchers_data.voucher v
  JOIN
    vouchers_data.dim_location dl
    ON dl.id = v.dim_location_id
  WHERE
    v.voucher_creation_date >= @since_when
    AND v.voucher_creation_date < @until_when
    AND dl.street {cities_condition}
  GROUP BY
    DATE(v.voucher_creation_date)
    {groupBy_grouping}
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
    ) END AS price_ma
    {groupBy_select}
    FROM
      daily_totals
    ORDER BY
      date;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
      bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start_date),
      bigquery.ScalarQueryParameter("until_when", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query,job_config)

  df = pd.DataFrame(rows)

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 
    
  return df

@st.cache_data(ttl=28800)
def get_vouchers_by_weekday(grouping_period, start_date, end_date, cities):

  cities_condition = utils.format_array_for_query(cities)

  group_years = start_date.year != end_date.year

  if grouping_period == "HOUR":

    select_statement = f"""
      TIMESTAMP_TRUNC(v.voucher_creation_date, {grouping_period}, "Europe/Warsaw") AS period,
      EXTRACT(DAY FROM v.voucher_creation_date) AS day,
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
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(v.voucher_creation_date, DAY)) AS period,
      EXTRACT({grouping_period} FROM TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY )) AS current_period,
      DATE(TIMESTAMP_TRUNC(v.voucher_creation_date, DAY)) AS day,
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
      {f"EXTRACT(YEAR FROM v.voucher_creation_date) AS year," if  group_years  else ""}
      EXTRACT({grouping_period} FROM v.voucher_creation_date) AS period,
      EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP() AT TIME ZONE 'Europe/Warsaw') AS current_period,
    """

    later_extract = f"""
      period,
      current_period
    """

    optional_groping = "year," if group_years else ""

    optional_where = f"""
      AND NOT (
        EXTRACT(YEAR FROM v.voucher_creation_date) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
        AND EXTRACT({grouping_period} FROM v.voucher_creation_date) >= EXTRACT({grouping_period} FROM CURRENT_TIMESTAMP())
      )
    """ if not grouping_period == "YEAR" else ""

    average_select = f"""
    AVG(daily_count) AS avg_count"""

  query = f"""

  WITH daily_counts AS (
    SELECT
      {select_statement}
      SUM(v.net_amount) AS daily_count
    FROM
      vouchers_data.voucher v
    JOIN
      vouchers_data.dim_location dl
    ON
      dl.id = v.dim_location_id
    WHERE
      DATE(v.voucher_creation_date) >= DATE(@since_when)
      AND DATE(v.voucher_creation_date) <= DATE(@end_when)
      AND dl.street {cities_condition}
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
def get_voucher_cumulative_income(groupBy, start_date, end_date, cities):
    
  cities_condition = utils.format_array_for_query(cities)

  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_grouping = f", street" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""
  
    
  query = f"""
  WITH daily_totals AS (
  SELECT
    DATE(v.voucher_creation_date) AS date,
    SUM(v.net_amount) AS price
    {groupBy_select}
  FROM
    vouchers_data.voucher v
  JOIN
    vouchers_data.dim_location dl
    ON dl.id = v.dim_location_id
  WHERE
    dl.street {cities_condition}
  GROUP BY
    DATE(v.voucher_creation_date)
    {groupBy_grouping}
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
      daily_totals
    ORDER BY
      date;
  """ 
  
  rows = run_query(query)
  
  df = pd.DataFrame(rows)  

  if df.empty:
    return df

  df['date'] = pd.to_datetime(df['date']).dt.date  
  start_date = start_date.date()
  end_date = end_date.date()
  
  df = df[df['date'] >= start_date]
  df = df[df['date'] < end_date]
  
  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 
    
  return df
