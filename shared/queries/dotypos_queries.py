import pandas as pd
from google.cloud import bigquery
from queries import run_query
import time
from datetime import datetime
from utils import format_array_for_query

def get_order_items(since_when, cities, moving_average_days, groupBy):

  cities_condition = format_array_for_query(cities)
  
  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_grouping = f", city" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
  WITH initial AS (
  SELECT
    CAST(order_items.price_brutto AS NUMERIC) as brutto,
    CAST(order_items.price_netto AS NUMERIC) as netto,
    o.creation_date as creation_date
    {groupBy_select}
  FROM
    POS_system_data.order_items order_items
  JOIN
    POS_system_data.order o ON order_items.order_id = o.id
  JOIN
    POS_system_data.dim_location location ON o.dim_location_id = location.id
  JOIN 
    POS_system_data.item i ON order_items.item_id = i.id  
  WHERE 
    DATE(o.creation_date) >= DATE(@since_when)  
    AND city {cities_condition}
    AND NOT REGEXP_CONTAINS(i.name, '(?i)bilet|zadatek|voucher|integracja|uczestnik|urodzin')
    AND o.document_number NOT LIKE '0'
  ),
  daily_sums AS (
    SELECT 
      DATE(creation_date) AS date,
      SUM(brutto) AS brutto,
      SUM(netto) AS netto
      {groupBy_select}
    FROM 
      initial
    GROUP BY 
      date
      {groupBy_grouping}
  )
  SELECT 
    date,
    brutto,
    netto,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(brutto) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    ) END AS brutto_rolling_avg,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(netto) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    ) END AS netto_rolling_avg
    {groupBy_select}
  FROM 
    daily_sums
  ORDER BY
  date ASC;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )

  rows =run_query(query, job_config)

  df = pd.DataFrame(rows)
  
  return df
def get_order_items_per_sale(since_when, cities, moving_average_days, groupBy):

  cities_condition = format_array_for_query(cities)
  
  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_grouping = f", city" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
  WITH initial AS (
  SELECT
    CAST(order_items.price_brutto AS NUMERIC) as brutto,
    CAST(order_items.price_netto AS NUMERIC) as netto,
    o.creation_date as creation_date
    {groupBy_select}
  FROM
    POS_system_data.order_items order_items
  JOIN
    POS_system_data.order o ON order_items.order_id = o.id
  JOIN
    POS_system_data.dim_location location ON o.dim_location_id = location.id
  JOIN 
    POS_system_data.item i ON order_items.item_id = i.id  
  WHERE 
    DATE(o.creation_date) >= DATE(@since_when)  
    AND city {cities_condition}
    AND NOT REGEXP_CONTAINS(i.name, '(?i)bilet|zadatek|voucher|integracja|uczestnik|urodzin')
    AND o.document_number NOT LIKE '0'
  ),
  daily_sums AS (
    SELECT 
      DATE(creation_date) AS date,
      AVG(brutto) AS brutto,
      AVG(netto) AS netto
      {groupBy_select}
    FROM 
      initial
    GROUP BY 
      date
      {groupBy_grouping}
  )
  SELECT 
    date,
    brutto,
    netto,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(brutto) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    ) END AS brutto_rolling_avg,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(netto) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    ) END AS netto_rolling_avg
    {groupBy_select}
  FROM 
    daily_sums
  ORDER BY
  date ASC;
  """
  
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )

  rows =run_query(query, job_config)

  df = pd.DataFrame(rows)

  return df

def get_order_items_per_reservation(start_date, cities, moving_average_days, groupBy):

  df = get_order_items(start_date, cities, moving_average_days, groupBy)

  cities_condition = format_array_for_query(cities)

  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
  WITH initial AS(
    SELECT
      COUNT(*) AS count,
      DATE(ecr.start_date) AS date
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
      AND DATE(ecr.start_date) >= DATE(@since_when)
      AND DATE(ecr.start_date) < CURRENT_DATE
      AND city {cities_condition}
      AND CASE
        WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
        WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
        ELSE 'Zrealizowane'
      END IN ('Zrealizowane', 'Zrealizowane nieopłacone')
    GROUP BY
      DATE(ecr.start_date)
      {groupBy_condition}
  )
  SELECT
    date,
    CAST(count AS NUMERIC) AS res_count,
    CASE
      WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN CAST(AVG(count) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    ) AS NUMERIC)
    END AS res_moving_avg
    {groupBy_condition}
    FROM
      INITIAL
    ORDER BY
      date
      {groupBy_condition}
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start_date),
    ]
  )

  rows = run_query(query, job_config)

  df_res = pd.DataFrame(rows)

  df = df.merge(df_res, how='left', on=['date', groupBy])

  df['netto_per_reservation'] = df['netto'] / df['res_count']
  df['brutto_per_reservation'] = df['brutto'] / df['res_count']

  moving_average_days = moving_average_days + 1
  if groupBy:
    df['netto_per_reservation_moving_avg'] = df.groupby(groupBy)['netto_per_reservation'].transform(
        lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

    df['brutto_per_reservation_moving_avg'] = df.groupby(groupBy)['brutto_per_reservation'].transform(
      lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())
    
  else:
    df['netto_per_reservation_moving_avg'] = df['netto_per_reservation'].transform(
        lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

    df['brutto_per_reservation_moving_avg'] = df['brutto_per_reservation'].transform(
      lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())


  return df

def get_items_sales_per_day(start_date, end_date, moving_average_days, cities, items, groupBy): 

  cities_condition = format_array_for_query(cities)
  items_condition = f"AND name {format_array_for_query(items)}" if items else ""

  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_grouping = f", city" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
  WITH daily_counts AS (
  SELECT 
    DATE(o.creation_date) AS date,
    SUM(oi.quantity) AS count
    {groupBy_select}
  FROM
    POS_system_data.order_items oi
  JOIN
    POS_system_data.order o
  ON
    oi.order_id = o.id
  JOIN 
    POS_system_data.item i
  ON
    oi.item_id = i.id    
  JOIN
    POS_system_data.dim_location l
  ON
    o.dim_location_id = l.id  
  WHERE
    DATE(o.creation_date) >= DATE(@since_when)
    AND DATE(o.creation_date) < DATE(@end_when)
    AND l.city {cities_condition}
    AND NOT REGEXP_CONTAINS(i.name, '(?i)bilet|zadatek|voucher|integracja|uczestnik|urodzin')
    AND o.document_number NOT LIKE '0'
    {items_condition}
  GROUP BY
    DATE(o.creation_date)
    {groupBy_grouping}
  )
  SELECT 
    date,
    count,
    CASE
      WHEN COUNT(count) OVER (
        {groupBy_partition}
        ORDER BY date ASC 
        ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
      ) > {moving_average_days}
      THEN AVG(count) OVER (
        {groupBy_partition}
        ORDER BY date ASC 
        ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
      ) END AS count_moving_avg
      {groupBy_select}
  FROM 
    daily_counts
  ORDER BY
    date ASC;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)

  return df


def get_items_sold(start_date, end_date, cities, items, groupBy):

  cities_condition = format_array_for_query(cities)
  items_condition = f"AND name {format_array_for_query(items)}" if items else ""  

  groupBy_select = f", {groupBy}" if groupBy else ""

  query = f"""
  SELECT 
    i.name,
    SUM(oi.quantity) AS count,
    SUM(CAST(oi.price_brutto AS NUMERIC)) AS brutto,
    SUM(CAST(oi.price_netto AS NUMERIC)) AS netto,
    AVG(CAST(oi.price_brutto AS NUMERIC)) AS avg_brutto,
    AVG(CAST(oi.price_netto AS NUMERIC)) AS avg_netto
    {groupBy_select}
  FROM
    POS_system_data.order_items oi
  JOIN
    POS_system_data.order o
  ON
    oi.order_id = o.id
  JOIN 
    POS_system_data.item i
  ON
    oi.item_id = i.id    
  JOIN
    POS_system_data.dim_location l
  ON
    o.dim_location_id = l.id
  WHERE 
    DATE(o.creation_date) >= DATE(@since_when)
    AND DATE(o.creation_date) < DATE(@end_when)
    AND l.city {cities_condition}
    AND o.document_number NOT LIKE '0'
    {items_condition}  
  GROUP BY
    i.name
    {groupBy_select}
  ORDER BY
    i.name ASC;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)

  return df
