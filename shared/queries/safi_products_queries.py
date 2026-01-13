import pandas as pd
from google.cloud import bigquery
from queries import run_query
import utils

def get_items_sales_per_day(start_date, end_date, moving_average_days, cities, items, groupBy): 

  cities_condition = utils.format_array_for_query(cities)
  items_condition = f"AND name {utils.format_array_for_query(items)}" if items else ""

  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_grouping = f", street" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
    WITH daily_counts AS (
    SELECT 
      DATE(rp.date_of_sale) AS date,
      SUM(rp.quantity) AS count
      {groupBy_select}
    FROM
      reservation_data.reservation_product rp
    JOIN
      reservation_data.products p
    ON
      p.id = rp.product_id
    JOIN
      reservation_data.dim_location l
    ON
      rp.dim_location_id = l.id  
    WHERE
      DATE(rp.date_of_sale) >= DATE(@since_when)
      AND DATE(rp.date_of_sale) < DATE(@end_when)
      AND l.street {cities_condition}
      {items_condition}
    GROUP BY
      DATE(rp.date_of_sale)
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
  
  if groupBy:
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df

def get_items_sold(start_date, end_date, cities, items, groupBy):

  cities_condition = utils.format_array_for_query(cities)
  items_condition = f"AND name {utils.format_array_for_query(items)}" if items else ""  

  groupBy_select = f", {groupBy}" if groupBy else ""

  query = f"""
    SELECT 
      p.name as product_name,
      SUM(rp.quantity) AS count,
      SUM(rp.total_price_brutto) AS brutto,
      SUM(rp.total_price_netto) AS netto,
      AVG(rp.total_price_brutto / rp.quantity) AS avg_brutto,
      AVG(rp.total_price_netto / rp.quantity) AS avg_netto
      {groupBy_select}
    FROM
      reservation_data.reservation_product rp
    JOIN
      reservation_data.products p
    ON
      p.id = rp.product_id
    JOIN
      reservation_data.dim_location l
    ON
      rp.dim_location_id = l.id  
    WHERE 
      DATE(rp.date_of_sale) >= DATE(@since_when)
      AND DATE(rp.date_of_sale) < DATE(@end_when)
      AND l.street {cities_condition}
      {items_condition}  
    GROUP BY
      p.name
      {groupBy_select}
    ORDER BY
      p.name ASC;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)

  df = pd.DataFrame(rows)
  if groupBy:
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df
