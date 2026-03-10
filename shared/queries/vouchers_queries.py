import pandas as pd
from google.cloud import bigquery
from queries import run_query
import utils

def get_vouchers_worth(since_when, end_when, cities):

  if cities is None or len(cities) == 0:
    return pd.DataFrame(columns=["count", "name"])

  cities_condition = utils.format_array_for_query(cities)

  query = f"""
    SELECT
      SUM(voucher.net_amount) as worth,
      voucher.voucher_name as name
    FROM
      vouchers_data.voucher voucher
    JOIN
      vouchers_data.dim_location location
    ON
      voucher.dim_location_id = location.id
    WHERE
      location.street {cities_condition}
      AND DATE(voucher_creation_date) >= DATE(@since_when)
      AND DATE(voucher_creation_date) < DATE(@end_when)
    GROUP BY
      voucher.voucher_name
    ORDER BY
    voucher.voucher_name;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_query(query, job_config)

  return pd.DataFrame(rows)

def get_vouchers_count(since_when, end_when, cities):

  if cities is None or len(cities) == 0:
    return pd.DataFrame(columns=["count", "name"])

  cities_condition = utils.format_array_for_query(cities)

  query = f"""
    SELECT
      count(voucher.id) as count,
      voucher.voucher_name as name
    FROM
      vouchers_data.voucher voucher
    JOIN
      vouchers_data.dim_location location
    ON
      voucher.dim_location_id = location.id
    WHERE
      location.street {cities_condition}
      AND DATE(voucher_creation_date) >= DATE(@since_when)
      AND DATE(voucher_creation_date) < DATE(@end_when)
    GROUP BY
      voucher.voucher_name
    ORDER BY
    voucher.voucher_name;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_query(query, job_config)

  return pd.DataFrame(rows)
