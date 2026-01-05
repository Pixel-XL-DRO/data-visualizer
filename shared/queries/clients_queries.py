from queries import run_query
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import time
import utils

def get_retention_data(date_type, since_when, groupBy, streets, language, attraction_groups, status, visit_type_groups, notes=None):

  streets_condition = format_array_for_query(streets)
  language_condition = format_array_for_query(language)
  attraction_condition = format_array_for_query(attraction_groups)
  status_condition = format_array_for_query(status)
  visit_type_condition = format_array_for_query(visit_type_groups)

  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_select = f", {groupBy}" if groupBy else ""
  groupBy_select2 = f", {groupBy}" if groupBy else ""

  if groupBy == 'status':
    groupBy_select = f", CASE WHEN ecr.is_cancelled = TRUE THEN 'Anulowane' WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone' ELSE 'Zrealizowane' END AS {groupBy}"
  elif groupBy == "attraction_group":
    groupBy_select = f", dvt.attraction_group AS {groupBy}"
  elif groupBy == "visit_type":
    groupBy_select = f", dvt.name AS {groupBy}"
  elif groupBy == "street":
    groupBy_select = f", dl.street AS {groupBy}"
    groupBy_select2 = f", street AS {groupBy}"

  query = f"""
WITH client_first_appearance AS (
  SELECT
    client_id,
    MIN({date_type}) AS first_reservation_date
    {groupBy_select}
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
  GROUP BY
    client_id
    {groupBy_condition}
),
reservations_with_client_type AS (
  SELECT
    ecr.id id,
    ecr.client_id,
    {date_type} AS reservation_date,
    CASE
      WHEN DATE_TRUNC({date_type}, MONTH) = DATE_TRUNC(first_reservation_date, MONTH)
        THEN 'new'
      ELSE 'old'
    END AS client_type
    {groupBy_select}
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location dl
  ON
    ecr.location_id = dl.id
  JOIN
    client_first_appearance c
  ON
    ecr.client_id = c.client_id
  JOIN
    reservation_data.dim_client dc
  ON
    ecr.client_id = dc.id
  JOIN
    reservation_data.dim_visit_type dvt
  ON
    dvt.id = ecr.visit_type_id
  WHERE
    ecr.deleted_at IS NULL
    AND dl.street {streets_condition}
    AND dc.language {language_condition}
    AND dvt.name {visit_type_condition}
    AND dvt.attraction_group {attraction_condition}
    AND CASE
        WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
        WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
        ELSE 'Zrealizowane'
      END {status_condition}
),
reservations_per_month AS (
  SELECT
    EXTRACT(YEAR FROM reservation_date) AS year,
    EXTRACT(MONTH FROM reservation_date) AS month,
    COUNTIF(client_type = 'new') AS new_client_reservations,
    COUNTIF(client_type = 'old') AS old_client_reservations,
    COUNT(*) AS total_reservations
    {groupBy_select2}
  FROM
    reservations_with_client_type
  GROUP BY
    year, month
    {groupBy_condition}
)
SELECT
  year,
  CASE month
    WHEN 1 THEN 'Styczeń'
    WHEN 2 THEN 'Luty'
    WHEN 3 THEN 'Marzec'
    WHEN 4 THEN 'Kwiecień'
    WHEN 5 THEN 'Maj'
    WHEN 6 THEN 'Czerwiec'
    WHEN 7 THEN 'Lipiec'
    WHEN 8 THEN 'Sierpień'
    WHEN 9 THEN 'Wrzesień'
    WHEN 10 THEN 'Październik'
    WHEN 11 THEN 'Listopad'
    WHEN 12 THEN 'Grudzień'
  END AS month_name,
  old_client_reservations,
  total_reservations,
  ROUND(SAFE_DIVIDE(old_client_reservations * 100.0, total_reservations), 2) AS percentage_old_reservations
  {groupBy_select2}
FROM
  reservations_per_month
WHERE
  DATE(year, month, 1) >= DATE(@since_when)
ORDER BY

  year, month {groupBy_condition}
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
    ]
  )
  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)
  
  df['date'] = df.apply(lambda row: f"{int(row['year']) if pd.notna(row['year']) else ''} {row['month_name']}", axis=1)
  
  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location)

  return df

def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"