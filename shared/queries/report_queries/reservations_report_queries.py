import pandas as pd
from queries import run_query
import utils
from google.cloud import bigquery

def get_reservations_count(start, end):

  query = """
    SELECT
      EXTRACT(MONTH FROM ecr.start_date) AS month,
      LOWER(dvt.attraction_group) as attraction_group,
      dl.city,
      COUNT(DISTINCT ecr.id) AS count
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_visit_type dvt
    ON
      dvt.id = ecr.visit_type_id
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    WHERE
      ecr.start_date >= TIMESTAMP(@start)
    AND
      ecr.start_date <= TIMESTAMP(@end)
    AND
      NOT ecr.is_cancelled
    AND
      dl.street != "arkadia"
    AND
      LOWER(dvt.attraction_group) != "blokada"
    GROUP BY
      month, dvt.attraction_group, dl.city
    ORDER BY
      month;
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
        bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
    ]
  )

  rows = run_query(query, job_config)

  return pd.DataFrame(rows)


def get_city_opening_order():
  query = """
    SELECT
      dl.city,
      MIN(ecr.start_date) AS first_reservation
    FROM
      reservation_data.event_create_reservation ecr
    JOIN
      reservation_data.dim_location dl
    ON
      dl.id = ecr.location_id
    WHERE
      NOT ecr.is_cancelled
    AND
      dl.street != "arkadia"
    GROUP BY
      dl.city
    ORDER BY
      first_reservation
  """
  rows = run_query(query)
  return pd.DataFrame(rows)["city"].tolist()