import pandas as pd
import streamlit as st
from google.cloud import bigquery
from queries import run_query

@st.cache_data(ttl=28800)
def get_reservations_for_report(streets, attraction_groups, visit_types, start_date, end_date):
  query = """
    SELECT
      res.id,
      res.time_taken AS reservation_time_taken,
      res.slots_taken AS reservation_slots_taken,
      res.reservation_system AS reservation_system,
      res.start_date AS start_date,
      loc.street AS street,
      dvt.attraction_group AS attraction_group,
    FROM
      `pixelxl-database-dev.reservation_data.event_create_reservation` res
    JOIN
      `pixelxl-database-dev.reservation_data.dim_location` loc
    ON
      res.location_id = loc.id
    JOIN
      `pixelxl-database-dev.reservation_data.dim_visit_type` dvt
    ON
      dvt.id = res.visit_type_id
    WHERE
      res.deleted_at IS NULL
    AND
      res.is_cancelled IS FALSE
    AND
      loc.street IN UNNEST(@streets)
    AND
      dvt.attraction_group IN UNNEST(@attraction_groups)
    AND
      dvt.name IN UNNEST(@visit_types)
    AND
      res.start_date >= @start
    AND
      res.start_date <= @end
    AND
      dvt.name != 'Arena'
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
      bigquery.ArrayQueryParameter("streets", "STRING", list(streets)),
      bigquery.ArrayQueryParameter("attraction_groups", "STRING", list(attraction_groups)),
      bigquery.ArrayQueryParameter("visit_types", "STRING", list(visit_types)),
      bigquery.ScalarQueryParameter("start", "TIMESTAMP", start_date),
      bigquery.ScalarQueryParameter("end", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)
  return df