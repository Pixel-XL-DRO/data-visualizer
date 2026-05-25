import pandas as pd
import streamlit as st
from google.cloud import bigquery
from queries import run_query
import utils


def get_reservations_data(streets, attraction_groups, start_date, end_date):

  attraction_groups_condition = utils.format_array_for_query(attraction_groups)

  query = f"""
    SELECT
      res.id,
      res.location_id AS location_id,
      res.time_taken AS reservation_time_taken,
      res.slots_taken AS reservation_slots_taken,
      res.reservation_system AS reservation_system,
      res.start_date AS start_date,
      loc.street AS street,
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
      res.is_cancelled is FALSE
    AND
      loc.street IN UNNEST(@streets)
    AND
      dvt.attraction_group {attraction_groups_condition}
    AND
      res.start_date >= @start
    AND
      res.start_date <= @end
    AND
      dvt.name != "Arena"
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ArrayQueryParameter("streets", "STRING", streets),
        bigquery.ScalarQueryParameter("start", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  return df