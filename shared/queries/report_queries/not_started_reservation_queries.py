import pandas as pd
from queries import run_query
import utils
from google.cloud import bigquery

def get_started_reservation_external_ids(reservations_ids):

  if len(reservations_ids) == 0:
    return pd.DataFrame(columns=["reservation_external_id"])
  
  reservations_ids_to_use = utils.format_array_for_query(reservations_ids)

  query = f"""
    SELECT 
      DISTINCT ess.reservation_external_id AS reservation_external_id
    FROM
      board_playthroughs.event_start_session ess
    WHERE 
      ess.reservation_external_id {reservations_ids_to_use}
  """

  rows = run_query(query)
  return pd.DataFrame(rows)

def get_starts_percent_without_reservation_selected(start, end):

  query = f"""
    SELECT 
      COUNTIF(reservation_external_id = 'NO-RESERVATION') / COUNT(*) * 100 AS percent
    FROM
      board_playthroughs.event_start_session ess
    WHERE 
      date >= TIMESTAMP(@start)
      AND date <= TIMESTAMP(@end)
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
        bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  return df["percent"].loc[0]