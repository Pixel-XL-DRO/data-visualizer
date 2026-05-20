import pandas as pd
from queries import run_query
import utils
from google.cloud import bigquery

BATCH_SIZE = 5000

def get_started_reservation_external_ids(reservations_ids):

  if len(reservations_ids) == 0:
    return pd.DataFrame(columns=["reservation_external_id"])

  results = []

  for i in range(0, len(reservations_ids), BATCH_SIZE):
    batch = reservations_ids[i:i + BATCH_SIZE]
    batch_ids = utils.format_array_for_query(batch)

    query = f"""
      SELECT
        DISTINCT ess.reservation_external_id AS reservation_external_id
      FROM
        board_playthroughs.event_start_session ess
      WHERE
        ess.reservation_external_id {batch_ids}
    """

    rows = run_query(query)
    results.append(pd.DataFrame(rows))

  return pd.concat(results, ignore_index=True).drop_duplicates(subset=["reservation_external_id"])

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