import pandas as pd
from queries import run_query
import utils

def get_started_reservation_percent_without_mark_as_started(reservations_ids):

  if len(reservations_ids) == 0:
    return pd.DataFrame([{"started_but_unchecked": 0}])
  
  reservations_ids_to_use = utils.format_array_for_query(reservations_ids)

  query = f"""
    SELECT 
      COUNT(DISTINCT ess.reservation_external_id) AS started_but_unchecked
    FROM
      board_playthroughs.event_start_session ess
    WHERE 
      ess.reservation_external_id {reservations_ids_to_use}
  """

  rows = run_query(query)
  return pd.DataFrame(rows)