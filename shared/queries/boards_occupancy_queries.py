import pandas as pd
from google.cloud import bigquery
from queries import run_query
import utils

def get_reservations_data(street, attraction_groups, selected_week_start, selected_week_end):

  attraction_groups_condition = utils.format_array_for_query(attraction_groups)
  
  query = f"""
    SELECT
      res.id,
      res.location_id AS location_id,
      res.time_taken AS reservation_time_taken,
      res.slots_taken AS reservation_slots_taken,
      res.reservation_system AS reservation_system,
      res.start_date AS start_date,
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
      loc.street = @street
    AND
      dvt.attraction_group {attraction_groups_condition}
    AND
      res.start_date >= @start
    AND 
      res.start_date <= @end  
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("street", "STRING", street),
        bigquery.ScalarQueryParameter("start", "TIMESTAMP", selected_week_start),
        bigquery.ScalarQueryParameter("end", "TIMESTAMP", selected_week_end),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)
 
  return df