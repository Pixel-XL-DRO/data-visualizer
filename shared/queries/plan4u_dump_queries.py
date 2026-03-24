from queries import run_query

def get_clients(utc_start, utc_end):

  query = f"""
    SELECT DISTINCT
      c.first_name AS customer_name,
      c.last_name AS customer_surname,
      c.email AS customer_email,
      c.phone AS customer_phone,
      dl.city AS location_name,
      ecr.attraction_group AS visit_name
    FROM
      plan4u_dump_data.client c
    JOIN
      plan4u_dump_data.sale s
    ON
      s.client_id = c.id
    JOIN 
      reservation_data.event_create_reservation ecr  
    ON
      s.reservation_id = ecr.reservation_external_id
    JOIN
      reservation_data.dim_location dl
    ON
      ecr.location_id = dl.id
    WHERE 
      ecr.start_date >= TIMESTAMP("{utc_start}")
      AND ecr.start_date < TIMESTAMP("{utc_end}")
      AND NOT ecr.is_cancelled 
      AND s.reservation_system_url = ecr.reservation_system_url
      AND NOT EXISTS (
        SELECT 1 
        FROM reservation_data.event_create_reservation future_ecr
        JOIN plan4u_dump_data.sale future_s ON future_s.reservation_id = future_ecr.reservation_external_id
        WHERE future_s.client_id = c.id 
          AND future_s.reservation_system_url = future_ecr.reservation_system_url
          AND future_ecr.start_date > TIMESTAMP("{utc_end}")
          AND NOT future_ecr.is_cancelled
      );    
  """
  rows = run_query(query)
  return rows