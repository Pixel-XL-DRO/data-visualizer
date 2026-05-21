import pandas as pd
import streamlit as st
from google.cloud import bigquery
from queries import run_query
import utils


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


@st.cache_data(ttl=28800)
def get_reservations_with_visit_type(streets, attraction_groups, visit_types, start_date, end_date):

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
      dvt.name IN UNNEST(@visit_types)
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
        bigquery.ArrayQueryParameter("visit_types", "STRING", list(visit_types)),
        bigquery.ScalarQueryParameter("start", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end", "TIMESTAMP", end_date),
    ]
  )

  rows = run_query(query, job_config)
  df = pd.DataFrame(rows)

  return df


@st.cache_data(ttl=28800)
def get_occupancy_by_hour(streets, attraction_groups, visit_types, start_date, end_date) -> pd.DataFrame:
  """
  Returns per-hour slot occupancy aggregated in SQL.
  Columns: street (str), slot_date (date), slot_hour (int), slots_taken (float), total_boards (float).
  """
  query = """
    WITH plan4u_agg AS (
      SELECT
        reservation_id AS slots_occupancy_reservation_id,
        SUM(slots_taken) / (SUM(time_taken) / 60.0) AS slots_per_hour,
        SUM(time_taken) AS total_time_minutes
      FROM `pixelxl-database-dev.reservation_data.reservation_slots_occupancy`
      WHERE datetime_slot BETWEEN
        TIMESTAMP_SUB(@start, INTERVAL 1 DAY)
        AND TIMESTAMP_ADD(@end, INTERVAL 1 DAY)
      GROUP BY 1
    ),

    reservations AS (
      SELECT
        loc.street,
        res.start_date,
        CASE
          WHEN res.reservation_system = 'plan4u' THEN p4u.slots_per_hour
          ELSE CAST(res.slots_taken AS FLOAT64)
        END AS slots_per_hour,
        CASE
          WHEN res.reservation_system = 'plan4u' THEN p4u.total_time_minutes
          ELSE CAST(res.time_taken AS FLOAT64)
        END AS time_taken_minutes
      FROM `pixelxl-database-dev.reservation_data.event_create_reservation` res
      JOIN `pixelxl-database-dev.reservation_data.dim_location` loc
        ON loc.id = res.location_id
      JOIN `pixelxl-database-dev.reservation_data.dim_visit_type` dvt
        ON dvt.id = res.visit_type_id
      LEFT JOIN plan4u_agg p4u
        ON p4u.slots_occupancy_reservation_id = res.id
      WHERE
        res.deleted_at IS NULL
        AND res.is_cancelled IS FALSE
        AND loc.street IN UNNEST(@streets)
        AND dvt.attraction_group IN UNNEST(@attraction_groups)
        AND dvt.name IN UNNEST(@visit_types)
        AND dvt.name != 'Arena'
        AND res.start_date >= @start
        AND res.start_date <= @end
        AND (res.reservation_system != 'plan4u' OR (p4u.slots_per_hour IS NOT NULL AND p4u.total_time_minutes > 0))
    ),

    hourly_slots AS (
      SELECT
        street,
        DATE(
          DATETIME_ADD(
            DATETIME_TRUNC(DATETIME(start_date, 'UTC'), HOUR),
            INTERVAL h HOUR
          )
        ) AS slot_date,
        EXTRACT(HOUR FROM
          DATETIME_ADD(
            DATETIME_TRUNC(DATETIME(start_date, 'UTC'), HOUR),
            INTERVAL h HOUR
          )
        ) AS slot_hour,
        SUM(slots_per_hour) AS slots_taken
      FROM reservations
      CROSS JOIN UNNEST(
        GENERATE_ARRAY(0, CAST(CEIL(time_taken_minutes / 60.0) - 1 AS INT64))
      ) AS h
      GROUP BY 1, 2, 3
    ),

    effective_hours AS (
      SELECT
        hs.street,
        hs.slot_date,
        ha.starting_hour,
        ha.number_of_hours,
        ROW_NUMBER() OVER (
          PARTITION BY hs.street, hs.slot_date
          ORDER BY ha.since_when DESC
        ) AS rn
      FROM (SELECT DISTINCT street, slot_date FROM hourly_slots) hs
      JOIN `pixelxl-database-dev.reservation_data.dim_location` loc
        ON loc.street = hs.street
      JOIN `pixelxl-database-dev.reservation_data.historical_location_hours_availability` ha
        ON ha.dim_location_id = loc.id
        AND ha.day_of_week = MOD(EXTRACT(DAYOFWEEK FROM hs.slot_date) + 6, 7)
        AND ha.since_when <= TIMESTAMP(hs.slot_date)
    ),

    capacity AS (
      SELECT
        hs.street,
        hs.slot_date,
        ba.number_of_boards AS total_boards,
        ROW_NUMBER() OVER (
          PARTITION BY hs.street, hs.slot_date
          ORDER BY ba.since_when DESC
        ) AS rn
      FROM (SELECT DISTINCT street, slot_date FROM hourly_slots) hs
      JOIN `pixelxl-database-dev.reservation_data.dim_location` loc
        ON loc.street = hs.street
      JOIN `pixelxl-database-dev.reservation_data.historical_location_boards_availability` ba
        ON ba.dim_location_id = loc.id
        AND ba.since_when <= TIMESTAMP(hs.slot_date)
        AND (ba.until_when IS NULL OR ba.until_when >= TIMESTAMP(hs.slot_date))
    )

    SELECT
      hs.street,
      hs.slot_date,
      hs.slot_hour,
      hs.slots_taken,
      CAST(c.total_boards AS FLOAT64) AS total_boards
    FROM hourly_slots hs
    JOIN effective_hours eh
      ON eh.street = hs.street
      AND eh.slot_date = hs.slot_date
      AND eh.rn = 1
      AND hs.slot_hour >= eh.starting_hour
      AND hs.slot_hour < eh.starting_hour + eh.number_of_hours
    JOIN capacity c
      ON c.street = hs.street
      AND c.slot_date = hs.slot_date
      AND c.rn = 1
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
  if df.empty:
    return pd.DataFrame(columns=['street', 'slot_date', 'slot_hour', 'slots_taken', 'total_boards'])
  return df
