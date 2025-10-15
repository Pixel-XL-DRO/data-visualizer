from queries import run_reviews_query
import pandas as pd
from google.cloud import bigquery

def get_google_reviews_monthly(since_when, end_when, rating, cities):

  cities_condition = format_array_for_query(cities)

  if rating == "Wszystkie (suma)":
    rating_condition = ""
  else:
    rating_condition = f"AND ratings.value = {rating}"

  query = f"""
    SELECT
      COUNT(ratings.value) AS count,
      FORMAT_DATE('%m/%Y', DATE(ratings.create_time)) AS month_year
    FROM
      reviews.star_rating ratings
    JOIN
      reviews.dim_location dim_location
      ON ratings.location_id = dim_location.name
    WHERE
      ratings.create_time >= @since_when
      AND ratings.create_time < @end_when
      AND dim_location.locality {cities_condition}
      {rating_condition}
    GROUP BY
      month_year
    ORDER BY
      PARSE_DATE('%m/%Y', month_year);
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_reviews_query(query,job_config)
  return pd.DataFrame(rows)

def get_google_reviews_daily(since_when, end_when, rating, cities):

  cities_condition = format_array_for_query(cities)

  if rating == "Wszystkie (suma)":
    rating_condition = ""
  else:
    rating_condition = f"AND ratings.value = {rating}"

  query = f"""
    SELECT
      COUNT(ratings.value) AS count,
      FORMAT_DATE('%d/%m/%Y', DATE(ratings.create_time)) AS day
    FROM
      reviews.star_rating ratings
    JOIN
      reviews.dim_location dim_location
      ON ratings.location_id = dim_location.name
    WHERE
      ratings.create_time >= @since_when
      AND ratings.create_time < @end_when
      AND dim_location.locality {cities_condition}
      {rating_condition}
    GROUP BY
      day
    ORDER BY
      PARSE_DATE('%d/%m/%Y', day);

  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_reviews_query(query,job_config)
  return pd.DataFrame(rows)

def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"
