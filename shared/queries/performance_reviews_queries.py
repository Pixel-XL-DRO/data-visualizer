import pandas as pd
from google.cloud import bigquery
from queries import run_performance_review_query
from queries import run_query
import utils
from datetime import datetime

def get_reviews_count(since_when, end_when, cities):

  cities_condition = format_array_for_query(cities)

  query = f"""
    SELECT
      COUNT(DISTINCT r.reservationId) AS count,
      r.score
    FROM
      performance_data.mail_review r
    JOIN
      performance_data.dim_location l
    ON
      l.id = r.dim_location_id
    WHERE
      DATE(date) >= DATE(@since_when)
      AND DATE(date) < DATE(@end_when)
      AND l.city {cities_condition}
    GROUP BY
      r.score
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_performance_review_query(query,job_config)

  return pd.DataFrame(rows)

def get_performance_reviews(since_when, end_when, cities, display_reviews_above):

  cities_condition = format_array_for_query(cities)

  query = f"""
    SELECT
      review.date as Data,
      location.city as Miasto,
      review.score as Ocena,
      review.feedback as Feedback,
    FROM
      performance_data.mail_review review
    JOIN
      performance_data.dim_location location
    ON
      review.dim_location_id = location.id
    WHERE
      DATE(date) >= DATE(@since_when)
      AND DATE(date) < DATE(@end_when)
      AND location.city {cities_condition}
      AND review.feedback IS NOT NULL
      AND (
      ({display_reviews_above}  AND review.score > 8)
      OR
      (NOT {display_reviews_above} AND review.score <= 8)
      )
    ORDER BY
      review.date DESC
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_performance_review_query(query, job_config)
  df = pd.DataFrame(rows)
  return df

def get_cumulative_NPS(since_when, end_when, cities, groupBy):

  cities_condition = format_array_for_query(cities)

  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""


  query = f"""
  WITH DailyCategorizedReviews AS (
  SELECT
    DISTINCT reservationId,
    DATE(review.date) AS review_day,
    CASE
      WHEN review.score BETWEEN 0 AND 6 THEN 'Detractor'
      WHEN review.score BETWEEN 7 AND 8 THEN 'Passive'
      WHEN review.score BETWEEN 9 AND 10 THEN 'Promoter'
    END AS category
    {groupBy_condition}
  FROM
    performance_data.mail_review review
  JOIN
    performance_data.dim_location location
  ON
    review.dim_location_id = location.id
  WHERE
    location.city {cities_condition}
),
DailyNPS AS (
  SELECT
    review_day AS date,
    COUNT(*) AS count,
    SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) AS detractors
    {groupBy_condition}
  FROM
    DailyCategorizedReviews
  GROUP BY
    review_day {groupBy_condition}
)
SELECT
  date,
  SUM(count) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_cumsum,
  SUM(promoters) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS promoters_cumsum,
  SUM(detractors) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS detractors_cumsum,
  (
    (SUM(promoters) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 /
     SUM(count) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
    -
    (SUM(detractors) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 /
     SUM(count) OVER ({groupBy_partition} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
  ) AS nps_cumsum
  {groupBy_condition}
FROM
  DailyNPS
ORDER BY
  date ASC {groupBy_condition};
  """

  rows = run_performance_review_query(query)
  df = pd.DataFrame(rows)

  df['date'] = pd.to_datetime(df['date']).dt.date
  since_when = since_when.date()
  end_when = end_when.date()

  df = df[df['date'] >= since_when]
  df = df[df['date'] < end_when]

  return df
def get_NPS(since_when, end_when, cities, moving_average_days, groupBy):

  cities_condition = format_array_for_query(cities)
  groupBy_condition  = f", {groupBy}" if groupBy else ""
  groupBy_partition = f"PARTITION BY {groupBy}" if groupBy else ""

  query = f"""
 WITH DailyCategorizedReviews AS (
  SELECT
    DISTINCT reservationId,
    DATE(review.date) AS review_day,
    CASE
      WHEN review.score BETWEEN 0 AND 6 THEN 'Detractor'
      WHEN review.score BETWEEN 7 AND 8 THEN 'Passive'
      WHEN review.score BETWEEN 9 AND 10 THEN 'Promoter'
    END AS category
    {groupBy_condition}
  FROM
    performance_data.mail_review review
  JOIN
    performance_data.dim_location location
  ON
    review.dim_location_id = location.id
  WHERE
    location.city {cities_condition}
    AND DATE(review.date) >= DATE(@since_when)
    AND DATE(review.date) < DATE(@end_when)
),
DailyNPS AS (
  SELECT
    review_day AS date,
    COUNT(*) AS count,
    SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) AS detractors,
    (
      (SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
      -
      (SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
    ) AS nps
    {groupBy_condition}
  FROM
    DailyCategorizedReviews
  GROUP BY
    review_day {groupBy_condition}
)
SELECT
  date
  {groupBy_condition},
  count,
  promoters,
  detractors,
  nps,
  CASE
    WHEN ROW_NUMBER() OVER (
      {groupBy_partition}
      ORDER BY date
    ) > {moving_average_days}
    THEN AVG(nps) OVER (
      {groupBy_partition}
      ORDER BY date
      ROWS BETWEEN {moving_average_days} PRECEDING AND CURRENT ROW
    )
  END AS nps_ma
FROM
  DailyNPS
ORDER BY
  date ASC {groupBy_condition};

  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("since_when", "TIMESTAMP", since_when),
        bigquery.ScalarQueryParameter("end_when", "TIMESTAMP", end_when),
    ]
  )

  rows = run_performance_review_query(query, job_config)
  df = pd.DataFrame(rows)
  return df

def get_cumulative_count(since_when, end_when, cities):

  cities_condition = format_array_for_query(cities)

  query = f"""
    SELECT
  review.date AS Data,
  COUNT(review.score) OVER (
    PARTITION BY location.city
    ORDER BY review.date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_count
FROM
  performance_data.mail_review AS review
JOIN
  performance_data.dim_location AS location
ON
  review.dim_location_id = location.id
WHERE
  location.city {cities_condition}
ORDER BY
  review.date;

  """

  rows = run_performance_review_query(query)
  df = pd.DataFrame(rows)

  df['Data'] = pd.to_datetime(df['Data']).dt.date
  since_when = since_when.date()
  end_when = end_when.date()

  df = df[df['Data'] >= since_when]
  df = df[df['Data'] < end_when]

  return df

def get_monthly_nps(city, year):

  query = f"""
    WITH MonthlyCategorizedReviews AS (
  SELECT
    DISTINCT reservationId,
    EXTRACT(MONTH FROM review.date) AS review_month,
    CASE
      WHEN review.score BETWEEN 0 AND 6 THEN 'Detractor'
      WHEN review.score BETWEEN 9 AND 10 THEN 'Promoter'
    END AS category

  FROM
    performance_data.mail_review review
  JOIN
    performance_data.dim_location location
  ON
    review.dim_location_id = location.id
  WHERE
    location.city = @city
    AND EXTRACT(YEAR FROM review.date) = CAST(@year AS INT64)
)
  SELECT
    review_month AS month,
    COUNT(*) AS count,
    (
      (SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
      -
      (SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
    ) AS NPS

  FROM
    MonthlyCategorizedReviews
  GROUP BY
    review_month
  ORDER BY
    review_month
  """

  query2 = f"""

  SELECT
    COUNT(DISTINCT ecr.id) AS count,
    EXTRACT(MONTH FROM ecr.booked_date) AS month
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location location
  ON
    ecr.location_id = location.id
  WHERE
    location.city = @city
    AND EXTRACT(YEAR FROM ecr.booked_date) = CAST(@year AS INT64)
    AND CASE
      WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
      WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
      ELSE 'Zrealizowane'
    END IN ('Zrealizowane', 'Zrealizowane nieopłacone')
    AND EXTRACT(MONTH FROM ecr.booked_date) > 4
  GROUP BY
    month
  ORDER BY
    month
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("city", "STRING", city),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ]
  )

  rows = run_performance_review_query(query, job_config)
  rows2 = run_query(query2, job_config)

  df = pd.DataFrame(rows)
  df_count = pd.DataFrame(rows2)

  df['Procent ocenionych wizyt'] = (df['count'] / df_count['count']) * 100
  df['Miesiac'] = df['month'].map(utils.get_month_from_month_number)
  df['Liczba ocen'] = df['count']


  return df[['NPS', 'Miesiac', 'Liczba ocen', 'Procent ocenionych wizyt']]


def get_nps_metric(metric_change_days, metric_display_percent, cities):

  cities_condition = format_array_for_query(cities)

  query = f"""
  WITH DailyCategorizedReviews AS (
  SELECT
    DISTINCT reservationId,
    DATE(review.date) AS review_day,
    CASE
      WHEN review.score BETWEEN 0 AND 6 THEN 'Detractor'
      WHEN review.score BETWEEN 7 AND 8 THEN 'Passive'
      WHEN review.score BETWEEN 9 AND 10 THEN 'Promoter'
    END AS category
  FROM performance_data.mail_review AS review
  JOIN
    performance_data.dim_location location
  ON
    review.dim_location_id = location.id
  WHERE
    location.city {cities_condition}
),
DailyNPS AS (
  SELECT
    review_day AS date,
    COUNT(*) AS count,
    SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) AS detractors
  FROM DailyCategorizedReviews
  GROUP BY review_day
)
SELECT
  SUM(count) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_cumsum,
  (
    (SUM(promoters) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 /
     SUM(count) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
    -
    (SUM(detractors) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 /
     SUM(count) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
  ) AS nps_cumsum
FROM DailyNPS
ORDER BY date desc
LIMIT {metric_change_days + 1};
  """

  rows = run_performance_review_query(query)
  df = pd.DataFrame(rows)

  nps_value = df["nps_cumsum"].iloc[0]
  delta_nps = round(nps_value - df["nps_cumsum"].iloc[metric_change_days], 4)

  count_value = df["count_cumsum"].iloc[0]
  delta_count = int(count_value - df["count_cumsum"].iloc[metric_change_days])

  if metric_display_percent:
    delta_nps = round(((nps_value - df["nps_cumsum"].iloc[metric_change_days]) / df["nps_cumsum"].iloc[metric_change_days]) * 100, 4)
    delta_count = round(((count_value - df["count_cumsum"].iloc[metric_change_days]) / df["count_cumsum"].iloc[metric_change_days]) * 100, 4)

  return round(nps_value, 4), delta_nps, count_value, delta_count

def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"
