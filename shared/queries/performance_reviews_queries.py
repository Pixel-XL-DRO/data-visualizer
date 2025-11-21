import pandas as pd
from google.cloud import bigquery
from queries import run_performance_review_query
from queries import run_query
import utils
from datetime import timedelta
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
      AND l.street {cities_condition}
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

def get_performance_reviews(since_when, end_when, cities):

  cities_condition = format_array_for_query(cities)

  query = f"""
    SELECT
      review.date as Data,
      location.street as Miasto,
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
      AND location.street {cities_condition}
      AND review.feedback IS NOT NULL
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

  df['Miasto'] = df['Miasto'].replace(utils.street_to_location)   

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
    location.street {cities_condition}
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

  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 

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
    location.street {cities_condition}
    AND DATE(review.date) >= DATE(@since_when)
    AND DATE(review.date) < DATE(@end_when)
),
DailyNPS AS (
  SELECT
    review_day AS date,
    COUNT(*) AS count,
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
  if groupBy == 'street':
    df['street'] = df['street'].replace(utils.street_to_location) 

  return df

def get_cumulative_count(since_when, end_when, cities):

  cities_condition = format_array_for_query(cities)

  query = f"""
    SELECT
  review.date AS Data,
  COUNT(review.score) OVER (
    PARTITION BY location.street
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
  location.street {cities_condition}
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

def get_monthly_nps(street, year):

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
    location.street = @street
    AND EXTRACT(YEAR FROM review.date) = CAST(@year AS INT64)
)
  SELECT
    review_month AS month,
    COUNT(*) AS count,
    ROUND((
      (SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
      -
      (SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
    ),2) AS NPS
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
    EXTRACT(MONTH FROM ecr.start_date) AS month
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location location
  ON
    ecr.location_id = location.id
  WHERE
    location.street = @street
    AND EXTRACT(YEAR FROM ecr.booked_date) = CAST(@year AS INT64)
    AND CASE
      WHEN ecr.is_cancelled = TRUE THEN 'Anulowane'
      WHEN ecr.is_payed = FALSE THEN 'Zrealizowane nieopłacone'
      ELSE 'Zrealizowane'
    END IN ('Zrealizowane', 'Zrealizowane nieopłacone')
    AND EXTRACT(MONTH FROM ecr.start_date) > 4
  GROUP BY
    month
  ORDER BY
    month
  """

  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("street", "STRING", street),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ]
  )

  rows = run_performance_review_query(query, job_config)
  rows2 = run_query(query2, job_config)

  df = pd.DataFrame(rows)
  df_count = pd.DataFrame(rows2)
  
  df['Procent ocenionych wizyt'] = round((df['count'] / df_count['count']) * 100, 2)
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
    location.street {cities_condition}
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

  query_res = f"""
 SELECT
  date,
  SUM(daily_count) OVER (ORDER BY date) AS count
FROM (
  SELECT
    DATE(ecr.start_date) AS date,
    COUNT(DISTINCT ecr.id) AS daily_count
  FROM
    reservation_data.event_create_reservation AS ecr
  JOIN
    reservation_data.dim_location AS location
    ON ecr.location_id = location.id
  WHERE
    location.street {cities_condition}
    AND ecr.start_date >= TIMESTAMP('2025-05-11')
    AND ecr.start_date <= CURRENT_TIMESTAMP()
    AND ecr.is_cancelled = FALSE
  GROUP BY
    date
) AS daily_counts
ORDER BY
  date desc;

  """

  rows = run_performance_review_query(query)
  df = pd.DataFrame(rows)
  rows_res = run_query(query_res)
  df_res = pd.DataFrame(rows_res)

  nps_value = df["nps_cumsum"].iloc[0]
  delta_nps = round(nps_value - df["nps_cumsum"].iloc[metric_change_days], 2)

  count_value = df["count_cumsum"].iloc[0]
  delta_count = int(count_value - df["count_cumsum"].iloc[metric_change_days])

  review_percent = round((df["count_cumsum"].iloc[0] / df_res["count"].iloc[0]) * 100, 2)
  review_percent_delta = round(review_percent - ((df["count_cumsum"].iloc[metric_change_days] / df_res["count"].iloc[metric_change_days]) * 100), 2)

  if metric_display_percent:
    delta_nps = round(((nps_value - df["nps_cumsum"].iloc[metric_change_days]) / df["nps_cumsum"].iloc[metric_change_days]) * 100, 2)
    delta_count = round(((count_value - df["count_cumsum"].iloc[metric_change_days]) / df["count_cumsum"].iloc[metric_change_days]) * 100, 2)
    review_percent_delta = round(((review_percent - ((df["count_cumsum"].iloc[metric_change_days] / df_res["count"].iloc[metric_change_days]) * 100)) / review_percent) * 100, 2)

  return round(nps_value, 2), delta_nps, count_value, delta_count, review_percent, review_percent_delta

def get_nps_metric_by_city(metric_change_days, metric_display_percent, cities, start_date):

  cities_condition = format_array_for_query(cities)

  query = f"""
WITH DailyCategorizedReviews AS (
  SELECT
    DISTINCT reservationId,
    DATE(review.date) AS review_day,
    location.street,
    CASE
      WHEN review.score BETWEEN 0 AND 6 THEN 'Detractor'
      WHEN review.score BETWEEN 7 AND 8 THEN 'Passive'
      WHEN review.score BETWEEN 9 AND 10 THEN 'Promoter'
    END AS category
  FROM
    performance_data.mail_review AS review
  JOIN
    performance_data.dim_location location
    ON review.dim_location_id = location.id
  WHERE
    location.street {cities_condition}
),
DailyNPS AS (
  SELECT
    DATE(review_day) AS date,
    street,
    COUNT(*) AS count,
    SUM(CASE WHEN category = 'Promoter' THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN category = 'Detractor' THEN 1 ELSE 0 END) AS detractors
  FROM
    DailyCategorizedReviews
  GROUP BY
    review_day, street
),
CumulativeNPS AS (
  SELECT
    DATE(date) AS date,
    street,
    SUM(count) OVER (PARTITION BY street ORDER BY DATE(date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_cumsum,
    (
      (
        SUM(promoters) OVER (PARTITION BY street ORDER BY DATE(date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        - SUM(detractors) OVER (PARTITION BY street ORDER BY DATE(date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) * 100.0
      / SUM(count) OVER (PARTITION BY street ORDER BY DATE(date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS nps_cumsum
  FROM
    DailyNPS
),
DateRange AS (
  SELECT
    street,
    MIN(date) AS min_date,
    CURRENT_DATE() AS max_date
  FROM
    CumulativeNPS
  GROUP BY
    street
),
DateCityGrid AS (
  SELECT
    street,
    day AS date
  FROM
    DateRange,
    UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS day
),

-- Left join to cumulative data (some dates will be missing)
Joined AS (
  SELECT
    g.street,
    g.date,
    c.count_cumsum,
    c.nps_cumsum
  FROM
    DateCityGrid g
  LEFT JOIN
    CumulativeNPS c
  ON
    g.street = c.street
    AND g.date = c.date
),

FilledForward AS (
  SELECT
    street,
    date,
    IFNULL(
      count_cumsum,
      LAST_VALUE(count_cumsum IGNORE NULLS)
        OVER (PARTITION BY street ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS count_cumsum,
    IFNULL(
      nps_cumsum,
      LAST_VALUE(nps_cumsum IGNORE NULLS)
        OVER (PARTITION BY street ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS nps_cumsum
  FROM
    Joined
)
SELECT
  street,
  date,
  count_cumsum,
  nps_cumsum
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY street ORDER BY date DESC) AS rn
  FROM
    FilledForward
)
WHERE
  rn <= {metric_change_days + 1}
ORDER BY
  street,
  date DESC;

  """

  query_res = f"""
  WITH initial AS (
  SELECT
    COUNT(DISTINCT ecr.id) AS count,
    DATE(ecr.start_date) AS date,
    location.street AS street
  FROM
    reservation_data.event_create_reservation ecr
  JOIN
    reservation_data.dim_location location
  ON
    ecr.location_id = location.id
  WHERE
    location.street {format_array_for_query(cities)}
    AND ecr.start_date >= TIMESTAMP("2025-05-11") -- START OF NPS
    AND ecr.start_date <= CURRENT_TIMESTAMP()
    AND ecr.is_cancelled = false
  GROUP BY
    date, location.street
),

cumulative_sum AS (
  SELECT
    date,
    street,
    SUM(count) OVER (
      PARTITION BY street
      ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS count_cumsum_res
  FROM
    initial
),
date_city_grid AS (
  SELECT
    street,
    day AS date
  FROM
    UNNEST(GENERATE_DATE_ARRAY(
      DATE_SUB(CURRENT_DATE(), INTERVAL {metric_change_days} DAY),
      CURRENT_DATE()
    )) AS day
  CROSS JOIN (
    SELECT DISTINCT street FROM cumulative_sum
  )
),
filled AS (
  SELECT
    g.date,
    g.street,
    c.count_cumsum_res
  FROM
    date_city_grid g
  LEFT JOIN
    cumulative_sum c
  ON
    g.street = c.street AND g.date = c.date
),
filled_forward AS (
  SELECT
    date,
    street,
    IFNULL(
      count_cumsum_res,
      LAST_VALUE(count_cumsum_res IGNORE NULLS)
        OVER (
          PARTITION BY street
          ORDER BY date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS count_cumsum_res
  FROM
    filled
)
SELECT
  date,
  street,
  count_cumsum_res
FROM
  filled_forward
ORDER BY
  street,
  date DESC;

  """
  job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
    ]
  )

  rows = run_performance_review_query(query)
  rows_res = run_query(query_res, job_config)


  df = pd.DataFrame(rows)
  df_res = pd.DataFrame(rows_res)

  max_date = df['date'].max()
  min_date = max_date - timedelta(days=metric_change_days)
  dates_to_keep = [min_date, max_date]

  df = df[df['date'].isin(dates_to_keep)]
  df_res = df_res[df_res['date'].isin(dates_to_keep)]

  all_cities = pd.concat([df['street'], df_res['street']]).unique()
  full_index = pd.MultiIndex.from_product([all_cities, dates_to_keep], names=['street', 'date'])
  full_df = pd.DataFrame(index=full_index).reset_index()

  df = full_df.merge(df, on=['street', 'date'], how='left').fillna(0)
  df_res = full_df.merge(df_res, on=['street', 'date'], how='left').fillna(0)

  merged_df = df.merge(
    df_res[['date', 'street', 'count_cumsum_res']],
    on=['date', 'street'],
    how='inner'
  )

  merged_df = merged_df.sort_values(["street", "date"])

  merged_df["review_percent"] = round(merged_df["count_cumsum"] / merged_df["count_cumsum_res"] * 100, 2)

  if metric_display_percent:
    merged_df["nps_change"] = merged_df.groupby("street")["nps_cumsum"].transform(
        lambda x: ((x.diff() / x.shift(1)) * 100).round(2)
    )
    merged_df["count_change"] = merged_df.groupby("street")["count_cumsum"].transform(
        lambda x: ((x.diff() / x.shift(1)) * 100).round(2)
    )
    merged_df["review_percent_change"] = merged_df.groupby("street")["review_percent"].transform(
        lambda x: ((x.diff() / x.shift(1)) * 100).round(2)
    )
  else:
    merged_df["nps_change"] = merged_df.groupby("street")["nps_cumsum"].transform(
        lambda x: x.diff().round(2)
    )
    merged_df["count_change"] = merged_df.groupby("street")["count_cumsum"].transform(
        lambda x: x.diff()
    )
    merged_df["review_percent_change"] = merged_df.groupby("street")["review_percent"].transform(
        lambda x: x.diff().round(2)
    )

  merged_df = merged_df[merged_df["date"] == max_date].reset_index(drop=True)

  merged_df['street'] = merged_df['street'].replace(utils.street_to_location) 

  return merged_df

def format_array_for_query(array):
  return f"IN {tuple(array)}" if len(array) > 1 else f"= '{array[0]}'"
