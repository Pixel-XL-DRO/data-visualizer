def group_data(df, period):
  df_grouped = df.groupby(df['create_time'].dt.to_period(period)).agg(
    ratings_count=('rating', 'count'),
  ).reset_index()

  return df_grouped
