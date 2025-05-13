def group_data_and_calculate_nps(df, group_by, moving_average_days):

  nps_rolling_averages = []

  df['promoters'] = (df['score'] >= 9).astype(int)
  df['detractors'] = (df['score'] <= 6).astype(int)

  if group_by:

    df_grouped = df.groupby([df['date'].dt.to_period('D'), df[group_by]]).agg(
        score_count=('score', 'count'),
        promoters_count=('promoters', 'sum'),
        detractors_count=('detractors', 'sum')
    ).reset_index()

  else:

    df_grouped = df.groupby(df['date'].dt.to_period('D')).agg(
      score_count=('score', 'count'),
      promoters_count=('promoters', 'sum'),
      detractors_count=('detractors', 'sum')
    ).reset_index()

  df_grouped['nps'] = ((df_grouped['promoters_count'] / df_grouped['score_count']) - (df_grouped['detractors_count'] / df_grouped['score_count'])) * 100

  if group_by:

    for value in df_grouped[group_by].unique():
      group_data = df_grouped[df_grouped[group_by] == value]
      nps_rolling_averages.append(group_data['nps'].rolling(window=moving_average_days).mean())
      df_grouped.loc[df_grouped[group_by] == value, 'nps_cum'] = group_data['nps'].expanding().mean()

    df_grouped['score_cum'] = df_grouped.groupby(group_by)['score_count'].cumsum()

  else:

    nps_rolling_averages.append(df_grouped['nps'].rolling(window=moving_average_days).mean())
    df_grouped['score_cum'] = df_grouped['score_count'].cumsum()
    df_grouped['nps_cum'] = df_grouped['nps'].expanding().mean()

  return df_grouped, nps_rolling_averages
