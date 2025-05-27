import utils
def group_data_and_calculate_nps(df, group_by, moving_average_days):

  nps_rolling_averages = []

  #each promoter and detractor is resulted as bool so we save them as 0 or 1 for easier calculations
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

    df_grouped['prom_cum'] = df_grouped.groupby(group_by)['promoters_count'].cumsum()
    df_grouped['det_cum'] = df_grouped.groupby(group_by)['detractors_count'].cumsum()
    df_grouped['score_cum'] = df_grouped.groupby(group_by)['score_count'].cumsum()

    df_grouped['nps_cum'] = ((df_grouped['prom_cum'] / df_grouped['score_cum']) - (df_grouped['det_cum'] / df_grouped['score_cum'])) * 100

  else:

    nps_rolling_averages.append(df_grouped['nps'].rolling(window=moving_average_days).mean())
    df_grouped['prom_cum'] = df_grouped['promoters_count'].cumsum()
    df_grouped['det_cum'] = df_grouped['detractors_count'].cumsum()
    df_grouped['score_cum'] = df_grouped['score_count'].cumsum()

    df_grouped['nps_cum'] = ((df_grouped['prom_cum'] / df_grouped['score_cum']) - (df_grouped['det_cum'] / df_grouped['score_cum'])) * 100

  return df_grouped, nps_rolling_averages

def group_data_and_calculate_nps_for_each_month(df, city, year):

  #each promoter and detractor is resulted as bool so we save them as 0 or 1 for easier calculations
  df['promoters'] = (df['score'] >= 9).astype(int)
  df['detractors'] = (df['score'] <= 6).astype(int)

  df_grouped = df.groupby([df['date'].dt.to_period('M'), 'city']).agg(
      score_count=('score', 'count'),
      promoters_count=('promoters', 'sum'),
      detractors_count=('detractors', 'sum')
  ).reset_index()

  df_grouped['nps'] = ((df_grouped['promoters_count'] / df_grouped['score_count']) - (df_grouped['detractors_count'] / df_grouped['score_count'])) * 100

  df_grouped['month'] = df_grouped['date'].dt.month.map(utils.get_month_from_month_number)

  df_grouped = df_grouped[(df_grouped['date'].dt.year == year) & (df_grouped['city'] == city)]

  return df_grouped