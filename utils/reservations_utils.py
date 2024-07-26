def group_data_and_calculate_moving_average(df, x_axis_type, moving_average_days, grouping_field):
  reservations_rolling_averages = []
  total_cost_rolling_averages = []
  total_people_rolling_averages = []

  if grouping_field:
    df_grouped = df.groupby([df[x_axis_type].dt.to_period('D'), df[grouping_field]]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum')
    ).reset_index()

    for value in df_grouped[grouping_field].unique():
      reservations_rolling_averages.append(df_grouped[df_grouped[grouping_field] == value]['reservations'].rolling(window=moving_average_days).mean())
      total_cost_rolling_averages.append(df_grouped[df_grouped[grouping_field] == value]['total_cost'].rolling(window=moving_average_days).mean())
      total_people_rolling_averages.append(df_grouped[df_grouped[grouping_field] == value]['total_people'].rolling(window=moving_average_days).mean())

  else:
    df_grouped = df.groupby(df[x_axis_type].dt.to_period('D')).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum')
    ).reset_index()

    reservations_rolling_averages.append(df_grouped['reservations'].rolling(window=moving_average_days).mean())
    total_cost_rolling_averages.append(df_grouped['total_cost'].rolling(window=moving_average_days).mean())
    total_people_rolling_averages.append(df_grouped['total_people'].rolling(window=moving_average_days).mean())

  return df_grouped, reservations_rolling_averages, total_cost_rolling_averages, total_people_rolling_averages
