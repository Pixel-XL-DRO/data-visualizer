def group_data_cumulative(df, x_axis_type, grouping_field):
  if grouping_field:
    df_grouped = df.groupby([df[x_axis_type].dt.to_period('D'), df[grouping_field]]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum'),
      boardhours_taken=('boardhours_taken', 'sum')
    ).reset_index()

    df_grouped['reservations'] = df_grouped.groupby(grouping_field)['reservations'].cumsum()
    df_grouped['total_cost'] = df_grouped.groupby(grouping_field)['total_cost'].cumsum()
    df_grouped['total_people'] = df_grouped.groupby(grouping_field)['total_people'].cumsum()
    df_grouped['boardhours_taken'] = df_grouped.groupby(grouping_field)['boardhours_taken'].cumsum()

  else:
    df_grouped = df.groupby(df[x_axis_type].dt.to_period('D')).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum'),
      boardhours_taken=('boardhours_taken', 'sum')
    ).reset_index()

    df_grouped['reservations'] = df_grouped['reservations'].cumsum()
    df_grouped['total_cost'] = df_grouped['total_cost'].cumsum()
    df_grouped['total_people'] = df_grouped['total_people'].cumsum()
    df_grouped['boardhours_taken'] = df_grouped['boardhours_taken'].cumsum()

  return df_grouped
