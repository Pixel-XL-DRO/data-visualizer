import utils

def group_data(df, x_axis_type, grouping_field):
  if grouping_field:
    df_grouped = df.groupby([df[x_axis_type].dt.to_period('M'), df[grouping_field]]).agg(
      reservations=('id', 'count'),
      returning_clients=('client_id', lambda x: x.duplicated(keep=False).sum()),
      past_reservation_clients=('has_past_reservation', 'sum')
    ).reset_index()
  else:
    df_grouped = df.groupby(df[x_axis_type].dt.to_period('M')).agg(
      reservations=('id', 'count'),
      returning_clients=('client_id', lambda x: x.duplicated(keep=False).sum()),
      past_reservation_clients=('has_past_reservation', 'sum')
    ).reset_index()

  df_grouped[x_axis_type] = df_grouped[x_axis_type].dt.month.map(utils.get_month_from_month_number)
  df_grouped['retention_percent'] = df_grouped['returning_clients'] / df_grouped['reservations'] * 100
  df_grouped['past_retention_percent'] = df_grouped['past_reservation_clients'] / df_grouped['reservations'] * 100

  return df_grouped
