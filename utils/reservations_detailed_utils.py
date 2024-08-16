import pandas as pd
from datetime import datetime

def group_data_cumulative(df, x_axis_type, grouping_field):
  if grouping_field:
    df_grouped = df.groupby([df[x_axis_type].dt.to_period('D'), df[grouping_field]]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum')
    ).reset_index()

    df_grouped['reservations'] = df_grouped.groupby(grouping_field)['reservations'].cumsum()
    df_grouped['total_cost'] = df_grouped.groupby(grouping_field)['total_cost'].cumsum()
    df_grouped['total_people'] = df_grouped.groupby(grouping_field)['total_people'].cumsum()

  else:
    df_grouped = df.groupby(df[x_axis_type].dt.to_period('D')).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum')
    ).reset_index()

    df_grouped['reservations'] = df_grouped['reservations'].cumsum()
    df_grouped['total_cost'] = df_grouped['total_cost'].cumsum()
    df_grouped['total_people'] = df_grouped['total_people'].cumsum()

  return df_grouped

def group_data_and_calculate_moving_average(df, x_axis_type, period, grouping_field):
  if grouping_field:
    df_grouped = df.groupby([df[period], df[grouping_field]]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum')
    ).reset_index()

  else:
    df_grouped = df.groupby(df[period]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum')
    ).reset_index()

  return df_grouped

  first_date = df[x_axis_type].min()

  a = df.sort_values(by=x_axis_type)

  # return a

  today = pd.to_datetime(datetime.today().date())

  first_monday = first_date - pd.to_timedelta(first_date.weekday(), unit='d')
  current_monday = today - pd.to_timedelta(today.weekday(), unit='d')

  weeks_passed = ((current_monday - first_monday).days // 7) + 1

  total_reservations_per_day = df.groupby(period).size()
  average_reservations_per_day = total_reservations_per_day / weeks_passed

  return average_reservations_per_day

def divideKPIBasedOnPeriod(df, kpi_key, period, date_of_first_reservation, days_passed, weeks_passed, months_passed, years_passed):
  today = pd.to_datetime(datetime.today().date())
  if period == 'Godzina':
    days_passed = (today - date_of_first_reservation).days
    df[kpi_key] = df[kpi_key] / days_passed
  elif period == 'Dzień tygodnia':
    days_passed = (today - date_of_first_reservation).days
    df[kpi_key] = df[kpi_key] / (days_passed / 7)
  elif period == 'Dzień miesiaca':
    months_passed = (today.year - date_of_first_reservation.year) * 12 + today.month - date_of_first_reservation.month
    df[kpi_key] = df[kpi_key] / months_passed
  elif period == 'Tydzien miesiaca':
    months_passed = (today.year - date_of_first_reservation.year) * 12 + today.month - date_of_first_reservation.month
    df[kpi_key] = df[kpi_key] / (months_passed)
  elif period == 'Rok':
    return

def mapGroupDatesByToPeriod(x_axis_type, group_dates_by):
  if group_dates_by == 'Godzina':
    return 'start_date_hour' if x_axis_type == 'start_date' else 'booked_date_hour'
  elif group_dates_by == 'Dzień tygodnia':
    return 'start_date_day_of_week' if x_axis_type == 'start_date' else 'booked_date_day_of_week'
  elif group_dates_by == 'Dzień miesiaca':
    return 'start_date_day_of_month' if x_axis_type == 'start_date' else 'booked_date_day_of_month'
  elif group_dates_by == 'Tydzien miesiaca':
    return 'start_date_week_of_month' if x_axis_type == 'start_date' else 'booked_date_week_of_month'
  elif group_dates_by == 'Tydzien roku':
    return 'start_date_week_of_year' if x_axis_type == 'start_date' else 'booked_date_week_of_year'
  elif group_dates_by == 'Miesiac':
    return 'start_date_month' if x_axis_type == 'start_date' else 'booked_date_month'
  elif group_dates_by == 'Rok':
    return 'start_date_year' if x_axis_type == 'start_date' else 'booked_date_year'
