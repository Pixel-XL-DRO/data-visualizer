import pandas as pd
from datetime import datetime, timedelta

import utils

def group_data_and_calculate_moving_average(df, period, grouping_field):
  if grouping_field:
    df_grouped = df.groupby([df[period], df[grouping_field]]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum'),
      boardhours_taken=('boardhours_taken', 'sum')
    ).reset_index()

  else:
    df_grouped = df.groupby(df[period]).agg(
      reservations=('id', 'count'),
      total_cost=('whole_cost_with_voucher', 'sum'),
      total_people=('no_of_people', 'sum'),
      boardhours_taken=('boardhours_taken', 'sum')
    ).reset_index()

  return df_grouped

def mapGroupDatesByToPeriod(x_axis_type, group_dates_by):
  if group_dates_by == 'Godzina':
    return 'start_date_hour' if x_axis_type == 'start_date' else 'booked_date_hour'
  elif group_dates_by == 'Dzień tygodnia':
    return 'start_date_day_of_week' if x_axis_type == 'start_date' else 'booked_date_day_of_week'
  elif group_dates_by == 'Dzień miesiaca':
    return 'start_date_day_of_month' if x_axis_type == 'start_date' else 'booked_date_day_of_month'
  elif group_dates_by == 'Tydzien roku':
    return 'start_date_week_of_year' if x_axis_type == 'start_date' else 'booked_date_week_of_year'
  elif group_dates_by == 'Miesiac':
    return 'start_date_month' if x_axis_type == 'start_date' else 'booked_date_month'
  elif group_dates_by == 'Rok':
    return 'start_date_year' if x_axis_type == 'start_date' else 'booked_date_year'

def count_occurences_and_group_data(type, min_date, max_date, df, period, groupBy):
  appearance_map = {}

  date = min_date

  def extract_value_based_on_type(date):
    if type == "hours":
      return date.hour
    if type == "days":
      return date.day
    if type == "days_of_week":
      return (date.weekday() + 1) if date.weekday() != 6 else 0
    if type == "weeks":
      return date.isocalendar()[1]
    if type == "months":
      return date.month
    if type == "years":
      return date.year

  def extract_time_delta_based_on_type(date):
    if type == "hours":
      return timedelta(hours=1)
    if type == "days":
      return timedelta(days=1)
    if type == "days_of_week":
      return timedelta(days=1)
    if type == "weeks":
      return timedelta(weeks=1)
    if type == "months":
      return timedelta(days=utils.get_month_days_count(date.year, date.month))
    if type == "years":
      return timedelta(days=utils.get_year_days_count(date.year))

  while True:
    if date >= max_date:
      break

    if extract_value_based_on_type(date) not in appearance_map:
      appearance_map[extract_value_based_on_type(date)] = 1
    else:
      appearance_map[extract_value_based_on_type(date)] += 1

    date += extract_time_delta_based_on_type(date)

  def adjust_reservations(row):
    # this should only take place in case of year (which is our exception)
    if row[period] in appearance_map:
      return row[y_type] / appearance_map[row[period]]
    else:
      return row[y_type] / 1

  df_grouped = group_data_and_calculate_moving_average(df, period, groupBy)

  y_type = 'reservations'
  df_grouped[y_type] = df_grouped.apply(adjust_reservations, axis=1)
  y_type = 'total_people'
  df_grouped[y_type] = df_grouped.apply(adjust_reservations, axis=1)
  y_type = 'total_cost'
  df_grouped[y_type] = df_grouped.apply(adjust_reservations, axis=1)
  y_type = 'boardhours_taken'
  df_grouped[y_type] = df_grouped.apply(adjust_reservations, axis=1)
  return df_grouped

# hack used here - using ⠀ empty U+2800 character to ensure 10, 11, 12 are in correct order (streamlit orders X axis alphabetically by default - probably should research whether this is changable)
months_labels = ['1. Styczeń', '2. Luty', '3. Marzec', '4. Kwiecień', '5. Maj', '6. Czerwiec', '7. Lipiec', '8. Sierpień', '9. Wrzesień', '⠀10. Październik', '⠀11. Listopad', '⠀12. Grudzień']

def parse_month_period(row, period):
  month_index = int(row[period])
  return months_labels[month_index - 1]

def periodize(df, x_axis_type, period, groupBy):
  min_date = df[x_axis_type].min()
  max_date = df[x_axis_type].max()
  today = datetime.today()

  if (period == 'start_date_month' or period == 'booked_date_month'):
    exported_current_period = months_labels[today.month - 1]

    if max_date.month == today.month and max_date.year == today.year:
      year_field = 'start_date_year' if x_axis_type == 'start_date' else 'booked_date_year'
      df = df.query(f"{period} != {today.month} | {year_field} != {today.year}")

    max_date = df[x_axis_type].max()

    df_grouped = count_occurences_and_group_data("months", min_date, max_date, df, period, groupBy)

    df_grouped['parsed_period'] = df_grouped.apply(parse_month_period, axis=1, period=period)

  elif (period == 'start_date_week_of_year' or period == 'booked_date_week_of_year'):
    exported_current_period = today.isocalendar()[1]

    year_field = 'start_date_year' if x_axis_type == 'start_date' else 'booked_date_year'
    month_field = 'start_date_month' if x_axis_type == 'start_date' else 'booked_date_month'

    if max_date.isocalendar()[1] == today.isocalendar()[1] and max_date.year == today.year:
      df = df.query(f"{period} != {today.isocalendar()[1]} | {year_field} != {today.year}")

    max_date = df[x_axis_type].max()

    if min_date.isocalendar()[0] != min_date.year:
      df = df.query(f"{period} != {min_date.isocalendar()[1]} | {year_field} != {min_date.year} | {month_field} != {min_date.month}")
    if max_date.isocalendar()[0] != max_date.year:
      df = df.query(f"{period} != {max_date.isocalendar()[1]} | {year_field} != {max_date.year} | {month_field} != {max_date.month}")

    max_date = df[x_axis_type].max()
    min_date = df[x_axis_type].min()

    df_grouped = count_occurences_and_group_data("weeks", min_date, max_date, df, period, groupBy)

  elif (period == 'start_date_day_of_month' or period == 'booked_date_day_of_month'):
    exported_current_period = today.day
    df_grouped = count_occurences_and_group_data("days", min_date, max_date, df, period, groupBy)

  elif (period == 'start_date_day_of_week' or period == 'booked_date_day_of_week'):
    days = ['7. Niedziela', '1. Poniedziałek', '2. Wtorek', '3. Środa', '4. Czwartek', '5. Piatek', '6. Sobota']
    exported_current_period = days[today.weekday() + 1 if today.weekday() != 6 else 0]

    df_grouped = count_occurences_and_group_data("days_of_week", min_date, max_date, df, period, groupBy)

    def parse_day_of_week_period(row, period):
      day_of_week = int(row[period])
      return days[day_of_week]

    df_grouped['parsed_period'] = df_grouped.apply(parse_day_of_week_period, axis=1, period=period)

  elif (period == 'start_date_hour' or period == 'booked_date_hour'):
    exported_current_period = today.hour
    df_grouped = count_occurences_and_group_data("hours", min_date, max_date, df, period, groupBy)

  elif (period == 'start_date_year' or period == 'booked_date_year'):
    exported_current_period = str(today.year)

    df_grouped = count_occurences_and_group_data("years", min_date, max_date, df, period, groupBy)

    def parse_period(row, period):
      # using str here cause x axis was acting weird. this should work as long as years are represented in 4 digits. so - sorry if you are reading this in year 10000 and after
      return str(int(row[period]))

    df_grouped['parsed_period'] = df_grouped.apply(parse_period, axis=1, period=period)

  if 'parsed_period' not in df_grouped.columns:
    df_grouped['parsed_period'] = df_grouped[period]

  return df_grouped, exported_current_period
