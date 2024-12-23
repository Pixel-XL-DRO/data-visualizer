import sys
sys.path.append("shared")
import pandas as pd

import reservations_cumulative_utils

from datetime import datetime, timedelta
import utils
import streamlit as st
import pandas as pd
import numpy as np

def determine_status(row):
  if row['is_cancelled']:
    return 'Anulowane'
  elif not row['is_payed']:
    return 'Zrealizowane nieopłacone'
  return 'Zrealizowane'

def ensure_status():
  if st.session_state.ms1[0] == "Wszystkie":
    st.session_state.ms1 = st.session_state.ms1[1:]
  elif st.session_state.ms1[-1] == "Wszystkie":
    st.session_state.ms1 = ["Wszystkie"]

def filter_data(df):
  start_date = None
  end_date = None

  with st.sidebar:
    time_range = st.selectbox('Pokazuj z ostatnich', ['7 dni', '1 miesiaca', '6 miesiecy', '1 roku', '2 lat', '3 lat', 'Od poczatku', "Przedział"], index=6)
    if time_range == "Przedział":
      start_date = st.date_input('Data rozpoczecia')
      end_date = st.date_input('Data konca')
    # with st.expander("Filtry"):
    #   with st.container(border=True):
    #     city_checkboxes = st.multiselect("Miasta", df['city'].unique(), default=df['city'].unique())
    #     seperate_cities = st.checkbox('Rozdziel miasta', key="t3", on_change=lambda:utils.make_sure_only_one_toggle_is_on(["t3", "t4", "t5", "t6"], "t3"))

    x_axis_type = 'start_date'

    if end_date is None:
      # 23:59:59 the day before - to account for data collection that takes place at 01:00:00 - we want to cut out single reservations at 00:00:00
      end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    else:
      end_date = datetime.combine(end_date, datetime.min.time())

    if start_date is None:
      if time_range == '7 dni':
        start_date = end_date - timedelta(days=7)
      elif time_range == '1 miesiaca':
        start_date = end_date - timedelta(days=30)
      elif time_range == '6 miesiecy':
        start_date = end_date - timedelta(days=182)
      elif time_range == '1 roku':
        start_date = end_date - timedelta(days=365)
      elif time_range == '2 lat':
        start_date = end_date - timedelta(days=730)
      elif time_range == '3 lat':
        start_date = end_date - timedelta(days=1095)
      elif time_range == 'Od poczatku':
        min_date = df[x_axis_type].min()
        start_date = datetime.now().replace(hour=min_date.hour, minute=min_date.minute, second=min_date.second, microsecond=min_date.microsecond, day=min_date.day, month=min_date.month, year=min_date.year)
    else:
      start_date = datetime.combine(start_date, datetime.min.time())

    df['start_date'] = pd.to_datetime(df['start_date']).dt.tz_localize(None)

    df['status'] = df.apply(determine_status, axis=1)
    df = df[df['status'] != "Anulowane"]
    # df = df[df['city'].isin(city_checkboxes)]

    # group_by = 'city' if seperate_cities else None

    # df = reservations_cumulative_utils.group_data_cumulative(df, x_axis_type, group_by)

    # df[x_axis_type] = df[x_axis_type].dt.to_timestamp()
    # df[x_axis_type] = pd.to_datetime(df[x_axis_type])

    df = df[df[x_axis_type] >= start_date]
    # df = df[df[x_axis_type] <= end_date]

    return (df, x_axis_type)
    # return (df, x_axis_type, seperate_cities, group_by)
