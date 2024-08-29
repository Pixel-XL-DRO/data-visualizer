import sys
sys.path.append("shared")

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

  years_possible = list(range(2022, datetime.now().year + 1))

  with st.sidebar:
    x_axis_type = st.selectbox('Wybierz rodzaj daty', ['Data stworzenia', 'Data rozpoczecia'])
    group_dates_by = st.selectbox('Wybierz grupowanie po dacie', ['Godzina', 'Dzień tygodnia', 'Tydzien roku', 'Dzień miesiaca', 'Miesiac', 'Rok'], index=1)
    time_range = st.selectbox('Wybierz okres czasu', [*years_possible, 'Od poczatku', "Przedział"], index=3)
    if time_range == "Przedział":
      start_year = st.slider('Rok rozpoczecia', *years_possible)
      end_year = st.slider('Rok konca', *years_possible)
    with st.expander("Filtry"):
      city_checkboxes = st.multiselect("Miasta", df['city'].unique(), default=df['city'].unique())
      language_checkboxes = st.multiselect('Język klienta', df['language'].unique(), default=df['language'].unique())
      attraction_groups_checkboxes = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())
      status_checkboxes = st.multiselect("Status", ["Zrealizowane", "Anulowane", "Zrealizowane nieopłacone"], default="Zrealizowane")
      visit_type_groups_checkboxes = st.multiselect('Typy wizyty', np.concatenate([df['visit_type'].unique(), np.array(["Wszystkie"])]), default="Wszystkie", on_change=lambda:ensure_status(), key="ms1")

    if x_axis_type == 'Data stworzenia':
      x_axis_type = 'booked_date'
    elif x_axis_type == 'Data rozpoczecia':
      x_axis_type = 'start_date'

    if type(time_range) is int:
      start_date = datetime(time_range, 1, 1)

      if time_range == datetime.now().year:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
      else:
        end_date = datetime(time_range, 12, 31, 23, 59, 59)
    elif time_range == 'Od poczatku':
      min_date = df[x_axis_type].min()
      start_date = datetime.now().replace(hour=min_date.hour, minute=min_date.minute, second=min_date.second, microsecond=min_date.microsecond, day=min_date.day, month=min_date.month, year=min_date.year)
      # 23:59:59 the day before - to account for data collection that takes place at 01:00:00 - we want to cut out single reservations at 00:00:00
      end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    elif time_range == 'Przedział':
      start_date = datetime(start_year, 1, 1)

      if end_year == datetime.now().year:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
      else:
        end_date = datetime(end_year, 12, 31, 23, 59, 59)

    df['start_date'] = pd.to_datetime(df['start_date']).dt.tz_localize(None)
    df['booked_date'] = pd.to_datetime(df['booked_date']).dt.tz_localize(None)

    df['status'] = df.apply(determine_status, axis=1)
    df = df[df['status'].isin(status_checkboxes)]
    df = df[df['city'].isin(city_checkboxes)]
    df = df[df['language'].isin(language_checkboxes)]
    df = df[df['attraction_group'].isin(attraction_groups_checkboxes)]
    df = df if "Wszystkie" in visit_type_groups_checkboxes else df[df['visit_type'].isin(visit_type_groups_checkboxes)]

    df = df[df[x_axis_type] >= start_date]
    df = df[df[x_axis_type] <= end_date]


    return (df, x_axis_type, group_dates_by)
