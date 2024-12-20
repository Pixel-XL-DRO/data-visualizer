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

  with st.sidebar:
    x_axis_type = st.selectbox('Wybierz rodzaj daty', ['Data stworzenia', 'Data rozpoczecia'])
    time_range = st.selectbox('Pokazuj z ostatnich', ['7 dni', '1 miesiaca', '6 miesiecy', '1 roku', '2 lat', '3 lat', 'Od poczatku', "Przedział"], index=2)
    if time_range == "Przedział":
      start_date = st.date_input('Data rozpoczecia')
      end_date = st.date_input('Data konca')
    with st.expander("Średnia kroczaca"):
      moving_average_toggle = st.checkbox('Pokazuj', key="t1", value=True, on_change=lambda:utils.chain_toggle_off("t1", "t2"))
      show_only_moving_average = st.checkbox('Pokazuj tylko srednia kroczaca', key="t2", value=False, on_change=lambda:utils.chain_toggle_on("t2", "t1"))
      moving_average_days = st.slider('Ile dni', 1, 30, 7)
    with st.expander("Rodzaj wykresów"):
      chart_type = st.radio('', ["stary", "nowy"], index=1)
    with st.expander("Filtry"):
      with st.container(border=True):
        city_checkboxes = st.multiselect("Miasta", df['city'].unique(), default=df['city'].unique())
        seperate_cities = st.checkbox('Rozdziel miasta', key="t3", on_change=lambda:utils.make_sure_only_one_toggle_is_on(["t3", "t4", "t5", "t6"], "t3"))
      language_checkboxes = st.multiselect('Język klienta', df['language'].unique(), default=df['language'].unique())
      with st.container(border=True):
        attraction_groups_checkboxes = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())
        seperate_attractions = st.checkbox('Rozdziel atrakcje', key="t4", on_change=lambda:utils.make_sure_only_one_toggle_is_on(["t3", "t4", "t5", "t6"], "t4"))
      with st.container(border=True):
        status_checkboxes = st.multiselect("Status", ["Zrealizowane", "Anulowane", "Zrealizowane nieopłacone"], default=["Zrealizowane", "Zrealizowane nieopłacone"])
        seperate_status = st.checkbox('Rozdziel status', key="t5", on_change=lambda:utils.make_sure_only_one_toggle_is_on(["t3", "t4", "t5", "t6"], "t5"))
      with st.container(border=True):
        visit_type_groups_checkboxes = st.multiselect('Typy wizyty', np.concatenate([df['visit_type'].unique(), np.array(["Wszystkie"])]), default="Wszystkie", on_change=lambda:ensure_status(), key="ms1")
        seperate_visit_types = st.checkbox('Rozdziel typy wizyty', key="t6", on_change=lambda:utils.make_sure_only_one_toggle_is_on(["t3", "t4", "t5", "t6"], "t6"))

    if x_axis_type == 'Data stworzenia':
      x_axis_type = 'booked_date'
    elif x_axis_type == 'Data rozpoczecia':
      x_axis_type = 'start_date'

    if chart_type == 'stary':
      chart_type = 'old'
    elif chart_type == 'nowy':
      chart_type = 'new'

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
    df['booked_date'] = pd.to_datetime(df['booked_date']).dt.tz_localize(None)

    df['status'] = df.apply(determine_status, axis=1)
    df = df[df['status'].isin(status_checkboxes)]
    df = df[df['city'].isin(city_checkboxes)]
    df = df[df['language'].isin(language_checkboxes)]
    df = df[df['attraction_group'].isin(attraction_groups_checkboxes)]
    df = df if "Wszystkie" in visit_type_groups_checkboxes else df[df['visit_type'].isin(visit_type_groups_checkboxes)]

    df = df[df[x_axis_type] >= start_date]
    df = df[df[x_axis_type] <= end_date]


    return (df, x_axis_type, moving_average_toggle,
      show_only_moving_average, moving_average_days,
      seperate_cities,seperate_attractions, seperate_status,
      seperate_visit_types, chart_type)
