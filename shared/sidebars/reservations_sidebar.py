import sys
sys.path.append("shared")

from datetime import datetime, timedelta
import utils
import streamlit as st
import pandas as pd
import numpy as np

def ensure_status():
  if (not st.session_state.ms1):
    st.session_state.ms1 = ["Wszystkie"]
    return

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
      moving_average_toggle = st.checkbox('Pokazuj', key="t1", value=True, on_change=lambda:utils.chain_toggle_off("t1", "t2","t7"))
      show_only_moving_average = st.checkbox('Pokazuj tylko srednia kroczaca', key="t2", value=False, on_change=lambda:utils.chain_toggle_on("t2", "t1"))
      moving_average_days = st.slider('Ile dni', 1, 30, 7)
    with st.expander("Notatki"):
      show_notes = st.checkbox('Pokazuj notatki', key="t7", value=False, on_change=lambda:utils.chain_toggle_on("t7","t1","t2"))
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
        min_date = df['booked_date'].min()
        start_date = datetime.now().replace(hour=min_date.hour, minute=min_date.minute, second=min_date.second, microsecond=min_date.microsecond, day=min_date.day, month=min_date.month, year=min_date.year)
    else:
      start_date = datetime.combine(start_date, datetime.min.time())
    cities = df['city'][df['city'].isin(city_checkboxes)].unique()
    language = df['language'][df['language'].isin(language_checkboxes)].unique()
    visit_types = df['visit_type'].unique() if "Wszystkie" in visit_type_groups_checkboxes else df['visit_type'][df['visit_type'].isin(visit_type_groups_checkboxes)].unique()

    moving_average_days -= 1 # sql index starts from 0 so we have to subtract 1

    groupBy = 'city' if seperate_cities else 'attraction_group' if seperate_attractions else 'status' if seperate_status else 'visit_type' if seperate_visit_types else None

    return (x_axis_type, moving_average_toggle,
      show_only_moving_average, moving_average_days,
      show_notes, start_date, cities, language, attraction_groups_checkboxes,status_checkboxes,visit_types, groupBy)
