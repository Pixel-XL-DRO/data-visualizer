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

def filter_data(df):
  start_date = None
  end_date = None

  if df.empty:
    return (start_date, end_date, [])

  df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])

  with st.sidebar:
    with st.container(border=True):
      time_range = st.selectbox('Pokazuj z ostatnich', ['7 dni', '1 miesiaca', '6 miesiecy', '1 roku', '2 lat', '3 lat', 'Od poczatku', 'Przedział'], index=6)
      if time_range == "Przedział":
        yesterday = datetime.now() - timedelta(days=1)
        start_date = st.date_input('Data rozpoczecia', max_value=yesterday)
        end_date = st.date_input('Data konca', min_value=start_date, max_value="today")

    with st.expander("Filtry", expanded=True):
      with st.container(border=True):
        location_checkboxes = st.multiselect("Miasta", df['location'].unique(), default=df['location'].unique())

    if end_date is None:
      end_date = datetime.now()
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
        min_date = df['min_creation_date'].min()
        start_date = datetime.now().replace(hour=min_date.hour, minute=min_date.minute, second=min_date.second, microsecond=min_date.microsecond, day=min_date.day, month=min_date.month, year=min_date.year)
    else:
      start_date = datetime.combine(start_date, datetime.min.time())

    cities = df['street'][df['location'].isin(location_checkboxes)].unique()
    
    return (start_date, end_date, cities)
