import sys
sys.path.append("shared")

from datetime import datetime, timedelta
import utils
import streamlit as st

def filter_data(df):
  start_date = None
  end_date = None

  df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])

  with st.sidebar:
    with st.container(border=True):
      time_range = st.selectbox('Pokazuj z ostatnich', ['7 dni', '1 miesiaca', '6 miesiecy', '1 roku', '2 lat', '3 lat', 'Od poczatku'], index=6)

    with st.expander("Średnia krocząca"):
      moving_average_toggle = st.checkbox('Pokazuj', key="t1", value=True, on_change=lambda:utils.chain_toggle_off("t1", "t2","t7"))
      show_only_moving_average = st.checkbox('Pokazuj tylko srednia kroczaca', key="t2", value=False, on_change=lambda:utils.chain_toggle_on("t2", "t1"))
      moving_average_days = st.slider('Ile dni', 1, 30, 7)
      moving_average_days = moving_average_days - 1 # we have to decrement to adjust for SQL indexing from 0

    with st.expander("Filtry", expanded=True):
      with st.container(border=True):
        location_checkboxes = st.multiselect("Miasta", df['location'].unique(), default=df['location'].unique())
        separate_cities = st.checkbox('Rozdziel miasta')
        
    if end_date is None:
      end_date = datetime.now() + timedelta(days=1)

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
        min_date = df['date'].min()
        start_date = datetime.now().replace(hour=min_date.hour, minute=min_date.minute, second=min_date.second, microsecond=min_date.microsecond, day=min_date.day, month=min_date.month, year=min_date.year)
        
    else:
      start_date = datetime.combine(start_date, datetime.min.time())

    cities = df['street'][df['location'].isin(location_checkboxes)].unique()

    return (start_date, end_date, cities, separate_cities, moving_average_toggle, show_only_moving_average, moving_average_days)
