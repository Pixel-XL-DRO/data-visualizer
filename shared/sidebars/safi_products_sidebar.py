import sys
import utils
import streamlit as st

sys.path.append("shared")

def filter_data(df):

  df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])

  with st.sidebar:
    with st.expander("Średnia kroczaca"):
      moving_average_toggle = st.checkbox('Pokazuj', key="t1", value=True, on_change=lambda:utils.chain_toggle_off("t1", "t2"))
      show_only_moving_average = st.checkbox('Pokazuj tylko srednia kroczaca', key="t2", value=False, on_change=lambda:utils.chain_toggle_on("t2", "t1"))
      moving_average_days = st.slider('Ile dni', 1, 30, 7)

    with st.expander("Filtry", expanded=True):
      with st.container(border=True):
        location_checkboxes = st.multiselect("Miasta", df['location'].unique(), default=df['location'].unique())
        separate_cities = st.checkbox('Rozdziel miasta')

    cities = df['street'][df['location'].isin(location_checkboxes)].unique()
    groupBy = 'street' if separate_cities else None
    moving_average_days = moving_average_days - 1 # we have to decrement to adjust for SQL indexing from 0
    return (groupBy, show_only_moving_average, moving_average_days, moving_average_toggle, cities)
