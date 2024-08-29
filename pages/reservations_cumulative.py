import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd
from datetime import datetime

import queries
import utils
import reservations_cumulative_sidebar
import reservations_cumulative_utils

st.set_page_config(layout="wide")

def determine_status(row):
  if row['is_cancelled']:
    return 'Anulowane'
  elif not row['is_payed']:
    return 'Zrealizowane nieopłacone'
  return 'Zrealizowane'

with st.spinner():
  df = queries.get_reservation_data()

# side bar
(df_cumulative, x_axis_type, seperate_cities,seperate_attractions,
seperate_status, seperate_visit_types, group_by) = reservations_cumulative_sidebar.filter_data(df)


st.text("Kumulujaca sie liczba rezerwacji")
reservations_chart = utils.create_chart(df_cumulative, x_axis_type, "Data", None, 'reservations', "Liczba rezerwacji", group_by, 2 if group_by else 4, "month")
st.altair_chart(reservations_chart, use_container_width=True)
st.text("Kumulujacy sie przychód")
reservations_chart = utils.create_chart(df_cumulative, x_axis_type, "Data", None, 'total_cost', "Przychód (PLN)", group_by, 2 if group_by else 4, "month")
st.altair_chart(reservations_chart, use_container_width=True)
st.text("Kumulujaca sie liczba osób")
reservations_chart = utils.create_chart(df_cumulative, x_axis_type, "Data", None, 'total_people', "Liczba osób", group_by, 2 if group_by else 4, "month")
st.altair_chart(reservations_chart, use_container_width=True)

