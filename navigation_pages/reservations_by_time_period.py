import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

import auth
import queries
import utils
import reservations_by_time_period_sidebar
import reservations_by_time_period_utils

def determine_status(row):
  if row['is_cancelled']:
    return 'Anulowane'
  elif not row['is_payed']:
    return 'Zrealizowane nieopłacone'
  return 'Zrealizowane'

with st.spinner():
  df = queries.get_reservation_data()

df = auth.filter_locations(df)

# side bar
(df, x_axis_type, group_dates_by) = reservations_by_time_period_sidebar.filter_data(df)

period = reservations_by_time_period_utils.mapGroupDatesByToPeriod(x_axis_type, group_dates_by)
df_grouped, current_period = reservations_by_time_period_utils.periodize(df, x_axis_type, period, None)

st.subheader("uwaga: dane z aktualnego niepełnego okresu sa pomijane (poza rokiem)")

st.text("Średnia liczba rezerwacji w danej grupie czasowej")
reservations_chart = utils.create_bar_chart(df_grouped, 'parsed_period', group_dates_by, 'reservations', 'Rezerwacje', None, current_period)
st.altair_chart(reservations_chart, use_container_width=True)

st.text("Średnia liczba matogodzin w danej grupie czasowej")
boardhours_taken_chart = utils.create_bar_chart(df_grouped, 'parsed_period', group_dates_by, 'boardhours_taken', 'Liczba zajętych matogodzin', None, current_period)
st.altair_chart(boardhours_taken_chart, use_container_width=True)

st.text("Średnia liczba osób w danej grupie czasowej")
reservations_chart = utils.create_bar_chart(df_grouped, 'parsed_period', group_dates_by, 'total_people', 'Liczba użytkowników', None, current_period)
st.altair_chart(reservations_chart, use_container_width=True)
