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
import reservations_by_time_period_queries

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_initial_data()

df = auth.filter_locations(df)

# side bar
(x_axis_type, group_dates_by, start_date, end_date, status_checkboxes, city_checkboxes, language_checkboxes, attraction_groups_checkboxes, visit_type_groups_checkboxes) = reservations_by_time_period_sidebar.filter_data(df)

if group_dates_by == "Miesiac":
    grouping_period = "MONTH"
if group_dates_by == "Dzień tygodnia":
    grouping_period = "DAYOFWEEK"
if group_dates_by == "Tydzien roku":
    grouping_period = "ISOWEEK"
if group_dates_by == "Rok":
    grouping_period = "YEAR"
if group_dates_by == "Godzina":
    grouping_period = "HOUR"
if group_dates_by == "Dzień miesiaca":
    grouping_period = "DAY"

with st.spinner("Ładowanie danych...", show_time=True):

  df_reservations, df_boardhours, df_people = utils.run_in_parallel(
      (reservations_by_time_period_queries.get_reservations_by_time_period,
       (x_axis_type, start_date, end_date, status_checkboxes, city_checkboxes, language_checkboxes, attraction_groups_checkboxes, visit_type_groups_checkboxes, grouping_period)),

      (reservations_by_time_period_queries.get_boardhours_by_time_period,
       (x_axis_type, start_date, end_date, status_checkboxes, city_checkboxes, language_checkboxes, attraction_groups_checkboxes, visit_type_groups_checkboxes, grouping_period)),

      (reservations_by_time_period_queries.get_people_by_time_period,
       (x_axis_type, start_date, end_date, status_checkboxes, city_checkboxes, language_checkboxes, attraction_groups_checkboxes, visit_type_groups_checkboxes, grouping_period))
  )

st.subheader("uwaga: dane z aktualnego niepełnego okresu sa pomijane (poza rokiem)")

st.text("Średnia liczba rezerwacji w danej grupie czasowej")
reservations_chart = utils.create_bar_chart(df_reservations, 'period', group_dates_by, 'avg_count', 'Rezerwacje', None, df_reservations['current_period'].iloc[0])
st.altair_chart(reservations_chart, use_container_width=True)

st.text("Średnia liczba matogodzin w danej grupie czasowej")
boardhours_taken_chart = utils.create_bar_chart(df_boardhours, 'period', group_dates_by, 'avg_boardhours', 'Liczba zajętych matogodzin', None, df_boardhours['current_period'][0])
st.altair_chart(boardhours_taken_chart, use_container_width=True)

st.text("Średnia liczba osób w danej grupie czasowej")
reservations_chart = utils.create_bar_chart(df_people, 'period', group_dates_by, 'avg_people', 'Liczba użytkowników', None, df_people['current_period'][0])
st.altair_chart(reservations_chart, use_container_width=True)

utils.lazy_load_initials()