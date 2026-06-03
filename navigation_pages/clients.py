import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import altair as alt

import queries
import clients_sidebar
import auth
import utils
import clients_queries

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_initial_data()
  df = auth.filter_locations(df)

(x_axis_type, start_date, streets, language, attraction_groups_checkboxes, status_checkboxes,visit_types, groupBy, client_retention_length) = clients_sidebar.filter_data(df)

if len(streets) == 0:
  st.warning("Pole miasta nie może być puste")
  st.stop()

if len(language) == 0:
  st.warning("Pole języki nie może być puste")
  st.stop()

if len(attraction_groups_checkboxes) == 0:
  st.warning("Pole grupy atrakcji nie może być puste")
  st.stop()

if len(status_checkboxes) == 0:
  st.warning("Pole status nie może być puste")
  st.stop()

with st.spinner("Ładowanie danych...", show_time=True):
  df_grouped = clients_queries.get_retention_data(x_axis_type, start_date, groupBy, streets, language, attraction_groups_checkboxes,status_checkboxes,visit_types, client_retention_length)
  avg_retention_days = clients_queries.get_avg_return_day(x_axis_type, start_date, streets, language, attraction_groups_checkboxes,status_checkboxes,visit_types)

st.metric("Średnia długość między ponowną wizytą (dni)", value=avg_retention_days)

st.info("Retencja na miesiac (procent wizyt, które zostały stworzone przez klientów którzy już u nas byli \n")
reservations_chart = utils.create_chart_new(df_grouped, 'date', "Data", None, 'percentage_old_reservations', "Procent wizyt", groupBy, 4, "Procent wizyt", False)
st.plotly_chart(reservations_chart, use_container_width=True)

utils.lazy_load_initials()