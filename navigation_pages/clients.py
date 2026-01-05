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

(x_axis_type, start_date, streets, language, attraction_groups_checkboxes,status_checkboxes,visit_types, groupBy) = clients_sidebar.filter_data(df)

with st.spinner("Ładowanie danych...", show_time=True):
  df_grouped = clients_queries.get_retention_data(x_axis_type, start_date, groupBy, streets, language, attraction_groups_checkboxes,status_checkboxes,visit_types)

st.text("Retencja na miesiac (procent wizyt, które zostały stworzone przez klientów którzy już u nas byli \n")
reservations_chart = utils.create_chart_new(df_grouped, 'date', "Data", None, 'percentage_old_reservations', "Procent wizyt", groupBy, 4, "Procent wizyt", False)
st.plotly_chart(reservations_chart, use_container_width=True)

utils.lazy_load_initials()