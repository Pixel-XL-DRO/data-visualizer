import sys
from datetime import datetime

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

CLIENT_RETENTION_DAYS_BACK = [14, 30, 60, 90, 180, 365]

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_initial_data()
  df = auth.filter_locations(df)

(
  x_axis_type,
  start_date,
  streets,
  language,
  attraction_groups_checkboxes,
  status_checkboxes,
  visit_types,
  groupBy,
) = clients_sidebar.filter_data(df)

first_date = df[x_axis_type].sort_values().reset_index(drop=True)[0]
days_since_first = (datetime.now() - first_date.to_pydatetime()).days
CLIENT_RETENTION_DAYS_BACK.append("Od początku")

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

client_retention_length = st.selectbox(
  "Od kiedy liczymy klienta jako powrót (dni)",
  CLIENT_RETENTION_DAYS_BACK,
  index=len(CLIENT_RETENTION_DAYS_BACK) - 1,
)

with st.spinner("Ładowanie danych...", show_time=True):
  df_grouped = clients_queries.get_retention_data(
    x_axis_type,
    start_date,
    groupBy,
    streets,
    language,
    attraction_groups_checkboxes,
    status_checkboxes,
    visit_types,
    days_since_first if client_retention_length == "Od początku" else client_retention_length,
  )
  avg_retention_days = clients_queries.get_avg_return_day(
    x_axis_type,
    start_date,
    streets,
    language,
    attraction_groups_checkboxes,
    status_checkboxes,
    visit_types,
  )


st.metric("Średnia długość między ponowną wizytą (dni)", value=avg_retention_days)

st.info(
  "Retencja na miesiac (procent wizyt, które zostały stworzone przez klientów którzy już u nas byli \n"
)
reservations_chart = utils.create_chart_new(
  df_grouped,
  "date",
  "Data",
  None,
  "percentage_old_reservations",
  "Procent wizyt",
  groupBy,
  4,
  "Procent wizyt",
  False,
)
st.plotly_chart(reservations_chart, use_container_width=True)

utils.lazy_load_initials()