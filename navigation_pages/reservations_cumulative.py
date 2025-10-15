import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st

import queries
import utils
import reservations_cumulative_sidebar
import reservations_cumulative_queries
def determine_status(row):
  if row['is_cancelled']:
    return 'Anulowane'
  elif not row['is_payed']:
    return 'Zrealizowane nieopłacone'
  return 'Zrealizowane'

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_initial_data()

(x_axis_type, moving_average_toggle,
 show_only_moving_average,
  start_date, end_date, cities, language, attraction_groups_checkboxes,status_checkboxes,visit_type_groups_checkboxes, groupBy) = reservations_cumulative_sidebar.filter_data(df)

with st.spinner("Ładowanie danych...", show_time=True):

  df_reservations, df_boardhours, df_people = utils.run_in_parallel(
    (reservations_cumulative_queries.get_reservations_cumulative, (x_axis_type, start_date, end_date, groupBy,
          cities, language, attraction_groups_checkboxes,
          status_checkboxes, visit_type_groups_checkboxes)),
    (reservations_cumulative_queries.get_reservations_boardhours_cumulative, (x_axis_type, start_date, end_date, groupBy,
          cities, language, attraction_groups_checkboxes,
          status_checkboxes, visit_type_groups_checkboxes)),
    (reservations_cumulative_queries.get_reservations_people_cumulative, (x_axis_type, start_date, end_date, groupBy,
          cities, language, attraction_groups_checkboxes,
          status_checkboxes, visit_type_groups_checkboxes))
  )

reservations_chart = utils.create_chart_new(df_reservations, 'date', "Data", None, 'cumulative_count', "Kumulujaca sie liczba rezerwacji", groupBy, 2 if groupBy else 4, "Liczba rezerwacji", False)
st.plotly_chart(reservations_chart, use_container_width=True)
reservations_chart = utils.create_chart_new(df_boardhours, 'date', "Data", None, 'cumulative_boardhours_taken', "Kumulujaca sie liczba matogodzin", groupBy, 2 if groupBy else 4, "Liczba matogodzin", False)
st.plotly_chart(reservations_chart, use_container_width=True)
reservations_chart = utils.create_chart_new(df_people, 'date', "Data", None, 'cumulative_people_taken', "Kumulujaca sie liczba osób", groupBy, 2 if groupBy else 4, "Liczba osób", False)
st.plotly_chart(reservations_chart, use_container_width=True)

