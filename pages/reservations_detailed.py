import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd
from datetime import datetime

import queries
import utils
import reservations_detailed_sidebar
import reservations_detailed_utils

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
(df, x_axis_type, seperate_cities,seperate_attractions,
seperate_status, seperate_visit_types, group_dates_by) = reservations_detailed_sidebar.filter_data(df)

period = reservations_detailed_utils.mapGroupDatesByToPeriod(x_axis_type, group_dates_by)

groupBy = 'city' if seperate_cities else 'attraction_group' if seperate_attractions else 'status' if seperate_status else 'visit_type' if seperate_visit_types else None

df_grouped = reservations_detailed_utils.group_data_and_calculate_moving_average(df, x_axis_type, period, groupBy)

df_cumulative = reservations_detailed_utils.group_data_cumulative(df, x_axis_type, groupBy)

# date_of_first_reservation = df[x_axis_type].min()
# today = pd.to_datetime(datetime.today().date())
# days_passed = (today - date_of_first_reservation).days
# weeks_passed = days_passed / 7
# months_passed = (today.year - date_of_first_reservation.year) * 12 + today.month - date_of_first_reservation.month
# years_passed = today.year - date_of_first_reservation.year
# reservations_detailed_utils.divideKPIBasedOnPeriod(df_grouped, 'reservations', period, date_of_first_reservation, )

df_cumulative[x_axis_type] = df_cumulative[x_axis_type].dt.to_timestamp()

# with st.expander("Grupy czasowe"):
#   st.write(df_grouped)
  # st.text("Średnia liczba rezerwacji w danej grupie czasowej")


# reservations_chart = utils.create_bar_chart(df_grouped, period, group_dates_by, 'reservations', 'Rezerwacje', groupBy)
# st.altair_chart(reservations_chart, use_container_width=True)

with st.expander("Dane kumulujace sie"):
  st.text("Kumulujaca sie liczba rezerwacji")
  reservations_chart = utils.create_chart(df_cumulative, x_axis_type, "Data", None, 'reservations', "Liczba rezerwacji", groupBy, 2 if groupBy else 4, "month")
  st.altair_chart(reservations_chart, use_container_width=True)
  st.text("Kumulujacy sie przychód")
  reservations_chart = utils.create_chart(df_cumulative, x_axis_type, "Data", None, 'total_cost', "Przychód (PLN)", groupBy, 2 if groupBy else 4, "month")
  st.altair_chart(reservations_chart, use_container_width=True)
  st.text("Kumulujaca sie liczba osób")
  reservations_chart = utils.create_chart(df_cumulative, x_axis_type, "Data", None, 'total_people', "Liczba osób", groupBy, 2 if groupBy else 4, "month")
  st.altair_chart(reservations_chart, use_container_width=True)

# st.text("Liczba osób")

