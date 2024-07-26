import sys
sys.path.append("shared")
sys.path.append("utils")

import streamlit as st
import pandas as pd

import queries
import utils
import sidebar
import reservations_utils

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
(df, x_axis_type, moving_average_toggle,
show_only_moving_average, moving_average_days,
seperate_cities,seperate_attractions, seperate_status,
seperate_visit_types) = sidebar.filter_data(df)

groupBy = 'city' if seperate_cities else 'attraction_group' if seperate_attractions else 'status' if seperate_status else 'visit_type' if seperate_visit_types else None

(df_grouped, reservations_rolling_averages, total_cost_rolling_averages,
total_people_rolling_averages) = reservations_utils.group_data_and_calculate_moving_average(df, x_axis_type, moving_average_days, groupBy)

if moving_average_toggle:
  df_grouped['reservations_ma'] = pd.concat(reservations_rolling_averages)
  df_grouped['total_cost_ma'] = pd.concat(total_cost_rolling_averages)
  df_grouped['total_people_ma'] = pd.concat(total_people_rolling_averages)

df_grouped[x_axis_type] = df_grouped[x_axis_type].dt.to_timestamp()

st.text("Liczba rezerwacji")
reservations_chart = utils.create_chart(df_grouped, x_axis_type, "Data", 'reservations' if not show_only_moving_average else None, 'reservations_ma' if moving_average_toggle else None, "Liczba rezerwacji", groupBy, 2 if groupBy else 4, "month")
st.altair_chart(reservations_chart, use_container_width=True)

st.text("Przychód (PLN)")
cost_chart = utils.create_chart(df_grouped, x_axis_type, "Data", 'total_cost' if not show_only_moving_average else None, 'total_cost_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "month")
st.altair_chart(cost_chart, use_container_width=True)

st.text("Liczba osób")
people_chart = utils.create_chart(df_grouped, x_axis_type, "Data", 'total_people' if not show_only_moving_average else None, 'total_people_ma' if moving_average_toggle else None, "Liczba osób", groupBy, 2 if groupBy else 4, "month")
st.altair_chart(people_chart, use_container_width=True)
