import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import altair as alt

import queries
import clients_sidebar
import clients_utils
import utils

def determine_status(row):
  if row['is_cancelled']:
    return 'Anulowane'
  elif not row['is_payed']:
    return 'Zrealizowane nieopłacone'
  return 'Zrealizowane'

with st.spinner():
  df = queries.get_reservation_data()

# side bar
(df, x_axis_type, seperate_cities, seperate_attractions,
seperate_status, seperate_visit_types) = clients_sidebar.filter_data(df)

groupBy = 'city' if seperate_cities else 'attraction_group' if seperate_attractions else 'status' if seperate_status else 'visit_type' if seperate_visit_types else None

df_grouped = clients_utils.group_data(df, x_axis_type, groupBy)

st.text("Retencja na miesiac (procent wizyt, które zostały stworzone przez klientów którzy już u nas byli \n")
reservations_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", None, 'past_retention_percent', "Procent wizyt", groupBy, 4, "Procent wizyt", False)
st.plotly_chart(reservations_chart, use_container_width=True)
