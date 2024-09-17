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

st.set_page_config(layout="wide")

with st.spinner():
  df = queries.get_reservation_data()

# side bar
(df, x_axis_type, seperate_cities, seperate_attractions,
seperate_status, seperate_visit_types) = clients_sidebar.filter_data(df)

groupBy = 'city' if seperate_cities else 'attraction_group' if seperate_attractions else 'status' if seperate_status else 'visit_type' if seperate_visit_types else None

df_grouped = clients_utils.group_data(df, x_axis_type, groupBy)

st.text("Retencja na miesiac (procent osób które odwiedziły nas conajmniej 2 razy w historii) \n [WIP]")
reservations_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", None, 'past_retention_percent', "Procent osób", groupBy, 4, "Procent osób")
st.plotly_chart(reservations_chart, use_container_width=True)

client_reservations_count = df.groupby('client_id').size().reset_index(name='reservation_count')

client_reservations_count['category'] = client_reservations_count['reservation_count'].apply(
    lambda x: '1 raz' if x == 1 else '2 razy' if x == 2 else ('3 razy' if x == 3 else ('4 razy' if x == 4 else 'więcej'))
)

category_counts = client_reservations_count['category'].value_counts().reset_index()
category_counts.columns = ['category', 'count']
st.write("Ile razy ludzie u nas byli?")
category_counts
