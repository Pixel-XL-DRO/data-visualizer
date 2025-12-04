import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")
sys.path.append("navigation_pages/boards_occupancy")
sys.path.append("/shared/queries")

import streamlit as st

import queries
import utils
import boards_occupancy_sidebar
import auth
import plan4u_view
import safi_view

SAFI_CITIES = ["lubicz", "ogrodowa", "kijowska", "swietego-marcina", "sokolska"] 

with st.spinner("Ładowanie danych...", show_time=True):

  df_initial, df_locations,  df_location_hours_availability, df_location_boards_availability = utils.run_in_parallel(
    (queries.get_initial_data, ()),
    (queries.get_locations_data, ()),
    (queries.get_historical_location_hours_availability, ()),
    (queries.get_historical_location_boards_availability, ()),
  )

  df_initial = auth.filter_locations(df_initial)
  df_locations = auth.filter_locations(df_locations)


(attraction_groups) = boards_occupancy_sidebar.filter_data(df_initial)

df_locations['location'] = df_locations['street'].map(utils.street_to_location).fillna(df_locations['street'])
city_selection = st.selectbox('Wybierz miasto', df_locations['location'].unique())
selected_city = df_locations['street'][df_locations['location'] == city_selection].iloc[0]

df_initial = df_initial[df_initial['street'] == selected_city]

if not selected_city in SAFI_CITIES:
  plan4u_view.render_plan4u_view(
    df_initial,
    df_locations,
    df_location_hours_availability,
    df_location_boards_availability,
    selected_city,
    attraction_groups
  )
else:
  safi_view.render_safi_view(
    df_initial,
    df_locations,
    df_location_hours_availability,
    df_location_boards_availability,
    selected_city,
    attraction_groups
  )

utils.lazy_load_initials()