import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")
sys.path.append("navigation_pages/boards_occupancy")

import streamlit as st

import queries
import utils
import boards_occupancy_sidebar

import plan4u_view
import safi_view

with st.spinner():
  df = queries.get_reservation_data()
  df_locations = queries.get_locations_data()
  df_visit_types = queries.get_visit_types_data()
  df_location_hours_availability = queries.get_historical_location_hours_availability()
  df_location_boards_availability = queries.get_historical_location_boards_availability()
  df_visit_type_availability = queries.get_historical_visit_type_availability()
  df_slots_occupancy = queries.get_slots_occupancy()

(df, x_axis_type) = boards_occupancy_sidebar.filter_data(df)

for location_id in df['location_id'].unique():
  location = df_locations[df_locations['id'] == location_id].iloc[0]

  location_boards_availability = df_location_boards_availability.loc[(df_location_boards_availability['boards_availability_dim_location_id'] == location_id)]
  if location_boards_availability.empty:
      st.error(f"location {location['city']} {location['street']} not associated with boards availability")
      st.stop()

  for day_of_week in range(7):
    location_hours_availability = df_location_hours_availability.loc[(df_location_hours_availability['hours_availability_dim_location_id'] == location_id) & (df_location_hours_availability['hours_availability_day_of_week'] == day_of_week)]
    if location_hours_availability.empty:
      st.error(f"location {location['city']} {location['street']} for {utils.map_day_of_week_number_to_string(day_of_week)} not associated with hours availability")
      st.stop()

city_selection = st.selectbox('Wybierz miasto', df_locations['city'].unique())

if city_selection != "krakow" and city_selection != "lodz":
  plan4u_view.render_plan4u_view(
    df,
    df_locations,
    df_visit_types,
    df_location_hours_availability,
    df_location_boards_availability,
    df_visit_type_availability,
    df_slots_occupancy,
    city_selection
  )
else:
  safi_view.render_safi_view(
    df,
    df_locations,
    df_visit_types,
    df_location_hours_availability,
    df_location_boards_availability,
    df_visit_type_availability,
    df_slots_occupancy,
    city_selection
  )
