import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")
sys.path.append("navigation_pages/boards_occupancy")

import streamlit as st

import queries
import utils
import boards_occupancy_sidebar
import auth
import plan4u_view
import safi_view

SAFI_CITIES = ["lubicz", "ogrodowa", "kijowska"]

with st.spinner("Ładowanie danych...", show_time=True):

  df, df_locations, df_visit_types, df_location_hours_availability, df_location_boards_availability, df_visit_type_availability, df_slots_occupancy = utils.run_in_parallel(
    (queries.get_reservation_data, ()),
    (queries.get_locations_data, ()),
    (queries.get_visit_types_data, ()),
    (queries.get_historical_location_hours_availability, ()),
    (queries.get_historical_location_boards_availability, ()),
    (queries.get_historical_visit_type_availability, ()),
    (queries.get_slots_occupancy, ()),
  )

  df = auth.filter_locations(df)
  df_locations = auth.filter_locations(df_locations)

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

df_locations['location'] = df_locations['street'].map(utils.street_to_location).fillna(df_locations['street'])
city_selection = st.selectbox('Wybierz miasto', df_locations['location'].unique())
selected_city = df_locations['street'][df_locations['location'] == city_selection].iloc[0]

if not selected_city in SAFI_CITIES:
  plan4u_view.render_plan4u_view(
    df,
    df_locations,
    df_visit_types,
    df_location_hours_availability,
    df_location_boards_availability,
    df_visit_type_availability,
    df_slots_occupancy,
    selected_city
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
    selected_city
  )

utils.lazy_load_initials()