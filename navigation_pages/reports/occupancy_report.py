import streamlit as st
import utils
import queries
import auth
import occupancy_report_sidebar

with st.spinner("Ładowanie danych..."):
  df_initial, df_locations,  df_location_hours_availability, df_location_boards_availability = utils.run_in_parallel(
    (queries.get_initial_data, ()),
    (queries.get_locations_data, ()),
    (queries.get_historical_location_hours_availability, ()),
    (queries.get_historical_location_boards_availability, ()),
  )

  df_initial = auth.filter_locations(df_initial)
  df_locations = auth.filter_locations(df_locations)

(attraction_groups, filtered_locations) = occupancy_report_sidebar.filter_data(df_initial, df_locations) 



def parse_data(df_initial, df_locations, df_location_hours_availability, df_location_boards_availability, selected_city, attraction_groups):

  st.write("asd")


def view():

  parse_data(df_initial, df_locations, df_location_hours_availability, df_location_boards_availability, filtered_locations[0], attraction_groups)


view()