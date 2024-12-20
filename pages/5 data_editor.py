import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import datetime

import queries
import utils

st.set_page_config(layout="wide")

with st.spinner():
  locations_df = queries.get_locations_data()
  hours_availability_df = queries.get_historical_location_hours_availability()
  boards_availability_df = queries.get_historical_location_boards_availability()
  visit_types_df = queries.get_visit_types_data()
  visit_type_availability_df = queries.get_historical_visit_type_availability()

st.button("Odśwież", on_click=lambda: queries.refresh_data_editor_data())

with st.expander("Dane lokalizacyjne (liczba dostępnych godzin)"):
  location_selectbox = st.selectbox("Wybierz miasto", (locations_df['city'] + ' ' + locations_df['street']))
  day_of_week = st.selectbox("Wybierz dzień tygodnia", ["Poniedzialek", "Wtorek", "Sroda", "Czwartek", "Piatek", "Sobota", "Niedziela"])
  parsed_day_of_week = utils.map_day_of_week_string_to_number(day_of_week)

  selected_location = locations_df.loc[(locations_df['city'] == location_selectbox.split(' ')[0]) & (locations_df['street'] == location_selectbox.split(' ')[1])]
  filtered_hours_availability_df = hours_availability_df.loc[(hours_availability_df['hours_availability_dim_location_id'] == selected_location['id'].values[0]) & (hours_availability_df['hours_availability_day_of_week'] == parsed_day_of_week)]

  with st.container(border=True):
    df_current_mappings = filtered_hours_availability_df[['hours_availability_number_of_hours', 'hours_availability_starting_hour', 'hours_availability_since_when', 'hours_availability_until_when']]
    st.write("Aktualne mapowania: ")
    st.write(df_current_mappings)

  with st.container(border=True):
    st.write("Dodaj mapowanie: ")
    since_when = st.date_input("Od kiedy")
    since_when_dt = datetime.datetime(since_when.year, since_when.month, since_when.day)

    number_of_hours = st.number_input("Ile godzin", step=0.5, value=12.0, min_value=0.0, max_value=24.0)
    starting_hour = st.number_input("O której startuje lokal", step=0.5, value=8.0, min_value=0.0, max_value=24.0)
    st.button("Dodaj", on_click=lambda: queries.add_historical_location_hours_availability(selected_location['id'].values[0], since_when_dt, parsed_day_of_week, number_of_hours, starting_hour))

with st.expander("Dane lokalizacyjne (liczba dostępnych mat)"):
  boards_location_selectbox = st.selectbox(key="boards_select_box", label="Wybierz miasto", options=(locations_df['city'] + ' ' + locations_df['street']))
  boards_selected_location = locations_df.loc[(locations_df['city'] == boards_location_selectbox.split(' ')[0]) & (locations_df['street'] == boards_location_selectbox.split(' ')[1])]

  filtered_boards_availability_df = boards_availability_df.loc[boards_availability_df['boards_availability_dim_location_id'] == boards_selected_location['id'].values[0]]

  with st.container(border=True):
    boards_df_current_mappings = filtered_boards_availability_df[['boards_availability_number_of_boards', 'boards_availability_time_unit_in_hours', 'boards_availability_since_when', 'boards_availability_until_when']]
    st.write("Aktualne mapowania: ")
    st.write(boards_df_current_mappings)

  with st.container(border=True):
    st.write("Dodaj mapowanie: ")
    boards_since_when = st.date_input(key="boards_date_input", label="Od kiedy")
    boards_since_when_dt = datetime.datetime(boards_since_when.year, boards_since_when.month, boards_since_when.day)

    number_of_boards = st.number_input("Ile mat", step=1, value=4, min_value=0, max_value=99)
    time_unit_in_hours = st.number_input("Jednostka czasowa (w godzinach)", step=0.5, value=1.0, min_value=0.5, max_value=5.0)
    st.button(key="boards_add_button", label="Dodaj", on_click=lambda: queries.add_historical_location_boards_availability(boards_selected_location['id'].values[0], boards_since_when_dt, number_of_boards, time_unit_in_hours))

with st.expander("Dane typów wizyt (liczba zajmowanych mat i godzin)"):
  visit_type_city_selectbox = st.selectbox(key="visit_types_select_box", label="Wybierz miasto", options=(locations_df['city'] + ' ' + locations_df['street']))
  visit_type_selected_location = locations_df.loc[(locations_df['city'] == visit_type_city_selectbox.split(' ')[0]) & (locations_df['street'] == visit_type_city_selectbox.split(' ')[1])]

  filtered_visit_types_df = visit_types_df.loc[visit_types_df['visit_type_dim_location_id'] == visit_type_selected_location['id'].values[0]]
  visit_type_selectbox = st.selectbox(label="Wybierz typ wizyty", options=filtered_visit_types_df['name'])
  selected_visit_type = filtered_visit_types_df.loc[visit_types_df['name'] == visit_type_selectbox]

  filtered_visit_type_availability_df = visit_type_availability_df.loc[(visit_type_availability_df['visit_type_availability_dim_visit_type_id'] == selected_visit_type['visit_type_id'].values[0])]

  with st.container(border=True):
    visit_types_df_current_mappings = filtered_visit_type_availability_df[['visit_type_availability_number_of_boards_per_time_unit', 'visit_type_availability_duration_in_time_units', 'visit_type_availability_since_when', 'visit_type_availability_until_when']]
    st.write("Aktualne mapowania: ")
    st.write(visit_types_df_current_mappings)

  with st.container(border=True):
    st.write("Dodaj mapowanie: ")
    visit_types_since_when = st.date_input(key="visit_types_date_input", label="Od kiedy")
    visit_types_since_when_dt = datetime.datetime(visit_types_since_when.year, visit_types_since_when.month, visit_types_since_when.day)

    visit_types_number_of_boards_per_time_unit = st.number_input(key="visit_type_number_of_boards", label="Ile mat", step=1, value=4, min_value=0, max_value=99)
    visit_types_duration_in_time_units = st.number_input(key="visit_type_number_of_hours", label="Dlugośc w jednostkach czasowych", step=0.5, value=1.0, min_value=0.0, max_value=24.0)
    st.button(key="visit_type_add_button", label="Dodaj", on_click=lambda: queries.add_historical_visit_type_availability(selected_visit_type['visit_type_id'].values[0], visit_types_since_when_dt, visit_types_number_of_boards_per_time_unit, visit_types_duration_in_time_units))

#   st.write("Aktualna liczba mat: " + str(chosen_city['number_of_boards'].values[0]))
#   new_number_of_boards = st.text_input("Nowa liczba mat: ")


#   st.write(df)

# with st.expander("Liczba mat na wizyte"):
#   city_selectbox = st.selectbox("Wybierz typ wizyty", df['city'] + ' ' + df['street'])
#   chosen_city = df[df['city'] == city_selectbox.split(' ')[0]]

#   st.write("Aktualna liczba mat: " + str(chosen_city['number_of_boards'].values[0]))
#   new_number_of_boards = st.text_input("Nowa liczba mat: ")
#   st.button("Zapisz", on_click=lambda: queries.change_number_of_boards(chosen_city['id'].values[0], int(new_number_of_boards)))

#   st.write(df)
