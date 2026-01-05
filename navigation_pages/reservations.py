import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("shared/queries")
sys.path.append("utils")

import streamlit as st
import pandas as pd
import queries
import utils
import reservations_sidebar
import reservations_queries
import auth

with st.spinner("Inicjalizacja...", show_time=True):

    df = queries.get_initial_data()
    df_notes = queries.get_notes()
    df = auth.filter_locations(df)

(x_axis_type, moving_average_toggle,
 show_only_moving_average, moving_average_days,
 show_notes, start_date, cities, language, attraction_groups_checkboxes,status_checkboxes,visit_type_groups_checkboxes, groupBy) = reservations_sidebar.filter_data(df)
notes = df_notes if show_notes else None

with st.spinner("Ładowanie danych...", show_time=True):

    df_reservation_count, df_people_count, df_boardhours = utils.run_in_parallel(
        (reservations_queries.get_reservations_count,
         (x_axis_type, start_date, moving_average_days, groupBy,
          cities, language, attraction_groups_checkboxes,
          status_checkboxes, visit_type_groups_checkboxes, notes)),

        (reservations_queries.get_people_count,
         (x_axis_type, start_date, moving_average_days, groupBy,
          cities, language, attraction_groups_checkboxes,
          status_checkboxes, visit_type_groups_checkboxes, notes)),

        (reservations_queries.get_boardhours,
         (x_axis_type, start_date, moving_average_days, groupBy,
          cities, language, attraction_groups_checkboxes,
          status_checkboxes, visit_type_groups_checkboxes, notes))
    )

reservations_chart = utils.create_chart_new(df_reservation_count, 'date', "Data", 'count' if not show_only_moving_average else None, 'moving_avg' if moving_average_toggle else None, "Liczba rezerwacji", groupBy, 2 if groupBy else 4, "Średnia", show_notes)
st.plotly_chart(reservations_chart, use_container_width=True)

boardhours_chart = utils.create_chart_new(df_boardhours, 'date', "Data", 'boardhours_taken' if not show_only_moving_average else None, 'moving_avg' if moving_average_toggle else None, "Liczba zajętych matogodzin", groupBy, 2 if groupBy else 4, "Średnia", show_notes)
st.plotly_chart(boardhours_chart, use_container_width=True)

people_count_chart = utils.create_chart_new(df_people_count, 'date', "Data", 'person_count' if not show_only_moving_average else None, 'moving_avg' if moving_average_toggle else None, "Liczba osób", groupBy, 2 if groupBy else 4, "Średnia", show_notes)
st.plotly_chart(people_count_chart, use_container_width=True)

st.divider()
st.subheader("Średnia ilość dni rezerwacji w przód")

tab1, tab2 = st.tabs(["Średni okres podzielony na miasta", "Liczba rezerwacji podzielona na dni w przód"])

with tab1:
    @st.fragment
    def tab_one():
        with st.spinner("Ładowanie danych...", show_time=True):
            df_days_ahead = reservations_queries.get_mean_days_ahead(x_axis_type, start_date, cities)
            true_avg = (df_days_ahead['days'] * df_days_ahead['count']).sum() / df_days_ahead['count'].sum()

        st.markdown(f"Średni okres między rezerwacją a wizytą: **{true_avg:.2f}**")

        reservations_chart = utils.create_bar_chart(df_days_ahead, 'city', 'Miasto', 'days', 'Średni okres między rezerwacją a wizytą', None, moving_average_toggle)
        st.altair_chart(reservations_chart, use_container_width=True, )

    tab_one()

with tab2:
    @st.fragment
    def tab_two():

        df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])
        street = st.selectbox('Wybierz miasto', df['location'].unique(), index=0)
        period = st.selectbox('Wybierz okres', ['7 dni', '14 dni', '1 miesiaca', '2 miesiace'], index=1)
        period = {'7 dni': 7, '14 dni': 14, '1 miesiaca': 30, '2 miesiace': 60}[period]
        street = df['street'][df['location'] == street].iloc[0]

        with st.spinner("Ładowanie danych...", show_time=True):
            df_ahead_by_city = reservations_queries.get_days_ahead_by_city(x_axis_type, period, start_date, street)

        reservations_chart = utils.create_bar_chart(df_ahead_by_city, 'days', 'Dni w przód', 'reservations', 'Liczba rezerwacji', None)
        st.altair_chart(reservations_chart, use_container_width=True)

    tab_two()

utils.lazy_load_initials()