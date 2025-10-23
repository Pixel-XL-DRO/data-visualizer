import sys

sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd
import dotypos_sidebar
import queries
import dotypos_queries
import utils
import auth
from datetime import timedelta

with st.spinner("Inicjalizacja...", show_time=True):
    df, df_items = queries.get_dotypos_initial_data()
    df = auth.filter_locations(df)

# side bar
(groupBy, show_only_moving_average, moving_average_days, moving_average_toggle, cities, start_date) = dotypos_sidebar.filter_data(df)

with st.spinner("Ładowanie danych...", show_time=True):
    df_bar, df_sale, df_res = utils.run_in_parallel(
        (dotypos_queries.get_order_items, (start_date, cities, moving_average_days, groupBy)),
        (dotypos_queries.get_order_items_per_sale, (start_date, cities, moving_average_days, groupBy)),
        (dotypos_queries.get_order_items_per_reservation, (start_date, cities, moving_average_days, groupBy)),
    )

tab1, tab2, tab3, tab4 = st.tabs(["SPRZEDAŻ BAROWA", "SPRZEDAŻ BAROWA NA RACHUNEK", "SPRZEDAŻ BAROWA NA REZERWACJE", "PRODUKTY"])

with tab1:

    @st.fragment
    def tab_one():
        total_brutto_chart = utils.create_chart_new(df_bar, 'date', "Data", 'brutto' if not show_only_moving_average else None, 'brutto_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa brutto [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia sprzedaż barowa brutto", False)
        st.plotly_chart(total_brutto_chart, use_container_width=True)

        total_netto_chart = utils.create_chart_new(df_bar, 'date', "Data", 'netto' if not show_only_moving_average else None, 'netto_rolling_avg', "Sprzedaż barowa netto [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia sprzedaż barowa netto", False)
        st.plotly_chart(total_netto_chart, use_container_width=True)

        utils.download_button(df_bar, "sprzedaz_barowa")
    tab_one()

with tab2:

    @st.fragment
    def tab_two():
        brutto_per_visit_chart = utils.create_chart_new(df_sale, 'date', "Data",  'brutto' if not show_only_moving_average else None, 'brutto_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa brutto na rachunek [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
        st.plotly_chart(brutto_per_visit_chart, use_container_width=True)

        netto_per_visit_chart = utils.create_chart_new(df_sale, 'date', "Data", 'netto' if not show_only_moving_average else None, 'netto_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa netto na rachunek [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
        st.plotly_chart(netto_per_visit_chart, use_container_width=True)

        utils.download_button(df_sale, "sprzedaz_na_wizyte")
    tab_two()

with tab3:
    @st.fragment
    def tab_three():

        reservation_count_chart = utils.create_chart_new(df_res, 'date', "Data", 'res_count' if not show_only_moving_average else None, 'res_moving_avg' if moving_average_toggle else None, "ilosc odbytych rezerwacji", groupBy, 2 if groupBy else 4, "Średnia", False)
        st.plotly_chart(reservation_count_chart, use_container_width=True)

        brutto_per_reservation_chart = utils.create_chart_new(df_res, 'date', "Data",  'brutto_per_reservation' if not show_only_moving_average else None, 'brutto_per_reservation_moving_avg' if moving_average_toggle else None, "Sprzedaż barowa brutto na rezerwacje [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
        st.plotly_chart(brutto_per_reservation_chart, use_container_width=True)

        netto_per_reservation_chart = utils.create_chart_new(df_res, 'date', "Data", 'netto_per_reservation' if not show_only_moving_average else None, 'netto_per_reservation_moving_avg' if moving_average_toggle else None, "Sprzedaż barowa netto na rezerwacje [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
        st.plotly_chart(netto_per_reservation_chart, use_container_width=True)

        utils.download_button(df_res, "sprzedaz_na_rezerwacje")
    tab_three()

with tab4:

    @st.fragment
    def tab_four():
        col1, col2 = st.columns(2)

        min_date = df['min_creation_date'].min()
        max_date = df['max_creation_date'].max()
        with col1:
            current_period_start = st.date_input('Od kiedy', value=min_date, min_value=min_date, max_value=max_date - timedelta(days=1)) # first of february is the date we fully adapted dotypos

        with col2:
            current_period_end = st.date_input('Do kiedy', value=max_date +  + timedelta(days=1), min_value=min_date + timedelta(days=1), max_value=max_date +  + timedelta(days=1))

        items = st.multiselect("Produkty", df_items['name'].unique(), default=None)

        current_period_start = pd.to_datetime(current_period_start)
        current_period_end = pd.to_datetime(current_period_end)

        with st.spinner("Ładowanie danych", show_time=True):
            df_sales, df_sold_items = utils.run_in_parallel(
                (dotypos_queries.get_items_sales_per_day, (current_period_start, current_period_end, moving_average_days, cities, items, groupBy)),
                (dotypos_queries.get_items_sold, (current_period_start, current_period_end, cities, items, groupBy))
            )

        items_chart  = utils.create_chart_new(df_sales, 'date', "Data", 'count' if not show_only_moving_average else None, 'count_moving_avg' if moving_average_toggle else None, "Ilość sprzedaży na dany dzień", groupBy, 2 if groupBy else 4, "Średnia ilość sprzedaży", False)
        st.plotly_chart(items_chart, use_container_width=True)

        df_sold_items

        utils.download_button(df_sold_items, "produkty")

    tab_four()

utils.lazy_load_initials()