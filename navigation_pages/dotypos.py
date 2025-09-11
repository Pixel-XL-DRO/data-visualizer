import sys

sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")
import dotypos_utils
import streamlit as st
import datetime
import pandas as pd
import dotypos_sidebar
import queries
import utils
import auth
from datetime import datetime

with st.spinner():
    df,_ = queries.get_order_items()
    df_reservation = queries.get_reservation_data()

df = auth.filter_locations(df)
df_reservation = auth.filter_locations(df_reservation)

# side bar
(df, df_reservation, separate_cities, show_only_moving_average, moving_average_days, moving_average_toggle) = dotypos_sidebar.filter_data(df, df_reservation)
groupBy = 'city' if separate_cities else None

df_grouped, df_upsell_grouped = dotypos_utils.group_order_items(df, groupBy, moving_average_days)
df_res = dotypos_utils.calc_earnings_per_reservation(df_reservation, df, groupBy, moving_average_days)

st.markdown("<div style='text-align: center;'><p style='font-size:24px; color: #FFAA33' >Dane <b style='color: #FF7B1C'>nie uwzględniają</b> sprzedaży rezerwacyjnej.<br> Sprzedaż rezerwacyjna jest dostępna w zakładce <b><a href='/income' style='color: #FF4433'>Przychody</a></b>.</p></div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["SPRZEDAŻ BAROWA", "SPRZEDAŻ BAROWA NA RACHUNEK", "SPRZEDAŻ BAROWA NA REZERWACJE", "PRODUKTY"])

with tab1:

    total_brutto_chart = utils.create_chart_new(df_grouped, df_grouped['date_only'], "Data", 'brutto' if not show_only_moving_average else None, 'brutto_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa brutto [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia sprzedaż barowa brutto", False)
    st.plotly_chart(total_brutto_chart, use_container_width=True)

    total_netto_chart = utils.create_chart_new(df_grouped, df_grouped['date_only'], "Data", 'netto' if not show_only_moving_average else None, 'netto_rolling_avg', "Sprzedaż barowa netto [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia sprzedaż barowa netto", False)
    st.plotly_chart(total_netto_chart, use_container_width=True)

    utils.download_button(df_grouped, "sprzedaz_barowa")

with tab2:

    brutto_per_visit_chart = utils.create_chart_new(df_upsell_grouped, df_upsell_grouped['date_only'], "Data",  'brutto_by_quantity' if not show_only_moving_average else None, 'avg_brutto_per_purchase' if moving_average_toggle else None, "Sprzedaż barowa brutto na rachunek [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(brutto_per_visit_chart, use_container_width=True)

    netto_per_visit_chart = utils.create_chart_new(df_upsell_grouped, df_upsell_grouped['date_only'], "Data", 'netto_by_quantity' if not show_only_moving_average else None, 'avg_netto_per_purchase' if moving_average_toggle else None, "Sprzedaż barowa netto na rachunek [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(netto_per_visit_chart, use_container_width=True)

    utils.download_button(df_upsell_grouped, "sprzedaz_na_wizyte")

with tab3:

    reservation_count_chart = utils.create_chart_new(df_res, df_res['start_date'], "Data", 'count' if not show_only_moving_average else None, 'count_rolling_avg' if moving_average_toggle else None, "ilosc rezerwacji", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(reservation_count_chart, use_container_width=True)

    brutto_per_reservation_chart = utils.create_chart_new(df_res, df_res['start_date'], "Data",  'mean_brutto_per_reservation' if not show_only_moving_average else None, 'mean_brutto_per_reservation_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa brutto na rezerwacje [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(brutto_per_reservation_chart, use_container_width=True)

    netto_per_reservation_chart = utils.create_chart_new(df_res, df_res['start_date'], "Data", 'mean_netto_per_reservation' if not show_only_moving_average else None, 'mean_netto_per_reservation_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa netto na rezerwacje [ZŁ]", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(netto_per_reservation_chart, use_container_width=True)

    utils.download_button(df_res, "sprzedaz_na_rezerwacje")

with tab4:

    col1, col2 = st.columns(2)

    with col1:
        current_period_start = st.date_input('Od kiedy', value=df['creation_date'].min()) # first of february is the date we fully adapted dotypos

    with col2:
        current_period_end = st.date_input('Do kiedy', value=datetime.now())

    current_period_start = pd.to_datetime(current_period_start)
    current_period_end = pd.to_datetime(current_period_end)

    df_filtered = df.loc[
        (df['creation_date'] >= current_period_start) &
        (df['creation_date'] <= current_period_end)
    ]

    items = st.multiselect("Produkty", df_filtered['name'].unique(), default=None)
    df_filtered = df_filtered if len(items) == 0 or not items else df_filtered[df_filtered['name'].isin(items)]

    df_items, df_filt = dotypos_utils.calc_items(df_filtered, groupBy, moving_average_days)

    items_chart  = utils.create_chart_new(df_items, 'creation_date', "Data", 'quantity' if not show_only_moving_average else None, 'quantity_ma' if moving_average_toggle else None, "Ilość sprzedaży na dany dzień", groupBy, 2 if groupBy else 4, "Średnia ilość sprzedaży", False)
    st.plotly_chart(items_chart, use_container_width=True)

    df_filt

    utils.download_button(df_filt, "produkty")
