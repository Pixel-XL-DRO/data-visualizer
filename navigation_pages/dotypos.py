import sys

sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")
from io import BytesIO
import dotypos_utils
import streamlit as st
import datetime
import pandas as pd
import dotypos_sidebar
import queries
import utils
from datetime import datetime


with st.spinner():
    df = queries.get_order_items()
    df_reservation = queries.get_reservation_data()

# side bar
(df, df_reservation, separate_cities, show_only_moving_average, moving_average_days, moving_average_toggle) = dotypos_sidebar.filter_data(df, df_reservation)

groupBy = 'city' if separate_cities else None

df_grouped, df_upsell_grouped = dotypos_utils.group_order_items(df, groupBy, moving_average_days)
df_res = dotypos_utils.calc_earnings_per_reservation(df_reservation, df, groupBy, moving_average_days)
df_items, df_grouped_items = dotypos_utils.calc_items(df, groupBy, moving_average_days)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["SPRZEDAŻ BAROWA", "SPRZEDAŻ BAROWA NA RACHUNEK", "SPRZEDAŻ BAROWA NA REZERWACJE", "SUMA", "PRODUKTY"])

with tab1:

    total_brutto_chart = utils.create_chart_new(df_grouped, df_grouped['date_only'], "Data", 'brutto' if not show_only_moving_average else None, 'brutto_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa brutto", groupBy, 2 if groupBy else 4, "Średnia sprzedaż barowa brutto", False)
    st.plotly_chart(total_brutto_chart, use_container_width=True)

    total_netto_chart = utils.create_chart_new(df_grouped, df_grouped['date_only'], "Data", 'netto' if not show_only_moving_average else None, 'netto_rolling_avg', "Sprzedaż barowa netto", groupBy, 2 if groupBy else 4, "Średnia sprzedaż barowa netto", False)
    st.plotly_chart(total_netto_chart, use_container_width=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_grouped.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
        processed_data = output.getvalue()

    st.download_button(
    label="Pobierz plik .xlxs",
    data=processed_data,
    icon="😈",
    file_name="upsell.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with tab2:

    upsell_brutto_per_visit_chart = utils.create_chart_new(df_upsell_grouped, df_upsell_grouped['date_only'], "Data",  'brutto_by_quantity' if not show_only_moving_average else None, 'avg_brutto_per_purchase' if moving_average_toggle else None, "Sprzedaż barowa brutto na rachunek", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(upsell_brutto_per_visit_chart, use_container_width=True)

    upsell_netto_per_visit_chart = utils.create_chart_new(df_upsell_grouped, df_upsell_grouped['date_only'], "Data", 'netto_by_quantity' if not show_only_moving_average else None, 'avg_netto_per_purchase' if moving_average_toggle else None, "Sprzedaż barowa netto na rachunek", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(upsell_netto_per_visit_chart, use_container_width=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_upsell_grouped.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
        processed_data = output.getvalue()


    st.download_button(
    label="Pobierz plik .xlxs",
    data=processed_data,
    icon="😎",
    file_name="upsell.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with tab3:

    reservation_count_chart = utils.create_chart_new(df_res, df_res['start_date'], "Data", 'count' if not show_only_moving_average else None, 'count_rolling_avg' if moving_average_toggle else None, "ilosc rezerwacji", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(reservation_count_chart, use_container_width=True)

    upsell_netto_per_reservation_chart = utils.create_chart_new(df_res, df_res['start_date'], "Data", 'mean_netto_per_reservation' if not show_only_moving_average else None, 'mean_netto_per_reservation_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa netto na rezerwacje", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(upsell_netto_per_reservation_chart, use_container_width=True)

    upsell_brutto_per_reservation_chart = utils.create_chart_new(df_res, df_res['start_date'], "Data",  'mean_brutto_per_reservation' if not show_only_moving_average else None, 'mean_brutto_per_reservation_rolling_avg' if moving_average_toggle else None, "Sprzedaż barowa netto na rezerwacje", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(upsell_brutto_per_reservation_chart, use_container_width=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_res.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
        processed_data = output.getvalue()

    st.download_button(
    label="Pobierz plik .xlxs",
    data=processed_data,
    icon="👽",
    file_name="upsell.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with tab4:

    total_income  = utils.create_chart_new(df_res, df_res['start_date'], "Data", 'total_brutto_income' if not show_only_moving_average else None, "total_brutto_income_rolling_avg" if moving_average_toggle else None, "Sprzedaż barowa brutto + rezerwacje", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(total_income, use_container_width=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_res.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
        processed_data = output.getvalue()

    st.download_button(
    label="Pobierz plik .xlxs",
    data=processed_data,
    icon="👹",
    file_name="f.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with tab5:

    col1, col2 = st.columns(2)

    with col1:
        current_period_start = st.date_input('Od kiedy', value=datetime(2025,1,1))

    with col2:
        current_period_end = st.date_input('Do kiedy', value=datetime.now())

    current_period_start = pd.to_datetime(current_period_start)
    current_period_end = pd.to_datetime(current_period_end)

    df_filtered = df.loc[
        (df['creation_date'] >= current_period_start) &
        (df['creation_date'] <= current_period_end)
    ]

    df_items, df_filtered = dotypos_utils.calc_items(df_filtered, groupBy, moving_average_days)

    items  = utils.create_chart_new(df_items, 'creation_date', "Data", 'quantity' if not show_only_moving_average else None, 'quantity_ma' if moving_average_toggle else None, "Ilość sprzedaży na dany dzień", groupBy, 2 if groupBy else 4, "Średnia ilość sprzedaży", False)
    st.plotly_chart(items, use_container_width=True)

    df_filtered

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_grouped_items.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
        processed_data = output.getvalue()

    st.download_button(
    label="Pobierz plik .xlxs",
    data=processed_data,
    icon="👻",
    file_name="sprzedaż produktów.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )