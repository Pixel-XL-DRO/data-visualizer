import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import queries
import utils
import vouchers_sidebar
import auth
import vouchers_queries


with st.spinner("Inicjalizacja...", show_time=True):
    df = queries.get_vouchers_initial_data()
    df = auth.filter_locations(df)

(start_date, end_date, cities) = vouchers_sidebar.filter_data(df)

with st.spinner("Ładowanie danych...", show_time=True):

    df_vouchers, df_vouchers_worth = utils.run_in_parallel(
        (vouchers_queries.get_vouchers_count, (start_date, end_date, cities)),
        (vouchers_queries.get_vouchers_worth, (start_date, end_date, cities)),
    )

    st.text("Ilość sprzedanych voucherów według nazwy")
    voucher_count_chart = utils.create_bar_chart(df_vouchers, 'name', 'Nazwa', 'count', 'Liczba sprzedanych kopii', None)
    st.altair_chart(voucher_count_chart, use_container_width=True)

    st.text("Wartość sprzedanych voucherów według nazwy")
    voucher_worth_chart = utils.create_bar_chart(df_vouchers_worth, 'name', 'Nazwa', 'worth', 'Wartość sprzedanych kopii', None)
    st.altair_chart(voucher_worth_chart, use_container_width=True)

utils.lazy_load_initials()