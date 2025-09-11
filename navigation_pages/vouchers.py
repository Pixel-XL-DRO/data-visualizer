import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import queries
import utils
import vouchers_utils
import vouchers_sidebar
import auth

with st.spinner():
    df_voucher = queries.get_voucher_data()

df = auth.filter_locations(df_voucher)
df = vouchers_sidebar.filter_data(df)

df_grouped = vouchers_utils.group_data(df)

st.text("Ilość sprzedanych voucherów według nazwy")
voucher_count_chart = utils.create_bar_chart(df_grouped, 'voucher_name', 'Nazwa', 'count', 'Liczba sprzedanych kopii', None)
st.altair_chart(voucher_count_chart, use_container_width=True)