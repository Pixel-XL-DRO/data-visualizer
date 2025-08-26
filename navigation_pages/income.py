import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import queries
import utils
import income_sidebar
import income_utils
import auth
import extra_streamlit_components as stx


with st.spinner():
    df = queries.get_reservation_data()
    _, df_dotypos = queries.get_order_items()
    df_voucher = queries.get_voucher_data()

df = auth.filter_locations(df)

def render_online_view(df):
    (df, group_by, show_only_moving_average, moving_average_days, moving_average_toggle, group_dates_by, start_date, end_date, x_axis) = income_sidebar.filter_online_data(df)
    df_online, _, _, _ = income_utils.group_data_cumulative(df, df_dotypos, df_voucher, moving_average_days, group_by, start_date, end_date, x_axis)
    df_by_weekday = income_utils.average_by_weekday(df, df_dotypos, df_voucher, group_by, group_dates_by, start_date, end_date, x_axis)
    online_chart = utils.create_chart_new(df_online, df_online['date'], "Data", 'total_online_sum' if not show_only_moving_average else None, 'total_online_sum_ma' if moving_average_toggle else None, "Przychód (PLN)", group_by, 2 if group_by else 4, "Średnia", False)
    st.plotly_chart(online_chart, use_container_width=True)

    st.text("Średni przychód online w danej grupie czasowej")
    reservations_chart = utils.create_bar_chart(df_by_weekday, 'group', group_dates_by, 'total_online_mean', 'Przychód (PLN)', None, None)
    st.altair_chart(reservations_chart, use_container_width=True)

    reservations_chart = utils.create_chart_new(df_online, df_online['date'], "Data", None, 'cumsum_online', "Kumulujacy sie przychód (PLN)", group_by, 2 if group_by else 4, "Przychód (PLN)", False)
    st.plotly_chart(reservations_chart, use_container_width=True)

def render_pos_view(df_dotypos):
    filter_checkbox = st.checkbox(
    'Pokaz tylko sprzedaz rezerwacyjna',
    key="filter_checkbox",
    value=False
    )

    if filter_checkbox:
        mask = df_dotypos['name'].str.contains('bilet|zadatek|voucher|integracja|uczestnik|urodzin', case=False, na=False)
        df_dotypos = df_dotypos[mask]

    (df_dotypos, separate_cities, show_only_moving_average, moving_average_days, moving_average_toggle, group_dates_by, start_date, end_date) = income_sidebar.filter_pos_data(df_dotypos)
    groupBy = 'city' if separate_cities else None

    _, df_pos, _, _ = income_utils.group_data_cumulative(df, df_dotypos, df_voucher, moving_average_days, groupBy, start_date, end_date)
    df_by_weekday = income_utils.average_by_weekday(df, df_dotypos, df_voucher, groupBy, group_dates_by, start_date, end_date)

    pos_chart = utils.create_chart_new(df_pos, df_pos['date'], "Data", 'total_pos_sum' if not show_only_moving_average else None, 'total_pos_sum_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(pos_chart, use_container_width=True)

    st.text("Średni przychód kasowy w danej grupie czasowej")
    reservations_chart = utils.create_bar_chart(df_by_weekday, 'group', group_dates_by, 'total_pos_mean', 'Przychód (PLN)', None, None)
    st.altair_chart(reservations_chart, use_container_width=True)

    reservations_chart = utils.create_chart_new(df_pos, df_pos['date'], "Data", None, 'cumsum_pos', "Kumulujacy sie przychód (PLN)", groupBy, 2 if groupBy else 4, "Przychód (PLN)", False)
    st.plotly_chart(reservations_chart, use_container_width=True)

def render_total_view(df, df_dotypos):

    filter_checkbox = st.checkbox(
    'Pokaz tylko sprzedaz rezerwacyjna',
    key="filter_checkbox",
    value=False
    )

    if filter_checkbox:
        mask = df_dotypos['name'].str.contains('bilet|zadatek|voucher|integracja|uczestnik|urodzin', case=False, na=False)
        df_dotypos = df_dotypos[mask]

    (df, df_dotypos, separate_cities, show_only_moving_average, moving_average_days, moving_average_toggle, group_dates_by, start_date, end_date) = income_sidebar.filter_total_data(df, df_dotypos)
    groupBy = 'city' if separate_cities else None

    _, _, df_total, _ = income_utils.group_data_cumulative(df, df_dotypos, df_voucher, moving_average_days, groupBy, start_date, end_date)
    df_by_weekday = income_utils.average_by_weekday(df, df_dotypos, df_voucher, groupBy, group_dates_by, start_date, end_date)

    total_chart = utils.create_chart_new(df_total, df_total['date'], "Data", 'total_reservations_sum' if not show_only_moving_average else None, 'total_reservations_sum_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(total_chart, use_container_width=True)

    st.text("Średni przychód sumaryczny w danej grupie czasowej")
    reservations_chart = utils.create_bar_chart(df_by_weekday, 'group', group_dates_by, 'total_reservations_mean', 'Przychód (PLN)', None, None)
    st.altair_chart(reservations_chart, use_container_width=True)

    reservations_chart = utils.create_chart_new(df_total, df_total['date'], "Data", None, 'cumsum_total', "Kumulujacy sie przychód (PLN)", groupBy, 2 if groupBy else 4, "Przychód (PLN)", False)
    st.plotly_chart(reservations_chart, use_container_width=True)

def render_voucher_view(df_voucher):

    (df_voucher, separate_cities, show_only_moving_average, moving_average_days, moving_average_toggle, group_dates_by, start_date, end_date) = income_sidebar.filter_voucher_data(df_voucher)
    groupBy = 'city' if separate_cities else None

    _, _, _, df_voucher_grouped = income_utils.group_data_cumulative(df, df_dotypos, df_voucher, moving_average_days, groupBy, start_date, end_date)
    df_by_weekday = income_utils.average_by_weekday(df, df_dotypos, df_voucher, groupBy, group_dates_by, start_date, end_date)

    voucher_chart = utils.create_chart_new(df_voucher_grouped, df_voucher_grouped['date'], "Data", 'total_voucher_sum' if not show_only_moving_average else None, 'total_voucher_sum_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "Średnia", False)
    st.plotly_chart(voucher_chart, use_container_width=True)

    st.text("Średni przychód sumaryczny w danej grupie czasowej")
    reservations_chart = utils.create_bar_chart(df_by_weekday, 'group', group_dates_by, 'total_voucher_mean', 'Przychód (PLN)', None, None)
    st.altair_chart(reservations_chart, use_container_width=True)

    reservations_chart = utils.create_chart_new(df_voucher_grouped, df_voucher_grouped['date'], "Data", None, 'cumsum_voucher', "Kumulujacy sie przychód (PLN)", groupBy, 2 if groupBy else 4, "Przychód (PLN)", False)
    st.plotly_chart(reservations_chart, use_container_width=True)

current_tab_id = stx.tab_bar(data=[
    stx.TabBarItemData(id=1, title="Online", description="Rezerwacje online"),
    stx.TabBarItemData(id=2, title="Kasy", description="Sprzedaż kasowa"),
    stx.TabBarItemData(id=3, title="Vouchery", description="Sprzedaż voucherów"),
    stx.TabBarItemData(id=4, title="Suma", description="Przychody sumaryczne"),
], default=1)

if current_tab_id == '1':
    render_online_view(df)
elif current_tab_id == '2':
    render_pos_view(df_dotypos)
elif current_tab_id == '3':
    render_voucher_view(df_voucher)
elif current_tab_id == '4':
    render_total_view(df, df_dotypos)
