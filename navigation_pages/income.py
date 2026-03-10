import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")
sys.path.append("shared/queries/income_queries")

import streamlit as st
import queries
import utils
import income_sidebar
import online_queries
import pos_queries
import total_queries
import vouchers_income_queries
import auth
import extra_streamlit_components as stx

with st.spinner("Ładowanie danych...", show_time=True):

    df, (df_dotypos, _), df_voucher = utils.run_in_parallel(
        (queries.get_initial_data, ()),
        (queries.get_dotypos_initial_data, ()),
        (queries.get_vouchers_initial_data, ()),
    )

    df = auth.filter_locations(df)
    df_dotypos = auth.filter_locations(df_dotypos)
    df_voucher = auth.filter_locations(df_voucher)

def render_online_view(df):
    (group_by, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start, end, date_type, cities, languages, attractions, status, attraction_types) = income_sidebar.filter_online_data(df,filter_only_cities=False)

    grouping_period, group_dates_by = utils.parse_grouping_period(group_dates_by)

    with st.spinner("Ładowanie danych...", show_time=True):

        df_online, df_online_period, df_online_cumulative = utils.run_in_parallel(
            (online_queries.get_online_income, (group_by, moving_average_days, start, end, date_type, cities, languages, attractions, status, attraction_types)),
            (online_queries.get_online_income_by_time_period, (date_type, start, end, status, cities, languages, attractions, attraction_types, grouping_period)),
            (online_queries.get_online_income_cumulative, (group_by, start, end, date_type, cities, languages, attractions, status, attraction_types)),
        )
    
    online_chart = utils.create_chart_new(df_online, 'date', "Data", 'price' if not show_moving_average_only else None, 'price_ma' if show_moving_average else None, "Przychód (PLN)", group_by, 2 if group_by else 4, "Średnia", False)
    st.plotly_chart(online_chart, use_container_width=True)

    st.text("Średni przychód online w danej grupie czasowej")

    if df_online_period.empty:
        st.warning("Brak danych dla danej grupy czasowej")
    else:
        online_by_period_chart = utils.create_bar_chart(df_online_period, 'period', group_dates_by, 'avg_count', 'Przychód (PLN)', None, df_online_period['current_period'].iloc[0])
        st.altair_chart(online_by_period_chart, use_container_width=True)

    online_cumulative_chart = utils.create_chart_new(df_online_cumulative, 'date', "Data", None, 'price', "Kumulujacy sie przychód (PLN)", group_by, 2 if group_by else 4, "Przychód (PLN)", False)
    st.plotly_chart(online_cumulative_chart, use_container_width=True)

def render_pos_view(df_dotypos):
    filter_checkbox = st.checkbox(
        'Pokaz tylko sprzedaz rezerwacyjna',
        key="filter_checkbox",
        value=False
    )
    (separate_cities, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start, end, cities) = income_sidebar.filter_pos_data(df_dotypos)
    group_by = 'street' if separate_cities else None

    grouping_period, group_dates_by = utils.parse_grouping_period(group_dates_by)

    with st.spinner("Ładowanie danych...", show_time=True):

        df_dotypos, df_dotypos_period, df_dotypos_cumulative = utils.run_in_parallel(
            (pos_queries.get_pos_income, (start, end, cities, filter_checkbox, moving_average_days, group_by)),
            (pos_queries.get_pos_income_by_period, (grouping_period, start, end, cities, filter_checkbox)),
            (pos_queries.get_pos_cumulative_income, (start, end, cities, filter_checkbox, group_by)),
        )

    pos_chart = utils.create_chart_new(df_dotypos, 'date', "Data", 'price' if not show_moving_average_only else None, 'price_ma' if show_moving_average else None, "Przychód (PLN)", group_by, 2 if group_by else 4, "Średnia", False)
    st.plotly_chart(pos_chart, use_container_width=True)

    st.text("Średni przychód kasowy w danej grupie czasowej")
    
    if df_dotypos_period.empty:
        st.warning("Brak danych dla danej grupy czasowej")
    else:
        pos_by_period_chart = utils.create_bar_chart(df_dotypos_period, 'period', group_dates_by, 'avg_count', 'Przychód (PLN)', None, df_dotypos_period['current_period'].iloc[0])
        st.altair_chart(pos_by_period_chart, use_container_width=True)

    pos_cumulative_chart = utils.create_chart_new(df_dotypos_cumulative, 'date', "Data", None, 'price', "Kumulujacy sie przychód (PLN)", group_by, 2 if group_by else 4, "Przychód (PLN)", False)
    st.plotly_chart(pos_cumulative_chart, use_container_width=True)

def render_total_view(df):

    filter_checkbox = st.checkbox(
        'Pokaz tylko sprzedaz rezerwacyjna',
        key="filter_checkbox",
        value=False
    )

    (group_by, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start, end, date_type, cities, languages, attractions, status, attraction_types) = income_sidebar.filter_online_data(df, filter_only_cities=True)
    
    grouping_period, group_dates_by = utils.parse_grouping_period(group_dates_by)

    with st.spinner("Ładowanie danych...", show_time=True):

        df_total, df_total_period, df_total_cumulative = utils.run_in_parallel(
            (total_queries.get_total_income, (group_by, moving_average_days, start, end, date_type, cities, languages, attractions, status, attraction_types, filter_checkbox)),
            (total_queries.get_total_income_by_period, (grouping_period, start, end, date_type, cities, languages, attractions, status, attraction_types, filter_checkbox)),
            (total_queries.get_total_cumulative_income, (group_by, start, end, date_type, cities, languages, attractions, status, attraction_types, filter_checkbox)),
        )

    income_chart = utils.create_chart_new(df_total, 'date', "Data", 'price' if not show_moving_average_only else None, 'price_ma' if show_moving_average else None, "Przychód (PLN)", group_by, 2 if group_by else 4, "Średnia", False)
    st.plotly_chart(income_chart, use_container_width=True)

    st.text("Średni przychód sumaryczny w danej grupie czasowej")

    if df_total_period.empty:
        st.warning("Brak danych dla danej grupy czasowej")    
    else:
        income_by_period_chart = utils.create_bar_chart(df_total_period, 'period', group_dates_by, 'avg_count', 'Przychód (PLN)', None, df_total_period['current_period'].iloc[0])
        st.altair_chart(income_by_period_chart, use_container_width=True)

    income_cumulative_chart = utils.create_chart_new(df_total_cumulative, 'date', "Data", None, 'total_price', "Kumulujacy sie przychód (PLN)", group_by, 2 if group_by else 4, "Przychód (PLN)", False)
    st.plotly_chart(income_cumulative_chart, use_container_width=True)

def render_voucher_view(df_voucher):

    (separate_cities, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start_date, end_date, cities) = income_sidebar.filter_voucher_data(df_voucher)
    group_by = 'street' if separate_cities else None
    
    grouping_period, group_dates_by = utils.parse_grouping_period(group_dates_by)

    with st.spinner("Ładowanie danych...", show_time=True):

        df_voucher_income, df_voucher_by_period, df_voucher_cumulative_income = utils.run_in_parallel(
            (vouchers_income_queries.get_voucher_income, (group_by, moving_average_days, start_date, end_date, cities)),
            (vouchers_income_queries.get_vouchers_by_weekday, (grouping_period, start_date, end_date, cities)),
            (vouchers_income_queries.get_voucher_cumulative_income, (group_by, start_date, end_date, cities)),
        )

    voucher_chart = utils.create_chart_new(df_voucher_income, 'date', "Data", 'price' if not show_moving_average_only else None, 'price_ma' if show_moving_average else None, "Przychód (PLN)", group_by, 2 if group_by else 4, "Średnia", False)
    st.plotly_chart(voucher_chart, use_container_width=True)

    st.text("Średni przychód sumaryczny w danej grupie czasowej")

    if df_voucher_by_period.empty:
        st.warning("Brak danych dla danej grupy czasowej")
    else:
        voucher_by_period_chart = utils.create_bar_chart(df_voucher_by_period, 'period', group_dates_by, 'avg_count', 'Przychód (PLN)', None, df_voucher_by_period['current_period'].iloc[0])
        st.altair_chart(voucher_by_period_chart, use_container_width=True)

    voucher_cumulative_chart = utils.create_chart_new(df_voucher_cumulative_income, 'date', "Data", None, 'price', "Kumulujacy sie przychód (PLN)", group_by, 2 if group_by else 4, "Przychód (PLN)", False)
    st.plotly_chart(voucher_cumulative_chart, use_container_width=True)

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
    render_total_view(df)

utils.lazy_load_initials()