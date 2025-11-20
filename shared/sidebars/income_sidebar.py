import sys
sys.path.append("shared")

from datetime import datetime, timedelta
import utils
import streamlit as st
import pandas as pd
import numpy as np

def ensure_status():
    if st.session_state.online_attraction_types[0] == "Wszystkie":
        st.session_state.online_attraction_types = st.session_state.online_attraction_types[1:]
    elif st.session_state.online_attraction_types[-1] == "Wszystkie":
        st.session_state.online_attraction_types = ["Wszystkie"]

def filter_online_data(df, filter_only_cities=True):
    years_possible = list(range(2022, datetime.now().year + 1))

    df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])

    with st.sidebar:

        group_dates_by = st.selectbox('Wybierz grupowanie po dacie', ['Godzina', 'Dzień tygodnia', 'Tydzien roku', 'Dzień miesiaca', 'Miesiac', 'Rok'], index=1, key='online_grouping')

        with st.container(border=True):
            time_range = st.selectbox('Pokazuj ', ['Od poczatku', *years_possible], index=len(years_possible), key='online_timerange')

        date_type = st.selectbox('Wybierz rodzaj daty', ['Data stworzenia', 'Data rozpoczecia'], index=0)

        if date_type == 'Data stworzenia':
            date_type = 'booked_date'
        elif date_type == 'Data rozpoczecia':
            date_type = 'start_date'

        with st.expander("Średnia kroczaca"):
            show_moving_average = st.checkbox('Pokazuj', key="online_show_moving_average", value=True, on_change=lambda: utils.chain_toggle_off("online_show_moving_average", "online_show_moving_average_only"))
            show_moving_average_only = st.checkbox('Pokazuj tylko srednia kroczaca', key="online_show_moving_average_only", value=False, on_change=lambda: utils.chain_toggle_on("online_show_moving_average_only", "online_show_moving_average"))
            moving_average_days = st.slider('Ile dni', 1, 30, 7)

        with st.expander("Filtry", expanded=True):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df['location'].unique(), default=df['location'].unique(), key="online_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="online_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_sep"))

            if not filter_only_cities:

                with st.container(border=True):
                    languages = st.multiselect('Język klienta', df['language'].unique(), default=df['language'].unique(), key="online_lang")

                with st.container(border=True):
                    attractions = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique(), key="online_attr")
                    separate_attractions = st.checkbox('Rozdziel atrakcje', key="online_attr_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_attr_sep"))

                with st.container(border=True):
                    status = st.multiselect("Status", ["Zrealizowane", "Anulowane", "Zrealizowane nieopłacone"], default=["Zrealizowane", "Zrealizowane nieopłacone"], key="online_status")
                    separate_status = st.checkbox('Rozdziel status', key="online_status_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_status_sep"))

                with st.container(border=True):
                    attraction_types = st.multiselect('Typy wizyty', np.concatenate([df['visit_type'].unique(), np.array(["Wszystkie"])]), default="Wszystkie", on_change=ensure_status, key="online_attraction_types")
                    separate_visit_types = st.checkbox('Rozdziel typy wizyty', key="online_visit_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_visit_sep"))

                group_by = 'street' if separate_cities else 'attraction_group' if separate_attractions else 'status' if separate_status else 'visit_type' if separate_visit_types else None

            else:
                languages = df['language'].unique()
                attractions = df['attraction_group'].unique()
                status = ["Zrealizowane", "Zrealizowane nieopłacone"]
                attraction_types = df['visit_type'].unique()
                group_by = 'street' if separate_cities else None

            cities = df['street'][df['location'].isin(cities)].unique()

    current_ts = pd.Timestamp(datetime.now())
    if time_range == 'Od poczatku':
        start = pd.to_datetime(df[date_type]).dt.tz_localize(None).min()
        end = datetime(year=current_ts.year, month=current_ts.month, day=current_ts.day, hour=0, minute=0)
    else:
        start = datetime(year=time_range, month=1, day=1, hour=0, minute=0)
        end = pd.Timestamp(year=time_range, month=12, day=31, hour=23, minute=59)
        
    if time_range == current_ts.year:
        end = datetime(year=time_range, month=current_ts.month, day=current_ts.day, hour=0, minute=0)


    attraction_types = df['visit_type'].unique() if "Wszystkie" in attraction_types else df['visit_type'][df['visit_type'].isin(attraction_types)].unique()
    moving_average_days -= 1 # adjust for SQL indexing

    return (group_by, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start, end, date_type, cities, languages, attractions, status, attraction_types)


def filter_pos_data(df_dotypos):
    years_possible = list(range(2025, datetime.now().year + 1))

    df_dotypos['location'] = df_dotypos['street'].map(utils.street_to_location).fillna(df_dotypos['street'])

    with st.sidebar:
        group_dates_by = st.selectbox('Wybierz grupowanie po dacie',
            ['Godzina', 'Dzień tygodnia', 'Tydzien roku', 'Dzień miesiaca', 'Miesiac', 'Rok'],
            index=1, key='pos_grouping')

        with st.container(border=True):
            time_range = st.selectbox('Pokazuj ', ['Od poczatku', *years_possible],
                index=len(years_possible), key='pos_timerange')

        with st.expander("Średnia kroczaca"):
            show_moving_average = st.checkbox('Pokazuj', key="pos_show_moving_average", value=True, on_change=lambda: utils.chain_toggle_off("pos_show_moving_average", "pos_show_moving_average_only"))
            show_moving_average_only = st.checkbox('Pokazuj tylko srednia kroczaca', key="pos_show_moving_average_only", value=False, on_change=lambda: utils.chain_toggle_on("pos_show_moving_average_only", "pos_show_moving_average"))
            moving_average_days = st.slider('Ile dni', 1, 30, 7, key="pos_slider")

        with st.expander("Filtry", expanded=True):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df_dotypos['location'].unique(), default=df_dotypos['location'].unique(), key="pos_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="pos_sep")


    cities = df_dotypos['street'][df_dotypos['location'].isin(cities)].unique()
    df_dotypos = df_dotypos[df_dotypos['street'].isin(cities)]
    
    current_ts = pd.Timestamp(datetime.now())
    if time_range == 'Od poczatku':
        start = pd.to_datetime(df_dotypos['min_creation_date']).dt.tz_localize(None).min()
        end = datetime(year=current_ts.year, month=current_ts.month, day=current_ts.day, hour=0, minute=0)
       
    else:
        start = datetime(year=time_range, month=1, day=1, hour=0, minute=0)
        end = datetime(year=time_range, month=12, day=31, hour=23, minute=59)
 
    if time_range == current_ts.year:
        end = datetime(year=time_range, month=current_ts.month, day=current_ts.day, hour=0, minute=0)


    return (separate_cities, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start, end, cities)

def filter_voucher_data(df_voucher):
    years_possible = list(range(2023, datetime.now().year + 1))
    
    df_voucher['location'] = df_voucher['street'].map(utils.street_to_location).fillna(df_voucher['street'])

    with st.sidebar:
        group_dates_by = st.selectbox('Wybierz grupowanie po dacie', [ 'Dzień tygodnia', 'Tydzien roku', 'Dzień miesiaca', 'Miesiac', 'Rok'],index=1, key='voucher_grouping')

        with st.container(border=True):
            time_range = st.selectbox('Pokazuj ', ['Od poczatku', *years_possible], index=len(years_possible), key='voucher_timerange')

        with st.expander("Średnia kroczaca"):
            show_moving_average = st.checkbox('Pokazuj', key="total_show_moving_average", value=True, on_change=lambda: utils.chain_toggle_off("total_show_moving_average", "total_show_moving_average_only"))
            show_moving_average_only = st.checkbox('Pokazuj tylko srednia kroczaca', key="total_show_moving_average_only", value=False, on_change=lambda: utils.chain_toggle_on("total_show_moving_average_only", "total_show_moving_average"))
            moving_average_days = st.slider('Ile dni', 1, 30, 7, key="voucher_slider")

        with st.expander("Filtry", expanded=True):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df_voucher['location'].unique(), default=df_voucher['location'].unique(), key="voucher_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="voucher_sep")

    cities = df_voucher['street'][df_voucher['location'].isin(cities)].unique()
    df_voucher = df_voucher[df_voucher['street'].isin(cities)]

    current_ts = pd.Timestamp(datetime.now())

    if time_range == 'Od poczatku':
        start = pd.to_datetime(df_voucher['min_creation_date']).dt.tz_localize(None).min()
        end = datetime(year=current_ts.year, month=current_ts.month, day=current_ts.day, hour=0, minute=0)

    else:
        start = datetime(year=time_range, month=1, day=1, hour=0, minute=0)
        end = datetime(year=time_range, month=12, day=31, hour=23, minute=59)

    if time_range == current_ts.year:
        end = datetime(year=time_range, month=current_ts.month, day=current_ts.day, hour=0, minute=0)


    return (separate_cities, show_moving_average_only, moving_average_days, show_moving_average, group_dates_by, start, end, cities)
