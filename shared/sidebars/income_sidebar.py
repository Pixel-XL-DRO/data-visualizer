import sys
sys.path.append("shared")

from datetime import datetime, timedelta
import utils
import streamlit as st
import pandas as pd
import numpy as np

def ensure_status():
    if st.session_state.online_ms1[0] == "Wszystkie":
        st.session_state.online_ms1 = st.session_state.online_ms1[1:]
    elif st.session_state.online_ms1[-1] == "Wszystkie":
        st.session_state.online_ms1 = ["Wszystkie"]

def filter_online_data(df):
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
            t1 = st.checkbox('Pokazuj', key="online_t1", value=True, on_change=lambda: utils.chain_toggle_off("online_t1", "online_t2"))
            t2 = st.checkbox('Pokazuj tylko srednia kroczaca', key="online_t2", value=False, on_change=lambda: utils.chain_toggle_on("online_t2", "online_t1"))
            days_count = st.slider('Ile dni', 1, 30, 7)

        with st.expander("Filtry", expanded=True):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df['location'].unique(), default=df['location'].unique(), key="online_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="online_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_sep"))

            with st.container(border=True):
                languages = st.multiselect('Język klienta', df['language'].unique(), default=df['language'].unique(), key="online_lang")

            with st.container(border=True):
                attractions = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique(), key="online_attr")
                separate_attractions = st.checkbox('Rozdziel atrakcje', key="online_attr_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_attr_sep"))

            with st.container(border=True):
                status = st.multiselect("Status", ["Zrealizowane", "Anulowane", "Zrealizowane nieopłacone"], default=["Zrealizowane", "Zrealizowane nieopłacone"], key="online_status")
                separate_status = st.checkbox('Rozdziel status', key="online_status_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_status_sep"))

            with st.container(border=True):
                ms1 = st.multiselect('Typy wizyty', np.concatenate([df['visit_type'].unique(), np.array(["Wszystkie"])]), default="Wszystkie", on_change=ensure_status, key="online_ms1")
                separate_visit_types = st.checkbox('Rozdziel typy wizyty', key="online_visit_sep", on_change=lambda: utils.make_sure_only_one_toggle_is_on(["online_sep", "online_attr_sep", "online_status_sep", "online_visit_sep"], "online_visit_sep"))

            group_by = 'street' if separate_cities else 'attraction_group' if separate_attractions else 'status' if separate_status else 'visit_type' if separate_visit_types else None

            cities = df['street'][df['location'].isin(cities)].unique()

    def determine_status(row):
        if row['is_cancelled']:
            return 'Anulowane'
        elif not row['is_payed']:
            return 'Zrealizowane nieopłacone'
        else:
            return 'Zrealizowane'

    df['status'] = df.apply(determine_status, axis=1)
    df = df[df['status'].isin(status)]
    df = df[df['street'].isin(cities)]
    df = df[df['language'].isin(languages)]
    df = df[df['attraction_group'].isin(attractions)]
    if "Wszystkie" not in ms1:
        df = df[df['visit_type'].isin(ms1)]

    if time_range == 'Od poczatku':
        start = pd.to_datetime(df[date_type]).dt.tz_localize(None).min()
        end = pd.to_datetime(df[date_type]).dt.tz_localize(None).max()
    else:
        year = time_range
        localized_dates = pd.to_datetime(df[date_type]).dt.tz_localize(None)
        year_mask = localized_dates.dt.year == year
        start = localized_dates[year_mask].min()
        end = localized_dates[year_mask].max()

    return df, group_by, t2, days_count, t1, group_dates_by, start, end, date_type


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
            t1 = st.checkbox('Pokazuj', key="pos_t1", value=True, on_change=lambda: utils.chain_toggle_off("pos_t1", "pos_t2"))
            t2 = st.checkbox('Pokazuj tylko srednia kroczaca', key="pos_t2", value=False, on_change=lambda: utils.chain_toggle_on("pos_t2", "pos_t1"))
            days_count = st.slider('Ile dni', 1, 30, 7, key="pos_slider")

        with st.expander("Filtry"):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df_dotypos['location'].unique(), default=df_dotypos['location'].unique(), key="pos_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="pos_sep")


    cities = df_dotypos['street'][df_dotypos['location'].isin(cities)].unique()
    df_dotypos = df_dotypos[df_dotypos['street'].isin(cities)]
    if time_range == 'Od poczatku':
        start = pd.to_datetime(df_dotypos['creation_date']).dt.tz_localize(None).min()
        end = pd.to_datetime(df_dotypos['creation_date']).dt.tz_localize(None).max()
    else:
        year = time_range
        localized_dates = pd.to_datetime(df_dotypos['creation_date']).dt.tz_localize(None)
        year_mask = localized_dates.dt.year == year
        start = localized_dates[year_mask].min()
        end = localized_dates[year_mask].max()

    return df_dotypos, separate_cities, t2, days_count, t1, group_dates_by, start, end


def filter_total_data(df, df_dotypos):
    years_possible = list(range(2022, datetime.now().year + 1))

    df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])

    with st.sidebar:
        group_dates_by = st.selectbox('Wybierz grupowanie po dacie', ['Godzina', 'Dzień tygodnia', 'Tydzien roku', 'Dzień miesiaca', 'Miesiac', 'Rok'],index=1, key='total_grouping')

        with st.container(border=True):
            time_range = st.selectbox('Pokazuj ', ['Od poczatku', *years_possible], index=len(years_possible), key='total_timerange')

        with st.expander("Średnia kroczaca"):
            t1 = st.checkbox('Pokazuj', key="total_t1", value=True, on_change=lambda: utils.chain_toggle_off("total_t1", "total_t2"))
            t2 = st.checkbox('Pokazuj tylko srednia kroczaca', key="total_t2", value=False, on_change=lambda: utils.chain_toggle_on("total_t2", "total_t1"))
            days_count = st.slider('Ile dni', 1, 30, 7, key="total_slider")

        with st.expander("Filtry"):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df['location'].unique(), default=df['location'].unique(), key="total_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="total_sep")
            with st.container(border=True):
                status = st.multiselect("Status", ["Zrealizowane", "Anulowane", "Zrealizowane nieopłacone"], default=["Zrealizowane", "Zrealizowane nieopłacone"], key="total_status")


    def determine_status(row):
        if row['is_cancelled']:
            return 'Anulowane'
        elif not row['is_payed']:
            return 'Zrealizowane nieopłacone'
        else:
            return 'Zrealizowane'

    cities = df['street'][df['location'].isin(cities)].unique()

    df['status'] = df.apply(determine_status, axis=1)
    df = df[df['status'].isin(status)]
    df = df[df['street'].isin(cities)]

    df['booked_date'] = pd.to_datetime(df['booked_date']).dt.tz_localize(None)
    # df = df[(df['booked_date'] >= start) & (df['booked_date'] <= end)]

    df_dotypos['creation_date'] = pd.to_datetime(df_dotypos['creation_date']).dt.tz_localize(None)
    # df_dotypos = df_dotypos[(df_dotypos['creation_date'] >= start) & (df_dotypos['creation_date'] <= end)]
    df_dotypos = df_dotypos[df_dotypos['street'].isin(cities)]

    if time_range == 'Od poczatku':
        start = pd.to_datetime(df['booked_date']).dt.tz_localize(None).min()
        end = pd.to_datetime(df['booked_date']).dt.tz_localize(None).max()
    else:
        year = time_range
        localized_dates = pd.to_datetime(df['booked_date']).dt.tz_localize(None)
        year_mask = localized_dates.dt.year == year
        start = localized_dates[year_mask].min()
        end = localized_dates[year_mask].max()
    return df, df_dotypos, separate_cities, t2, days_count, t1, group_dates_by, start, end

def filter_voucher_data(df_voucher):
    years_possible = list(range(2023, datetime.now().year + 1))

    df_voucher['location'] = df_voucher['street'].map(utils.street_to_location).fillna(df_voucher['street'])

    with st.sidebar:
        group_dates_by = st.selectbox('Wybierz grupowanie po dacie', [ 'Dzień tygodnia', 'Tydzien roku', 'Dzień miesiaca', 'Miesiac', 'Rok'],index=1, key='voucher_grouping')

        with st.container(border=True):
            time_range = st.selectbox('Pokazuj ', ['Od poczatku', *years_possible], index=len(years_possible), key='voucher_timerange')

        with st.expander("Średnia kroczaca"):
            t1 = st.checkbox('Pokazuj', key="total_t1", value=True, on_change=lambda: utils.chain_toggle_off("total_t1", "total_t2"))
            t2 = st.checkbox('Pokazuj tylko srednia kroczaca', key="total_t2", value=False, on_change=lambda: utils.chain_toggle_on("total_t2", "total_t1"))
            days_count = st.slider('Ile dni', 1, 30, 7, key="voucher_slider")

        with st.expander("Filtry"):
            with st.container(border=True):
                cities = st.multiselect("Miasta", df_voucher['location'].unique(), default=df_voucher['location'].unique(), key="voucher_cities")
                separate_cities = st.checkbox('Rozdziel miasta', key="voucher_sep")

    cities = df_voucher['street'][df_voucher['location'].isin(cities)].unique()
    df_voucher = df_voucher[df_voucher['street'].isin(cities)]

    if time_range == 'Od poczatku':
        start = pd.to_datetime(df_voucher['creation_date']).dt.tz_localize(None).min()
        end = pd.to_datetime(df_voucher['creation_date']).dt.tz_localize(None).max()
    else:
        year = time_range
        localized_dates = pd.to_datetime(df_voucher['creation_date']).dt.tz_localize(None)
        year_mask = localized_dates.dt.year == year
        start = localized_dates[year_mask].min()
        end = localized_dates[year_mask].max()

    return df_voucher, separate_cities, t2, days_count, t1, group_dates_by, start, end