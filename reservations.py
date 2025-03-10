import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd

import queries
import utils
import reservations_sidebar
import reservations_utils

st.set_page_config(layout="wide")
st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>', unsafe_allow_html=True)

def determine_status(row):
    if row['is_cancelled']:
        return 'Anulowane'
    elif not row['is_payed']:
        return 'Zrealizowane nieopłacone'
    return 'Zrealizowane'

with st.spinner():
    df = queries.get_reservation_data()
    df_notes = queries.get_notes()

(df, x_axis_type, moving_average_toggle,
 show_only_moving_average, moving_average_days,
 seperate_cities, show_notes, seperate_attractions, seperate_status,
 seperate_visit_types) = reservations_sidebar.filter_data(df)

groupBy = 'city' if seperate_cities else 'attraction_group' if seperate_attractions else 'status' if seperate_status else 'visit_type' if seperate_visit_types else None

(df_grouped, reservations_rolling_averages, total_cost_rolling_averages,
 total_people_rolling_averages) = reservations_utils.group_data_and_calculate_moving_average(df, df_notes, x_axis_type, moving_average_days, groupBy)

if moving_average_toggle:
    df_grouped['reservations_ma'] = pd.concat(reservations_rolling_averages)
    df_grouped['total_cost_ma'] = pd.concat(total_cost_rolling_averages)
    df_grouped['total_people_ma'] = pd.concat(total_people_rolling_averages)

df_grouped[x_axis_type] = df_grouped[x_axis_type].dt.to_timestamp()
reservations_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", 'reservations' if not show_only_moving_average else None, 'reservations_ma' if moving_average_toggle else None, "Liczba rezerwacji", groupBy, 2 if groupBy else 4, "Średnia",show_notes)
st.plotly_chart(reservations_chart, use_container_width=True)

cost_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", 'total_cost' if not show_only_moving_average else None, 'total_cost_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "Średnia", show_notes)
st.plotly_chart(cost_chart, use_container_width=True)

people_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", 'total_people' if not show_only_moving_average else None, 'total_people_ma' if moving_average_toggle else None, "Liczba osób", groupBy, 2 if groupBy else 4, "Średnia", show_notes)
st.plotly_chart(people_chart, use_container_width=True)
