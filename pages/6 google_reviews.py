import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd

import queries
import utils
import google_reviews_sidebar
import google_reviews_utils


st.set_page_config(layout="wide")
# tooltip didnt work on fullscreen without this hack
st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>', unsafe_allow_html=True)

with st.spinner():
  df = queries.get_reviews()

# side bar
# (df, seperate_locations) = google_reviews_sidebar.filter_data(df)
(df) = google_reviews_sidebar.filter_data(df)

# groupBy = 'location' if seperate_locations else None

tab1, tab2 = st.tabs(["Grupowanie miesięczne", "Grupowanie dniami"])

# df_grouped['sort_time'] = df_grouped['create_time'].dt.to_timestamp()

with tab1:
  (df_grouped) = google_reviews_utils.group_data(df, 'M')
  df_grouped['create_time'] = df_grouped['create_time'].dt.to_timestamp().dt.strftime('%m/%Y')

  if len(df_grouped) > 1:
    df_grouped = df_grouped.iloc[1:]  # cut first row cause its not fully filled

  reviews_chart = utils.create_bar_chart(df_grouped, 'create_time', "Data", 'ratings_count', "Liczba ocen", None)
  st.altair_chart(reviews_chart, use_container_width=True)
with tab2:
  (df_grouped) = google_reviews_utils.group_data(df, 'D')
  df_grouped['create_time'] = df_grouped['create_time'].dt.to_timestamp().dt.strftime('%d/%m/%Y')

  if len(df_grouped) > 1:
    df_grouped = df_grouped.iloc[1:]  # cut first row cause its not fully filled

  reviews_chart = utils.create_bar_chart(df_grouped, 'create_time', "Data", 'ratings_count', "Liczba ocen", None)
  st.altair_chart(reviews_chart, use_container_width=True)

# reservations_chart = utils.create_chart_new(df_grouped, 'create_time', "Data", 'ratings_count', None, "Liczba ocen", None, 4, "")
# st.plotly_chart(reservations_chart, use_container_width=True)

# if moving_average_toggle:
#   df_grouped['reservations_ma'] = pd.concat(reservations_rolling_averages)
#   df_grouped['total_cost_ma'] = pd.concat(total_cost_rolling_averages)
#   df_grouped['total_people_ma'] = pd.concat(total_people_rolling_averages)

# df_grouped[x_axis_type] = df_grouped[x_axis_type].dt.to_timestamp()

# if chart_type == "new":
#   reservations_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", 'reservations' if not show_only_moving_average else None, 'reservations_ma' if moving_average_toggle else None, "Liczba rezerwacji", groupBy, 2 if groupBy else 4, "Średnia")
#   st.plotly_chart(reservations_chart, use_container_width=True)
# else:
#   st.text("Liczba rezerwacji")
#   reservations_chart = utils.create_chart(df_grouped, x_axis_type, "Data", 'reservations' if not show_only_moving_average else None, 'reservations_ma' if moving_average_toggle else None, "Liczba rezerwacji", groupBy, 2 if groupBy else 4, "month")
#   st.altair_chart(reservations_chart, use_container_width=True)

# if chart_type == "new":
#   cost_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", 'total_cost' if not show_only_moving_average else None, 'total_cost_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "Średnia")
#   st.plotly_chart(cost_chart, use_container_width=True)
# else:
#   st.text("Przychód (PLN)")
#   cost_chart = utils.create_chart(df_grouped, x_axis_type, "Data", 'total_cost' if not show_only_moving_average else None, 'total_cost_ma' if moving_average_toggle else None, "Przychód (PLN)", groupBy, 2 if groupBy else 4, "month")
#   st.altair_chart(cost_chart, use_container_width=True)

# if chart_type == "new":
#   people_chart = utils.create_chart_new(df_grouped, x_axis_type, "Data", 'total_people' if not show_only_moving_average else None, 'total_people_ma' if moving_average_toggle else None, "Liczba osób", groupBy, 2 if groupBy else 4, "Średnia")
#   st.plotly_chart(people_chart, use_container_width=True)
# else:
#   st.text("Liczba osób")
#   people_chart = utils.create_chart(df_grouped, x_axis_type, "Data", 'total_people' if not show_only_moving_average else None, 'total_people_ma' if moving_average_toggle else None, "Liczba osób", groupBy, 2 if groupBy else 4, "month")
#   st.altair_chart(people_chart, use_container_width=True)
