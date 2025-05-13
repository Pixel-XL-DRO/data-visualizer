import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd

import queries
import utils
import performance_reviews_sidebar
import performance_reviews_utils

st.set_page_config(layout="wide")
# tooltip didnt work on fullscreen without this hack
st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>', unsafe_allow_html=True)

with st.spinner():
  df = queries.get_performance_reviews()

df_unique = df.drop_duplicates(subset=['reservationId'], keep='first')

(df_unique, separete_cities, moving_average_toggle, show_only_moving_average, moving_average_days) = performance_reviews_sidebar.filter_data(df_unique)

groupBy = 'city' if separete_cities else None

(df_grouped, nps_rolling_averages) = performance_reviews_utils.group_data_and_calculate_nps(df_unique, groupBy, moving_average_days)

if moving_average_toggle:
  df_grouped['nps_ma'] = pd.concat(nps_rolling_averages)

df_grouped['date'] = df_grouped['date'].dt.to_timestamp().dt.strftime('%d/%m/%Y')

tab1, tab2 = st.tabs(["Kumulatywne", "Normalne"])

with tab1:

  performance_score_chart = utils.create_chart_new(df_grouped, df_grouped['date'], "Data", None, 'nps_cum', "Kumulujaca się średnia NPS", groupBy, 2 if groupBy else 4, "Średnia NPS", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

  performance_score_chart = utils.create_chart_new(df_grouped, df_grouped['date'], "Data", None, 'score_cum', "Kumulujaca się liczba ocen", groupBy, 2 if groupBy else 4, "Liczba ocen", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

with tab2:

  performance_score_chart = utils.create_chart_new(df_grouped, df_grouped['date'], "Data", 'nps' if not show_only_moving_average else None, 'nps_ma' if moving_average_toggle else None, "Ocena NPS", groupBy, 2 if groupBy else 4, "Średnia", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

  performance_score_chart = utils.create_chart_new(df_grouped, df_grouped['date'], "Data", None, 'score_count' , "Liczba ocen", groupBy, 2 if groupBy else 4, "Liczba ocen", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

st.divider()
st.subheader("Opinie")

df_unique['feedback'] = df_unique['feedback'].apply(lambda x: '' if pd.isna(x) else x)
st.table(df_unique[['date', 'city', 'score', 'feedback']])
