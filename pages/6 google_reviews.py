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
(df) = google_reviews_sidebar.filter_data(df)

tab1, tab2 = st.tabs(["Grupowanie miesięczne", "Grupowanie dniami"])

with tab1:
  (df_grouped) = google_reviews_utils.group_data(df, 'M')
  df_grouped['create_time'] = df_grouped['create_time'].dt.to_timestamp().dt.strftime('%m/%Y')

  reviews_chart = utils.create_bar_chart(df_grouped, 'create_time', "Data", 'ratings_count', "Liczba ocen", None)
  st.altair_chart(reviews_chart, use_container_width=True)
with tab2:
  (df_grouped) = google_reviews_utils.group_data(df, 'D')
  df_grouped['create_time'] = df_grouped['create_time'].dt.to_timestamp().dt.strftime('%d/%m/%Y')

  reviews_chart = utils.create_bar_chart(df_grouped, 'create_time', "Data", 'ratings_count', "Liczba ocen", None)
  st.altair_chart(reviews_chart, use_container_width=True)
