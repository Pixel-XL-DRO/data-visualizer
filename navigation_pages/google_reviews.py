import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd

import queries
import utils
import google_reviews_sidebar
import google_reviews_queries

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_reviews_initial_data()

# side bar
(start_date, end_date, rating_to_show, location_checkboxes) = google_reviews_sidebar.filter_data(df)

with st.spinner("Ładowanie danych...", show_time=True):

  df_monthly, df_daily = utils.run_in_parallel(
    (google_reviews_queries.get_google_reviews_monthly, (start_date, end_date, rating_to_show, location_checkboxes)),
    (google_reviews_queries.get_google_reviews_daily, (start_date, end_date, rating_to_show, location_checkboxes))
  )

tab1, tab2 = st.tabs(["Grupowanie miesięczne", "Grupowanie dniami"])

with tab1:

  reviews_chart = utils.create_bar_chart(df_monthly, 'month_year', "Data", 'count', "Liczba ocen", None)
  st.altair_chart(reviews_chart, use_container_width=True)

with tab2:

  reviews_chart = utils.create_bar_chart(df_daily, 'day', "Data", 'count', "Liczba ocen", None)
  st.altair_chart(reviews_chart, use_container_width=True)

utils.lazy_load_initials()