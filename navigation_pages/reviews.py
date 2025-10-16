import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd

import queries
import utils
import performance_reviews_sidebar
import performance_reviews_queries
import auth

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_nps_initial_data()

(start_date, end_date, location_checkboxes, separate_cities, moving_average_toggle, show_only_moving_average, moving_average_days,metric_change_days, metric_display_percent, display_reviews_above) = performance_reviews_sidebar.filter_data(df)

groupBy = 'city' if separate_cities else None

with st.spinner("Ładowanie danych...", show_time=True):

  df_reviews_count, df_reviews, df_nps_cum, df_nps = utils.run_in_parallel(
      (performance_reviews_queries.get_reviews_count, (start_date, end_date, location_checkboxes)),
      (performance_reviews_queries.get_performance_reviews, (start_date, end_date, location_checkboxes, display_reviews_above)),
      (performance_reviews_queries.get_cumulative_NPS, (start_date, end_date, location_checkboxes, groupBy)),
      (performance_reviews_queries.get_NPS, (start_date, end_date, location_checkboxes, moving_average_days, groupBy))
  )
  nps_score, delta_nps, count, delta_count = performance_reviews_queries.get_nps_metric(metric_change_days, metric_display_percent, location_checkboxes)
  df_all_reviews = performance_reviews_queries.get_percent_of_evaluated_reviews(start_date, end_date, location_checkboxes, metric_change_days)

nps_metric, count_metric, reviews_metric = st.columns(3)

with nps_metric:
  nps_metric_help = f"Aktualna ocena NPS, zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"
  st.metric(label="NPS", value=nps_score ,delta=f"{delta_nps}%" if metric_display_percent else delta_nps, help=nps_metric_help)

with count_metric:
  nps_metric_help = f"Aktualna liczba ocen, zmiana dotyczy aktualnej liczby ocen względem liczby ocen {metric_change_days} dni wstecz"
  st.metric(label="Liczba ocen", value=count ,delta=f"{delta_count}%" if metric_display_percent else delta_count, help=nps_metric_help)

with reviews_metric:

  df_nps_cum_rev = df_nps_cum.sort_values('date', ascending=False).reset_index(drop=True)
  
  df_nps_cum_rev['review_metric'] = (df_nps_cum_rev['count_cumsum'] / df_all_reviews['count_cumsum']) * 100
  
  if metric_display_percent:
    delta_review = round(((df_nps_cum_rev['review_metric'].iloc[0] - df_nps_cum_rev['review_metric'].iloc[metric_change_days]) / df_nps_cum_rev['review_metric'].iloc[metric_change_days]) * 100, 2)
  else: 
    delta_review = round(df_nps_cum_rev['review_metric'].iloc[0] - df_nps_cum_rev['review_metric'].iloc[metric_change_days], 2)

  review_metric_help = f"Aktualna procent ocenionych wizyt, które zostały ocenione. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"
  st.metric(label="Procent ocenionych wizyt", value=round(df_nps_cum_rev['review_metric'].iloc[0], 2) ,delta=f"{delta_review}%" if metric_display_percent else delta_review, help=review_metric_help)



tab1, tab2, tab3 = st.tabs(["Kumulatywne", "Normalne", "Miesieczne"])


with tab1:

  performance_score_chart = utils.create_chart_new(df_nps_cum, 'date', "Data", None, 'nps_cumsum' , "Kumulatywna średnia ocena NPS", groupBy, 2 if groupBy else 4, "Średnia", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

  performance_score_chart = utils.create_chart_new(df_nps_cum, 'date', "Data", None, 'count_cumsum' , "Kumulatywna liczba ocen", groupBy, 2 if groupBy else 4, "Liczba ocen", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

with tab2:

  performance_score_chart = utils.create_chart_new(df_nps, 'date', "Data", 'nps' if not show_only_moving_average else None, 'nps_ma' if moving_average_toggle else None, "Ocena NPS", groupBy, 2 if groupBy else 4, "Średnia", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

  performance_score_chart = utils.create_chart_new(df_nps, 'date', "Data", None, 'count' , "Liczba ocen", groupBy, 2 if groupBy else 4, "Liczba ocen", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

with tab3:

  @st.fragment
  def tab_three():

    city = st.selectbox('Wybierz miasto', df['city'].unique(), index=0)
    year = st.selectbox('Wybierz rok', df['date'].dt.year.unique(), index=0)

    with st.spinner("Ładowanie danych...", show_time=True):
      df_monthly = performance_reviews_queries.get_monthly_nps(city, year)

    st.text("Ocena NPS w miesiacu w danym mieście")
    performance_bar_chart = utils.create_bar_chart(df_monthly, 'Miesiac', 'Miesiac', 'NPS', 'Wynik NPS', None, None, None, None, True)
    st.altair_chart(performance_bar_chart, use_container_width=True)

  tab_three()


st.divider()
st.subheader("Liczba ocen")

score_count_chart = utils.create_bar_chart(df_reviews_count, 'score', 'Ocena', 'count', 'Ilość', None, None, None, 10)
st.altair_chart(score_count_chart, use_container_width=True)

st.divider()
st.subheader("Opinie")

st.table(df_reviews)