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

(start_date, end_date, location_checkboxes, separate_cities, moving_average_toggle, show_only_moving_average, moving_average_days,metric_change_days, metric_display_percent) = performance_reviews_sidebar.filter_data(df)

groupBy = 'city' if separate_cities else None

with st.spinner("Ładowanie danych...", show_time=True):

  df_reviews_count, df_reviews, df_nps_cum, df_nps, df_all_reviews, df_metrics_by_city = utils.run_in_parallel(
      (performance_reviews_queries.get_reviews_count, (start_date, end_date, location_checkboxes)),
      (performance_reviews_queries.get_performance_reviews, (start_date, end_date, location_checkboxes)),
      (performance_reviews_queries.get_cumulative_NPS, (start_date, end_date, location_checkboxes, groupBy)),
      (performance_reviews_queries.get_NPS, (start_date, end_date, location_checkboxes, moving_average_days, groupBy)),
      (performance_reviews_queries.get_percent_of_evaluated_reviews, (start_date, location_checkboxes, metric_change_days)),
      (performance_reviews_queries.get_nps_metric_by_city, (metric_change_days, metric_display_percent, location_checkboxes, start_date))
  )
  nps_score, delta_nps, count, delta_count, review_percent, review_percent_delta = performance_reviews_queries.get_nps_metric(metric_change_days, metric_display_percent, location_checkboxes)

nps_metric, count_metric, reviews_metric = st.columns(3)

with nps_metric:
  nps_metric_help = f"Aktualna ocena NPS, zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"
  st.metric(label="NPS", value=nps_score ,delta=f"{delta_nps}%" if metric_display_percent else delta_nps, help=nps_metric_help)

with count_metric:
  nps_metric_help = f"Aktualna liczba ocen, zmiana dotyczy aktualnej liczby ocen względem liczby ocen {metric_change_days} dni wstecz"
  st.metric(label="Liczba ocen", value=count ,delta=f"{delta_count}%" if metric_display_percent else delta_count, help=nps_metric_help)

with reviews_metric:

  review_metric_help = f"Aktualna procent ocenionych wizyt, które zostały ocenione. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"
  st.metric(label="Procent ocenionych wizyt", value=review_percent ,delta=f"{review_percent_delta}%" if metric_display_percent else review_percent_delta, help=review_metric_help)

tab1, tab2, tab3, tab4 = st.tabs(["Metryki", "Kumulatywne", "Normalne", "Miesieczne"])

with tab1:

  @st.fragment
  def tab_one():

    with st.container(border=True):

      with st.container(horizontal=True, horizontal_alignment="center"):

        sort_by = st.segmented_control("Sortuj dane po", options=["NPS", "Liczba ocen", "Procent ocenionych wizyt"])
        ascending = st.segmented_control("jak",options=["Rosnąco", "Malejąco"], label_visibility="hidden")

      if sort_by:
        sort_by = {"NPS": "nps_cumsum", "Liczba ocen": "count_cumsum", "Procent ocenionych wizyt": "review_percent"}[sort_by]
        df_metrics_by_city.sort_values(by=sort_by, ascending= True if ascending == "Rosnąco" else False, inplace=True)

      nps_cities, count_cities, reviewed_cities = st.columns(3)

      with nps_cities:

        for city in df_metrics_by_city['city'].unique():

          city_condition = df_metrics_by_city['city'] == city

          nps_metric_city_help = f"Aktualna ocena NPS w mieście {city}. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"
          st.metric(label=f"{city.upper()} NPS ", value=round(df_metrics_by_city[city_condition]['nps_cumsum'].iloc[0], 4) ,delta=f"{df_metrics_by_city[city_condition]['nps_change'].iloc[0]}%" if metric_display_percent else df_metrics_by_city[city_condition]['nps_change'].iloc[0], help=nps_metric_city_help, delta_color="off" if df_metrics_by_city[city_condition]['nps_change'].iloc[0] == 0 else "normal")

      with count_cities:

        for city in df_metrics_by_city['city'].unique():

          city_condition = df_metrics_by_city['city'] == city

          nps_metric_city_help = f"Aktualna liczba ocen w mieście {city}. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"

          st.metric(label=f"{city.upper()} LICZBA OCEN", value=int(df_metrics_by_city[city_condition]['count_cumsum'].iloc[0]) ,delta=f"{df_metrics_by_city[city_condition]['count_change'].iloc[0]}%" if metric_display_percent else int(df_metrics_by_city[city_condition]['count_change'].iloc[0]), help=nps_metric_city_help, delta_color="off" if df_metrics_by_city[city_condition]['count_change'].iloc[0] == 0 else "normal")

      with reviewed_cities:

        for city in df_metrics_by_city['city'].unique():

          city_condition = df_metrics_by_city['city'] == city

          nps_metric_city_help = f"Aktualna procent ocenionych wizyt w mieście {city}. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"

          st.metric(label=f"{city.upper()} PROCENT OCENIONYCH WIZYT", value=round(df_metrics_by_city[city_condition]['review_percent'].iloc[0], 4) ,delta=f"{df_metrics_by_city[city_condition]['review_percent_change'].iloc[0]}%" if metric_display_percent else df_metrics_by_city[city_condition]['review_percent_change'].iloc[0], help=nps_metric_city_help, delta_color="off" if df_metrics_by_city[city_condition]['review_percent_change'].iloc[0] == 0 else "normal")

  tab_one()

with tab2:

  performance_score_chart = utils.create_chart_new(df_nps_cum, 'date', "Data", None, 'nps_cumsum' , "Kumulatywna średnia ocena NPS", groupBy, 2 if groupBy else 4, "Średnia", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

  performance_score_chart = utils.create_chart_new(df_nps_cum, 'date', "Data", None, 'count_cumsum' , "Kumulatywna liczba ocen", groupBy, 2 if groupBy else 4, "Liczba ocen", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

with tab3:

  performance_score_chart = utils.create_chart_new(df_nps, 'date', "Data", 'nps' if not show_only_moving_average else None, 'nps_ma' if moving_average_toggle else None, "Ocena NPS", groupBy, 2 if groupBy else 4, "Średnia", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

  performance_score_chart = utils.create_chart_new(df_nps, 'date', "Data", None, 'count' , "Liczba ocen", groupBy, 2 if groupBy else 4, "Liczba ocen", False)
  st.plotly_chart(performance_score_chart, use_container_width=True)

with tab4:

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

@st.fragment
def reviews_table():

  start_score, end_score = st.slider(
    "Wybierz zakres ocen",
    min_value=0,
    max_value=10,
    value=(0, 8),
    step=1,
    width=200
  )

  df_reviews_filtered = df_reviews[
      df_reviews.apply(lambda x: start_score <= x['Ocena'] <= end_score, axis=1)
  ]
  st.table(df_reviews_filtered)

reviews_table()