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

with st.spinner("Inicjalizacja...", show_time=True):
  df = queries.get_nps_initial_data()

(start_date, end_date, location_checkboxes, separate_cities, moving_average_toggle, show_only_moving_average, moving_average_days) = performance_reviews_sidebar.filter_data(df)

groupBy = 'street' if separate_cities else None

with st.spinner("Ładowanie danych...", show_time=True):

  df_reviews_count, df_reviews, df_nps_cum, df_nps = utils.run_in_parallel(
      (performance_reviews_queries.get_reviews_count, (start_date, end_date, location_checkboxes)),
      (performance_reviews_queries.get_performance_reviews, (start_date, end_date, location_checkboxes)),
      (performance_reviews_queries.get_cumulative_NPS, (start_date, end_date, location_checkboxes, groupBy)),
      (performance_reviews_queries.get_NPS, (start_date, end_date, location_checkboxes, moving_average_days, groupBy)),
  )

tab1, tab2, tab3, tab4 = st.tabs(["Metryki", "Kumulatywne", "Normalne", "Miesieczne"])

with tab1:

  @st.fragment()
  def tab_one():

    if 'metric_change_days' not in st.session_state:
      st.session_state.metric_change_days = 1
    if 'metric_display_percent' not in st.session_state:
      st.session_state.metric_display_percent = False

    metric_change_days = st.session_state["metric_change_days"]
    metric_display_percent = st.session_state["metric_display_percent"]

    with st.spinner("Ładowanie danych...", show_time=True):
      (nps_score, delta_nps, count, delta_count, review_percent, review_percent_delta), df_metrics_by_city = utils.run_in_parallel(
        (performance_reviews_queries.get_nps_metric, (metric_change_days, metric_display_percent, location_checkboxes)),
        (performance_reviews_queries.get_nps_metric_by_city, (metric_change_days, metric_display_percent, location_checkboxes, start_date))
      )

    with st.container(border=True):
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

    with st.container(border=True):

      with st.container(border=None, horizontal_alignment="center", vertical_alignment="center", horizontal=True):
        metric_change_days = st.slider('Zmiana względem sprzed (dni)', min_value=1, max_value=60, value=1, step=1, key="metric_change_days", width=200)
        metric_display_percent = st.checkbox('Pokazuj zmianę jako procent', value=False, key="metric_display_percent")

      @st.fragment
      def cities_view():
        with st.container(horizontal=True, horizontal_alignment="center"):

          sort_by = st.segmented_control(
            "Sortuj dane po",
            options=["NPS", "Liczba ocen", "Procent ocenionych wizyt"],
            key="sort_by",
            default="NPS",
            on_change=lambda: st.session_state.update(
                {"sort_by": st.session_state.get("sort_by") or "NPS"}
            )
        )

          ascending = st.segmented_control(
            "jak",
            options=["Malejąco","Rosnąco"],
            label_visibility="hidden",
            key="ascending",
            default="Malejąco",
            on_change=lambda: st.session_state.update(
                {"ascending": st.session_state.get("ascending") or "Malejąco"}
            )
        )

        sort_by = {"NPS": "nps_cumsum", "Liczba ocen": "count_cumsum", "Procent ocenionych wizyt": "review_percent"}[sort_by]
        df_metrics_by_city.sort_values(by=sort_by, ascending= True if ascending == "Rosnąco" else False, inplace=True)

        nps_cities, count_cities, reviewed_cities = st.columns(3)

        with nps_cities:

          st.write("### NPS")

          for city in df_metrics_by_city['street'].unique():

            city_condition = df_metrics_by_city['street'] == city

            nps_metric_city_help = f"Aktualna ocena NPS w mieście {city}. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"
            st.metric(label=f"{city.upper()} NPS ", value=round(df_metrics_by_city[city_condition]['nps_cumsum'].iloc[0], 2) ,delta=f"{df_metrics_by_city[city_condition]['nps_change'].iloc[0]}%" if metric_display_percent else df_metrics_by_city[city_condition]['nps_change'].iloc[0], help=nps_metric_city_help, delta_color="off" if df_metrics_by_city[city_condition]['nps_change'].iloc[0] == 0 else "normal")

        with count_cities:

          st.write("### Liczba ocen")

          for city in df_metrics_by_city['street'].unique():

            city_condition = df_metrics_by_city['street'] == city

            nps_metric_city_help = f"Aktualna liczba ocen w mieście {city}. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"

            st.metric(label=f"{city.upper()} LICZBA OCEN", value=int(df_metrics_by_city[city_condition]['count_cumsum'].iloc[0]) ,delta=f"{df_metrics_by_city[city_condition]['count_change'].iloc[0]}%" if metric_display_percent else int(df_metrics_by_city[city_condition]['count_change'].iloc[0]), help=nps_metric_city_help, delta_color="off" if df_metrics_by_city[city_condition]['count_change'].iloc[0] == 0 else "normal")

        with reviewed_cities:

          st.write("### Procent ocenionych wizyt")

          for city in df_metrics_by_city['street'].unique():

            city_condition = df_metrics_by_city['street'] == city

            nps_metric_city_help = f"Aktualny procent ocenionych wizyt w mieście {city}. Zmiana dotyczy aktualnej oceny względem oceny {metric_change_days} dni wstecz"

            st.metric(label=f"{city.upper()} PROCENT OCENIONYCH WIZYT", value=round(df_metrics_by_city[city_condition]['review_percent'].iloc[0], 2) ,delta=f"{df_metrics_by_city[city_condition]['review_percent_change'].iloc[0]}%" if metric_display_percent else df_metrics_by_city[city_condition]['review_percent_change'].iloc[0], help=nps_metric_city_help, delta_color="off" if df_metrics_by_city[city_condition]['review_percent_change'].iloc[0] == 0 else "normal")
      cities_view()
                    # kinda cursed
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

    df['location'] = df['street'].map(utils.street_to_location).fillna(df['street'])

    city = st.selectbox('Wybierz miasto', df['location'].unique(), index=0)
    year = st.selectbox('Wybierz rok', df['date'].dt.year.unique(), index=0)

    street = df['street'][df['location'] == city].iloc[0]

    with st.spinner("Ładowanie danych...", show_time=True):
      df_monthly = performance_reviews_queries.get_monthly_nps(street, year)

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
    value=(0, 10),
    step=1,
    width=200
  )

  df_reviews_filtered = df_reviews[
      df_reviews.apply(lambda x: start_score <= x['Ocena'] <= end_score, axis=1)
  ]
  st.table(df_reviews_filtered)

reviews_table()

utils.lazy_load_initials()