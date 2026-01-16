import sys

sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd
import safi_products_sidebar
import queries
import safi_products_queries
import utils
import auth
from datetime import timedelta

with st.spinner("Inicjalizacja...", show_time=True):
  df, df_items = queries.get_safi_products_initial_data()
  df = auth.filter_locations(df)
  
# side bar
(groupBy, show_only_moving_average, moving_average_days, moving_average_toggle, cities) = safi_products_sidebar.filter_data(df)

@st.fragment
def view():

  min_date = df['min_creation_date'].min()
  max_date = df['max_creation_date'].max()

  items = st.multiselect("Produkty", df_items['product_name'].unique(), default=None)

  col1, col2 = st.columns(2)
  with col1:
    current_period_start = st.date_input('Od kiedy', value=min_date, min_value=min_date, max_value=max_date - timedelta(days=1))

  with col2:
    current_period_end = st.date_input('Do kiedy', value=max_date +  + timedelta(days=1), min_value=min_date + timedelta(days=1), max_value=max_date +  + timedelta(days=1))

  current_period_start = pd.to_datetime(current_period_start)
  current_period_end = pd.to_datetime(current_period_end)

  with st.spinner("Ładowanie danych...", show_time=True):
    df_sales, df_sold_items = utils.run_in_parallel(
      (safi_products_queries.get_items_sales_per_day, (current_period_start, current_period_end, moving_average_days, cities, items, groupBy)),
      (safi_products_queries.get_items_sold, (current_period_start, current_period_end, cities, items, groupBy)),
    )

  items_chart  = utils.create_chart_new(df_sales, 'date', "Data", 'count' if not show_only_moving_average else None, 'count_moving_avg' if moving_average_toggle else None, "Ilość sprzedaży na dany dzień", groupBy, 2 if groupBy else 4, "Średnia ilość sprzedaży", False)
  st.plotly_chart(items_chart, use_container_width=True)

  df_sold_items

  utils.download_button(df_sold_items, "produkty")

view()

utils.lazy_load_initials()