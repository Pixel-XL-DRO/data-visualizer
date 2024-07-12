import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import altair as alt

from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
  st.secrets["gcp_service_account"]
)

client = bigquery.Client(credentials=credentials)

def create_chart(data, y_field, y_field_ma, title):
    base = alt.Chart(data).encode(
        x=alt.X(x_axis_type, title='Date')
    ).interactive()

    line = base.mark_line(color='blue').encode(
        y=alt.Y(y_field, title=title),
        tooltip=[x_axis_type, y_field]
    )

    if moving_average_toggle:
        points = base.mark_point().encode(
            y=alt.Y(y_field_ma, title=title),
            tooltip=[x_axis_type, y_field_ma],
            color=alt.value('red')
        )
        return line + points
    else:
        return line

@st.cache_data(ttl=600)
def run_query(query):
  query_job = client.query(query)
  rows_raw = query_job.result()
  # dict cause of caching
  rows = [dict(row) for row in rows_raw]
  return rows

query = """
  SELECT
    res.id,
    res.reservation_external_id,
    res.start_date_id,
    res.booked_date_id,
    res.location_id,
    res.client_id,
    res.is_payed,
    res.is_cancelled,
    res.no_of_people,
    res.with_voucher,
    res.whole_cost_with_voucher,
    res.whole_cost_without_voucher,
    res.additional_items_cost,
    res.reservation_system,
    res.visit_type,
    res.attraction_group,
    res.reservation_system_url,
    start_date.date AS start_date,
    booked_date.date AS booked_date,
    location.city AS city,
    client.language AS language,
  FROM
    `pixelxl-database-dev.reservation_data.event_create_reservation` res
  JOIN
    `pixelxl-database-dev.reservation_data.dim_date` start_date
  ON
    res.start_date_id = start_date.id
  JOIN
    `pixelxl-database-dev.reservation_data.dim_date` booked_date
  ON
    res.booked_date_id = booked_date.id
  JOIN
    `pixelxl-database-dev.reservation_data.dim_location` location
  ON
    res.location_id = location.id
  JOIN
    `pixelxl-database-dev.reservation_data.dim_client` client
  ON
    res.client_id = client.id
"""

with st.spinner():
  rows = run_query(query)
  df = pd.DataFrame(rows)

start_date = None
end_date = None

# side bar
with st.sidebar:
  x_axis_type = st.selectbox('Wybierz rodzaj daty', ['Data stworzenia', 'Data rozpoczecia'])
  time_range = st.selectbox('Pokazuj z ostatnich', ['7 dni', '1 miesiaca', '6 miesiecy', '1 roku', '2 lat', '3 lat', 'Od poczatku', "Przedział"], index=3)
  if time_range == "Przedział":
    start_date = st.date_input('Data rozpoczecia')
    end_date = st.date_input('Data konca')
  with st.expander("Średnia kroczaca"):
    moving_average_toggle = st.checkbox('Pokazuj', value=True)
    moving_average_days = st.slider('Ile dni', 1, 30, 7)
  with st.expander("Filtry"):
    city_checkboxes = st.multiselect('Miasta', df['city'].unique(), default=df['city'].unique())
    language_checkboxes = st.multiselect('Język klienta', df['language'].unique(), default=df['language'].unique())
    attraction_groups_checkboxes = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())

# transform data
df['start_date'] = pd.to_datetime(df['start_date']).dt.tz_localize(None)
df['booked_date'] = pd.to_datetime(df['booked_date']).dt.tz_localize(None)

if x_axis_type == 'Data stworzenia':
  x_axis_type = 'booked_date'
elif x_axis_type == 'Data rozpoczecia':
  x_axis_type = 'start_date'

if end_date is None:
  end_date = datetime.now()
else:
  end_date = datetime.combine(end_date, datetime.min.time())

if start_date is None:
  if time_range == '7 dni':
    start_date = end_date - timedelta(days=7)
  elif time_range == '1 miesiaca':
    start_date = end_date - timedelta(days=30)
  elif time_range == '6 miesiecy':
    start_date = end_date - timedelta(days=182)
  elif time_range == '1 roku':
    start_date = end_date - timedelta(days=365)
  elif time_range == '2 lat':
    start_date = end_date - timedelta(days=730)
  elif time_range == '3 lat':
    start_date = end_date - timedelta(days=1095)
  elif time_range == 'Od poczatku':
    start_date = df[x_axis_type].min()
else:
  start_date = datetime.combine(start_date, datetime.min.time())


df = df[df['city'].isin(city_checkboxes)]
df = df[df['language'].isin(language_checkboxes)]
df = df[df['attraction_group'].isin(attraction_groups_checkboxes)]
df = df[df[x_axis_type] >= start_date]
df = df[df[x_axis_type] <= end_date]

df_grouped = df.groupby(df[x_axis_type].dt.to_period('D')).agg(
  reservations=('id', 'count'),
  total_cost=('whole_cost_with_voucher', 'sum'),
  total_people=('no_of_people', 'sum')
).reset_index()

df_grouped[x_axis_type] = df_grouped[x_axis_type].dt.to_timestamp()

if moving_average_toggle:
  df_grouped['reservations_ma'] = df_grouped['reservations'].rolling(window=moving_average_days).mean()
  df_grouped['total_cost_ma'] = df_grouped['total_cost'].rolling(window=moving_average_days).mean()
  df_grouped['total_people_ma'] = df_grouped['total_people'].rolling(window=moving_average_days).mean()

st.text("Liczba rezerwacji")
reservations_chart = create_chart(df_grouped, 'reservations', 'reservations_ma', "Liczba rezerwacji")
st.altair_chart(reservations_chart, use_container_width=True)

st.text("Przychód (PLN)")
cost_chart = create_chart(df_grouped, 'total_cost', 'total_cost_ma', "Przychód (PLN)")
st.altair_chart(cost_chart, use_container_width=True)

st.text("Liczba osób")
people_chart = create_chart(df_grouped, 'total_people', 'total_people_ma', "Liczba osób")
st.altair_chart(people_chart, use_container_width=True)
