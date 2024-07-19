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

def change_t2_false():
  if not st.session_state.t1:
    st.session_state.t2 = False

def make_sure_only_one_separate_toggle_is_on(key):
  if key == "t3":
    if st.session_state.t3:
      st.session_state.t4 = False
  else:
    if st.session_state.t4:
      st.session_state.t3 = False

def create_chart_with_ma(data, y_field, y_field_ma, title):
    base = alt.Chart(data).encode(
      x=alt.X(x_axis_type, title='Data', axis=alt.Axis(tickCount="month")),
    ).interactive()

    points = base.mark_point().encode(
      y=alt.Y(y_field, title=title),
      tooltip=[x_axis_type, y_field],
      color=alt.Color('city', scale=alt.Scale(scheme='dark2')) if seperate_cities else alt.Color('attraction_group', scale=alt.Scale(scheme='dark2')) if seperate_attractions else alt.value('red')
    )

    if moving_average_toggle:
      line = base.mark_line().encode(
        y=alt.Y(y_field_ma, title=title),
        tooltip=[x_axis_type, y_field_ma],
        strokeWidth=alt.value(2 if seperate_cities else 2 if seperate_attractions else 4),
        color=alt.Color('city', scale=alt.Scale(scheme='dark2')) if seperate_cities else alt.Color('attraction_group', scale=alt.Scale(scheme='dark2')) if seperate_attractions else alt.value('blue')
      )

      if show_only_moving_average:
        return line
      return line + points
    else:
        return points


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
    res.start_date_id,
    res.booked_date_id,
    res.location_id,
    res.client_id,
    res.is_payed,
    res.is_cancelled,
    res.no_of_people,
    res.whole_cost_with_voucher,
    res.attraction_group,
    start_date.date AS start_date,
    booked_date.date AS booked_date,
    location.city AS city,
    client.language AS language,
    client.id AS client_id
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
  time_range = st.selectbox('Pokazuj z ostatnich', ['7 dni', '1 miesiaca', '6 miesiecy', '1 roku', '2 lat', '3 lat', 'Od poczatku', "Przedział"], index=2)
  if time_range == "Przedział":
    start_date = st.date_input('Data rozpoczecia')
    end_date = st.date_input('Data konca')
  with st.expander("Średnia kroczaca"):
    moving_average_toggle = st.checkbox('Pokazuj', key="t1", value=True, on_change=change_t2_false)
    show_only_moving_average = st.checkbox('Pokazuj tylko srednia kroczaca', key="t2", value=False)
    moving_average_days = st.slider('Ile dni', 1, 30, 7)
  with st.expander("Filtry"):
    with st.container(border=True):
      city_checkboxes = st.multiselect("Miasta", df['city'].unique(), default=df['city'].unique())
      seperate_cities = st.checkbox('Rozdziel miasta', key="t3", on_change=lambda:make_sure_only_one_separate_toggle_is_on("t3"))
    language_checkboxes = st.multiselect('Język klienta', df['language'].unique(), default=df['language'].unique())
    with st.container(border=True):
      attraction_groups_checkboxes = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())
      seperate_attractions = st.checkbox('Rozdziel atrakcje', key="t4", on_change=lambda:make_sure_only_one_separate_toggle_is_on("t4"))

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


df = df[df['is_payed'] == True]
df = df[df['is_cancelled'] == False]
df = df[df['city'].isin(city_checkboxes)]
df = df[df['language'].isin(language_checkboxes)]
df = df[df['attraction_group'].isin(attraction_groups_checkboxes)]
df = df[df[x_axis_type] >= start_date]
df = df[df[x_axis_type] <= end_date]

df = df.sort_values(by=x_axis_type, ascending=True)
df['cumulative_reservations'] = df.groupby('client_id').cumcount() + 1
df['has_past_reservation'] = df['cumulative_reservations'] > 1

if seperate_cities:
  df_grouped = df.groupby([df[x_axis_type].dt.to_period('M'), df['city']]).agg(
    reservations=('id', 'count'),
    returning_clients=('client_id', lambda x: x.duplicated(keep=False).sum()),
    past_reservation_clients=('has_past_reservation', 'sum')
  ).reset_index()

elif seperate_attractions:
  df_grouped = df.groupby([df[x_axis_type].dt.to_period('M'), df['attraction_group']]).agg(
    reservations=('id', 'count'),
    returning_clients=('client_id', lambda x: x.duplicated(keep=False).sum()),
    past_reservation_clients=('has_past_reservation', 'sum')
  ).reset_index()

else:
  df_grouped = df.groupby(df[x_axis_type].dt.to_period('M')).agg(
    reservations=('id', 'count'),
    returning_clients=('client_id', lambda x: x.duplicated(keep=False).sum()),
    past_reservation_clients=('has_past_reservation', 'sum')
  ).reset_index()

df_grouped[x_axis_type] = df_grouped[x_axis_type].dt.to_timestamp()
df_grouped['retention_percent'] = df_grouped['returning_clients'] / df_grouped['reservations'] * 100
df_grouped['past_retention_percent'] = df_grouped['past_reservation_clients'] / df_grouped['reservations'] * 100

# st.text("Retencja na miesiac (procent osób które odwiedziły nas conajmniej 2 razy)")
# reservations_chart = create_chart_with_ma(df_grouped, 'retention_percent', 'retention_percent', "Procent osób")
# st.altair_chart(reservations_chart, use_container_width=True)

st.text("Retencja na miesiac (procent osób które odwiedziły nas conajmniej 2 razy w historii)")
reservations_chart = create_chart_with_ma(df_grouped, 'past_retention_percent', 'past_retention_percent', "Procent osób")
st.altair_chart(reservations_chart, use_container_width=True)

# df_grouped['cumulative_reservations'] = df_grouped['reservations'].cumsum()
# df_grouped['cumulative_client_reservations'] = df_grouped['returning_clients'].cumsum()
# df_grouped['cumulative_retention_percent'] = df_grouped['cumulative_client_reservations'] / df_grouped['cumulative_reservations'] * 100

# st.text("Kumulatywna retencja (procent osób powracajacych)")
# reservations_chart = create_chart_with_ma(df_grouped, 'cumulative_retention_percent', 'cumulative_retention_percent', "Procent osób")
# st.altair_chart(reservations_chart, use_container_width=True)


client_reservations_count = df.groupby('client_id').size().reset_index(name='reservation_count')

client_reservations_count['category'] = client_reservations_count['reservation_count'].apply(
    lambda x: '1 raz' if x == 1 else '2 razy' if x == 2 else ('3 razy' if x == 3 else ('4 razy' if x == 4 else 'więcej'))
)

category_counts = client_reservations_count['category'].value_counts().reset_index()
category_counts.columns = ['category', 'count']
st.write("Ile razy ludzie u nas byli?")
category_counts
