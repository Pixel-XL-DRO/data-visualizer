import math
import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("utils")

import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
from datetime import datetime

import queries
import utils
import boards_occupancy_sidebar

st.set_page_config(layout="wide")

with st.spinner():
  df = queries.get_reservation_data()
  df_locations = queries.get_locations_data()
  df_visit_types = queries.get_visit_types_data()
  df_location_hours_availability = queries.get_historical_location_hours_availability()
  df_location_boards_availability = queries.get_historical_location_boards_availability()
  df_visit_type_availability = queries.get_historical_visit_type_availability()

(df, x_axis_type) = boards_occupancy_sidebar.filter_data(df)

for location_id in df['location_id'].unique():
  location = df_locations[df_locations['id'] == location_id].iloc[0]

  location_boards_availability = df_location_boards_availability.loc[(df_location_boards_availability['boards_availability_dim_location_id'] == location_id)]
  if location_boards_availability.empty:
      st.error(f"location {location['city']} {location['street']} not associated with boards availability")
      st.stop()

  for day_of_week in range(7):
    location_hours_availability = df_location_hours_availability.loc[(df_location_hours_availability['hours_availability_dim_location_id'] == location_id) & (df_location_hours_availability['hours_availability_day_of_week'] == day_of_week)]
    if location_hours_availability.empty:
      st.error(f"location {location['city']} {location['street']} for {utils.map_day_of_week_number_to_string(day_of_week)} not associated with hours availability")
      st.stop()

for visit_type_id in df['visit_type_id'].unique():
  visit_type_availability = df_visit_type_availability.loc[(df_visit_type_availability['visit_type_availability_dim_visit_type_id'] == visit_type_id)]
  visit_type = df_visit_types[df_visit_types['visit_type_id'] == visit_type_id].iloc[0]
  location = df_locations[df_locations['id'] == visit_type['visit_type_dim_location_id']].iloc[0]
  if visit_type_availability.empty:
    st.error(f"visit type {visit_type['name']} in  {location['city']} {location['street']} not associated with visit type availability")
    st.stop()

df = df.merge(df_visit_type_availability, how='left', left_on='visit_type_id', right_on='visit_type_availability_dim_visit_type_id')
# df = df.merge(df_location_boards_availability, how='left', left_on='location_id', right_on='boards_availability_dim_location_id')

city_selection = st.selectbox('Wybierz miasto', df_locations['city'].unique())
selected_location = df_locations[df_locations['city'] == city_selection]
selected_location_boards_availability = df_location_boards_availability[df_location_boards_availability['boards_availability_dim_location_id'].isin(selected_location['id'])]
selected_location_hours_availability = df_location_hours_availability[df_location_hours_availability['hours_availability_dim_location_id'].isin(selected_location['id'])]

df = df[df['location_id'].isin(selected_location['id'])]

min_date = df['start_date'].min()
max_date = df['start_date'].max()

if 'week_offset' not in st.session_state:
    st.session_state.week_offset = 0

# show since yesterday - since we dont have data for today
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=1)
current_week_start = (yesterday) - pd.Timedelta(days=yesterday.weekday())

def update_week_offset(offset):
  if offset == -1:
    st.session_state.week_offset -= 1
  elif offset == 1 and current_week_start - pd.Timedelta(days=7 * (st.session_state.week_offset + 1)) >= min_date:
    st.session_state.week_offset += 1

selected_week_start = current_week_start - pd.Timedelta(days=7 * st.session_state.week_offset)
selected_week_end = (selected_week_start + pd.Timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999)

col1, col2, col3 = st.columns(3)

if selected_week_start - pd.Timedelta(days=7) >= min_date:
  with col1:
    st.button("<=", on_click=lambda: update_week_offset(1))

with col2:
  st.write(f"{selected_week_start.day}.{selected_week_start.month}.{selected_week_start.year} - {selected_week_end.day}.{selected_week_end.month}.{selected_week_end.year}" )

if True or st.session_state.week_offset > 0:
  with col3:
    st.button("=>", on_click=lambda: update_week_offset(-1))

# take only df['start_date'] from last week
df['start_date'] = pd.to_datetime(df['start_date'])
df = df.loc[(df['start_date'] >= selected_week_start) & (df['start_date'] <= selected_week_end)]

# filter df to only include 2024-12-02. use INCLUDE not equal
# df = df[df['start_date'].dt.to_period('D') == '2024-12-13']

df = df.reset_index()

current_location_boards_availability = selected_location_boards_availability[selected_location_boards_availability['boards_availability_until_when'].isnull()]
current_location_hours_availability = selected_location_hours_availability[selected_location_hours_availability['hours_availability_until_when'].isnull()]

time_unit_in_hours = current_location_boards_availability['boards_availability_time_unit_in_hours'].values[0]

hours_map = {}

time_slots_taken_per_time_unit = 0
duration_in_time_units = 1

for _, row in df.iterrows():
  start_date_year = row['start_date_year']
  start_date_day = row['start_date_day_of_month']
  start_date_month = row['start_date_month']
  start_date_key = f"{start_date_day}.{start_date_month}.{start_date_year}"
  start_hour = row['start_date_hour']
  day_of_week = row['start_date_day_of_week']

  num_boards_per_time_unit = row['visit_type_availability_number_of_boards_per_time_unit']
  num_time_units = int(row['visit_type_availability_duration_in_time_units'])

  if start_date_key not in hours_map:
    hours_map[start_date_key] = {
      'day_of_week': day_of_week
    }

  current_hours_map = hours_map[start_date_key]

  hours_array = np.linspace(start_hour, start_hour + num_time_units * time_unit_in_hours, num_time_units, endpoint=False)

  for hour in hours_array:
    current_hour = str(hour)
    if current_hour not in current_hours_map:
      current_hours_map[current_hour] = num_boards_per_time_unit
    else:
      current_hours_map[current_hour] += num_boards_per_time_unit

  hours_map[start_date_key] = current_hours_map

for index, row in current_location_hours_availability.iterrows():
  day_of_week = row['hours_availability_day_of_week']
  starting_hour = int(row['hours_availability_starting_hour'])
  num_time_units = int(row['hours_availability_number_of_hours'] / time_unit_in_hours)

  hours_array = np.linspace(starting_hour, starting_hour + num_time_units * time_unit_in_hours, num_time_units, endpoint=False)

  for hour in hours_array:
    current_hour = str(hour)
    hours_map_for_day_of_week = [key for key in hours_map if hours_map[key]['day_of_week'] == day_of_week]
    if len(hours_map_for_day_of_week) > 0:
      hours_map_key = hours_map_for_day_of_week[0]
      if current_hour not in hours_map[hours_map_key]:
        hours_map[hours_map_key][current_hour] = 0

for key in hours_map:
  del hours_map[key]['day_of_week']

heatmap_data = []

for start_date_key, hours_data in hours_map.items():
  for hour, slots_taken in hours_data.items():
    formatted_date = utils.format_date(start_date_key)

    display_date = pd.to_datetime(formatted_date, format='%d.%m.%Y').strftime('%d.%m')
    day_name = utils.get_day_of_week_string_shortcut(datetime.strptime(start_date_key, '%d.%m.%Y').weekday())
    display_label = f"{display_date}, {day_name}"

    reservation_date = pd.to_datetime(start_date_key, format='%d.%m.%Y').tz_localize('UTC')

    selected_location_boards_availability_filtered = selected_location_boards_availability.loc[(selected_location_boards_availability['boards_availability_since_when'] <= reservation_date) & (selected_location_boards_availability['boards_availability_until_when'].isnull() | (selected_location_boards_availability['boards_availability_until_when'] >= reservation_date))].iloc[0]
    total_boards = selected_location_boards_availability_filtered['boards_availability_number_of_boards']

    if (selected_location.city.iloc[0] == "katowice" or selected_location.city.iloc[0] == "gdansk"):
      parsed_hour = int(float(hour))
      new_total_boards = np.float64(4)

      day_of_week = datetime.strptime(start_date_key, '%d.%m.%Y').weekday()
      if day_of_week < 4 and parsed_hour == 21:
        total_boards = new_total_boards
      elif day_of_week == 4 and parsed_hour == 22:
        total_boards = new_total_boards
      elif day_of_week == 5 and parsed_hour == 23:
        total_boards = new_total_boards
      elif day_of_week == 6 and parsed_hour == 20:
        total_boards = new_total_boards

    heatmap_data.append({
      'start_date_key': formatted_date,
      'display_label': display_label,
      'start_date_hour': utils.parse_hour(hour),
      'slots_taken': slots_taken,
      'boards_occupancy': (slots_taken / total_boards * 100).clip(max=100).round(0)
    })


heatmap_df = pd.DataFrame(heatmap_data)

if heatmap_df.empty:
  raise Exception("Pusty zbiór danych. Popraw zakres dat.")

date_sort_order = sorted(heatmap_df['start_date_key'].unique(), key=lambda x: pd.to_datetime(x, format='%d.%m.%Y'))

heatmap_df['sort_key'] = pd.to_datetime(heatmap_df['start_date_key'], format='%d.%m.%Y')

daily_avg = heatmap_df.groupby("display_label")["boards_occupancy"].mean().round(2).reset_index()

average_dataframe = pd.DataFrame({})

for index, row in daily_avg.iterrows():
  display_label = row['display_label']
  boards_occupancy = row['boards_occupancy']
  df = pd.DataFrame({
    'start_date_key': display_label,
    'display_label': display_label,
    'start_date_hour': 'Średnia',
    'slots_taken': 0,
    'boards_occupancy': boards_occupancy,
  }, index=[index])

  average_dataframe = pd.concat([average_dataframe, df], ignore_index=True)

heatmap_df = pd.concat([heatmap_df, average_dataframe], ignore_index=True)

heatmap = alt.Chart(heatmap_df).mark_rect(stroke="black", strokeWidth=3).encode(
  x=alt.X('display_label:O', title='Data', sort=alt.EncodingSortField('sort_key', order='ascending'), axis=alt.Axis(orient='top', labelFontSize=14, labelAngle=-90)),
  y=alt.Y('start_date_hour:O', title='Godzina', axis=alt.Axis(labelFontSize=14),),
  color=alt.Color('boards_occupancy:Q', scale=alt.Scale(scheme='redyellowgreen', domain=[0, 100]), title='Procent zajętych mat'),
  tooltip=[
    alt.Tooltip('start_date_key:O', title='Data'),
    alt.Tooltip('start_date_hour:O', title='Godzina'),
    alt.Tooltip('boards_occupancy:Q', title='Zajętośc (%)')
  ]
).properties(
  width=800,
  height=600,
  title="Matryca zajętości mat"
)

text = heatmap.mark_text(fontSize=14, fontWeight='bold', baseline='middle').encode(
    alt.Text('boards_occupancy:Q', format=".0%"), color=alt.value('black')
).transform_calculate(
    boards_occupancy="datum.boards_occupancy / 100"
)

st.altair_chart(heatmap + text, use_container_width=True)
