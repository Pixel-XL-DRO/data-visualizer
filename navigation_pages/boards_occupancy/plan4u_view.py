import math
import sys

sys.path.append("utils")

import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
from datetime import datetime

import utils

NUMPY_FOUR = np.float64(4) # for now every city has 4 boards that start at last hour

LAST_HOURS_AVAILABILITY = {
    "katowice": {
        0: {21: NUMPY_FOUR},
        1: {21: NUMPY_FOUR},
        2: {21: NUMPY_FOUR},
        3: {21: NUMPY_FOUR},
        4: {21: NUMPY_FOUR},
        5: {21: NUMPY_FOUR},
        6: {19: NUMPY_FOUR},
    },
    "gdansk": {
        0: {21: NUMPY_FOUR},
        1: {21: NUMPY_FOUR},
        2: {21: NUMPY_FOUR},
        3: {21: NUMPY_FOUR},
        4: {22: NUMPY_FOUR},
        5: {22: NUMPY_FOUR},
        6: {21: NUMPY_FOUR},
    }
}


def render_plan4u_view(
  df,
  df_locations,
  df_visit_types,
  df_location_hours_availability,
  df_location_boards_availability,
  df_visit_type_availability,
  df_slots_occupancy,
  city_selection
  ):
  selected_location = df_locations[df_locations['street'] == city_selection]
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
    else:
      st.session_state.week_offset -= offset

  selected_week_start = current_week_start - pd.Timedelta(days=7 * st.session_state.week_offset)
  selected_week_end = (selected_week_start + pd.Timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999)

  col1, col2, col3 = st.columns(3)

  if selected_week_start.date() > min_date.date():
    with col1:
      st.button(":material/arrow_back:", on_click=lambda: update_week_offset(1))

  with col2:
    st.write(f"{selected_week_start.day}.{selected_week_start.month}.{selected_week_start.year} - {selected_week_end.day}.{selected_week_end.month}.{selected_week_end.year}" )

  if True or st.session_state.week_offset > 0:
    with col3:
      st.button(":material/arrow_forward:", on_click=lambda: update_week_offset(-1))

  df['start_date'] = pd.to_datetime(df['start_date'])
  df = df.loc[(df['start_date'] >= selected_week_start) & (df['start_date'] <= selected_week_end)]

  df = df.reset_index()

  current_location_boards_availability = selected_location_boards_availability[selected_location_boards_availability['boards_availability_until_when'].isnull()]
  current_location_hours_availability = selected_location_hours_availability[selected_location_hours_availability['hours_availability_until_when'].isnull()]
  current_location_slots_occupancy = df_slots_occupancy[df_slots_occupancy['slots_occupancy_reservation_id'].isin(df['id'])]

  time_unit_in_hours = current_location_boards_availability['boards_availability_time_unit_in_hours'].values[0]

  hours_map = {}

  current_iterator_date = selected_week_start.date()
  end_iterator_date = selected_week_end.date()

  while current_iterator_date <= end_iterator_date:
    weekday = (current_iterator_date.weekday() + 1) % 7
    starting_hour = current_location_hours_availability[current_location_hours_availability['hours_availability_day_of_week'] == weekday]['hours_availability_starting_hour'].values[0]
    num_time_units = int(current_location_hours_availability[current_location_hours_availability['hours_availability_day_of_week'] == weekday]['hours_availability_number_of_hours'].values[0] / time_unit_in_hours)

    hours_map[str(current_iterator_date)] = {}

    hours_array = np.linspace(starting_hour, starting_hour + num_time_units * time_unit_in_hours, num_time_units, endpoint=False)

    for hour in hours_array:
      hours_map[str(current_iterator_date)][str(hour)] = 0

    current_iterator_date += pd.Timedelta(days=1)

  for _, slot in current_location_slots_occupancy.iterrows():
    datetime_slot = slot['slots_occupancy_datetime_slot']
    slots_taken = slot['slots_occupancy_slots_taken']
    time_taken = slot['slots_occupancy_time_taken']
    
    date = datetime_slot.date()
    hour = datetime_slot.hour

    minutes_multiplier = 1 if datetime_slot.minute > 0 and time_taken / 60 != 1 else 0

    hour_key = str(f'{hour}.{minutes_multiplier * int(time_taken / 60 * 10)}')

    hours_map[str(date)][hour_key] += slots_taken
  heatmap_data = []

  try:

    for start_date_key, hours_data in hours_map.items():
      for hour, slots_taken in hours_data.items():
        formatted_date = utils.format_date(start_date_key)

        display_date = pd.to_datetime(formatted_date, format='%d-%m-%Y').strftime('%d.%m')
        day_name = utils.get_day_of_week_string_shortcut(datetime.strptime(start_date_key, '%Y-%m-%d').weekday())
        display_label = f"{display_date}, {day_name}"

        reservation_date = pd.to_datetime(start_date_key, format='%Y-%m-%d').tz_localize('UTC')

        selected_location_boards_availability_filtered = selected_location_boards_availability.loc[(selected_location_boards_availability['boards_availability_since_when'] <= reservation_date) & (selected_location_boards_availability['boards_availability_until_when'].isnull() | (selected_location_boards_availability['boards_availability_until_when'] >= reservation_date))].iloc[0]
        total_boards = selected_location_boards_availability_filtered['boards_availability_number_of_boards']

        city = selected_location.city.iloc[0]
        parsed_hour = int(float(hour))
        day_of_week = datetime.strptime(start_date_key, '%Y-%m-%d').weekday()

        new_slots_taken = LAST_HOURS_AVAILABILITY.get(city, {}).get(day_of_week, {}).get(parsed_hour)

        if new_slots_taken: 
          total_boards = new_slots_taken

        heatmap_data.append({
          'start_date_key': formatted_date,
          'display_label': display_label,
          'start_date_hour': utils.parse_hour(hour),
          'slots_taken': slots_taken,
          # TODO: revert
          # 'boards_occupancy': (slots_taken / total_boards * 100).clip(max=100).round(0)
          'boards_occupancy': (slots_taken / total_boards * 100).round(0)
        })
  except:
    pass

  heatmap_df = pd.DataFrame(heatmap_data)

  if heatmap_df.empty:
    st.write(f"W aktualnym przedziale nie wykryto mapowania rezerwacji")
    st.write(f"Najbliższe rezerwacje są w tygodniu :orange[{min_date.date()} - {min_date.date() + pd.Timedelta(days=7)}]")
    st.button("Przejdź :material/fast_forward:", on_click=lambda: update_week_offset((min_date - selected_week_start).days // 7))

    raise Exception("Pusty zbiór danych. Popraw zakres dat.")

  date_sort_order = sorted(heatmap_df['start_date_key'].unique(), key=lambda x: pd.to_datetime(x, format='%d-%m-%Y'))

  heatmap_df['sort_key'] = pd.to_datetime(heatmap_df['start_date_key'], format='%d-%m-%Y')

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
    # TODO: revert
    # color=alt.Color('boards_occupancy:Q', scale=alt.Scale(scheme='redyellowgreen', domain=[0, 100]), title='Procent zajętych mat'),
    color=alt.Color('boards_occupancy:Q', scale=alt.Scale(scheme='redyellowgreen'), title='Procent zajętych mat'),
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
