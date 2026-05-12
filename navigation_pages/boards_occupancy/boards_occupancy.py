import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("shared/queries")

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import math
from datetime import datetime

import queries
import utils
import auth
import boards_occupancy_sidebar
import boards_occupancy_queries

NUMPY_FOUR = np.float64(4)

LAST_HOURS_AVAILABILITY = {
    "lubicz": {
        0: {20: NUMPY_FOUR},
        1: {20: NUMPY_FOUR},
        2: {20: NUMPY_FOUR},
        3: {20: NUMPY_FOUR},
        4: {22: NUMPY_FOUR},
        5: {22: NUMPY_FOUR},
        6: {20: NUMPY_FOUR},
    },
    "swietego-marcina": {
        0: {22: NUMPY_FOUR},
        1: {22: NUMPY_FOUR},
        2: {22: NUMPY_FOUR},
        3: {22: NUMPY_FOUR},
        4: {23: NUMPY_FOUR},
        5: {23: NUMPY_FOUR},
        6: {21: NUMPY_FOUR},
    },
    "sokolska": {
        0: {21: NUMPY_FOUR},
        1: {21: NUMPY_FOUR},
        2: {21: NUMPY_FOUR},
        3: {21: NUMPY_FOUR},
        4: {22: NUMPY_FOUR},
        5: {22: NUMPY_FOUR},
        6: {20: NUMPY_FOUR},
    },
    "grunwaldzka": {
        0: {21: NUMPY_FOUR},
        1: {21: NUMPY_FOUR},
        2: {21: NUMPY_FOUR},
        3: {21: NUMPY_FOUR},
        4: {22: NUMPY_FOUR},
        5: {22: NUMPY_FOUR},
        6: {21: NUMPY_FOUR},
    },
    "kijowska": {
        0: {21: np.float64(5)},
        1: {21: np.float64(5)},
        2: {21: np.float64(5)},
        3: {21: np.float64(5)},
        4: {23: np.float64(5)},
        5: {23: np.float64(5)},
        6: {21: np.float64(5)},
    } 
}

with st.spinner("Ładowanie danych..."):
  df_initial, df_locations, df_location_hours_availability, df_location_boards_availability = utils.run_in_parallel(
    (queries.get_initial_data, ()),
    (queries.get_locations_data, ()),
    (queries.get_historical_location_hours_availability, ()),
    (queries.get_historical_location_boards_availability, ()),
  )

  df_initial = auth.filter_locations(df_initial)
  df_locations = auth.filter_locations(df_locations)

(attraction_groups, selected_streets, granularity) = boards_occupancy_sidebar.filter_data(df_initial, df_locations)

if not selected_streets or not attraction_groups:
  st.info("Wybierz co najmniej jedną lokację i grupę atrakcji.")
  st.stop()

import datetime as _dt
_default_end = _dt.date.today() - _dt.timedelta(days=1)
_default_start = _default_end - _dt.timedelta(days=7)
col_from, col_to = st.columns(2)
with col_from:
  start_date = st.date_input('Data od', value=_default_start)
with col_to:
  end_date = st.date_input('Data do', value=_default_end)

if start_date > end_date:
  st.error("Data początku musi być wcześniej niż data końca")
  st.stop()

start_datetime = pd.Timestamp(start_date).tz_localize("UTC")
end_datetime = pd.Timestamp(end_date).replace(hour=23, minute=59, second=59).tz_localize("UTC")

with st.spinner("Ładowanie danych rezerwacji...", show_time=True):
  df_reservations, df_slots_occupancy = utils.run_in_parallel(
    (boards_occupancy_queries.get_reservations_data, (selected_streets, attraction_groups, start_datetime, end_datetime)),
    (queries.get_slots_occupancy, (start_datetime, end_datetime)),
  )

with st.spinner("Obliczanie zajętości...", show_time=True):

  all_rows = []

  for street in selected_streets:
    location_row = df_locations[df_locations['street'] == street]
    if location_row.empty:
      continue
    location_ids = location_row['id'].tolist()

    location_boards_avail = df_location_boards_availability[
      df_location_boards_availability['boards_availability_dim_location_id'].isin(location_ids)
    ]
    location_hours_avail = df_location_hours_availability[
      df_location_hours_availability['hours_availability_dim_location_id'].isin(location_ids)
    ]

    current_boards_avail = location_boards_avail[location_boards_avail['boards_availability_until_when'].isnull()]
    if current_boards_avail.empty:
      continue
    time_unit_in_hours = current_boards_avail['boards_availability_time_unit_in_hours'].values[0]
    time_unit_in_minutes = int(time_unit_in_hours * 60)

    hours_map = {}

    current_iter_date = start_date
    while current_iter_date <= end_date:
      weekday = (current_iter_date.weekday() + 1) % 7

      sorted_avail = location_hours_avail[
        location_hours_avail['hours_availability_day_of_week'] == weekday
      ].sort_values(by='hours_availability_since_when', ascending=False)

      avail_for_date = sorted_avail[
        sorted_avail['hours_availability_since_when'] <= pd.to_datetime(current_iter_date).tz_localize('UTC')
      ]
      if avail_for_date.empty:
        current_iter_date += pd.Timedelta(days=1)
        continue

      current_avail = avail_for_date.iloc[0]
      starting_hour = int(current_avail['hours_availability_starting_hour'])
      num_hours = int(current_avail['hours_availability_number_of_hours'])

      hours_map[str(current_iter_date)] = {str(h): 0 for h in range(starting_hour, starting_hour + num_hours)}

      current_iter_date += pd.Timedelta(days=1)

    street_reservations = df_reservations[df_reservations['street'] == street] if not df_reservations.empty else pd.DataFrame()

    for _, reservation in street_reservations.iterrows():
      if reservation['reservation_system'] == "plan4u":
        current_slot = df_slots_occupancy[df_slots_occupancy['slots_occupancy_reservation_id'] == reservation['id']]

        time_sum = 0
        slots_sum = 0
        for _, slot in current_slot.iterrows():
          time_sum += slot['slots_occupancy_time_taken']
          slots_sum += slot['slots_occupancy_slots_taken']

        time_taken = time_sum
        if time_taken == 0:
          continue

        slots_taken = slots_sum / (time_taken / time_unit_in_minutes)
      else:
        slots_taken = reservation['reservation_slots_taken']
        time_taken = reservation['reservation_time_taken']

      start_dt = reservation['start_date']
      date = start_dt.date()
      floor_hour = start_dt.hour
      num_hour_slots = math.ceil(time_taken / 60)
      date_str = str(date)
      for i in range(num_hour_slots):
        h_key = str(floor_hour + i)
        if date_str in hours_map and h_key in hours_map[date_str]:
          hours_map[date_str][h_key] += slots_taken

    for date_key, hours_data in hours_map.items():
      reservation_date = pd.to_datetime(date_key, format='%Y-%m-%d').tz_localize('UTC')

      location_boards_filtered = location_boards_avail[
        (location_boards_avail['boards_availability_since_when'] <= reservation_date) &
        (location_boards_avail['boards_availability_until_when'].isnull() |
         (location_boards_avail['boards_availability_until_when'] >= reservation_date))
      ]
      if location_boards_filtered.empty:
        continue
      total_boards_base = location_boards_filtered.iloc[0]['boards_availability_number_of_boards']
      day_of_week = datetime.strptime(date_key, '%Y-%m-%d').weekday()

      for hour_key, slots_taken in hours_data.items():
        parsed_hour = int(hour_key)
        override = LAST_HOURS_AVAILABILITY.get(street, {}).get(day_of_week, {}).get(parsed_hour)
        total_boards = override if override else total_boards_base

        boards_occupancy = round(slots_taken / total_boards * 100, 0) if total_boards > 0 else 0

        all_rows.append({
          'street': street,
          'date': date_key,
          'hour_key': hour_key,
          'slots_taken': slots_taken,
          'total_boards': float(total_boards),
          'boards_occupancy': boards_occupancy,
        })

  df_all = pd.DataFrame(all_rows)

if df_all.empty:
  st.info("Brak danych dla wybranego zakresu dat.")
  st.stop()

# --- Granularity aggregation ---

location_label = df_all['street'].map(utils.street_to_location).fillna(df_all['street'])
df_all['location_name'] = location_label

def weighted_occupancy(grp):
  total = grp['total_boards'].sum()
  return round(grp['slots_taken'].sum() / total * 100, 0) if total > 0 else 0

if granularity == "Godzina":
  df_all['sort_key'] = pd.to_datetime(df_all['date'])
  df_all['display_date'] = df_all['date'].apply(lambda d: pd.to_datetime(d).strftime('%d.%m'))
  df_all['day_name'] = df_all['date'].apply(
    lambda d: utils.get_day_of_week_string_shortcut(datetime.strptime(d, '%Y-%m-%d').weekday())
  )
  df_all['display_label'] = df_all['display_date'] + ', ' + df_all['day_name']
  df_all['start_date_hour'] = df_all['hour_key'].apply(lambda h: f"{int(h):02d}:00")

  agg = (
    df_all
    .groupby(['sort_key', 'display_label', 'start_date_hour'], as_index=False)
    .apply(lambda g: pd.Series({'boards_occupancy': weighted_occupancy(g)}), include_groups=False)
  )

  daily_avg = (
    df_all
    .groupby(['sort_key', 'display_label'], as_index=False)
    .apply(lambda g: pd.Series({'boards_occupancy': weighted_occupancy(g)}), include_groups=False)
  )
  daily_avg['start_date_hour'] = 'Średnia'

  heatmap_df = pd.concat([agg, daily_avg], ignore_index=True)

  x_field = 'display_label'
  y_field = 'start_date_hour'
  sort_field = 'sort_key'

else:
  if granularity == "Dzień":
    df_all['sort_key'] = pd.to_datetime(df_all['date'])
    df_all['display_label'] = df_all['date'].apply(lambda d: pd.to_datetime(d).strftime('%d.%m'))
  elif granularity == "Tydzień":
    df_all['_dt'] = pd.to_datetime(df_all['date'])
    df_all['sort_key'] = pd.to_datetime(df_all['_dt'].dt.to_period('W').dt.start_time)
    def _week_label(monday):
      week_start = max(monday.date(), start_date)
      week_end = min((monday + pd.Timedelta(days=6)).date(), end_date)
      return week_start.strftime('%d.%m') + '–' + week_end.strftime('%d.%m')
    df_all['display_label'] = df_all['sort_key'].apply(_week_label)
  elif granularity == "Miesiąc":
    df_all['_dt'] = pd.to_datetime(df_all['date'])
    df_all['sort_key'] = df_all['_dt'].dt.to_period('M').dt.start_time
    df_all['display_label'] = df_all['_dt'].apply(lambda d: d.strftime('%m.%Y'))

  agg = (
    df_all
    .groupby(['sort_key', 'display_label', 'street', 'location_name'], as_index=False)
    .apply(lambda g: pd.Series({'boards_occupancy': weighted_occupancy(g)}), include_groups=False)
  )

  avg_row = (
    df_all
    .groupby(['sort_key', 'display_label'], as_index=False)
    .apply(lambda g: pd.Series({'boards_occupancy': weighted_occupancy(g)}), include_groups=False)
  )
  avg_row['location_name'] = 'Średnia'
  avg_row['street'] = '__avg__'

  heatmap_df = pd.concat([agg, avg_row], ignore_index=True)

  x_field = 'display_label'
  y_field = 'location_name'
  sort_field = 'sort_key'

# --- Heatmap ---

x_sort = alt.EncodingSortField(sort_field, order='ascending')

heatmap = alt.Chart(heatmap_df).mark_rect(stroke='black', strokeWidth=1).encode(
  x=alt.X(f'{x_field}:O', title='', sort=x_sort, axis=alt.Axis(orient='top', labelFontSize=13, labelAngle=-90)),
  y=alt.Y(f'{y_field}:O', title='', axis=alt.Axis(labelFontSize=13)),
  color=alt.Color('boards_occupancy:Q', scale=alt.Scale(scheme='redyellowgreen'), title='Zajętość (%)'),
  tooltip=[
    alt.Tooltip(f'{x_field}:O', title='Okres'),
    alt.Tooltip(f'{y_field}:O', title=''),
    alt.Tooltip('boards_occupancy:Q', title='Zajętość (%)'),
  ]
).properties(
  width=800,
  height=500,
  title='Raport zajętości mat'
)

text = heatmap.mark_text(fontSize=12, fontWeight='bold', baseline='middle').encode(
  alt.Text('boards_occupancy:Q', format='.0%'),
  color=alt.value('black'),
).transform_calculate(
  boards_occupancy='datum.boards_occupancy / 100'
)

st.altair_chart(heatmap + text, use_container_width=True)

# --- Export ---

def build_export_sheet(street_df):
  if granularity == "Godzina":
    grp = street_df.groupby(['sort_key', 'start_date_hour'], as_index=False).agg(
      wszystkie_maty=('total_boards', 'sum'),
      zajete_maty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%d.%m.%Y') + ' ' + grp['start_date_hour']
  elif granularity == "Dzień":
    grp = street_df.groupby(['sort_key'], as_index=False).agg(
      wszystkie_maty=('total_boards', 'sum'),
      zajete_maty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%d.%m.%Y')
  elif granularity == "Tydzień":
    grp = street_df.groupby(['sort_key'], as_index=False).agg(
      wszystkie_maty=('total_boards', 'sum'),
      zajete_maty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].apply(
      lambda d: f"{d.strftime('%d.%m.%Y')} - {(d + pd.Timedelta(days=6)).strftime('%d.%m.%Y')}"
    )
  elif granularity == "Miesiąc":
    grp = street_df.groupby(['sort_key'], as_index=False).agg(
      wszystkie_maty=('total_boards', 'sum'),
      zajete_maty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%m.%Y')

  grp['zajetość (%)'] = (grp['zajete_maty'] / grp['wszystkie_maty'] * 100).round(1)
  return grp.sort_values('sort_key')[['okres', 'wszystkie_maty', 'zajete_maty', 'zajetość (%)']]

export_sheets = {}
for street in selected_streets:
  export_sheets[street] = build_export_sheet(df_all[df_all['street'] == street])

locations_part = "_".join(selected_streets)
groups_part = "_".join(attraction_groups)
file_name = f"raport_zajętości_mat_{locations_part}_{groups_part}_{start_date.strftime('%d.%m.%Y')}_{end_date.strftime('%d.%m.%Y')}"

@st.fragment
def download_section():
  utils.download_button(export_sheets, file_name)

download_section()
