import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("shared/queries")
sys.path.append("shared/queries/report_queries")
sys.path.append("navigation_pages/boards_occupancy")

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import datetime, date

import queries
import utils
import auth
import boards_occupancy_time_period_sidebar
import occupancy_report_queries
from safi_view import LAST_HOURS_AVAILABILITY


@st.cache_data(ttl=28800)
def compute_occupancy(df_reservations, df_slots_occupancy, df_locations,
                      df_location_hours_availability, df_location_boards_availability,
                      selected_streets, start_date, end_date):
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
      starting_hour = current_avail['hours_availability_starting_hour']
      num_time_units = int(current_avail['hours_availability_number_of_hours'] / time_unit_in_hours)

      hours_map[str(current_iter_date)] = {}
      hours_array = np.linspace(starting_hour, starting_hour + num_time_units * time_unit_in_hours, num_time_units, endpoint=False)

      for hour in hours_array:
        hours_map[str(current_iter_date)][str(hour)] = 0

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
      res_date = start_dt.date()
      hour = start_dt.hour
      minutes = start_dt.minute
      minutes_multiplier = 1 if time_unit_in_minutes != 60 else 0

      while time_taken > 0:
        hour_key = str(f'{hour}.{minutes_multiplier * int(minutes / 60 * 10)}')
        date_str = str(res_date)
        if date_str in hours_map and hour_key in hours_map[date_str]:
          hours_map[date_str][hour_key] += slots_taken
        time_taken -= time_unit_in_minutes
        minutes += time_unit_in_minutes
        if minutes >= 60:
          hour += 1
          minutes -= 60

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
        parsed_hour = int(float(hour_key))
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

  return pd.DataFrame(all_rows)


with st.spinner("Ładowanie danych..."):
  df_initial, df_locations, df_location_hours_availability, df_location_boards_availability = utils.run_in_parallel(
    (queries.get_initial_data, ()),
    (queries.get_locations_data, ()),
    (queries.get_historical_location_hours_availability, ()),
    (queries.get_historical_location_boards_availability, ()),
  )

  df_initial = auth.filter_locations(df_initial)
  df_locations = auth.filter_locations(df_locations)

(attraction_groups, visit_types, selected_streets, start_date, end_date, granularity, show_unended_period) = boards_occupancy_time_period_sidebar.filter_data(df_initial, df_locations)

if not selected_streets or not attraction_groups:
  st.info("Wybierz co najmniej jedną lokację i grupę atrakcji.")
  st.stop()

if not visit_types:
  st.info("Wybierz co najmniej jeden typ wizyty.")
  st.stop()

if start_date > end_date:
  st.error("Data początku musi być wcześniej niż data końca")
  st.stop()

start_datetime = pd.Timestamp(start_date).tz_localize("UTC")
end_datetime = pd.Timestamp(end_date).replace(hour=23, minute=59, second=59).tz_localize("UTC")

with st.spinner("Ładowanie danych rezerwacji...", show_time=True):
  df_reservations, df_slots_occupancy = utils.run_in_parallel(
    (occupancy_report_queries.get_reservations_with_visit_type, (selected_streets, attraction_groups, visit_types, start_datetime, end_datetime)),
    (queries.get_slots_occupancy, (start_datetime, end_datetime)),
  )

with st.spinner("Obliczanie zajętości...", show_time=True):
  df_all = compute_occupancy(
    df_reservations, df_slots_occupancy, df_locations,
    df_location_hours_availability, df_location_boards_availability,
    tuple(selected_streets), start_date, end_date,
  )

if df_all.empty:
  st.info("Brak danych dla wybranego zakresu dat.")
  st.stop()

today = date.today()
now = datetime.now()

if not show_unended_period:
  if granularity == "Godzina":
    current_hour = now.hour
    df_all = df_all[~(
      (pd.to_datetime(df_all['date']).dt.date == today) &
      (df_all['hour_key'].apply(lambda h: int(float(h))) == current_hour)
    )]
  elif granularity == "Dzień":
    df_all = df_all[pd.to_datetime(df_all['date']).dt.date != today]
  elif granularity == "Tydzień":
    current_iso = today.isocalendar()
    current_year, current_week = current_iso.year, current_iso.week
    _dt = pd.to_datetime(df_all['date'])
    iso = _dt.dt.isocalendar()
    df_all = df_all[~((iso.year == current_year) & (iso.week == current_week))]
  elif granularity == "Miesiąc":
    df_all = df_all[~(
      (pd.to_datetime(df_all['date']).dt.month == today.month) &
      (pd.to_datetime(df_all['date']).dt.year == today.year)
    )]

if df_all.empty:
  st.info("Brak danych dla wybranego zakresu dat.")
  st.stop()

# --- Granularity aggregation ---

df_all['location_name'] = df_all['street'].map(utils.street_to_location).fillna(df_all['street'])

def weighted_occupancy(grp):
  total = grp['total_boards'].sum()
  return round(grp['slots_taken'].sum() / total * 100, 0) if total > 0 else 0

if granularity == "Godzina":
  df_all['_dt'] = pd.to_datetime(df_all['date'])
  df_all['_full_hour'] = df_all['hour_key'].apply(lambda h: int(float(h)))
  df_all['sort_key'] = df_all['_dt'] + pd.to_timedelta(df_all['_full_hour'], unit='h')

elif granularity == "Dzień":
  df_all['sort_key'] = pd.to_datetime(df_all['date'])

elif granularity == "Tydzień":
  df_all['_dt'] = pd.to_datetime(df_all['date'])
  df_all['sort_key'] = pd.to_datetime(df_all['_dt'].dt.to_period('W').dt.start_time)

elif granularity == "Miesiąc":
  df_all['_dt'] = pd.to_datetime(df_all['date'])
  df_all['sort_key'] = df_all['_dt'].dt.to_period('M').dt.start_time

# --- Average by sub-period chart ---

if granularity == "Godzina":
  df_all['_sub_sort'] = df_all['_full_hour']
  df_all['_sub_label'] = df_all['_full_hour'].apply(lambda h: f"{h:02d}:00")
  sub_title = 'Średnia zajętość wg godziny'

elif granularity == "Dzień":
  df_all['_weekday'] = pd.to_datetime(df_all['date']).dt.weekday  # 0=Mon
  df_all['_sub_sort'] = df_all['_weekday']
  df_all['_sub_label'] = df_all['_weekday'].apply(utils.get_day_of_week_string_shortcut)
  sub_title = 'Średnia zajętość wg dnia tygodnia'

elif granularity == "Tydzień":
  _dt = pd.to_datetime(df_all['date'])
  df_all['_iso_week'] = _dt.dt.isocalendar().week.astype(int)
  df_all['_sub_sort'] = df_all['_iso_week']
  df_all['_sub_label'] = df_all['_iso_week'].apply(lambda w: f"Tydzień {w}")
  _week_monday = df_all.groupby('_iso_week')['sort_key'].min()
  df_all['_week_range'] = df_all['_iso_week'].map(
    lambda w: _week_monday[w].strftime('%d.%m') + ' – ' + (_week_monday[w] + pd.Timedelta(days=6)).strftime('%d.%m')
  )
  sub_title = 'Średnia zajętość wg tygodnia roku'

elif granularity == "Miesiąc":
  df_all['_month'] = pd.to_datetime(df_all['date']).dt.month
  df_all['_sub_sort'] = df_all['_month']
  df_all['_sub_label'] = df_all['_month'].apply(utils.get_month_from_month_number)
  sub_title = 'Średnia zajętość wg miesiąca'

_sub_groupby_keys = ['_sub_sort', '_sub_label', 'street', 'location_name']
if granularity == "Tydzień":
  _sub_groupby_keys.insert(2, '_week_range')

_sub_total_boards = df_all.groupby('_sub_sort')['total_boards'].sum().rename('_sub_total_boards')

sub_agg = (
  df_all
  .groupby(_sub_groupby_keys, as_index=False)
  .agg(slots_taken=('slots_taken', 'sum'), total_boards=('total_boards', 'sum'))
  .merge(_sub_total_boards, on='_sub_sort')
  .assign(
    boards_occupancy=lambda d: (d['slots_taken'] / d['_sub_total_boards'] * 100).round(0),
    city_occupancy=lambda d: (d['slots_taken'] / d['total_boards'] * 100).round(0).where(d['total_boards'] > 0, 0),
  )
  .sort_values('_sub_sort')
)

sorted_sub_labels = sub_agg.drop_duplicates('_sub_label').sort_values('_sub_sort')['_sub_label'].tolist()

_sub_tooltips = [
  alt.Tooltip('_sub_label:O', title='Tydzień'),
  alt.Tooltip('_week_range:N', title='Zakres dat'),
  alt.Tooltip('location_name:N', title='Lokacja'),
  alt.Tooltip('boards_occupancy:Q', title='Udział globalny (%)'),
  alt.Tooltip('city_occupancy:Q', title='Zajętość lokacji (%)'),
] if granularity == "Tydzień" else [
  alt.Tooltip('_sub_label:O', title='Okres'),
  alt.Tooltip('location_name:N', title='Lokacja'),
  alt.Tooltip('boards_occupancy:Q', title='Udział globalny (%)'),
  alt.Tooltip('city_occupancy:Q', title='Zajętość lokacji (%)'),
]

sub_bar = alt.Chart(sub_agg).mark_bar().encode(
  x=alt.X('_sub_label:O', sort=sorted_sub_labels, title='', axis=alt.Axis(labelAngle=-45)),
  y=alt.Y('boards_occupancy:Q', title='Zajętość mat (%)',),
  color=alt.Color('location_name:N', title='Lokacja'),
  tooltip=_sub_tooltips
).properties(width=800, title=sub_title)

st.altair_chart(sub_bar, use_container_width=True)

# --- Export ---

def build_export_sheet(street_df):
  if granularity == "Godzina":
    grp = street_df.groupby(['sort_key', '_full_hour'], as_index=False).agg(
      wszystkie_maty=('total_boards', 'sum'),
      zajete_maty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%d.%m.%Y') + ' ' + grp['_full_hour'].apply(lambda h: f"{h:02d}:00")
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
file_name = f"raport_zajętości_mat_po_okresie_{locations_part}_{groups_part}_{start_date.strftime('%d.%m.%Y')}_{end_date.strftime('%d.%m.%Y')}"

@st.fragment
def download_section():
  utils.download_button(export_sheets, file_name)

download_section()
