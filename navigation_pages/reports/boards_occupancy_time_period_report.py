import sys
sys.path.append("shared")
sys.path.append("shared/sidebars")
sys.path.append("shared/queries")
sys.path.append("shared/queries/report_queries")
sys.path.append("navigation_pages/boards_occupancy")

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, date

import queries
import utils
import auth
import boards_occupancy_time_period_sidebar
import occupancy_report_queries
from boards_occupancy import LAST_HOURS_AVAILABILITY


with st.spinner("Ładowanie danych..."):
  df_initial, df_locations = utils.run_in_parallel(
    (queries.get_initial_data, ()),
    (queries.get_locations_data, ()),
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
  df_all = occupancy_report_queries.get_occupancy_by_hour(
    tuple(selected_streets),
    tuple(attraction_groups),
    tuple(visit_types),
    start_datetime,
    end_datetime,
  )

if df_all.empty:
  st.info("Brak danych dla wybranego zakresu dat.")
  st.stop()

override_records = [
  {'street': s, 'day_of_week': dow, 'slot_hour': hr, '_override': cap}
  for s, dow_map in LAST_HOURS_AVAILABILITY.items()
  for dow, hr_map in dow_map.items()
  for hr, cap in hr_map.items()
]
df_override = pd.DataFrame(override_records)

df_all['day_of_week'] = pd.to_datetime(df_all['slot_date']).dt.weekday
df_all['slot_hour'] = df_all['slot_hour'].astype(int)
df_all = df_all.merge(df_override, on=['street', 'day_of_week', 'slot_hour'], how='left')
df_all['total_boards'] = df_all['_override'].where(df_all['_override'].notna(), df_all['total_boards'])
df_all['boards_occupancy'] = (df_all['slots_taken'] / df_all['total_boards'] * 100).round(0)
df_all.drop(columns=['day_of_week', '_override'], inplace=True)

df_all = df_all.rename(columns={'slot_date': 'date', 'slot_hour': 'hour_key'})
df_all['date'] = df_all['date'].astype(str)
df_all['hour_key'] = df_all['hour_key'].astype(float).astype(str)  # e.g. "10.0"

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

def build_export_sheet(street_df):
  if granularity == "Godzina":
    grp = street_df.groupby(['sort_key', '_full_hour'], as_index=False).agg(
      wszystkie_sloty=('total_boards', 'sum'),
      zajete_sloty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%d.%m.%Y') + ' ' + grp['_full_hour'].apply(lambda h: f"{h:02d}:00")
  elif granularity == "Dzień":
    grp = street_df.groupby(['sort_key'], as_index=False).agg(
      wszystkie_sloty=('total_boards', 'sum'),
      zajete_sloty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%d.%m.%Y')
  elif granularity == "Tydzień":
    grp = street_df.groupby(['sort_key'], as_index=False).agg(
      wszystkie_sloty=('total_boards', 'sum'),
      zajete_sloty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].apply(
      lambda d: f"{d.strftime('%d.%m.%Y')} - {(d + pd.Timedelta(days=6)).strftime('%d.%m.%Y')}"
    )
  elif granularity == "Miesiąc":
    grp = street_df.groupby(['sort_key'], as_index=False).agg(
      wszystkie_sloty=('total_boards', 'sum'),
      zajete_sloty=('slots_taken', 'sum'),
    )
    grp['okres'] = grp['sort_key'].dt.strftime('%m.%Y')

  grp['zajetość (%)'] = (grp['zajete_sloty'] / grp['wszystkie_sloty'] * 100).round(1)
  return grp.sort_values('sort_key')[['okres', 'wszystkie_sloty', 'zajete_sloty', 'zajetość (%)']]

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
