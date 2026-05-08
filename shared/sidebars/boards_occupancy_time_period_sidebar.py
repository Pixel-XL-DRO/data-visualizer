import streamlit as st
import utils
import datetime


def filter_data(df, df_locations):

  df_locations = df_locations.copy()
  df_locations['location'] = df_locations['street'].map(utils.street_to_location).fillna(df_locations['street'])
  location_to_street = dict(zip(df_locations['location'], df_locations['street']))

  years_possible = list(range(2022, datetime.datetime.now().year + 1))

  with st.sidebar:
    show_unended_period = st.checkbox('Pokazuj niepełny okres')
    time_range = st.selectbox('Wybierz okres', [*years_possible, 'Od początku', 'Przedział'], index=len(years_possible) - 1)

    start_date = None
    end_date = None

    if time_range == 'Przedział':
      start_date = st.date_input('Data od', value=datetime.date.today() - datetime.timedelta(days=30))
      end_date = st.date_input('Data do', value=datetime.date.today() - datetime.timedelta(days=1))

    with st.expander("Filtry", expanded=True):
      with st.container(border=True):
        attraction_groups = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())
      with st.container(border=True):
        visit_types = st.multiselect('Typy wizyty', df['visit_type'].unique(), default=df['visit_type'].unique())
      with st.container(border=True):
        filtered_locations = st.multiselect('Lokacje', df_locations['location'].unique(), default=df_locations['location'].unique())
      with st.container(border=True):
        granularity = st.selectbox('Granularność', ['Godzina', 'Dzień', 'Tydzień', 'Miesiąc'])

  if isinstance(time_range, int):
    start_date = datetime.date(time_range, 1, 1)
    end_date = datetime.date.today() - datetime.timedelta(days=1) if time_range == datetime.datetime.now().year else datetime.date(time_range, 12, 31)
  elif time_range == 'Od początku':
    min_ts = df['start_date'].min()
    start_date = min_ts.date() if hasattr(min_ts, 'date') else min_ts
    end_date = datetime.date.today() - datetime.timedelta(days=1)

  selected_streets = [location_to_street[loc] for loc in filtered_locations if loc in location_to_street]

  return (attraction_groups, visit_types, selected_streets, start_date, end_date, granularity, show_unended_period)
