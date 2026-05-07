import streamlit as st
import utils 

def ensure_status():
  if st.session_state.ms1[0] == "Wszystkie":
    st.session_state.ms1 = st.session_state.ms1[1:]
  elif st.session_state.ms1[-1] == "Wszystkie":
    st.session_state.ms1 = ["Wszystkie"]

def filter_data(df, df_locations):

  df_locations['location'] = df_locations['street'].map(utils.street_to_location).fillna(df_locations['street'])


  with st.sidebar:
    with st.expander("Filtry", expanded=True):
      with st.container(border=True):
        attraction_groups = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())
      with st.container(border=True):
        filtered_locations = st.multiselect('Lokacje', df_locations["location"].unique(), default=df_locations['location'].unique())

  return (attraction_groups, filtered_locations)
