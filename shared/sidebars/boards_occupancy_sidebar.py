import streamlit as st

def ensure_status():
  if st.session_state.ms1[0] == "Wszystkie":
    st.session_state.ms1 = st.session_state.ms1[1:]
  elif st.session_state.ms1[-1] == "Wszystkie":
    st.session_state.ms1 = ["Wszystkie"]

def filter_data(df):
  with st.sidebar:
    with st.expander("Filtry"):
      with st.container(border=True):
        attraction_groups = st.multiselect('Grupy atrakcji', df['attraction_group'].unique(), default=df['attraction_group'].unique())

  return (attraction_groups)
