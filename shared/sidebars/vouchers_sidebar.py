import streamlit as st


def filter_data(df):

  with st.sidebar:
    with st.expander("Filtry"):
      city_checkboxes = st.multiselect("Miasta", df['city'].unique(), default=df['city'].unique())


  df = df[df['city'].isin(city_checkboxes)]

  return df