import streamlit as st
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = dict(st.secrets["firebase"]['cred'])

if not firebase_admin._apps:
  cred = credentials.Certificate(cred)
  firebase_admin.initialize_app(cred)

db = firestore.client()

def authorize(roles):
  if not st.user.is_logged_in and 'sent_login' not in st.session_state:
    st.login()
    st.session_state.sent_login = True

  if not st.user.is_logged_in:
    st.stop()

  doc_ref = db.collection("streamlitUserRoles").document(st.user.email)
  doc = doc_ref.get()

  if doc.exists:
    role = doc.to_dict()['role']
    locations = doc.to_dict()['locations']
  else:
    st.error("You are not listed in the database. Contact support for access")
    st.stop()

  st.session_state.role = role
  st.session_state.locations = locations

  if role not in roles:
    if st.button("Wyloguj się"):
      st.logout()
    st.error("You are not authorized to view this page. Contact support for access")
    st.stop()

def filter_locations(df):
  if st.session_state.locations[0] != "all":
    df = df[(df['city'] + "-" + df['street']).isin(st.session_state.locations)]
  return df
