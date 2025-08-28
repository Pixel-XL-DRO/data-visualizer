import sys

sys.path.append("shared")

import streamlit as st
import auth

st.set_page_config(layout="wide")
st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>', unsafe_allow_html=True)

def logout():
  if st.button("Wyloguj się"):
    st.logout()

auth.authorize(["admin", "manager", "super-admin"])

logout_page = st.Page(logout, title="Wyloguj się", icon=":material/logout:")

reservations_page = st.Page("navigation_pages/reservations.py", title="Rezerwacje", icon=":material/calendar_month:")
clients_page = st.Page("navigation_pages/clients.py", title="Klienci", icon=":material/people:")
boards_occupancy_page = st.Page("navigation_pages/boards_occupancy.py", title="Zajętość mat", icon=":material/flex_wrap:")
reservations_by_time_period_page = st.Page("navigation_pages/reservations_by_time_period.py", title="Rezerwacje po okresie", icon=":material/history:")
reservations_cumulative_page = st.Page("navigation_pages/reservations_cumulative.py", title="Rezerwacje kumulacyjne", icon=":material/timeline:")
data_editor_page = st.Page("navigation_pages/data_editor.py", title="Edytor danych", icon=":material/edit:")
google_reviews_page = st.Page("navigation_pages/google_reviews.py", title="Opinie Google", icon=":material/reviews:")
reviews_page = st.Page("navigation_pages/reviews.py", title="NPS", icon=":material/sentiment_very_satisfied:")
dotypos = st.Page("navigation_pages/dotypos.py", title="System kasowy", icon=":material/point_of_sale:")
income = st.Page("navigation_pages/income.py", title="Przychody", icon=":material/money_bag:")
vouchers = st.Page("navigation_pages/vouchers.py", title="Vouchery", icon=":material/local_activity:")

pages_by_role = {
  "super-admin": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page, reservations_cumulative_page, dotypos, income, vouchers],
    "Opinie": [google_reviews_page, reviews_page],
    "Admin Panel": [data_editor_page],
    "Konto": [logout_page]
  },
  "admin": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page, reservations_cumulative_page, dotypos, income, vouchers],
    "Opinie": [google_reviews_page, reviews_page],
    "Admin Panel": [data_editor_page],
    "Konto": [logout_page]
  },
  "manager": {
    "Rezerwacje": [reservations_page, clients_page, reservations_by_time_period_page, dotypos, income, vouchers],
    "Opinie": [google_reviews_page, reviews_page],
    "Konto": [logout_page]
  }
}

pg = st.navigation(pages_by_role[st.session_state.role])
pg.run()
