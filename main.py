import sys

sys.path.append("shared")

import streamlit as st
import auth

st.set_page_config(layout="wide")
st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>', unsafe_allow_html=True)

def logout():
  if st.button("Wyloguj się"):
    st.logout()

auth.authorize(["admin", "call-center", "manager", "super-admin", "marketing"])

logout_page = st.Page(logout, title="Wyloguj się", icon=":material/logout:")

reservations_page = st.Page("navigation_pages/reservations.py", title="Rezerwacje", icon=":material/calendar_month:")
clients_page = st.Page("navigation_pages/clients.py", title="Klienci", icon=":material/people:")
boards_occupancy_page = st.Page("navigation_pages/boards_occupancy/boards_occupancy.py", title="Zajętość mat", icon=":material/flex_wrap:")
reservations_by_time_period_page = st.Page("navigation_pages/reservations_by_time_period.py", title="Rezerwacje po okresie", icon=":material/history:")
reservations_cumulative_page = st.Page("navigation_pages/reservations_cumulative.py", title="Rezerwacje kumulacyjne", icon=":material/timeline:")
data_editor_page = st.Page("navigation_pages/data_editor.py", title="Edytor danych", icon=":material/edit:")
google_reviews_page = st.Page("navigation_pages/google_reviews.py", title="Opinie Google", icon=":material/reviews:")
reviews_page = st.Page("navigation_pages/reviews.py", title="NPS", icon=":material/sentiment_very_satisfied:")
dotypos = st.Page("navigation_pages/dotypos.py", title="System kasowy", icon=":material/point_of_sale:")
income = st.Page("navigation_pages/income.py", title="Przychody", icon=":material/money_bag:")
vouchers = st.Page("navigation_pages/vouchers.py", title="Vouchery", icon=":material/local_activity:")
financial_report_page = st.Page("navigation_pages/reports/financial_report.py", title = "Raport finansowy", icon=":material/attach_money:")
safi_products_page = st.Page("navigation_pages/safi_products.py", title="Produkty dodatkowe", icon=":material/add_shopping_cart:")
voucher_report_page = st.Page("navigation_pages/reports/voucher_report.py", title = "Raport kodów promocyjnych", icon=":material/sell:")
voucher_accounting_report_page = st.Page("navigation_pages/reports/voucher_accounting_report.py", title="Raport voucherów – księgowość", icon=":material/receipt_long:")
reservations_for_year_page = st.Page("navigation_pages/reports/visits_report.py", title = "Rezerwacje z fakturą", icon=":material/vertical_split:")
bok_income_report = st.Page("navigation_pages/reports/bok_income_report.py", title = "Źródła tworzenia rezerwacji", icon=":material/note_add:")
clients_report = st.Page("navigation_pages/reports/clients_report.py", title = "Klienci", icon=":material/groups_3:")
not_started_reservations_report = st.Page("navigation_pages/reports/not_started_reservations_report.py", title = "Raport rezerwacji nieodbytych", icon=":material/event_busy:")
boards_occupancy_time_period_report = st.Page("navigation_pages/reports/boards_occupancy_time_period_report.py", title="Zajętość mat — okresy", icon=":material/bar_chart:")
reservations_report = st.Page("navigation_pages/reports/reservations_report.py", title = "Raport roczny rezerwacji", icon=":material/event_repeat:")
reservations_daily_report_page = st.Page("navigation_pages/reports/reservations_daily_report.py", title = "Raport dzienny rezerwacji", icon=":material/event_note:")

pages_by_role = {
  "super-admin": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page, reservations_cumulative_page],
    "Raporty": [financial_report_page, reservations_for_year_page, voucher_report_page, voucher_accounting_report_page, clients_report, not_started_reservations_report, bok_income_report, boards_occupancy_time_period_report, reservations_daily_report_page],
    "Sprzedaż": [dotypos, income, vouchers, safi_products_page],
    "Opinie": [google_reviews_page, reviews_page],
    "Admin Panel": [data_editor_page],
    "Konto": [logout_page]
  },
  "call-center": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page, reservations_cumulative_page],
    "Raporty": [bok_income_report, reservations_report],
    "Konto": [logout_page]
  },
  "admin": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page, reservations_cumulative_page],
    "Sprzedaż": [dotypos, income, vouchers, safi_products_page],
    "Raporty": [voucher_report_page, voucher_accounting_report_page, reservations_for_year_page, clients_report, bok_income_report, reservations_report, reservations_daily_report_page],
    "Opinie": [google_reviews_page, reviews_page],
    "Admin Panel": [data_editor_page],
    "Konto": [logout_page]
  },
  "manager": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page],
    "Sprzedaż": [dotypos, income, vouchers, safi_products_page],
    "Raporty": [voucher_report_page],
    "Opinie": [google_reviews_page, reviews_page],
    "Konto": [logout_page]
  },
  "marketing": {
    "Rezerwacje": [reservations_page, clients_page, boards_occupancy_page, reservations_by_time_period_page],
    "Sprzedaż": [vouchers, safi_products_page],
    "Raporty": [voucher_report_page],
    "Opinie": [google_reviews_page, reviews_page],
    "Konto": [logout_page]
  }
}

pg = st.navigation(pages_by_role[st.session_state.role])
pg.run()
