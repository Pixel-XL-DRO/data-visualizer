import streamlit as st
import requests
import pandas as pd
import calendar
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import utils
import auth
import queries

USER_TZ = ZoneInfo("Europe/Warsaw")

BASE_URL = "https://safi-api.pixel-xl.tech:9999"

POLISH_MONTHS = [
  "styczeń", "luty", "marzec", "kwiecień", "maj", "czerwiec",
  "lipiec", "sierpień", "wrzesień", "październik", "listopad", "grudzień"
]

SAFI_LOCATIONS = {
  "krakow-lubicz": "01976898-679c-70e0-9b4f-dc2a14131e3d",
  "lodz-ogrodowa": "01988093-0fa0-731f-9ca0-b864decd2e94",
  "warszawa-kijowska": "019a1050-b96b-7032-baee-8a69101d49d4",
  "poznan-swietego-marcina": "019a39f1-045f-713a-834d-a66fb85287c5",
  "katowice-sokolska": "019ae347-fb95-73cd-84a3-5b2101273631",
  "gdansk-grunwaldzka": "019b3130-6834-7373-8b4b-c22d2b8b086a",
  "warszawa-arkadia": "019bc67a-793e-705f-99db-3ee07379f1e1",
  "wroclaw-swidnicka": "019c32dd-e660-7073-8969-b350de2f45c9",
  "bydgoszcz-szajnochy": "019c6612-ff2e-711a-9646-78e9d3054c68"
}

PURCHASED_COLUMNS = {
  "voucher_code": "numer kodu vouchera",
  "net_amount": "kwota netto",
  "vat_tax": "podatek VAT",
  "vat_rate": "stawka VAT",
  "gross_amount": "kwota brutto",
  "purchase_date": "data zakupu",
  "cancellation_date": "data anulowania",
  "receipt_link": "nr paragonu",
  "document_url": "eparagony_link",
  "invoice_number": "nr faktury",
  "safi_order_number": "nr zamówienia Safi",
  "payment_type": "typ_platnosci",
  "email": "email",
  "first_name": "imię",
  "last_name": "nazwisko",
}

CANCELLED_COLUMNS = {
  "voucher_code": "numer kodu vouchera",
  "net_amount": "kwota netto",
  "vat_tax": "podatek VAT",
  "vat_rate": "stawka VAT",
  "gross_amount": "kwota brutto",
  "purchase_date": "data zakupu",
  "receipt_link": "nr paragonu",
  "document_url": "eparagony_link",
  "invoice_number": "nr faktury",
  "safi_order_number": "nr zamówienia Safi",
  "payment_type": "typ_platnosci",
  "email": "email",
  "first_name": "imię",
  "last_name": "nazwisko",
}

CANCELLATION_DATE_CANDIDATES = ["cancellation_date", "cancelled_at", "status_date"]

PAYMENT_TYPE_LABELS = {
  "ONLINE": "online",
  "ON_SPOT": "na miejscu",
  "DEFERRED": "odroczona",
}

REDEEMED_COLUMNS = {
  "voucher_code": "numer kodu vouchera",
  "net_amount": "kwota netto",
  "vat_tax": "podatek VAT",
  "vat_rate": "stawka VAT",
  "gross_amount": "kwota brutto",
  "reservation_number": "numer rezerwacji",
  "reservation_date": "data odbycia się rezerwacji",
  "document_url": "eparagony_link",
  "safi_order_number": "nr zamówienia Safi",
  "payment_type": "typ_platnosci",
  "email": "email",
  "first_name": "imię",
  "last_name": "nazwisko",
}

EXPIRED_COLUMNS = {
  "voucher_code": "numer kodu vouchera",
  "net_amount": "kwota netto",
  "vat_tax": "podatek VAT",
  "vat_rate": "stawka VAT",
  "gross_amount": "kwota brutto",
  "expiry_date": "data wygaśnięcia",
  "receipt_link": "nr paragonu",
  "document_url": "eparagony_link",
  "invoice_number": "nr faktury",
  "order_number": "nr zamówienia",
  "safi_order_number": "nr zamówienia Safi",
  "payment_type": "typ_platnosci",
  "email": "email",
  "first_name": "imię",
  "last_name": "nazwisko",
}


def _fetch_voucher_report(path, utc_start, utc_end, location_ids):
  auth_token = st.secrets["safi"].get("auth_token")

  if not auth_token:
    return {"ok": False, "data": [], "error": "Brak autoryzacji"}

  params = {
    "from_dt": utc_start,
    "to_dt": utc_end,
    "location_ids": location_ids,
  }

  headers = {
    "Authorization": f"Bearer {auth_token}"
  }

  try:
    response = requests.get(f"{BASE_URL}{path}", params=params, headers=headers, timeout=30)
  except requests.RequestException as e:
    return {"ok": False, "data": [], "error": str(e)}

  if response.status_code == 404:
    return {"ok": False, "data": [], "error": "Endpoint niedostępny (404)"}
  
  if not response.ok:
    return {"ok": False, "data": [], "error": f"Błąd API ({response.status_code})"}

  try:
    data = response.json()
  except ValueError:
    return {"ok": False, "data": [], "error": "Nieprawidłowa odpowiedź API"}

  if not isinstance(data, list):
    return {"ok": False, "data": [], "error": "Nieoczekiwany format odpowiedzi API"}

  return {"ok": True, "data": data, "error": None}


def fetch_vouchers_purchased(utc_start, utc_end, location_ids):
  return _fetch_voucher_report("/api/get-vouchers-purchased", utc_start, utc_end, location_ids)


def fetch_vouchers_cancelled(utc_start, utc_end, location_ids):
  return _fetch_voucher_report("/api/get-vouchers-cancelled", utc_start, utc_end, location_ids)


def fetch_vouchers_redeemed(utc_start, utc_end, location_ids):
  return _fetch_voucher_report("/api/get-vouchers-redeemed", utc_start, utc_end, location_ids)


def fetch_vouchers_expired(utc_start, utc_end, location_ids):
  return _fetch_voucher_report("/api/get-vouchers-expired", utc_start, utc_end, location_ids)


def _render_section(title, result, column_map, file_name, note=None):
  st.subheader(title)

  if not result["ok"]:
    st.warning(f"Brak danych — {result['error']}")
    return

  if not result["data"]:
    st.info("Brak danych w tym okresie")
    return

  if note:
    st.caption(note)

  df = pd.DataFrame(result["data"])

  for cents_col in ("net_amount", "vat_tax", "gross_amount"):
    if cents_col in df.columns:
      df[cents_col] = pd.to_numeric(df[cents_col], errors="coerce") / 100

  if "payment_type" in df.columns:
    df["payment_type"] = df["payment_type"].map(PAYMENT_TYPE_LABELS).fillna(df["payment_type"])

  present_cols = [key for key in column_map if key in df.columns]
  df = df[present_cols].rename(columns=column_map)

  utils.download_button({title: df}, file_name, label="Pobierz plik .xlsx")


@st.fragment
def render_purchased(result, start_date, end_date):
  _render_section(
    title="Vouchery zakupione",
    result=result,
    column_map=PURCHASED_COLUMNS,
    file_name=f"raport_voucherow_zakupione_{start_date}-{end_date}",
  )


@st.fragment
def render_cancelled(result, start_date, end_date):
  column_map = dict(CANCELLED_COLUMNS)
  note = None

  if result["ok"] and result["data"]:
    available_keys = result["data"][0].keys()
    detected = next((c for c in CANCELLATION_DATE_CANDIDATES if c in available_keys), None)
    if detected:
      column_map[detected] = "data anulowania"
    else:
      note = "Kolumna daty anulowania nieznana — do potwierdzenia z backendem"

  _render_section(
    title="Vouchery anulowane",
    result=result,
    column_map=column_map,
    file_name=f"raport_voucherow_anulowane_{start_date}-{end_date}",
    note=note,
  )


@st.fragment
def render_redeemed(result, start_date, end_date):
  _render_section(
    title="Vouchery zrealizowane",
    result=result,
    column_map=REDEEMED_COLUMNS,
    file_name=f"raport_voucherow_zrealizowane_{start_date}-{end_date}",
  )


@st.fragment
def render_expired(result, start_date, end_date):
  _render_section(
    title="Vouchery, które straciły ważność",
    result=result,
    column_map=EXPIRED_COLUMNS,
    file_name=f"raport_voucherow_wygasle_{start_date}-{end_date}",
  )


def view():

  with st.spinner(""):
    df = queries.get_initial_data()
    df = auth.filter_locations(df)
    locations = sorted(set(f"{city}-{street}" for city, street in df[["city", "street"]].values))

  today = datetime.now().date()
  CURRENT_YEAR = today.year
  CURRENT_MONTH = today.month
  MIN_YEAR = 2025

  previous_month = today.month - 1 if today.month > 1 else 12
  year_of_previous_month = today.year if previous_month != 12 else today.year - 1
  previous_month_start = date(year_of_previous_month, previous_month, 1)
  previous_month_end = date(
    year_of_previous_month,
    previous_month,
    calendar.monthrange(year_of_previous_month, previous_month)[1]
  )

  mode_col, month_col, year_col = st.columns(3)

  with mode_col:
    mode = st.selectbox("Tryb", ["Miesiąc", "Zakres"])

  if mode == "Miesiąc":
    with year_col:
      year = st.selectbox(
        "Rok",
        list(range(MIN_YEAR, CURRENT_YEAR + 1)),
        index=year_of_previous_month - MIN_YEAR
      )

    with month_col:
      month = st.selectbox(
        "Miesiąc",
        list(range(1, 13)),
        format_func=lambda m: POLISH_MONTHS[m - 1],
        index=(CURRENT_MONTH - 2 if CURRENT_MONTH - 2 >= 0 else 11)
      )

    start_date = date(year, month, 1)
    end_date = date(year, month, calendar.monthrange(year, month)[1])

  else:
    col1, col2 = st.columns(2)

    with col1:
      start_date = st.date_input("Od kiedy", value=previous_month_start, key="start_date")

    with col2:
      end_date = st.date_input("Do kiedy", value=previous_month_end, key="end_date")

  dt_start = datetime.combine(start_date, datetime.min.time(), tzinfo=USER_TZ)
  utc_start = dt_start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

  dt_end = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=USER_TZ)
  utc_end = dt_end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

  if utc_end < utc_start:
    st.warning("Data początku musi być wcześniejsza niż data końca")
    return

  if st.button("Generuj raport"):
    safi_location_ids = ",".join(SAFI_LOCATIONS[loc] for loc in locations if loc in SAFI_LOCATIONS)

    with st.spinner("Ładowanie danych...", show_time=True):
      purchased, cancelled, redeemed, expired = utils.run_in_parallel(
        (fetch_vouchers_purchased, (utc_start, utc_end, safi_location_ids)),
        (fetch_vouchers_cancelled, (utc_start, utc_end, safi_location_ids)),
        (fetch_vouchers_redeemed, (utc_start, utc_end, safi_location_ids)),
        (fetch_vouchers_expired, (utc_start, utc_end, safi_location_ids)),
      )

    st.caption('Zmiana zakresu dat wymaga ponownego kliknięcia "Generuj raport"')

    render_purchased(purchased, start_date, end_date)
    render_cancelled(cancelled, start_date, end_date)
    render_redeemed(redeemed, start_date, end_date)
    render_expired(expired, start_date, end_date)


view()
