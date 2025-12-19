import sys
import streamlit as st
import pandas as pd
import queries
import auth
import requests
from datetime import date, timedelta, datetime, timezone
import calendar

sys.path.append("shared")
import utils


min_date = date(2025, 1, 1)

POLISH_MONTHS = [
    "styczeń", "luty", "marzec", "kwiecień", "maj", "czerwiec",
    "lipiec", "sierpień", "wrzesień", "październik", "listopad", "grudzień"
]

def get_dotypos_data(iso_start, iso_end):
    dotypos_wroclaw_swidnicka_refresh_token = "8ec03e5689fe792ad3cdbd5428f98f20"

    url = "https://api.dotykacka.cz/v2/signin/token"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"User {dotypos_wroclaw_swidnicka_refresh_token}",
    }

    payload = {
        "_cloudId": 357162453,
    }

    response = requests.post(url, headers=headers, json=payload)

    response.raise_for_status()

    res = response.json()
    access_token = res.get("accessToken")

    branches_map = {}

    branches = get_branches(357162453, access_token)
    for branch in branches.get("data"):
      branches_map[branch.get("id")] = branch.get("name")

    page = 1
    orders = []

    while page:
        res = get_orders(
            cloud_id=357162453,
            token=access_token,
            page=page,
            since_when=iso_start,
            until_when=iso_end
        )

        print(page)

        data = res.get("data", [])
        if data:
            orders.extend(data)

        page = res.get("nextPage")

    df_orders = pd.DataFrame(orders)

    order_items_data = []

    for order in orders:
        for item in order.get("orderItems", []):
            item_data = item.copy()
            item_data["documentNumber"] = order.get("documentNumber")
            item_data["status"] = order.get("status")
            item_data["documentType"] = order.get("documentType")
            item_data["paid"] = order.get("paid")
            item_data["branch"] = branches_map.get(order.get("_branchId"))
            order_items_data.append(item_data)

    df_order_items = pd.DataFrame(order_items_data)
    df_order_items["quantity"] = pd.to_numeric(df_order_items["quantity"], errors="coerce")

    filtered_order_items = df_order_items[(df_order_items['paid'] == True) & (df_order_items['documentType'] == "RECEIPT") & (df_order_items['canceledDate'].isna()) & (df_order_items['quantity'] >= 0)]

    return filtered_order_items

def get_branches(cloud_id, token):
    url = f"https://api.dotykacka.cz/v2/clouds/{cloud_id}/branches"

    headers = {
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(e)
        return {}


def get_orders(cloud_id, token, page, since_when=None, until_when=None):
    if since_when is None:
        since_when = datetime.fromtimestamp(0, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    url = f"https://api.dotykacka.cz/v2/clouds/{cloud_id}/orders"

    data_filter = f"completed|gteq|{since_when}"

    if until_when is not None:
      data_filter = data_filter + f";completed|lteq|{until_when}"

    params = {
        "limit": 200,
        "include": "orderItems",
        "page": page,
        "filter": data_filter
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(e)
        return {}

today = date.today()
CURRENT_YEAR = today.year
CURRENT_MONTH = today.month
MIN_YEAR = 2025

previous_month_start = date(today.year, today.month - 1, 1)
previous_month_end = date(
    today.year,
    today.month - 1,
    calendar.monthrange(today.year, today.month - 1)[1]
)

mode_col, year_col, month_col = st.columns(3)

with mode_col:
    mode = st.selectbox("Tryb", ["Miesiąc", "Zakres"])

if mode == "Miesiąc":
    with year_col:
        year = st.selectbox(
            "Rok",
            list(range(MIN_YEAR, CURRENT_YEAR + 1)),
            index=CURRENT_YEAR - MIN_YEAR
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
        start_date = st.date_input(
            "Od kiedy",
            value=previous_month_start,
            min_value=min_date,
            key="range_start"
        )

    with col2:
        end_date = st.date_input(
            "Do kiedy",
            value=previous_month_end,
            min_value=min_date + timedelta(days=1),
            key="range_end"
        )

dt_start = datetime.combine(start_date, datetime.min.time())
iso_start = dt_start.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

dt_end = datetime.combine(end_date, datetime.max.time())
iso_end =  dt_end.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

if st.button("Generuj raport"):
  with st.spinner("Generowanie...", show_time=True):
    filtered_order_items = get_dotypos_data(iso_start, iso_end)

    filtered_order_items["totalPriceWithoutVat"] = pd.to_numeric(
        filtered_order_items["totalPriceWithoutVat"], errors="coerce"
    ).round(2)

    total_billed = filtered_order_items["totalPriceWithoutVat"].sum()

    st.write(f"Suma kas na lokalu NETTO: {round(total_billed, 2)}")

    df_export = pd.DataFrame({
        "nr paragonu": filtered_order_items["documentNumber"],
        "nazwa kasy": filtered_order_items["branch"],
        "typ przychodu": "kasa w lokalu",
        "data sprzedaży": filtered_order_items["completed"],
        "kwota netto produktu": pd.to_numeric(
            filtered_order_items["billedUnitPriceWithoutVat"], errors="coerce"
        ).round(2),

        "kwota brutto produktu": pd.to_numeric(
            filtered_order_items["billedUnitPriceWithVat"], errors="coerce"
        ).round(2),
        "ilość zakupionych produktów": filtered_order_items["quantity"],
        "kwota netto całości": filtered_order_items["totalPriceWithoutVat"],
        "kwota brutto całości": pd.to_numeric(
            filtered_order_items["totalPriceWithVat"], errors="coerce"
        ),
        "stawka VAT": (
            (pd.to_numeric(filtered_order_items["vat"], errors="coerce") - 1) * 100
        ).round(0).astype("Int64").astype(str) + "%",
        "produkt": filtered_order_items["name"],
        "lokalizacja": "Wrocław"
    })

    utils.download_button(df_export, f"raport_finansowy_dotykacka_{start_date}-{end_date}")



url = "https://safi-api.pixel-xl.tech:9999/api/receipts"

params = {
    "created_date_from": "2025-11-01",
    "created_date_to": "2025-11-30"
}

headers = {
    "Authorization": "Bearer kekVav-5xygga-wikdor"
}

response = requests.get(url, params=params, headers=headers)

data = response.json()
response.raise_for_status()
st.write(data)



