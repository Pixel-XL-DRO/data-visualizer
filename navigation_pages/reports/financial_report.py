import sys
import streamlit as st
import pandas as pd
import queries
import auth
import requests
import json
from datetime import date, timedelta, datetime, timezone
import calendar

sys.path.append("shared")
import utils


min_date = date(2025, 1, 1)

POLISH_MONTHS = [
    "styczeń", "luty", "marzec", "kwiecień", "maj", "czerwiec",
    "lipiec", "sierpień", "wrzesień", "październik", "listopad", "grudzień"
]

safi_locations = [
  {
    "label": "Kraków",
    "value": {
      "safi_id": "01976898-679c-70e0-9b4f-dc2a14131e3d",
      "dotypos_cloud_id": 347740963,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_347740963"),
    }
  },
  {
    "label": "Łódź",
    "value": {
      "safi_id": "01988093-0fa0-731f-9ca0-b864decd2e94",
      "dotypos_cloud_id": 386377536,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_386377536"),
    }
  },
  {
    "label": "Warszawa Kijowska",
    "value": {
      "safi_id": "019a1050-b96b-7032-baee-8a69101d49d4",
      "dotypos_cloud_id": 381567693,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_381567693"),
    }
  },
  {
    "label": "Poznań",
    "value": {
      "safi_id": "019a39f1-045f-713a-834d-a66fb85287c5",
      "dotypos_cloud_id": 355738408,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_355738408"),
    }
  },
  {
    "label": "Katowice",
    "value": {
      "safi_id": "019ae347-fb95-73cd-84a3-5b2101273631",
      "dotypos_cloud_id": 366525852,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_366525852"),
    }
  },
  {
    "label": "Gdańsk",
    "value": {
      "safi_id": "019b3130-6834-7373-8b4b-c22d2b8b086a",
      "dotypos_cloud_id": 321010692,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_321010692"),
    }
  }
]

def get_safi_data(iso_start, iso_end, city_label, safi_location_id, safi_auth_token):

    url = "https://safi-api.pixel-xl.tech:9999/api/receipts"

    params = {
        "created_date_from": iso_start,
        "created_date_to": iso_end,
        "location_id": safi_location_id
    }

    headers = {
        "Authorization": f"Bearer {safi_auth_token}"
    }

    response = requests.get(url, params=params, headers=headers)

    data = response.json()
    response.raise_for_status()


    online_sales = []

    for receipt in data:
      if receipt['status'] != "CONFIRMED":
        continue
      request_data_parsed = json.loads(receipt["request_data"])
      tax_rates = request_data_parsed["eReceipt"]["metadata"]["taxRates"]

      lines = request_data_parsed["eReceipt"]["lines"]

      for line in lines:
        tax_rate = int(tax_rates[line["taxRate"]])
        discounts = line.get("rebatesMarkups")

        # negative value
        discount_value = sum(d["value"] / 100 for d in discounts) if discounts else 0

        online_sales.append({
          "produkt": line["productOrServiceName"],
          "ilość zakupionych produktów": line["quantity"],
          "kwota brutto produktu": (line["unitPrice"] / 100),
          "kwota netto produktu": (line["unitPrice"] / 100) / (1+(tax_rate / 100)),
          "kwota brutto całości": (line["totalLineValue"] / 100),
          "kwota netto całości": (line["totalLineValue"] / 100) / (1 +( tax_rate / 100 )),
          "kwota obniżki": discount_value,
          "finalna kwota brutto": (line["totalLineValue"] / 100) + discount_value,
          "finalna kwota netto": ((line["totalLineValue"] / 100) + discount_value) / (1 +( tax_rate / 100 )),
          "stawka VAT": tax_rate,
          "link do eparagonu": receipt.get("document_url"),
          "data wystawienia paragonu": receipt.get("updated_at"),
          "id rezerwacji": receipt.get("reservation_id"),
          "lokalizacja": city_label,
          "typ przychodu": "online - safi",
        })

    df_safi_export = pd.DataFrame(online_sales)

    if len(df_safi_export) == 0:
      st.write("Brak danych w tym okresie")
    else:
      df_safi_export["finalna kwota netto"] = pd.to_numeric(
          df_safi_export["finalna kwota netto"], errors="coerce"
      ).round(2)

      total_billed = df_safi_export["finalna kwota netto"].sum()

      st.write(f"Suma NETTO safi: {round(total_billed, 2)}")

      utils.download_button(df_safi_export, f"raport_finansowy_safi_{start_date}-{end_date}", label="Pobierz raport safi .xlxs")


def get_dotypos_data(iso_start, iso_end, city_label, cloud_id, refresh_token):
    url = "https://api.dotykacka.cz/v2/signin/token"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"User {refresh_token}",
    }

    payload = {
        "_cloudId": cloud_id,
    }

    response = requests.post(url, headers=headers, json=payload)

    response.raise_for_status()

    res = response.json()
    access_token = res.get("accessToken")

    branches_map = {}

    branches = get_branches(cloud_id, access_token)
    for branch in branches.get("data"):
      branches_map[branch.get("id")] = branch.get("name")

    page = 1
    orders = []

    while page:
        res = get_orders(
            cloud_id=cloud_id,
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

    if len(df_order_items) == 0:
      st.write("Brak danych w tym okresie")
    else:
      df_order_items["quantity"] = pd.to_numeric(df_order_items["quantity"], errors="coerce")

      filtered_order_items = df_order_items[(df_order_items['paid'] == True) & (df_order_items['documentType'] == "RECEIPT") & (df_order_items['canceledDate'].isna()) & (df_order_items['quantity'] >= 0)]

      filtered_order_items["totalPriceWithoutVat"] = pd.to_numeric(
          filtered_order_items["totalPriceWithoutVat"], errors="coerce"
      ).round(2)

      total_billed = filtered_order_items["totalPriceWithoutVat"].sum()

      st.write(f"Suma NETTO dotykacka: {round(total_billed, 2)}")

      df_dotypos_export = pd.DataFrame({
          "nr paragonu": filtered_order_items["documentNumber"],
          "nazwa kasy": filtered_order_items["branch"],
          "typ przychodu": "kasa w lokalu",
          "data wystawienia paragonu": filtered_order_items["completed"],
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
          ).round(0),
          "produkt": filtered_order_items["name"],
          "lokalizacja": city_label
      })

      utils.download_button(df_dotypos_export, f"raport_finansowy_dotykacka_{start_date}-{end_date}", label="Pobierz raport dotykacka .xlxs")

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


previous_month = today.month - 1 if today.month > 1 else 12

previous_month_start = date(today.year, previous_month, 1)
previous_month_end = date(
    today.year,
    previous_month,
    calendar.monthrange(today.year, previous_month)[1]
)

mode_col, month_col, year_col = st.columns(3)

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
dotypos_start = dt_start.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
safi_start = dt_start.date().isoformat()

dt_end = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
dotypos_end = dt_end.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
safi_end = dt_end.date().isoformat()

selected = st.selectbox(
    "Wybierz lokalizacje",
    safi_locations,
    format_func=lambda x: x["label"]
)

st.divider()

safi, dotykacka = st.columns(2)

with dotykacka:
    if st.button("Generuj raport dotykacka"):
        with st.spinner("Generowanie...", show_time=True):
            get_dotypos_data(dotypos_start, dotypos_end, selected["label"], selected["value"].get("dotypos_cloud_id"), selected["value"].get("dotypos_refresh_token"))

with safi:
    if st.button("Generuj raport safi"):
        with st.spinner("Generowanie...", show_time=True):
            get_safi_data(safi_start, safi_end, selected["label"], selected["value"].get("safi_id"), st.secrets["safi"].get("auth_token"))
