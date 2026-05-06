import sys
import streamlit as st
import pandas as pd
import requests
import json
from datetime import date, timedelta, datetime, timezone
import calendar
import utils

from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Warsaw")

sys.path.append("shared")

if "location_select" not in st.session_state:
    st.session_state.location_select = ["Wszystkie"]

def ensure_status():
  if (not st.session_state.location_select):
    st.session_state.location_select = ["Wszystkie"]
    return

  if st.session_state.location_select[0] == "Wszystkie":
    st.session_state.location_select = st.session_state.location_select[1:]
  elif st.session_state.location_select[-1] == "Wszystkie":
    st.session_state.location_select = ["Wszystkie"]

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
  },
  {
    "label": "Warszawa BOX - arkadia",
    "value": {
      "safi_id": "019bc67a-793e-705f-99db-3ee07379f1e1",
      "dotypos_cloud_id": 359334480,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_359334480"),
    }
  },
  {
    "label": "Bydgoszcz Szajnochy",
    "value": {
      "safi_id": "019c32dd-e660-7073-8969-b350de2f45c9",
      "dotypos_cloud_id": 386402827,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_386402827"),
    }
  },
  {
    "label": "Wrocław Świdnicka",
    "value": {
      "safi_id": "019c32dd-e660-7073-8969-b350de2f45c9",
      "dotypos_cloud_id": 357162453,
      "dotypos_refresh_token": st.secrets["dotypos"].get("REFRESH_TOKEN_357162453"),
    }
  }
]

def get_safi_data(iso_start, iso_end):

    cities_sum = {
        "Suma NETTO Wszystkich lokacji": 0
    }

    safi_locations_ids = []

    for location in safi_locations:
        safi_locations_ids.append(location["value"]["safi_id"])

    safi_locations_ids = ",".join(safi_locations_ids)

    safi_auth_token = st.secrets["safi"].get("auth_token")


    url = "https://safi-api.pixel-xl.tech:9999/api/receipts"

    params = {
        "created_date_from": iso_start,
        "created_date_to": iso_end,
        "location_ids": safi_locations_ids
    }

    headers = {
        "Authorization": f"Bearer {safi_auth_token}"
    }

    response = requests.get(url, params=params, headers=headers)

    data = response.json()
    response.raise_for_status()

    online_sales = {
        "Wszystkie": []
    }

    for receipt in data:
        if receipt['status'] != "CONFIRMED":
            continue
        request_data_parsed = json.loads(receipt["request_data"])
        tax_rates = request_data_parsed["eReceipt"]["metadata"]["taxRates"]

        city_label = next(location["label"] for location in safi_locations if location["value"]["safi_id"] == receipt["location_id"])

        lines = request_data_parsed["eReceipt"]["lines"]

        utc_updated_at = datetime.fromisoformat(receipt.get("updated_at").replace("Z", "+00:00")).astimezone(USER_TZ)
        parsed_utc_updated_at = utc_updated_at.strftime("%Y-%m-%d")

        for line in lines:
            tax_rate = int(tax_rates[line["taxRate"]])
            discounts = line.get("rebatesMarkups")
            # negative value
            discount_value = sum(d["value"] / 100 for d in discounts) if discounts else 0
            total_tax_value = (line["totalLineValue"] / 100) * tax_rate / (100 + tax_rate)
            unit_tax_value = (line["unitPrice"] / 100) / (1+(tax_rate / 100)) * (tax_rate / 100)

            sale_data = {
                "produkt": line["productOrServiceName"],
                "ilość zakupionych produktów": line["quantity"],
                "cena jednostkowa brutto": round(line["unitPrice"] / 100, 2),
                "cena jednostkowa netto": round((line["unitPrice"] / 100) / (1+(tax_rate / 100)), 2),
                "wartość brutto": round((line["totalLineValue"] / 100), 2),
                "wartość netto": round((line["totalLineValue"] / 100) / (1 +( tax_rate / 100 )), 2),
                "kwota podatku": round(total_tax_value, 2),
                "kwota jednostkowa podatku": round(unit_tax_value, 2),
                "kwota obniżki": round(discount_value, 2),
                "wartość brutto po obniżce": round(((line["totalLineValue"] / 100) + discount_value), 2),
                "wartość netto po obniżce": round(((line["totalLineValue"] / 100) + discount_value) / (1 + ( tax_rate / 100 )), 2),
                "stawka VAT": tax_rate,
                "link do eparagonu": receipt.get("document_url"),
                "data wystawienia paragonu": parsed_utc_updated_at,
                "numer rezerwacji": receipt["reservation_number"],
                "lokalizacja": city_label,
                "typ przychodu": "online - safi",
                "atrakcja": receipt["visit_name"]
            }

            if f"Suma NETTO {city_label}" not in cities_sum:
                cities_sum[f"Suma NETTO {city_label}"] = 0
            cities_sum["Suma NETTO Wszystkich lokacji"] += sale_data["wartość netto po obniżce"]
            cities_sum[f"Suma NETTO {city_label}"] += sale_data["wartość netto po obniżce"]

            if city_label not in online_sales:
                online_sales[city_label] = []

            online_sales[city_label].append(sale_data)
            online_sales["Wszystkie"].append(sale_data)

    for key, value in cities_sum.items():
        st.write(key, f"{value:,.2f} PLN")

    utils.download_button(online_sales, f"raport_finansowy_safi_{start_date}-{end_date}", label="Pobierz raport safi .xlsx")
    utils.download_button({"Wszystkie_csv": online_sales["Wszystkie"]}, f"raport_finansowy_safi_{start_date}-{end_date}", label="Pobierz raport safi .csv", format="csv")


def get_dotypos_data(iso_start, iso_end):

    orders = []
    branches_map = {}

    for selected_city in safi_locations:

        refresh_token = selected_city["value"].get("dotypos_refresh_token")
        cloud_id = selected_city["value"].get("dotypos_cloud_id")

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

        page = 1

        while page:
            res = get_orders(
                cloud_id=cloud_id,
                token=access_token,
                page=page,
                since_when=iso_start,
                until_when=iso_end
            )

            data = res.get("data", [])
            if data:
                orders.extend(data)

            page = res.get("nextPage")

        branches = get_branches(cloud_id, access_token)
        for branch in branches.get("data"):
            branches_map[branch.get("id")] = branch.get("name")

    order_items_data = []

    for order in orders:
        for item in order.get("orderItems", []):
            item_data = item.copy()
            item_data["documentNumber"] = order.get("documentNumber")
            item_data["status"] = order.get("status")
            item_data["documentType"] = order.get("documentType")
            item_data["paid"] = order.get("paid")
            item_data["branch"] = branches_map.get(order.get("_branchId"))
            item_data["location"] = next(location["label"] for location in safi_locations if location["value"].get("dotypos_cloud_id") == int(item.get("_cloudId")))
            order_items_data.append(item_data)

    df_order_items = pd.DataFrame(order_items_data)

    df_dotypos_export = {}

    if len(df_order_items) == 0:
        st.write("Brak danych w tym okresie")
    else:
        df_order_items["quantity"] = pd.to_numeric(df_order_items["quantity"], errors="coerce")

        filtered_order_items = df_order_items[(df_order_items['paid'] == True) & (df_order_items['documentType'] == "RECEIPT") & (df_order_items['canceledDate'].isna()) & (df_order_items['quantity'] >= 0)]

        filtered_order_items["totalPriceWithoutVat"] = pd.to_numeric(
            filtered_order_items["totalPriceWithoutVat"], errors="coerce"
        ).round(2)

        total_billed = filtered_order_items["totalPriceWithoutVat"].sum()

        cut_dates = [pd.to_datetime(date).date() if date else None for date in filtered_order_items["completed"]]

        st.write(f"Suma NETTO dotykacka: {total_billed:,.2f} PLN")

        df_dotypos_export = pd.DataFrame({
            "nr paragonu": filtered_order_items["documentNumber"],
            "nazwa kasy": filtered_order_items["branch"],
            "typ przychodu": "kasa w lokalu",
            "data wystawienia paragonu": cut_dates,
            "cena jednostkowa netto": pd.to_numeric(
                filtered_order_items["billedUnitPriceWithoutVat"], errors="coerce"
            ).round(2),

            "cena jednostkowa brutto": pd.to_numeric(
                filtered_order_items["billedUnitPriceWithVat"], errors="coerce"
            ).round(2),
            "ilość zakupionych produktów": filtered_order_items["quantity"],
            "wartość netto": filtered_order_items["totalPriceWithoutVat"],
            "wartość brutto": pd.to_numeric(
                filtered_order_items["totalPriceWithVat"], errors="coerce"
            ),
            "stawka VAT": (
                (pd.to_numeric(filtered_order_items["vat"], errors="coerce") - 1) * 100
            ).round(0),
            "produkt": filtered_order_items["name"],
            "lokalizacja": filtered_order_items["location"]
        })

    utils.download_button({f"{start_date}-{end_date}": df_dotypos_export}, f"raport_finansowy_dotykacka_{start_date}-{end_date}", label="Pobierz raport dotykacka .xlsx")

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

dt_start = datetime.combine(
    start_date,
    datetime.min.time(),
    tzinfo=USER_TZ
)

utc_start = (
    dt_start
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
)

dt_end = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=USER_TZ)

utc_end = (
    dt_end
    .astimezone(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
)

st.divider()

safi, dotykacka = st.columns(2)

with dotykacka:
    @st.fragment
    def dotykacka_view():
      if st.button("Generuj raport dotykacka"):
          with st.spinner("Generowanie...", show_time=True):
              get_dotypos_data(utc_start, utc_end)
    dotykacka_view()

with safi:
    @st.fragment
    def safi_view():
      if st.button("Generuj raport safi"):
          with st.spinner("Generowanie...", show_time=True):
              get_safi_data(utc_start, utc_end)
    safi_view()
