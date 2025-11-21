import pandas as pd
import utils
import datetime as dt
from datetime import timedelta
import streamlit as st
import online_queries
import pos_queries
import vouchers_income_queries

@st.cache_data(ttl=28800)
def get_total_income(group_by, moving_average_days, start, end, date_type, cities, languages, attractions, status, attraction_types, filter_checkbox):

  df, df_pos, df_voucher = utils.run_in_parallel(
    (online_queries.get_online_income, (group_by, moving_average_days, start, end, date_type, cities, languages, attractions, status, attraction_types)),
    (pos_queries.get_pos_income, (start, end, cities, filter_checkbox, moving_average_days, group_by)),
    (vouchers_income_queries.get_voucher_income, (group_by, moving_average_days, start, end, cities))
  )
  
  df_all = pd.concat([df, df_pos, df_voucher], ignore_index=True)
  
  df_final = df_all.groupby(["date", group_by] if group_by else "date", as_index=False).agg({
    "price": "sum",      
  })
  
  if group_by:

    df_final['price_ma'] = df_final.groupby(group_by)['price'].transform(
      lambda x: x.rolling(moving_average_days + 1, min_periods=moving_average_days + 1).mean()
    )
  else:

    df_final['price_ma'] = df_final['price'].rolling(
      moving_average_days + 1, min_periods=moving_average_days + 1).mean()

  return df_final

@st.cache_data(ttl=28800)
def get_total_income_by_period(grouping_period, start, end, date_type, cities, languages, attractions, status, attraction_types, filter_checkbox):

  df, df_pos, df_voucher = utils.run_in_parallel(
    (online_queries.get_online_income_by_time_period, (date_type, start, end, status, cities, languages, attractions, attraction_types, grouping_period)),
    (pos_queries.get_pos_income_by_period, (grouping_period, start, end, cities, filter_checkbox)),
    (vouchers_income_queries.get_vouchers_by_weekday, (grouping_period, start, end, cities)),
  )

  df_all = pd.concat([df, df_pos, df_voucher], ignore_index=True)
  
  df_final = df_all.groupby(["period"], as_index=False, sort=False).agg({
    "avg_count": "sum",
    "current_period": "first"     
  })

  return df_final

@st.cache_data(ttl=28800)
def get_total_cumulative_income(group_by, start, end, date_type, cities, languages, attractions, status, attraction_types, filter_checkbox):

  df, df_pos, df_voucher = utils.run_in_parallel(
    (online_queries.get_online_income_cumulative, (group_by, start, end, date_type, cities, languages, attractions, status, attraction_types)),
    (pos_queries.get_pos_cumulative_income, (start, end, cities, filter_checkbox, group_by)),
    (vouchers_income_queries.get_voucher_cumulative_income, (group_by, start, end, cities)),
  )

  keys = ["date", "price", group_by]

  if df.empty:
     fill_empty_df(df, keys)
  if df_pos.empty:
     fill_empty_df(df_pos, keys)
  if df_voucher.empty:
     fill_empty_df(df_voucher, keys)

  
  df_all = df.merge(df_pos, on=["date", group_by] if group_by else "date", how="outer", suffixes=("", "_pos")).merge(df_voucher, on=["date", group_by] if group_by else "date", how="outer", suffixes=("", "_voucher"))

  sort_cols = ["date", group_by] if group_by else ["date"]
  df_all = df_all.sort_values(by=sort_cols)

  price_cols = ['price', 'price_pos', 'price_voucher']

  if group_by:
      for col in price_cols:   
          df_all[col] = df_all.groupby(group_by)[col].ffill()
  else:
      for col in price_cols:
          df_all[col] = df_all[col].ffill()

  df_all["price_pos"] = df_all.loc[df_all["date"] >= dt.date(2025, 2, 1), "price_pos"]
  df_all['total_price'] = df_all[price_cols].sum(axis=1)

  return df_all


def fill_empty_df(df, keys):
  for key in keys:
        if key not in df.columns:
            df[key] = pd.NA
  return df   