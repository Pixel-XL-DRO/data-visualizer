import pandas as pd

def group_order_items(df, group_by, moving_average_days):

    # df = df[df['status'] != 'canceled']
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['brutto'] = pd.to_numeric(df['brutto'], errors='coerce')
    df['netto'] = pd.to_numeric(df['netto'], errors='coerce')

    df = df[df['document_number'] != '0']
    df_upsell = df

    df['date_only'] = df['creation_date'].dt.date
    df_upsell['date_only'] = df_upsell['creation_date'].dt.date

    if group_by:
        df_grouped = df.groupby(['date_only', df[group_by]]).agg(
            quantity=('quantity', 'sum'),
            brutto=('brutto', 'sum'),
            netto=('netto', 'sum'),
        ).reset_index()

        df_grouped = df_grouped.sort_values(['date_only', group_by])

        df_grouped['netto_rolling_avg'] = df_grouped.groupby(group_by)['netto'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

        df_grouped['brutto_rolling_avg'] = df_grouped.groupby(group_by)['brutto'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

        df_upsell_grouped = df_upsell.groupby(['date_only', df_upsell[group_by]]).agg(
            quantity=('quantity', 'sum'),
            brutto=('brutto', 'sum'),
            netto=('netto', 'sum'),
        ).reset_index()

        df_upsell_grouped['avg_netto_per_purchase'] = df_upsell_grouped.groupby(group_by)['netto'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).sum() /
                      df_upsell_grouped.groupby(group_by)['quantity'].transform(
                          lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).sum()))

        df_upsell_grouped['avg_brutto_per_purchase'] = df_upsell_grouped.groupby(group_by)['brutto'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).sum() /
                      df_upsell_grouped.groupby(group_by)['quantity'].transform(
                          lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).sum()))

    else:
        df_grouped = df.groupby('date_only').agg(
            quantity=('quantity', 'sum'),
            brutto=('brutto', 'sum'),
            netto=('netto', 'sum'),
        ).reset_index()

        df_grouped = df_grouped.sort_values('date_only')

        df_grouped['netto_rolling_avg'] = df_grouped['netto'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

        df_grouped['brutto_rolling_avg'] = df_grouped['brutto'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

        df_upsell_grouped = df_upsell.groupby('date_only').agg(
            quantity=('quantity', 'sum'),
            brutto=('brutto', 'sum'),
            netto=('netto', 'sum'),
        ).reset_index()

        df_upsell_grouped['avg_netto_per_purchase'] = (
            df_upsell_grouped['netto'].rolling(window=moving_average_days, min_periods=moving_average_days).sum() /
            df_upsell_grouped['quantity'].rolling(window=moving_average_days, min_periods=moving_average_days).sum()
        )

        df_upsell_grouped['avg_brutto_per_purchase'] = (
            df_upsell_grouped['brutto'].rolling(window=moving_average_days, min_periods=moving_average_days).sum() /
            df_upsell_grouped['quantity'].rolling(window=moving_average_days, min_periods=moving_average_days).sum()
        )

    df_upsell_grouped['brutto_by_quantity'] = df_upsell_grouped['brutto'] / df_upsell_grouped['quantity']
    df_upsell_grouped['netto_by_quantity'] = df_upsell_grouped['netto'] / df_upsell_grouped['quantity']

    return df_grouped, df_upsell_grouped

def calc_earnings_per_reservation(df_reservation, df, group_by, moving_average_days):

    df = df[df['status'] != 'canceled']

    df['brutto'] = pd.to_numeric(df['brutto'], errors='coerce')
    df['netto'] = pd.to_numeric(df['netto'], errors='coerce')

    df_upsell = df
    df_res = df_reservation
    df_upsell['start_date'] = pd.to_datetime(df_upsell['creation_date']).dt.date
    df_reservation['start_date'] = pd.to_datetime(df_reservation['start_date']).dt.date
    df_res['start_date'] = pd.to_datetime(df_reservation['booked_date']).dt.date

    if group_by:
        df_grouped = df_reservation.groupby(['start_date', df_reservation[group_by]]).agg(
            count=('id', 'count'),
        ).reset_index()

        df_res = df_res.groupby(['start_date', df_res[group_by]]).agg(
            total_reservations_value=('whole_cost_with_voucher', 'sum')
        ).reset_index()

        df_upsell_grouped = df_upsell.groupby(['start_date', df_upsell[group_by]]).agg(
            total_brutto=('brutto', 'sum'),
            total_netto=('netto', 'sum'),
        ).reset_index()

        df_result = pd.merge(df_grouped, df_upsell_grouped, on=['start_date', group_by], how='right')
        df_result = pd.merge(df_result, df_res, on=['start_date', group_by], how='left')

        df_result['mean_brutto_per_reservation'] = df_result['total_brutto'] / df_result['count']
        df_result['mean_netto_per_reservation'] = df_result['total_netto'] / df_result['count']

        df_result['count_rolling_avg'] = df_result.groupby(group_by)['count'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

        df_result['mean_brutto_per_reservation_rolling_avg'] = df_result.groupby(group_by)['mean_brutto_per_reservation'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

        df_result['mean_netto_per_reservation_rolling_avg'] = df_result.groupby(group_by)['mean_netto_per_reservation'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

        df_result['total_brutto_income'] = df_result['total_brutto'] + df_result['total_reservations_value']

        df_result['total_brutto_income_rolling_avg'] = df_result.groupby(group_by)['total_brutto_income'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean())

    else:
        df_grouped = df_reservation.groupby('start_date').agg(
            count=('id', 'count'),
        ).reset_index()

        df_res = df_res.groupby('start_date').agg(
            total_reservations_value=('whole_cost_with_voucher', 'sum')
        ).reset_index()

        df_upsell_grouped = df_upsell.groupby('start_date').agg(
            total_brutto=('brutto', 'sum'),
            total_netto=('netto', 'sum'),
        ).reset_index()

        df_result = pd.merge(df_grouped, df_upsell_grouped,  on='start_date', how='right')
        df_result = pd.merge(df_result, df_res, on=['start_date'], how='left')

        df_result = df_result.sort_values(by='start_date')

        df_result['mean_brutto_per_reservation'] = df_result['total_brutto'] / df_result['count']
        df_result['mean_netto_per_reservation'] = df_result['total_netto'] / df_result['count']

        df_result['count_rolling_avg'] = df_result['count'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

        df_result['mean_brutto_per_reservation_rolling_avg'] = df_result['mean_brutto_per_reservation'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

        df_result['mean_netto_per_reservation_rolling_avg'] = df_result['mean_netto_per_reservation'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

        df_result['total_brutto_income'] = df_result['total_brutto'] + df_result['total_reservations_value']
        df_result['total_brutto_income_rolling_avg'] = df_result['total_brutto_income'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

    return df_result

def calc_items(df, groupBy, moving_average_days):

    df = df[df['document_number'] != '0'] # we want to filter weird orders without doc number

    df_grouped_items = df

    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.date
    df['brutto'] = pd.to_numeric(df['brutto'], errors='coerce')
    df['netto'] = pd.to_numeric(df['netto'], errors='coerce')
    if groupBy:

        df_grouped_items = df_grouped_items.groupby(['name', groupBy]).agg(
            quantity=('quantity', 'sum'),
            brutto=('brutto', 'sum'),
            netto=('netto', 'sum'),
            mean_brutto=('brutto', 'mean'),
            mean_netto=('netto', 'mean'),
        ).reset_index()

        df_grouped = df.groupby(["creation_date", groupBy]).agg(
            quantity=('quantity', 'sum'),
            brutto=('brutto', 'sum'),
            netto=('netto', 'sum'),
            mean_brutto=('brutto', 'mean'),
            mean_netto=('netto', 'mean'),
        ).reset_index()

        df_grouped['quantity_ma'] = df_grouped.groupby(groupBy)['quantity'].transform(
            lambda x: x.rolling(window=moving_average_days, min_periods=moving_average_days).mean()
        )

    else:

        df_grouped_items = df_grouped_items.groupby(['name']).agg(
            quantity=('quantity', 'sum'),
            total_brutto=('brutto', 'sum'),
            total_netto=('netto', 'sum'),
            mean_brutto=('brutto', 'mean'),
            mean_netto=('netto', 'mean'),
        ).reset_index()


        df_grouped = df.groupby(["creation_date"]).agg(
            quantity=('quantity', 'sum'),
            total_brutto=('brutto', 'sum'),
            total_netto=('netto', 'sum'),
            mean_brutto=('brutto', 'mean'),
            mean_netto=('netto', 'mean'),
        ).reset_index()

        df_grouped['quantity_ma'] = df_grouped['quantity'].rolling(
            window=moving_average_days, min_periods=moving_average_days).mean()

        df_grouped_items = df_grouped_items.rename(columns={
        'name': 'Nazwa',
        'quantity': 'Ilość',
        'mean_brutto': 'Średnia brutto',
        'mean_netto': 'średnia netto',
        'total_brutto': 'Suma brutto',
        'total_netto': 'Suma netto',
    })

    return df_grouped, df_grouped_items