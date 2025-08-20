import pandas as pd

def group_data_cumulative(df, df_dotypos, dfv, moving_average_days, grouping_field, start_date, end_date):
    df_dotypos['brutto'] = pd.to_numeric(df_dotypos['brutto'], errors='coerce')
    df_dotypos['date'] = pd.to_datetime(df_dotypos['start_date']).dt.date
    df['date'] = pd.to_datetime(df['booked_date']).dt.date
    dfv['date'] = pd.to_datetime(dfv['creation_date']).dt.date

    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    # df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    # df_dotypos = df_dotypos[(df_dotypos['date'] >= start_date) & (df_dotypos['date'] <= end_date)]

    if grouping_field:


        if grouping_field != 'city':

            df_online = df.groupby(['date', grouping_field]).agg(
                total_online_sum=('whole_cost_with_voucher', 'sum')
            ).reset_index()
            df_online['cumsum_online'] = df_online.groupby(grouping_field)['total_online_sum'].cumsum()
            df_online['total_online_sum_ma'] = df_online.groupby(grouping_field)['total_online_sum'].transform(
                lambda x: x.rolling(moving_average_days, min_periods=1).mean()
            )

            df_voucher = dfv.groupby(['date', grouping_field]).agg(
                total_voucher_sum=('net_amount', 'sum')
            ).reset_index()

            df_voucher['cumsum_voucher'] = df_voucher.groupby(grouping_field)['total_voucher_sum'].cumsum()
            df_voucher['total_voucher_sum_ma'] = df_voucher.groupby(grouping_field)['total_voucher_sum'].transform(
                lambda x: x.rolling(moving_average_days, min_periods=1).mean()
            )

            df_pos = []
            df_total = []
        else:


            df_online = df.groupby(['date', grouping_field]).agg(
                total_online_sum=('whole_cost_with_voucher', 'sum')
            ).reset_index()

            df_pos = df_dotypos.groupby(['date', grouping_field]).agg(
                total_pos_sum=('brutto', 'sum')
            ).reset_index()

            df_voucher = dfv.groupby(['date', grouping_field]).agg(
                total_voucher_sum=('net_amount', 'sum')
            ).reset_index()

            df_total = pd.merge(df_online, df_pos, on=['date', grouping_field], how='outer')
            df_total = pd.merge(df_total, df_voucher, on=['date', grouping_field], how='outer')

            df_total['total_online_sum'] = df_total['total_online_sum'].fillna(0)
            df_total['total_pos_sum'] = df_total['total_pos_sum'].fillna(0)
            df_total['total_voucher_sum'] = df_total['total_voucher_sum'].fillna(0)
            df_total['total_reservations_sum'] = df_total['total_online_sum'] + df_total['total_pos_sum'] + df_total['total_voucher_sum']

            df_online['cumsum_online'] = df_online.groupby(grouping_field)['total_online_sum'].cumsum()
            df_pos['cumsum_pos'] = df_pos.groupby(grouping_field)['total_pos_sum'].cumsum()
            df_total['cumsum_total'] = df_total.groupby(grouping_field)['total_reservations_sum'].cumsum()
            df_voucher['cumsum_voucher'] = df_voucher.groupby(grouping_field)['total_voucher_sum'].cumsum()

            df_voucher['total_voucher_sum_ma'] = df_voucher.groupby(grouping_field)['total_voucher_sum'].transform(
                lambda x: x.rolling(moving_average_days, min_periods=1).mean()
            )

            df_online['total_online_sum_ma'] = df_online.groupby(grouping_field)['total_online_sum'].transform(
                lambda x: x.rolling(moving_average_days, min_periods=1).mean()
            )
            df_pos['total_pos_sum_ma'] = df_pos.groupby(grouping_field)['total_pos_sum'].transform(
                lambda x: x.rolling(moving_average_days, min_periods=1).mean()
            )
            df_total['total_reservations_sum_ma'] = df_total.groupby(grouping_field)['total_reservations_sum'].transform(
                lambda x: x.rolling(moving_average_days, min_periods=1).mean()
            )

    else:

        df_online = df.groupby('date').agg(
            total_online_sum=('whole_cost_with_voucher', 'sum')
        ).reset_index()

        df_pos = df_dotypos.groupby('date').agg(
            total_pos_sum=('brutto', 'sum')
        ).reset_index()

        df_voucher = dfv.groupby('date').agg(
            total_voucher_sum=('net_amount', 'sum')
        ).reset_index()

        df_total = pd.merge(df_online, df_pos, on='date', how='outer')
        df_total = pd.merge(df_total, df_voucher, on='date', how='outer')
        df_total['total_online_sum'] = df_total['total_online_sum'].fillna(0)
        df_total['total_pos_sum'] = df_total['total_pos_sum'].fillna(0)
        df_total['total_voucher_sum'] = df_total['total_voucher_sum'].fillna(0)
        df_total['total_reservations_sum'] = df_total['total_online_sum'] + df_total['total_pos_sum'] + df_total['total_voucher_sum']

        df_online['cumsum_online'] = df_online['total_online_sum'].cumsum()
        df_pos['cumsum_pos'] = df_pos['total_pos_sum'].cumsum()
        df_voucher['cumsum_voucher'] = df_voucher['total_voucher_sum'].cumsum()
        df_total['cumsum_total'] = df_total['total_reservations_sum'].cumsum()

        df_online['total_online_sum_ma'] = df_online['total_online_sum'].rolling(
            moving_average_days, min_periods=1).mean()
        df_pos['total_pos_sum_ma'] = df_pos['total_pos_sum'].rolling(
            moving_average_days, min_periods=1).mean()
        df_voucher['total_voucher_sum_ma'] = df_voucher['total_voucher_sum'].rolling(
            moving_average_days, min_periods=1).mean()
        df_total['total_reservations_sum_ma'] = df_total['total_reservations_sum'].rolling(
            moving_average_days, min_periods=1).mean()

        df_online = df_online[df_online['date'] >= start_date]
        df_online = df_online[df_online['date'] <= end_date]

        df_pos = df_pos[df_pos['date'] >= start_date]
        df_pos = df_pos[df_pos['date'] <= end_date]

        df_voucher = df_voucher[df_voucher['date'] >= start_date]
        df_voucher = df_voucher[df_voucher['date'] <= end_date]

        df_total = df_total[df_total['date'] >= start_date]
        df_total = df_total[df_total['date'] <= end_date]

    return df_online, df_pos, df_total, df_voucher

def average_by_weekday(df, df_dotypos, dfv, grouping_field, grouping_type, start_date, end_date):
    df['whole_cost_with_voucher'] = pd.to_numeric(df['whole_cost_with_voucher'], errors='coerce')
    df_dotypos['brutto'] = pd.to_numeric(df_dotypos['brutto'], errors='coerce')

    df['date'] = pd.to_datetime(df['booked_date']).dt.tz_localize(None)
    df_dotypos['date'] = pd.to_datetime(df_dotypos['creation_date']).dt.tz_localize(None)
    dfv['date'] = pd.to_datetime(dfv['date']).dt.tz_localize(None)

    df = df[df['date'] >= start_date]
    df = df[df['date'] <= end_date]
    df_dotypos = df_dotypos[df_dotypos['date'] >= start_date]
    df_dotypos = df_dotypos[df_dotypos['date'] <= end_date]
    dfv = dfv[dfv['date'] >= start_date]
    dfv = dfv[dfv['date'] <= end_date]

    current_date = pd.Timestamp.now().tz_localize(None)

    polish_months_map = {
        1: 'Styczeń', 2: 'Luty', 3: 'Marzec', 4: 'Kwiecień', 5: 'Maj', 6: 'Czerwiec',
        7: 'Lipiec', 8: 'Sierpień', 9: 'Wrzesień', 10: 'Październik', 11: 'Listopad', 12: 'Grudzień'
    }

    polish_weekdays_map = {
        'Monday': 'Poniedziałek', 'Tuesday': 'Wtorek', 'Wednesday': 'Środa', 'Thursday': 'Czwartek',
        'Friday': 'Piątek', 'Saturday': 'Sobota', 'Sunday': 'Niedziela'
    }

    if grouping_type == 'Godzina':
        df['group'] = df['date'].dt.hour
        df_dotypos['group'] = df_dotypos['date'].dt.hour
        dfv['group'] = dfv['date'].dt.hour
        df['period_key'] = df['date'].dt.date
        df_dotypos['period_key'] = df_dotypos['date'].dt.date
        dfv['period_key'] = dfv['date'].dt.date
    elif grouping_type == 'Dzień tygodnia':
        df['group'] = df['date'].dt.day_name()
        df_dotypos['group'] = df_dotypos['date'].dt.day_name()
        dfv['group'] = dfv['date'].dt.day_name()
        df['period_key'] = df['date'].dt.to_period('W')
        df_dotypos['period_key'] = df_dotypos['date'].dt.to_period('W')
        dfv['period_key'] = dfv['date'].dt.to_period('W')
        current_week = current_date.to_period('W')
        df = df[df['period_key'] < current_week]
        df_dotypos = df_dotypos[df_dotypos['period_key'] < current_week]
        dfv = dfv[dfv['period_key'] < current_week]
    elif grouping_type == 'Tydzien roku':
        df['group'] = df['date'].dt.isocalendar().week
        df_dotypos['group'] = df_dotypos['date'].dt.isocalendar().week
        dfv['group'] = dfv['date'].dt.isocalendar().week
        df['period_key'] = df['date'].dt.to_period('W')
        df_dotypos['period_key'] = df_dotypos['date'].dt.to_period('W')
        dfv['period_key'] = dfv['date'].dt.to_period('W')
        current_week = current_date.to_period('W')
        df = df[df['period_key'] < current_week]
        df_dotypos = df_dotypos[df_dotypos['period_key'] < current_week]
        dfv = dfv[dfv['period_key'] < current_week]
    elif grouping_type == 'Dzień miesiaca':
        df['group'] = df['date'].dt.day
        df_dotypos['group'] = df_dotypos['date'].dt.day
        dfv['group'] = dfv['date'].dt.day
        df['period_key'] = df['date'].dt.to_period('M')
        df_dotypos['period_key'] = df_dotypos['date'].dt.to_period('M')
        dfv['period_key'] = dfv['date'].dt.to_period('M')
        current_month = current_date.to_period('M')
        df = df[df['period_key'] < current_month]
        df_dotypos = df_dotypos[df_dotypos['period_key'] < current_month]
        dfv = dfv[dfv['period_key'] < current_month]
    elif grouping_type == 'Miesiac':
        df['group'] = df['date'].dt.month
        df_dotypos['group'] = df_dotypos['date'].dt.month
        dfv['group'] = dfv['date'].dt.month
        df['period_key'] = df['date'].dt.to_period('M')
        df_dotypos['period_key'] = df_dotypos['date'].dt.to_period('M')
        dfv['period_key'] = dfv['date'].dt.to_period('M')
        current_month = current_date.to_period('M')
        df = df[df['period_key'] < current_month]
        df_dotypos = df_dotypos[df_dotypos['period_key'] < current_month]
        dfv = dfv[dfv['period_key'] < current_month]
    elif grouping_type == 'Rok':
        df['group'] = df['date'].dt.year.astype(str)
        df_dotypos['group'] = df_dotypos['date'].dt.year.astype(str)
        dfv['group'] = dfv['date'].dt.year.astype(str)
        df['period_key'] = df['date'].dt.year
        df_dotypos['period_key'] = df_dotypos['date'].dt.year
        dfv['period_key'] = dfv['date'].dt.year
    else:
        raise ValueError(f"Unsupported grouping_type: {grouping_type}")

    online_periods = df['period_key'].nunique()
    pos_periods = df_dotypos['period_key'].nunique()
    voucher_periods = dfv['period_key'].nunique()
    total_periods = max(online_periods, pos_periods, voucher_periods)

    online_daily = df.groupby('group').agg(online_sum=('whole_cost_with_voucher', 'sum')).reset_index()
    pos_daily = df_dotypos.groupby('group').agg(pos_sum=('brutto', 'sum')).reset_index()
    voucher_daily = dfv.groupby('group').agg(voucher_sum=('net_amount', 'sum')).reset_index()

    daily = pd.merge(online_daily, pos_daily, on='group', how='outer').fillna(0)
    daily = pd.merge(daily, voucher_daily, on='group', how='outer').fillna(0)

    if grouping_type == 'Rok':
        total_periods = 1

    daily['total_period_count'] = total_periods
    daily['total_online_mean'] = daily['online_sum'] / total_periods
    daily['total_pos_mean'] = daily['pos_sum'] / total_periods
    daily['total_voucher_mean'] = daily['voucher_sum'] / total_periods
    daily['total_reservations_mean'] = (daily['online_sum'] + daily['pos_sum'] + daily['voucher_sum']) / total_periods

    if grouping_type == 'Dzień tygodnia':
        daily['group'] = daily['group'].map(polish_weekdays_map)
        weekday_order = ['Poniedziałek', 'Wtorek', 'Środa', 'Czwartek', 'Piątek', 'Sobota', 'Niedziela']
        daily['group'] = pd.Categorical(daily['group'], categories=weekday_order, ordered=True)
        daily = daily.sort_values('group')
    elif grouping_type == 'Miesiac':
        daily['group'] = daily['group'].map(polish_months_map)
        month_order = [
            'Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj', 'Czerwiec',
            'Lipiec', 'Sierpień', 'Wrzesień', 'Październik', 'Listopad', 'Grudzień'
        ]
        daily['group'] = pd.Categorical(daily['group'], categories=month_order, ordered=True)
        daily = daily.sort_values('group')
    elif grouping_type in ['Godzina', 'Dzień miesiaca', 'Tydzien roku', 'Rok']:
        daily = daily.sort_values('group')

    return daily