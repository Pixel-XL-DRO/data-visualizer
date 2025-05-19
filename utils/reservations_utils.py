import pandas as pd

def group_data_and_calculate_moving_average(df, df_notes, x_axis_type, moving_average_days, grouping_field):

    reservations_rolling_averages = []
    total_cost_rolling_averages = []
    total_people_rolling_averages = []

    if not df_notes['date'].empty:
        df_notes['date'] = df_notes['date'].dt.to_period('D')

    if grouping_field:

        df_grouped = df.groupby([df[x_axis_type].dt.to_period('D'), df[grouping_field]]).agg(
            reservations=('id', 'count'),
            total_cost=('whole_cost_with_voucher', 'sum'),
            total_people=('no_of_people', 'sum'),
        ).reset_index()

        for value in df_grouped[grouping_field].unique():
            group_data = df_grouped[df_grouped[grouping_field] == value]
            reservations_rolling_averages.append(group_data['reservations'].rolling(window=moving_average_days).mean())
            total_cost_rolling_averages.append(group_data['total_cost'].rolling(window=moving_average_days).mean())
            total_people_rolling_averages.append(group_data['total_people'].rolling(window=moving_average_days).mean())

        if grouping_field == 'city':

            for index, row in df_grouped.iterrows():
                date = row[x_axis_type]
                city = row[grouping_field]
                matching_notes = df_notes[(df_notes['date'] == date) & (df_notes['note_city'] == city)]
                if not matching_notes.empty:
                    df_grouped.at[index, 'note-content'] = " | ".join(matching_notes['content'].dropna())

    else:

        df_grouped = df.groupby(df[x_axis_type].dt.to_period('D')).agg(
            reservations=('id', 'count'),
            total_cost=('whole_cost_with_voucher', 'sum'),
            total_people=('no_of_people', 'sum'),
        ).reset_index()

        reservations_rolling_averages.append(df_grouped['reservations'].rolling(window=moving_average_days).mean())
        total_cost_rolling_averages.append(df_grouped['total_cost'].rolling(window=moving_average_days).mean())
        total_people_rolling_averages.append(df_grouped['total_people'].rolling(window=moving_average_days).mean())

    if not grouping_field == 'city':
        new_df_grouped = []

        for _, group_row in df_grouped.iterrows():
            date_value = group_row[x_axis_type]
            matching_notes = df_notes[df_notes['date'] == date_value]

            if matching_notes.empty:
                group_row['note-content'] = None
                group_row['note_city'] = None
                new_df_grouped.append(group_row)
            else:
                for _, note_row in matching_notes.iterrows():
                    new_row = group_row.copy()
                    new_row['note-content'] = note_row['content']
                    new_row['city'] = note_row['note_city']
                    new_df_grouped.append(new_row)

        df_grouped = pd.DataFrame(new_df_grouped)

    return df_grouped, reservations_rolling_averages, total_cost_rolling_averages, total_people_rolling_averages

def calculate_reservations_ahead(df):

    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['booked_date'] = pd.to_datetime(df['booked_date'], errors='coerce')

    df = df.dropna(subset=['start_date', 'booked_date'])

    df['days'] = (df['start_date'] - df['booked_date']).dt.days
    df = df[df['days'] >= 0]

    df_grouped = df.groupby(['city', 'days']).size().reset_index(name='reservations')

    df_grouped['weighted_days'] = df_grouped['days'] * df_grouped['reservations']
    df_mean = df_grouped.groupby('city').agg(
        total_reservations=('reservations', 'sum'),
        total_weighted_days=('weighted_days', 'sum')
    ).reset_index()

    df_mean['mean_days'] = df_mean['total_weighted_days'] / df_mean['total_reservations']
    return df_mean[['city', 'mean_days']]

def calculate_reservations_ahead_by_city(df, city, period):
    df = df[df['city'] == city]

    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['booked_date'] = pd.to_datetime(df['booked_date'], errors='coerce')
    df = df.dropna(subset=['start_date', 'booked_date'])

    df['days'] = (df['start_date'] - df['booked_date']).dt.days
    df = df[df['days'] >= 0]

    df_period = df[df['days'] <= period]
    df_after = df[df['days'] > period]

    in_period = df_period.groupby('days').size().reset_index(name='reservations')
    after_sum = df_after.shape[0]
    after_period = pd.DataFrame([[f'{period}+', after_sum]], columns=['days', 'reservations'])
    df_grouped = pd.concat([in_period, after_period], ignore_index=True)

    df_grouped['days'] = df_grouped['days'].astype(str)

    return df_grouped
