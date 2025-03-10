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
                matching_notes = df_notes[(df_notes['date'] == date) & (df_notes['city'] == city)]
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
        df_grouped = df_grouped.merge(
            df_notes[['date', 'content', 'city']].rename(columns={'date': x_axis_type, 'content': 'note-content'}),
            how='left', on=x_axis_type
        )

    return df_grouped, reservations_rolling_averages, total_cost_rolling_averages, total_people_rolling_averages