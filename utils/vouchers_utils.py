def group_data(df):

    df = df[df['voucher_name'] != 'voucher kwotowy.'] # remove this cause this is (propably) invalid entry

    df_grouped = df.groupby(df['voucher_name']).agg(
        count=('id', 'count'),
    ).reset_index()

    return df_grouped