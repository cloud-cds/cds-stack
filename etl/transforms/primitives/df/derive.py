import logging

def combine(df, new_fid, list_of_fids, keep_originals=True):
    to_change = df['fid'].isin(list_of_fids)
    if keep_originals:
        new_features = df[to_change].assign(fid = new_fid)
        df = df.append(new_features)
    else:
        df[to_change]['fid'] = new_fid
    return df

def derive_lab_status(df):
    def get_status(row):
        order = row['order_status']
        proc = row['proc_status']
        if row['order_status'] == '' and row['proc_status'] == '':
            return 'Signed'
        elif row['order_status'] == 'Sent' and row['proc_status'] == '':
            return 'Sent'
        else:
            return row['proc_status']

    df['status'] = df.apply(get_status, axis=1)
    df.drop(['order_status', 'proc_status'], axis=1, inplace=True)
    return df

def derive_lab_status_clarity(df):

    def get_status(row):
        order = row['order_status']
        proc = row['proc_status']
        if row['order_status'] == '' and row['proc_status'] == '':
            return 'Signed'
        elif row['order_status'] == 'Sent' and row['proc_status'] == '':
            return 'Sent'
        else:
            return row['proc_status']

    df['status'] = df.apply(get_status, axis=1)
    df.drop(['order_status', 'proc_status'], axis=1, inplace=True)

    return df
