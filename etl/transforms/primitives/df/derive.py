import logging
import etl.transforms.primitives.row.transform as transform

def combine(df, new_fid, list_of_fids, keep_originals=True):
    to_change = df['fid'].isin(list_of_fids)
    if keep_originals:
        new_features = df[to_change].assign(fid = new_fid)
        df = df.append(new_features)
    else:
        df.loc[to_change,'fid'] = new_fid
    return df

def sum_values_at_same_tsp(df, list_of_fids):
    group_cols = list(df.columns)
    group_cols.remove('value')
    for fid in list_of_fids:
        fid_values = df['fid'].isin([fid])
        combined = df[fid_values].groupby(group_cols,
                                          as_index=False,
                                          group_keys=False, sort=False)['value'].sum()
        df = df[~fid_values].append(combined)
    return df

def use_correct_tsp(df, first, second):
    df[first] = df.apply(lambda row: row[first] if row[first] != "NaT" else row[second], axis=1)
    df = df.drop(second, 1)
    return df

def derive_procedure_status(df):
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
