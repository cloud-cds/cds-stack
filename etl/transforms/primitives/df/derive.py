import logging
import etl.transforms.primitives.row.transform as transform

def combine(df, new_fid, list_of_fids, keep_originals=True):
    to_change = df['fid'].isin(list_of_fids)
    if keep_originals:
        new_features = df[to_change].assign(fid = new_fid)
        df = df.append(new_features)
    else:
        df[to_change]['fid'] = new_fid
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


# def derive_fluids_intake(df, list_of_fids):
#     fi_values = df['fid'].isin(['fluids_intake'])
#     df_rest = df[~fi_values]
#     df_fi = df[fi_values]
#     group_cols = ['visit_id', 'full_name']
#     groups = df_fi.groupby(group_cols, sort=False)
#     for key, group in groups:
#         logging.debug('derive_fluids_intake %s' % key)
#         derived = transform.extract_fluids_intake_df(group.sort_values(by='tsp'), logging)
#         df_rest = df_rest.append(derived)
#     return df_rest

#     for fid in list_of_fids:
#         fid_values = df['fid'].isin([fid])
#         combined = df[fid_values].groupby(group_cols,
#                                           as_index=False,
#                                           group_keys=False)['value'].sum()
#         df = df[~fid_values].append(combined)
#     return df