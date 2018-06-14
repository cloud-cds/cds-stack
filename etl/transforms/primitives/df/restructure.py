import etl.transforms.primitives.df.pandas_utils as pandas_utils
import pandas as pd
import numpy as np
import logging

def select_columns(df, selection_dict):
    try:
        df = df[list(selection_dict.keys())]\
            .rename(index=str, columns=selection_dict)\
            .reset_index(drop=True)
    except KeyError as e:
        for col in selection_dict.values():
            df[col] = np.nan
    return df

def unlist(df, unlist_col):
    return pandas_utils.unlistify_pandas_column(df, unlist_col)

def extract(df, dict_column, selection_dict):
    def fill_none(val):
        if val is None or str(val) == 'nan':
            return {}
        return val
    df[dict_column] = df[dict_column].apply(fill_none)
    new_cols = pd.DataFrame(df[dict_column].tolist())
    new_cols = select_columns(new_cols, selection_dict)
    old_cols = df.drop(dict_column, axis=1)
    # logging.debug(old_cols)
    # logging.debug(new_cols)
    return pd.concat([old_cols, new_cols], axis=1)

def concat_str(df, new_col, col_1, col_2, drop_original=True):
    df[new_col] = df[col_1].str.cat(df[col_2], sep=' ')
    if drop_original:
        df.drop([col_1, col_2], axis=1, inplace=True)
    return df

import random
def make_null_time_midnight(df):
    df['time'] = df['time'].apply(lambda x: '12:00 AM' if x is None else x)
    return df

def extract_id_from_list(df, id_column, id_type, id_name='ID', type_name='Type'):
    def get_id(id_list):
        for x in id_list:
            if x.get(type_name) == id_type:
                return str(x[id_name])
        logging.error('Could not find an ID. Throwing away row.')
        return 'Invalid ID'

    df[id_column] = df[id_column].apply(get_id)
    return df[~(df[id_column] == 'Invalid ID')]
