import etl.transforms.primitives.df.pandas_utils as pandas_utils
from etl.core.exceptions import TransformError
from etl.mappings.med_regex import med_regex
import pandas as pd
import re
import logging
import etl.transforms.primitives.df.filter_rows as filter_rows
import functools

def translate_epic_id_to_fid(df, col, new_col, config_map, drop_original=False,
        add_string='', add_string_fid=None, remove_if_not_found=False):
    def convert_id(epic_id):
        for fid, epic_id_list in config_map:
            if epic_id in epic_id_list:
                return fid
        if remove_if_not_found:
            return 'INVALID FID'
        raise TransformError(
            'translate.translate_epic_id_to_fid',
            'Could not find an fid for this ID.',
            col + " = " + epic_id
        )

    pandas_utils.check_column_name(df, col)
    df[new_col] = df[col].apply(convert_id)
    if drop_original:
        df.drop(col, axis=1, inplace=True)
    if remove_if_not_found:
        df = df[df[new_col] != 'INVALID FID']
    if add_string != '' and add_string_fid is not None:
        for fid in add_string_fid:
            fid_rows = (df[new_col] == fid)
            df[new_col][fid_rows] += add_string
    return df

def translate_med_name_to_fid(med_data):
    def find_fid_with_regex(med_name, med):
        if re.search(med['pos'], med_name, flags=re.I):
            if 'neg' in med and len(med['neg']) > 0 and re.search(med['neg'], med_name, flags=re.I):
                return 'Invalid Medication'
            return med['fid']
        else:
            return 'Unknown Medication'
    res = None
    for med in med_regex:
        this_med_data = med_data.copy()
        this_med_data['fid'] = this_med_data['full_name'].apply(functools.partial(find_fid_with_regex, med=med))
        this_med_data = filter_rows.filter_medications(this_med_data)
        if not this_med_data.empty:
            if res is None:
                res = this_med_data
            else:
                res = pd.concat([res, this_med_data], ignore_index=True)
    return res

def override_empty_doses_with_rates(med_data, fid_col, fids):
    med_idx = med_data[fid_col].isin(fids) & \
                (pd.isnull(med_data['dose_unit']) | pd.isnull(med_data['dose_value']))

    med_data['dose_value'][med_idx] = med_data['rate_value'][med_idx]
    med_data['dose_unit'][med_idx] = med_data['rate_unit'][med_idx]
    return med_data


def extract_sys_dias_from_bp(df, fid_col, value_col, bp):
    def get_sys(row):
        if len(row[0]) == 0:
            return -1 # deleted bp
        if (len(row) != 2) or (not row[0].isdigit()):
            logging.error('Error in systolic.\n' + row.to_string())
            return 0
        return float(row[0])

    def get_dias(row):
        if len(row[0]) == 0:
            return -1 # deleted bp
        if (len(row) != 2) or (not row[1].isdigit()):
            logging.error('Error in diastolic.\n' + row.to_string())
            return 0
        return float(row[1])

    bp_rows = (df[fid_col] == bp)
    bp_df = df[bp_rows]
    if not bp_df.empty:
        bp_sys = bp_df[value_col].str.split("/").apply(get_sys, 1)
        bp_dias = bp_df[value_col].str.split("/").apply(get_dias, 1)
        bp_df = bp_df.drop([value_col, fid_col], axis=1)
        bp_sys.name = value_col
        bp_dias.name = value_col
        bp_sys = bp_df.copy().join(bp_sys).assign(fid="{}_sys".format(bp))
        bp_dias = bp_df.copy().join(bp_dias).assign(fid="{}_dias".format(bp))
        df = df[~bp_rows].append([bp_sys, bp_dias])
    else:
        logging.warn("bp_df is empty {}".format(bp))
    # # also assign to a new feature called bp -- combine nbp and abp
    # bp_sys = bp_sys.copy().assign(fid="bp_sys")
    # bp_dias = bp_dias.copy().assign(fid="bp_dias")
    # df = df.append([bp_sys, bp_dias])
    return df

def convert_to_boolean(df, fid_col, value_col, fid):
    def convert_to_boolean_in_row(row):
        row[value_col] = True if row[value_col] > 0 else False
        return row
    rows = (df[fid_col] == fid)
    df[rows] = df[rows].apply(convert_to_boolean_in_row, axis=1)
    return df

def convert_weight_value_to_float(df, fid_col, value_col, fid):
    def convert_weight_value_to_float_in_row(row):
        if '(' in row[value_col]:
            row[value_col] = row[value_col].split(" ")[0]
        return row
    rows = (df[fid_col] == fid)
    df[rows] = df[rows].apply(convert_weight_value_to_float_in_row, axis=1)
    return df

def convert_units(df, fid_col, fids, unit_col, from_unit, to_unit, value_col, convert_func):
    def convert_val_in_row(row):
        row[unit_col] = to_unit
        row[value_col] = convert_func(row[value_col])
        return row

    conds = (df[fid_col].isin(fids)) & (df[unit_col] == from_unit)
    df[conds] = df[conds].apply(convert_val_in_row, axis=1)
    return df[~df[value_col].isin(['Invalid Value',])]

def celcius_to_fahrenheit(value):
    return float(value) * 9.0/5.0 + 32.0

def g_per_l_to_g_per_dl(value):
    return float(value)/10.0

def g_to_mg(value):
    return float(value)*1000.0

def ml_to_boolean(value):
    return True if float(value) > 0 else False

def ml_per_hr_to_ml_for_1hr(value):
    return float(value)

def mg_per_l_to_mg_per_l_feu(value):
    return float(value)*2

def rass_str_to_number(rass_str):
    rass_dict = {
        'Combative':        '4',
        'Very Agitated':    '3',
        'Agitated':         '2',
        'Restless':         '1',
        'Alert and Calm':   '0',
        'Drowsy':           '-1',
        'Light Sedation':   '-2',
        'Moderate Sedation':'-3',
        'Deep Sedation':    '-4',
        'Unarousable':      '-5',
    }
    if rass_str in rass_dict:
        return rass_dict[rass_str]
    elif rass_str in rass_dict.items():
        return rass_str
    return 'Invalid Value'
