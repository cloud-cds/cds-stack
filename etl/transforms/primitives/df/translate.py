from inpatient_updater import pandas_utils
from inpatient_updater.config import app_config
from inpatient_updater.config import medication_regex as med_config
import re
import logging

def translate_epic_id_to_fid(df, col, new_col, config_map, drop_original=False,
        add_string='', remove_if_not_found=False):
    def convert_id(epic_id):
        for fid, epic_id_list in config_map:
            if epic_id in epic_id_list:
                return fid
        if remove_if_not_found:
            return 'INVALID FID'
        raise app_config.TransformError(
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
    if add_string != '':
        df[new_col] += add_string
    return df


def translate_med_name_to_fid(med_data):
    good_meds = str("|").join(med['pos'] for med in med_config.med_regex)

    def find_fid_with_regex(med_name):
        if not re.search(good_meds, med_name, flags=re.I):
            return 'Unknown Medication'
        for med in med_config.med_regex:
            if re.search(med['pos'], med_name, flags=re.I):
                if re.search(med['neg'], med_name, flags=re.I):
                    return 'Invalid Medication'
                return med['fid']
        raise app_config.TransformError(
            'translate.translate_med_name_to_fid',
            'Error in medication regex. Medication neither good nor bad.',
            med_name
        )

    med_data['fid'] = med_data['full_name'].apply(find_fid_with_regex)
    return med_data


def extract_sys_dias_from_nbp(df, fid_col, value_col):
    def split_sys_dias(row):
        bp = str(row[value_col]).split("/")
        if (len(bp) != 2) or (not bp[0].isdigit()) or (not bp[1].isdigit()):
            logging.error('Error in blood pressure value. Cannot extract' +
                          ' systolic and diastolic.\n' + row.to_string())
            return row
        sys_row = row.copy()
        dias_row = row.copy()
        sys_row[fid_col] = 'nbp_sys'
        dias_row[fid_col] = 'nbp_dias'
        sys_row[value_col] = float(bp[0])
        dias_row[value_col] = float(bp[1])
        return (sys_row, dias_row)

    new_dfs = df[df[fid_col] == 'nbp'].apply(split_sys_dias, axis=1)
    for sys, dias in new_dfs:
        df = df.append([sys, dias])
    return df[df[fid_col] != 'nbp']

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
    return float(value)/10.0

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
