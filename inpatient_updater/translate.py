from inpatient_updater.config import medication_regex as med_config
from inpatient_updater import pandas_utils
import re
import logging

def translate_epic_id_to_fid(df, col, new_col, config_map, drop_original=False):
    def convert_id(epic_id):
        for fid, epic_id_list in config_map:
            if epic_id in epic_id_list:
                return fid
        raise LookupError('Could not find an ID for this ' + column)

    pandas_utils.check_column_name(df, col)
    df[new_col] = df[col].apply(convert_id)
    if drop_original:
        df.drop(col, axis=1, inplace=True)
    return df


def translate_med_name_to_fid(med_data):
    good_meds = str("|").join(med['pos'] for med in med_config.med_regex)

    def find_fid_with_regex(med_name):
        if not re.search(good_meds, med_name, flags=re.I):
            return 'Unknown Medication'
        for med in med_config.med_regex:
            if re.search(med['neg'], med_name, flags=re.I):
                return 'Invalid Medication'
            if re.search(med['pos'], med_name, flags=re.I):
                return med['fid']
        else:
            raise LookupError('Error translating medication name: ' + med_name)

    med_data['fid'] = med_data['full_name'].apply(find_fid_with_regex)
    return med_data


def extract_sys_dias_from_nbp(df, fid_col, value_col):
    nbp_df = df[df[fid_col] == 'nbp']

    for idx, nbp in nbp_df.iterrows():
        bp = nbp[value_col].split("/")
        if (len(bp) != 2) or (not bp[0].isdigit()) or (not bp[1].isdigit()):
            logging.error('Error in blood pressure value. Cannot extract' +
                          ' systolic and diastolic.\n' + nbp.to_string())
            continue
        sys_col = nbp.copy()
        dias_col = nbp.copy()
        sys_col[fid_col] = 'nbp_sys'
        dias_col[fid_col] = 'nbp_dias'
        sys_col[value_col] = float(bp[0])
        dias_col[value_col] = float(bp[1])
        df = df.append(sys_col)
        df = df.append(dias_col)

    return df[df[fid_col] != 'nbp']

def convert_units(df, fid_col, fids, unit_col, from_unit, to_unit, value_col, convert_func):
    def convert_val_in_row(row):
        row[unit_col] = to_unit
        row[value_col] = convert_func(row[value_col])
        return row

    conds = (df[fid_col].isin(fids)) & (df[unit_col] == from_unit)
    df[conds] = df[conds].apply(convert_val_in_row, axis=1)
    return df

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
    return rass_dict[rass_str]
