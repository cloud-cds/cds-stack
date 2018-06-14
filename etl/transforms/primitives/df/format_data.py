import etl.mappings.unit_format as unit_format
from etl.mappings.cdm_definitions import cdm_defs
import etl.core.config as app_config
import datetime as dt
import pandas as pd
import numpy as np
import re
import logging
import itertools
import base64
import json

def initialize_tsp(df):
    df['tsp'] = pd.to_datetime('now')
    return df

def format_treatmentteam(df, column='value'):
    def _format_treatmentteam(val):
        res = []
        for member in val:
            formatted = {
                'start': member['BeginDateTime'],
                'end': member['EndDateTime'],
                'name': member['ProviderName'],
                'role': member['ProviderRole'],
                'specialty': member['ProviderSpecialty'],
            }
            for id_types in member['ProviderIdTypes']:
                if id_types['Type'] == 'JHED ID':
                    formatted['id'] = id_types['Id']
            res.append(formatted)
        return json.dumps(res)
    df[column] = df[column].apply(lambda x: _format_treatmentteam(x))
    return df

def format_active_problem_list(df, column='active_problem_list'):
    df[column] = df.apply(\
        lambda row: {'name'         : row['prob_name'],
                     row['dx_id']['IDType']  : row['dx_id']['ID']}, axis=1)
    return df#[['pat_id', 'visit_id', column]].groupby(['pat_id', 'visit_id'])[column].apply(list).to_frame().reset_index()

def add_order_to_fid(df):
    df['fid'] += '_order'
    return df

def format_numeric(df, column):
    df[column] = pd.to_numeric(df[column])
    return df

def format_age(df, column):
    df[column] = pd.to_numeric(df[column].str.replace('y.o.',''))
    return df

def format_gender_to_string(df, column):
    gender_map = {'Female': '0',
                  'Male': '1',
                  'F': '0',
                  'M': '1'}
    df[column] = df[column].map(lambda g: gender_map.get(g) if g in gender_map else None)
    return df

def format_tsp(df, column, no_NaT=False):
    df[column] = pd.to_datetime(df[column], errors='coerce')
    df[column] = df[column].dt.tz_localize(app_config.TIMEZONE, ambiguous='NaT', errors='coerce').dt.strftime(app_config.tsp_fmt)
    if no_NaT:
        df[column] = df[column].apply(lambda x: '' if x == 'NaT' else x)
    return df

def tsp_to_datetime(df, column):
    df[column] = df[column].apply(\
        lambda ts: dt.datetime.fromtimestamp(int(ts[6:-7]) / 1e3).strftime('%Y-%m-%d %H:%M:%S') + ts[-7:-4] if ts is not None else 'NaT'
    )
    return df

def base64_decode(df, column):
    df[column] = df[column].map(lambda x: base64.b64decode(x).decode('UTF-8'))
    return df

def base64_safe_decode(df, column):
    df[column] = df[column].map(lambda x: base64.b64decode(x).decode('unicode_escape').encode('UTF-8').decode('UTF-8'))
    return df

def base64_encode(df, column):
    df[column] = df[column].map(lambda x: base64.b64encode(x))
    return df

def json_decode(df, column):
    df[column] = df[column].map(lambda x: json.loads(x))
    return df

def json_encode(df, column):
    df[column] = df[column].map(lambda x: json.dumps(x))
    return df

def filter_empty_values(df, column):
    def remove_empty(row):
        if row[column] and str(row[column]).strip():
            return row[column]
        elif row[column] == 0:
            return row[column]
        else:
            return 'Empty Value'

    df = df.where((pd.notnull(df)), None)
    df[column] = df.apply(remove_empty, axis=1)
    return df[~df[column].isin(['Empty Value',])]

""" Removes units if they aren't the final type cdm_definitions """
def filter_to_final_units(df, unit_col):
    def filter_unit(row):
        if row[unit_col] != cdm_defs[row.fid]['unit']:
            logging.warning(
                'Incorrect unit. Not in cdm_definitions:\n' + row.to_string()
            )
            return 'Invalid Unit'
        return row[unit_col]

    df[unit_col] = df.apply(filter_unit, axis=1)
    return df[df[unit_col] != 'Invalid Unit']

""" Cleans unit strings """
def clean_units(df, fid_col, unit_col):
    def clean_unit(row):
        unit = row[unit_col]
        fid = row[fid_col]
        if (unit is None) or (unit.replace(' ', '') == ''):
            if fid in unit_format.empty_translation_map:
                return unit_format.empty_translation_map[fid]
            else:
                logging.info('No empty translation found:\n' + row.to_string())
                return 'Invalid Unit'
        attempts = [
            unit.lower(),
            unit.replace(' ', '').lower()
        ]
        for correct_name, accepted_names in unit_format.translation_map:
            for attempt in attempts:
                if attempt in accepted_names:
                    return correct_name
        logging.warning('No unit translation found:\n' + row.to_string())
        return 'Invalid Unit'

    df[unit_col] = df[unit_col].fillna(value='')
    df[unit_col] = df.apply(clean_unit, axis=1)
    return df[df[unit_col] != 'Invalid Unit']


def clean_values(df, fid_col, value_col, bad_values = ['see below', 'N/A', None, 'Unable to calculate', '---.--',
    'SEE COMMENT', 'TNP @COMM', '@COMM', 'Pending']):
    def clean_value(row):
        val = row[value_col]
        fid = row[fid_col]
        if val in bad_values:
            logging.info('Known bad value:\n' + row.to_string())
            return 'Invalid Value'
        if cdm_defs[fid]['value'] == float:
            if val == '':
                return val
            val = str(val).replace('<','').replace('>','').replace(',','')
            if val.replace('.','',1).isdigit():
                return float(val)
            elif re.search('.*-([\d]+)', val):
                sub_val = re.search('.*-([\d]+)', val)
                return float(sub_val.group(1))
            else:
                logging.warning('Invalid float value:\n' + row.to_string())
                return 'Invalid Value'
        elif cdm_defs[fid]['value'] == str:
            return str(val)
        elif cdm_defs[fid]['value'] == None:
            return ''
        else:
            logging.warning('Invalid value:\n' + row.to_string())
            return 'Invalid Value'

    df[value_col] = pd.Series(df[value_col], dtype='object')
    df[value_col] = df.apply(clean_value, axis=1)
    return df[~df[value_col].isin(['Invalid Value',])]

def to_numeric(df, fid_col, value_col, default_value):
    def to_numeric_row(row):
        val = row[value_col]
        fid = row[fid_col]
        if val is None or val == '':
            val = default_value
        else:
            val = str(val).replace('<','').replace('>','')
            if val.replace('.','',1).isdigit():
                return float(val)
            elif re.search('.*-([\d]+)', val):
                sub_val = re.search('.*-([\d]+)', val)
                return float(sub_val.group(1))
            else:
                logging.warning('Invalid float value:\n' + row.to_string())
                return default_value


    df[value_col] = pd.Series(df[value_col], dtype='object')
    df[value_col] = df.apply(to_numeric_row, axis=1)
    return df

def threshold_values(df, value_col):
    def apply_threshold(row):
        fid = row['fid']
        low, high = cdm_defs[fid]['thresh']
        if row[value_col] != '':
            if low and row[value_col] < low:
                logging.info('Lower than threshold:\n' + row.to_string())
                return 'Out of bounds'
            if high and row[value_col] > high:
                logging.info('Higher than threshold:\n' + row.to_string())
                return 'Out of bounds'
        return row[value_col]

    df[value_col] = df.apply(apply_threshold, axis=1)
    return df[~df[value_col].isin(['Out of bounds',])]