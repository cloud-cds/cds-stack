from inpatient_updater.config import cdm_definitions as cdm
from inpatient_updater.config import expected_response as expected

def validate_pre_transform(fid, value, units):
    validate_fid(fid)
    validate_translated_response_units(units)

def validate_post_transform(df, fid_col, value_col, unit_col=None):
    for idx, row in df.iterrows():
        validate_fid(row[fid_col])
        validate_value(row[fid_col], row[value_col])
        if unit_col:
            validate_final_units(row[fid_col], row[unit_col])

def validate_fid(fid):
    if fid not in cdm.cdm_defs:
        raise LookupError('Fid: ', fid, 'not in CDM')

def validate_value(fid, value):
    if type(value) != cdm.cdm_defs[fid]['value']:
        raise ValueError(
            'Value for ' + fid + ' not of correct type' +
            ' expected ' + str(cdm.cdm_defs[fid]['value']) +
            ' got ' + str(type(value)) + ' for ' + str(value)
        )

def validate_final_units(fid, unit):
    if unit != cdm.cdm_defs[fid]['unit']:
        raise ValueError(
            'Units for ' + fid + ' not in CDM definition' +
            ' expected ' + str(cdm.cdm_defs[fid]['unit']) +
            ' got ' + str(unit)
        )

def validate_translated_response_units(fid, unit):
    if unit not in expected.units[fid]:
        raise ValueError(
            'Units for ' + fid + ' not in expected response' +
            ' expected one of ' + str(expected.units[fid]) +
            ' got ' + str(unit)
        )

def validate_columns(df, cols):
    if type(cols) != list:
        raise TypeError('cols must be of type "list"')
    if len(df.columns) != len(set(df.columns)):
        raise ValueError('Dataframe contains duplicate column names')
    if len(cols) != len(set(cols)):
        raise ValueError('cols must not contain duplicates')
    if set(df.columns) != set(cols):
        if len(set(df.columns)) > len(set(cols)):
            raise ValueError('Extra in dataframe: ' + str(set(df.columns) - set(cols)))
        else:
            raise ValueError('Extra in cols: ' + str(set(cols) - set(df.columns)))