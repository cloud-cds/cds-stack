import pandas as pd

def bedded_patients_rename_columns(patient_data):
    patient_data.columns = ['pat_id', 'admittime', 'visit_id', 'age', 'gender',
                            'diagnosis', 'history', 'problem']
    return patient_data

def flowsheet_rename_keys(flowsheet_data):
    flowsheet_data = flowsheet_data.rename(
        index=str,
        columns={ 'FlowsheetRowID':    'FlowsheetRowID',
                  'Unit':              'unit',
                  'RawValue':          'value',
                  'Instant':           'tsp',})
    return flowsheet_data

def lab_results_rename_keys(lab_results):
    lab_results = lab_results.rename(
        index=str,
        columns={ 'ComponentID':  'ComponentID',
                  'Units':        'unit',
                  'Value':        'value',
                  'tsp':          'tsp',}
    )
    return lab_results

def medication_rename_keys(med_data):
    return med_data.rename(columns = {
        'tsp':                  'tsp',
        'fid':                  'fid',
        'Name':                 'full_name',
        'PatientFriendlyName':  'friendly_name',
        'Dose':                 'dose',
        'IDs':                  'ids',
        'Action':               'action',
        'AdministrationInstant':'tsp',
    })
