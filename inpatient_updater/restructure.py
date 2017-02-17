from inpatient_updater import pandas_utils
import pandas as pd
import numpy as np

def bedded_patients_extract_epic_ids(patient_data):
    def get_epic_id(patient_ids):
        for d in patient_ids:
            if d.get('Type') == 'EMRN':
                return d['ID']
        logging.error('Could not find an Epic ID. Throwing away patient.')
        return 'Invalid ID'

    patient_data['pat_id'] = patient_data.PatientIDs.apply(get_epic_id)
    return patient_data[~(patient_data['pat_id'] == 'Invalid ID')]

def bedded_patients_select_columns(patient_data):
    return patient_data[['pat_id', 'AdmitDateTime', 'CSN', 'Age', 'Gender',
                           'AdmitDx', 'MedicalHistory', 'ProblemList']]

def flowsheet_select_columns(flowsheet_data):
    return flowsheet_data[['FlowsheetRowID', 'FlowsheetColumns', 'Unit', 'pat_id', 'visit_id']]

def flowsheet_extract_internal_ids(flowsheet_data):
    def extract_id(flowsheet_row_id_list):
        for x in flowsheet_row_id_list:
            if x['Type'] == 'INTERNAL':
                return x['ID']
        raise LookupError('Could not find an ID for this flowsheet row')

    flowsheet_data['FlowsheetRowID'] = flowsheet_data['FlowsheetRowID'].apply(extract_id)
    return flowsheet_data

def flowsheet_extract_flowsheetcolumns(flowsheet_data):
    flowsheet_data = pandas_utils.unlistify_pandas_column(flowsheet_data, 'FlowsheetColumns')
    flowsheet_data = pandas_utils.turn_dict_into_columns(flowsheet_data, 'FlowsheetColumns')
    return flowsheet_data[['FlowsheetRowID', 'Unit', 'Instant', 'RawValue', 'pat_id', 'visit_id']]

def lab_results_select_columns(lab_results):
    return lab_results[['ResultDate', 'ResultTime', 'Value', 'Units', 'ComponentID', 'pat_id', 'visit_id']]

def lab_results_extract_results(lab_results):
    return pandas_utils.unlistify_pandas_column(lab_results, 'Value')

def lab_results_combine_date_and_time(lab_results):
    def combine_times(date_and_time=None):
        return ' '.join(tsp for tsp in date_and_time if tsp)

    lab_results['tsp'] = map(
        combine_times, lab_results[['ResultDate', 'ResultTime']].values
    )
    lab_results.drop('ResultDate', axis=1, inplace=True)
    lab_results.drop('ResultTime', axis=1, inplace=True)
    return lab_results

def medication_extract_orders(med_data):
    return pandas_utils.turn_dict_into_columns(med_data, 'MedicationOrders')

def medication_extract_administrations(med_data):
    med_data = pandas_utils.unlistify_pandas_column(med_data, 'MedicationAdministrations')
    return pandas_utils.turn_dict_into_columns(med_data, 'MedicationAdministrations')

def medication_drop_unused_order_columns(med_data, pat_id=None):
    return med_data[['Name', 'IDs', 'StartDateTime', 'StartDate', 'Dose',
        'DoseUnit', 'Frequency', 'PatientFriendlyName', 'pat_id', 'visit_id']]

def medication_drop_unused_admin_columns(med_data):
    return med_data[['Name', 'Action', 'Dose', 'AdministrationInstant', 'Rate', 'pat_id', 'visit_id']]

def medication_extract_nested_order_data(med_data):
    med_data['tsp'] = np.where(
        pd.notnull(med_data['StartDateTime']),
        med_data['StartDateTime'],
        med_data['StartDate']
    )
    med_data['dose_unit'] = med_data['DoseUnit'].map(
        lambda x: x['Title'] if x else None
    )
    med_data['frequency'] = med_data['Frequency'].map(
        lambda x: x['Name'] if x else ''
    )
    return med_data

def medication_extract_nested_admin_data(med_data):
    med_data['dose_unit'] = med_data['Dose'].map(
        lambda x: x['Unit'] if x else None
    )
    med_data['dose_value'] = med_data['Dose'].map(
        lambda x: x['Value'] if x else None
    )
    med_data['rate_unit'] = med_data['Rate'].map(
        lambda x: x['Unit'] if x else None
    )
    med_data['rate_value'] = med_data['Rate'].map(
        lambda x: x['Value'] if x else None
    )
    return med_data

def medication_drop_unused_order_columns2(med_data, pat_id=None):
    return med_data[['tsp', 'fid', 'full_name', 'friendly_name', 'dose',
        'dose_unit', 'frequency', 'ids', 'pat_id', 'visit_id']]

def medication_drop_unused_admin_columns2(med_data):
    return med_data[['tsp', 'fid', 'full_name', 'action', 'dose_value',
        'dose_unit', 'rate_value', 'rate_unit', 'pat_id', 'visit_id']]