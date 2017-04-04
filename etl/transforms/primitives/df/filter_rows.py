import etl.mappings.icd9_codes as icd9_codes
import pandas as pd
import re

def filter_on_icd9(patient_data):
    filter_function = build_filter_icd9_function(icd9_codes.DX_ICD9_MAPPING)
    patient_data['diagnosis'] = patient_data['diagnosis'].apply(filter_function)

    filter_function = build_filter_icd9_function(icd9_codes.HX_ICD9_MAPPING)
    patient_data['history'] = patient_data['history'].apply(filter_function)

    filter_function = build_filter_icd9_function(icd9_codes.PL_ICD9_MAPPING)
    patient_data['problem'] = patient_data['problem'].apply(filter_function)
    patient_data['problem_all'] = patient_data['problem_all'].apply(filter_function)

    return patient_data


""" Builds a function that amortizes the cost of compiling the icd9 regex """
def build_filter_icd9_function(icd9_codes):
    compiled_codes = [(n, re.compile(p)) for n, p in icd9_codes]

    def drop_unused_codes(diagnoses):
        relevant_codes = {}
        for d in diagnoses:
            for name, pattern in compiled_codes:
                if pattern.search(d['ICD9']):
                    relevant_codes[name] = True
        return relevant_codes

    return drop_unused_codes


def filter_medications(med_data):
    med_data = med_data[med_data['fid'] != 'Unknown Medication']
    med_data = med_data[med_data['fid'] != 'Invalid Medication']
    return med_data

def filter_location_history_events(lh):
    mask = [et in ['Admission', 'Transfer In','Discharge'] for et in lh.EventType]
    lh = lh[mask]
    lh = lh[~lh.duplicated(subset=['visit_id','EffectiveDateTime'])]
    return lh
