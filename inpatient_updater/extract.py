from inpatient_updater import http_utils
from inpatient_updater.config import flowsheet as fs_config
from inpatient_updater.config import lab_results as lr_config
import pandas as pd
import datetime as dt
import itertools

def extract_bedded_patients(hospital, server):
    resource = '/facilities/hospital/' + hospital + '/beddedpatients'
    return http_utils.make_request(server, resource)


def extract_patient(patient_id, server):
    resource = '/patients/mrn/' + patient_id
    return http_utils.make_request(server, resource)


def extract_flowsheet(patient_id, CSN, lookback_hours, server):
    resource = '/patients/flowsheetrows'
    flowsheet_row_ids = []
    for fid, internal_id_list in fs_config.flowsheet_ids:
        for internal_id in internal_id_list:
            flowsheet_row_ids.append({'ID': str(internal_id), 'Type': 'Internal'})
    payload = {
        'ContactID':        CSN,
        'ContactIDType':    'CSN',
        'FlowsheetRowIDs':  flowsheet_row_ids,
        'LookbackHours':    lookback_hours,
        'PatientID':        patient_id,
        'PatientIDType':    'EMRN'
    }
    return http_utils.make_request(server, resource, 'POST', payload)

def extract_flowsheets_nonblocking(pats_info, lookback_hours, server):
    resource = '/patients/flowsheetrows'
    flowsheet_row_ids = []
    for fid, internal_id_list in fs_config.flowsheet_ids:
        for internal_id in internal_id_list:
            flowsheet_row_ids.append({'ID': str(internal_id), 'Type': 'Internal'})

    payloads = [{
        'ContactID':    pat['visit_id'],
        'ContactIDType':    'CSN',
        'FlowsheetRowIDs':  flowsheet_row_ids,
        'LookbackHours':    lookback_hours,
        'PatientID':    pat['pat_id'], 
        'PatientIDType':    'EMRN'
    } for pat in pats_info]
    responses = http_utils.make_nonblocking_requests(server, resource, 'POST', payloads)
    return [ pd.DataFrame(r.json()).assign(pat_id = p['pat_id']).assign(visit_id=p['visit_id'])
                    for p,r in zip(pats_info, responses) if r is not None and r.status_code == 200]

def append_pat_info(main_dict, pat_id, visit_id=None):
    main_dict['pat_id'] = pat_id
    if visit_id:
        main_dict['visit_id'] = visit_id
    return main_dict

def request_nonblocking(data_type, pats_info, lookback_days, lookback_hours, server):
    if data_type == 'flowsheet':
        return extract_flowsheets_nonblocking(pats_info, lookback_hours, server)
    elif data_type == 'lab_results':
        return extract_lab_results_nonblocking(pats_info, lookback_days, server)
    elif data_type == 'medication_orders':
        return extract_medication_orders_nonblocking(pats_info, lookback_days, server)
    elif data_type == 'medication_administrations':
        return extract_medication_administration_nonblocking(pats_info, server)
    else:
        print "ERROR: now such resource to request!"

def extract_lab_results_nonblocking(pats_info, lookback_days, server):
    resource = '/patients/labs/component'
    component_types = []
    for fid, component_id_list in lr_config.component_ids:
        for cid in component_id_list:
            component_types.append({'Type': 'INTERNAL', 'Value': str(cid)})
    payloads = [{
        'Id':                   pat['pat_id'],
        'IdType':               'patient',
        'FromDate':             dt.datetime.today().strftime('%Y-%m-%d'),
        'MaxNumberOfResults':   200,
        'NumberDaysToLookBack': lookback_days,
        'ComponentTypes':       component_types
    } for pat in pats_info]
    responses = http_utils.make_nonblocking_requests(server, resource, 'POST', payloads)
    return [ pd.DataFrame(r.json()['ResultComponents']).assign(pat_id = p['pat_id']).assign(visit_id=p['visit_id'])
                  for p,r in zip(pats_info, responses) if r is not None and r.status_code == 200]



def extract_lab_results(patient_id, lookback_days, server):
    resource = '/patients/labs/component'
    component_types = []
    for fid, component_id_list in lr_config.component_ids:
        for cid in component_id_list:
            component_types.append({'Type': 'INTERNAL', 'Value': str(cid)})
    payload = {
        'Id':                   patient_id,
        'IdType':               'patient',
        'FromDate':             dt.datetime.today().strftime('%Y-%m-%d'),
        'MaxNumberOfResults':   200,
        'NumberDaysToLookBack': lookback_days,
        'ComponentTypes':       component_types
    }
    return http_utils.make_request(server, resource, 'POST', payload)


def extract_medication_orders(patient_id, lookback_days, server):
    resource = '/patients/medications'
    payload = {
        'id':           patient_id,
        'searchtype':   'IP',
        'dayslookback': str(lookback_days)
    }
    return http_utils.make_request(server, resource, 'GET', payload)

def extract_medication_orders_nonblocking(pats_info, lookback_days, server):
    resource = '/patients/medications'
    payloads = [{
        'id':           pat['pat_id'],
        'searchtype':   'IP',
        'dayslookback': str(lookback_days)
    } for pat in pats_info]
    responses = http_utils.make_nonblocking_requests(server, resource, 'GET', payloads)
    return [ pd.DataFrame(r.json()).assign(pat_id = p['pat_id']).assign(visit_id=p['visit_id'])
                    for p,r in zip(pats_info, responses) 
                        if r is not None and r.status_code == 200 and r.json() is not None]


def extract_medication_administration_nonblocking(med_orders, server):
    resource = '/patients/medicationadministrationhistory'
    payloads = [{
        'ContactID':        ord['visit_id'].values[0],
        'ContactIDType':    'CSN',
        'OrderIDs':         list(itertools.chain.from_iterable(ord['ids'])),
        'PatientID':        ord['pat_id'].values[0]
    } for ord in med_orders if not ord.empty and len(ord.ids.values)>0]
    responses = http_utils.make_nonblocking_requests(server, resource, 'POST', payloads)
    return [ pd.DataFrame(r.json()).assign(pat_id = p['PatientID']).assign(visit_id=p['ContactID']) 
                    for p,r in zip(payloads, responses) 
                        if r is not None and r.status_code == 200]


def extract_medication_administration(patient_id, CSN, med_order_ids, server):
    resource = '/patients/medicationadministrationhistory'
    payload = {
        'ContactID':        CSN,
        'ContactIDType':    'CSN',
        'OrderIDs':         med_order_ids,
        'PatientID':        patient_id
    }
    return http_utils.make_request(server, resource, 'POST', payload)
