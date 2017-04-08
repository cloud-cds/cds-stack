import extract
import restructure
import rename
import filter_rows
import format_data
import translate
import validate
from inpatient_updater.config import flowsheet as fs_config
from inpatient_updater.config import lab_results as lr_config

import os, sys
import pandas as pd
import datetime as dt
import pickle
import itertools
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import pandas as pd
import ujson as json
import logging

def set_options(serv, hosp, hours, days=None):
    global server
    global hospital
    global lookback_hours
    global lookback_days
    server = serv
    hospital = hosp
    lookback_hours = int(hours)
    if days:
        lookback_days = int(days)
    else:
        lookback_days = lookback_hours/24 + 1

def get_flowsheet(pat):
    fs = extract.extract_flowsheet(pat['pat_id'], pat['visit_id'], lookback_hours, server)
    fs = pd.DataFrame(fs)
    if not fs.empty:
        fs['pat_id'] = pat['pat_id']
        fs['visit_id'] = pat['visit_id']
    return fs

def get_bedded_patients():
    bp = extract.extract_bedded_patients(hospital, server)
    bp = pd.DataFrame(bp)
    return bp

def get_flowsheets(pats):
    return pd.concat([df for df in [get_flowsheet(pat) for idx, pat in pats.iterrows()] if not df.empty])

def get_flowsheets_nonblocking(pats):
    fs = extract.extract_flowsheets_nonblocking(pats, lookback_hours, server)
    return pd.concat([pd.DataFrame(row) for row in fs])

def request_nonblocking(data_type, pats):
    results = extract.request_nonblocking(data_type, pats, lookback_days, lookback_hours, server)
    return results

def transform_bedded_patients(bp):
    bp = restructure.bedded_patients_extract_epic_ids(bp)
    bp = restructure.bedded_patients_select_columns(bp)
    bp = rename.bedded_patients_rename_columns(bp)
    bp = filter_rows.filter_on_icd9(bp)
    bp = format_data.format_numeric(bp, 'age')
    bp = bp.assign(hospital = hospital)
    # validate.validate_columns(bp,
    #     cols = ['pat_id', 'admittime', 'visit_id', 'age', 'gender',
    #             'diagnosis', 'history', 'problem', 'hospital']
    # )
    return bp

def transform_flowsheet(fs):
    if fs.empty:
        return None
    fs = restructure.flowsheet_select_columns(fs)
    fs = restructure.flowsheet_extract_internal_ids(fs)
    fs = restructure.flowsheet_extract_flowsheetcolumns(fs)
    fs = rename.flowsheet_rename_keys(fs)
    fs = translate.translate_epic_id_to_fid(
        df = fs,
        col = 'FlowsheetRowID',
        new_col = 'fid',
        config_map = fs_config.flowsheet_ids,
        drop_original = True,
    )
    fs = format_data.clean_units(fs, 'fid', 'unit')
    fs = format_data.clean_values(fs, 'fid', 'value')
    fs = translate.extract_sys_dias_from_nbp(fs, 'fid', 'value')
    fs = translate.convert_units(fs,
        fid_col = 'fid', fids = ['temperature'],
        unit_col = 'unit', from_unit = 'Celcius', to_unit = 'Fahrenheit',
        value_col = 'value', convert_func = translate.celcius_to_fahrenheit
    )
    fs = translate.convert_units(fs,
        fid_col = 'fid', fids = ['rass'],
        unit_col = 'unit', from_unit = '', to_unit = '',
        value_col = 'value', convert_func = translate.rass_str_to_number
    )
    fs = format_data.filter_to_final_units(fs, 'unit')
    fs = format_data.threshold_values(fs, 'value')
    # validate.validate_columns(fs,
    #     cols = ['pat_id', 'visit_id', 'tsp', 'fid', 'unit', 'value']
    # )
    # validate.validate_post_transform(fs, 'fid', 'value', 'unit')
    return fs

def transform_lab_results(lr):
    if lr.empty:
        return None
    lr = restructure.lab_results_select_columns(lr)
    lr = restructure.lab_results_extract_results(lr)
    lr = restructure.lab_results_combine_date_and_time(lr)
    lr = rename.lab_results_rename_keys(lr)
    lr = translate.translate_epic_id_to_fid(
        df = lr,
        col = 'ComponentID',
        new_col = 'fid',
        config_map = lr_config.component_ids,
        drop_original = True,
    )
    lr = format_data.clean_units(lr, 'fid', 'unit')
    lr = format_data.clean_values(lr, 'fid', 'value')
    lr = format_data.threshold_values(lr, 'value')
    if lr.empty:
        return None
    lr = format_data.filter_to_final_units(lr, 'unit')
    # validate.validate_columns(
    #     df = lr,
    #     cols = ['pat_id','visit_id', 'tsp', 'fid', 'unit', 'value']
    # )
    # validate.validate_post_transform(lr, 'fid', 'value', 'unit')
    return lr

def transform_med_orders(mo):
    if mo.empty:
        return None
    mo = restructure.medication_extract_orders(mo)
    mo = restructure.medication_drop_unused_order_columns(mo)
    mo = restructure.medication_extract_nested_order_data(mo)
    mo = rename.medication_rename_keys(mo)
    mo = translate.translate_med_name_to_fid(mo)
    mo = filter_rows.filter_medications(mo)
    if mo.empty:
        return None
    mo = format_data.clean_units(mo, 'fid', 'dose_unit')
    mo = format_data.clean_values(mo, 'fid', 'dose')
    if mo.empty:
        return None
    mo = restructure.medication_drop_unused_order_columns2(mo)
    mo = translate.convert_units(mo,
        fid_col = 'fid',
        fids = ['piperacillin_tazbac_dose', 'vancomycin_dose',
                'cefazolin_dose'],
        unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
        value_col = 'dose', convert_func = translate.g_to_mg
    )
    mo = format_data.threshold_values(mo, 'dose')
    validate.validate_columns(mo,
        cols = ['pat_id', 'visit_id', 'tsp', 'fid', 'full_name',
                'friendly_name', 'dose', 'dose_unit', 'frequency', 'ids']
    )
    validate.validate_post_transform(mo, 'fid', 'dose')
    return mo

def transform_med_admin(ma):
    if ma.empty:
        return None # No medication administrations
    ma = restructure.medication_extract_administrations(ma)
    ma = restructure.medication_drop_unused_admin_columns(ma)
    ma = restructure.medication_extract_nested_admin_data(ma)
    ma = rename.medication_rename_keys(ma)
    ma = translate.translate_med_name_to_fid(ma)
    ma = filter_rows.filter_medications(ma)
    # ma = format_data.clean_units(ma, 'fid', 'dose_unit')
    # ma = format_data.clean_units(ma, 'fid', 'rate_unit')
    # ma = format_data.clean_values(ma, 'fid', 'dose_value')
    # ma = format_data.clean_values(ma, 'fid', 'rate_value')
    # for idx, admin in ma.iterrows():
    #     if admin['dose_value'] and admin['rate_value']:
    #         print admin
    ma = restructure.medication_drop_unused_admin_columns2(ma)
    # validate.validate_columns(
    #     df = ma,
    #     cols = ['pat_id', 'visit_id', 'tsp', 'fid', 'full_name', 'action',
    #         'dose_value', 'dose_unit', 'rate_value', 'rate_unit']
    # )
    # validate.validate_post_transform(ma, 'fid', 'dose_value')
    return ma

def api_request_task(data, data_type):
    print("%s: starting api_request_task for %s" % (etl_job_id, data_type))
    result =  request_nonblocking(data_type, data)
    print("%s: exiting api_request_task for %s" % (etl_job_id, data_type))
    return result

# Create job id
etl_job_id = "job_etl_test"

def main():
    # Create thread executor
    pool = ThreadPoolExecutor(3)
    api_request_futures = []

    # Configure inpatient-updater
    set_options(serv='prod', hosp='HCGH', hours=72, days=100)
    pd.set_option('display.width', 200)
    pd.set_option('display.max_rows', 1000)
    pd.set_option('display.max_colwidth', 100)
    pd.options.mode.chained_assignment = None
    logging.getLogger().setLevel(30)

    # Request for all bedded patients
    pats = get_bedded_patients()
    pats_transformed = transform_bedded_patients(pats.head(30))

    pats_info = [{'pat_id': pat['pat_id'], 'visit_id': pat['visit_id']} for idx, pat in pats_transformed.iterrows()]

    # request all raw features
    api_request_futures.append(pool.submit(api_request_task, pats_info, "flowsheet"))
    # api_request_futures.append(pool.submit(api_request_task, pats_info, "lab_results"))
    # api_request_futures.append(pool.submit(api_request_task, pats_info, "medication_orders"))

    try:
        print(wait(api_request_futures))
        flowsheets = api_request_futures[0].result()
        # lab_results = api_request_futures[1].result()
        # medication_orders = api_request_futures[2].result()
    except Exception as exc:
        print('generated an exception: %s' % exc)
        # TODO: when meet an error

    # pickle
    # pickle.dump(flowsheets, open('fs.raw', 'w'))
    # pickle.dump(lab_results, open('lr.raw', 'w'))
    # pickle.dump(medication_orders, open('mo.raw', 'w'))

    # flowsheets = pickle.load(open('fs.raw', 'r'))
    # lab_results = pickle.load(open('lr.raw', 'r'))
    # medication_orders = pickle.load(open('mo.raw', 'r'))

    flowsheets_df = pd.concat(flowsheets)
    flowsheets_t = transform_flowsheet(flowsheets_df)
    print(flowsheets_t[flowsheets_t['pat_id'] == 'E100426066'])
    pickle.dump(flowsheets_t, open('fs.trans', 'w'))
    sys.exit()

    lab_results_df = pd.concat(lab_results)
    lab_results_t = transform_lab_results(lab_results_df)
    pickle.dump(lab_results_t, open('lr.trans', 'w'))

    os.system('say "eee tee ell complete"')
    sys.exit()

    medication_orders_t = [transform_med_orders(mo) for mo in medication_orders]
    medication_orders_t = [ mot for mot in medication_orders_t if mot is not None and not mot.empty]

    if medication_orders_t is None:
        print("medication_orders_t is None")

    medication_administrations = api_request_task(medication_orders_t, "medication_administrations")
    if medication_administrations is None:
        print("medication_administrations is None")
    else:
        medication_administrations_df = pd.concat(medication_administrations)

    for mot in medication_orders_t:
        mot['ids'] = mot['ids'].to_json()

    medication_orders_t_df = pd.concat(medication_orders_t)

    medication_administrations_t = [ transform_med_admin(ma) \
        for ma in medication_administrations]

    if medication_administrations_t is None:
        print("medication_administrations_t is None")

    medication_administrations_t = [mat for mat in medication_administrations_t if mat is not None and not mat.empty]
    medication_administrations_t_df = pd.concat(medication_administrations_t)


    pickle.dump(medication_orders_t_df, open('mo.test', 'w'))
    pickle.dump(medication_administrations_t_df, open('ma.test', 'w'))

if __name__ == '__main__':
    main()

