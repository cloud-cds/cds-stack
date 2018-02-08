from etl.core.exceptions import TransformError
from etl.core.config import Config
from etl.transforms.pipelines import epic2op_transform as jhapi_transform_lists
import etl.load.pipelines.epic2op as loader
from etl.load.pipelines.criteria import get_criteria_tasks
from etl.core.task import Task
from etl.core.plan import Plan
from etl.core.engine import Engine
from etl.io_config.jhapi import JHAPIConfig
import etl.io_config.core as core
import os, sys, traceback, functools
import pandas as pd
import datetime as dt
import logging
import asyncio
import asyncpg, aiohttp
import ujson as json
import boto3, botocore
import argparse
from time import sleep
import IPython
import numpy as np

MODE = {
  1: 'real',
  2: 'test',
  3: 'real&test'
}


def main(max_pats=None, hospital=None, lookback_hours=None, db_name=None, repl=False):
  # Start
  global start_time
  start_time = dt.datetime.now()

  # Create config objects
  config = Config(debug=True, db_name=db_name)
  config_dict = {
    'db_name': db_name or core.get_environment_var('db_name'),
    'db_user': core.get_environment_var('db_user'),
    'db_pass': core.get_environment_var('db_password'),
    'db_host': core.get_environment_var('db_host'),
    'db_port': core.get_environment_var('db_port'),
  }
  hospital = hospital or core.get_environment_var('TREWS_ETL_HOSPITAL')
  # Create data for loader
  job_id = "job_etl_{}_{}".format(hospital, dt.datetime.now().strftime('%Y%m%d%H%M%S')).lower()
  archive = int(core.get_environment_var('TREWS_ETL_ARCHIVE', 0))
  lookback_hours = lookback_hours or core.get_environment_var('TREWS_ETL_HOURS')
  op_lookback_days = int(core.get_environment_var('TREWS_ET_OP_DAYS', 365))
  # Create jhapi_extractor
  extractor = JHAPIConfig(
    hospital       = hospital,
    lookback_hours = lookback_hours,
    jhapi_server   = core.get_environment_var('TREWS_ETL_SERVER', 'prod'),
    jhapi_id       = core.get_environment_var('jhapi_client_id'),
    jhapi_secret   = core.get_environment_var('jhapi_client_secret'),
    op_lookback_days = op_lookback_days
  )

  # Get stuff for boto client
  aws_region = core.get_environment_var('AWS_DEFAULT_REGION')
  prod_or_dev = core.get_environment_var('db_name')

  # Get mode (real, test, both)
  mode = MODE[int(core.get_environment_var('TREWS_ETL_MODE', 0))]

  # Get suppression alert mode
  suppression = int(core.get_environment_var('TREWS_ETL_SUPPRESSION', 0))

  ########################
  # Build plan
  all_tasks = []
  if 'real' in mode:
    all_tasks += get_extraction_tasks(extractor, max_pats)
    all_tasks += get_combine_tasks()
    all_tasks.append({
        'name': 'push_cloudwatch_metrics',
        'deps': ['combine_cloudwatch_data'],
        'fn':   push_cloudwatch_metrics,
        'args': [aws_region, prod_or_dev, hospital]
      })
    if suppression == 0:
      # NOTE: if suppression is 1, notify_epic will be done in suppression alert server
      all_tasks.append({
        'name': 'push_notifications',
        'deps': ['get_notifications_for_epic'],
        'fn': extractor.push_notifications
      })
      all_tasks.append({
        'name': 'notify_future_notification',
        'deps': ['get_notifications_for_epic'],
        'coro': loader.notify_future_notification
      })


  loading_tasks  = loader.get_tasks(job_id, 'combine_db_data', 'combine_extract_data', mode, archive, config.get_db_conn_string_sqlalchemy(), suppression=suppression)
  criteria_tasks = get_criteria_tasks(job_id,
    dependency      = 'workspace_submit',
    lookback_hours  = lookback_hours,
    hospital        = hospital,
    suppression     = suppression)

  ########################
  # Build plan for repl
  if repl:
    all_tasks = get_extraction_tasks(extractor, max_pats)
    loading_tasks = []
    criteria_tasks = []

  ########################
  # Run plan
  # TODO: Check if empty responses from extractor break the engine
  plan = Plan(name="epic2op_plan", config=config_dict)
  for task_def in all_tasks:
    plan.add(Task(**task_def))
  if suppression < 2:
    # NOTE: when suppression is 2, criteria calculation will be done in the alert server
    for task in criteria_tasks:
      plan.add(task)
  for task in loading_tasks:
    plan.add(task)
  engine = Engine(
    plan     = plan,
    name     = "epic2op_engine",
    nprocs   = 2,
    loglevel = logging.DEBUG,
    with_gc  = (not repl),
    with_graph = True
  )
  loop = asyncio.new_event_loop()
  loop.run_until_complete(engine.run())
  loop.close()

  ########################
  # Submit total time to cloudwatch
  if 'real' in mode:
    submit_time_to_cloudwatch(aws_region, prod_or_dev, hospital)

  return engine



def combine_extract_data(ctxt, pats, ed_pats, flowsheets, active_procedures, lab_orders,
                      lab_results, med_orders, med_admin, loc_history, notes,
                      note_texts, chiefcomplaint, treatmentteam):
  return {
    'bedded_patients': pats,
    'ed_patients': ed_pats,
    'flowsheets': flowsheets,
    'active_procedures': active_procedures,
    'lab_orders': lab_orders,
    'lab_results': lab_results,
    'med_orders': med_orders,
    'med_admin': med_admin,
    'location_history': loc_history,
    'notes': notes,
    'note_texts': note_texts,
    'chiefcomplaint': chiefcomplaint,
    'treatmentteam': treatmentteam
  }


def combine_db_data(ctxt, pats_t, flowsheets_t, active_procedures_t, lab_orders_t,
                 lab_results_t, med_orders_t, med_admin_t, loc_history_t,
                 notes_t, note_texts_t, chiefcomplaint_t, treatmentteam_t):
  pats_t.diagnosis = pats_t.diagnosis.apply(json.dumps)
  pats_t.history = pats_t.history.apply(json.dumps)
  pats_t.problem = pats_t.problem.apply(json.dumps)
  pats_t.problem_all = pats_t.problem_all.apply(json.dumps)
  if not med_orders_t.empty:
    med_orders_t['ids'] = med_orders_t['ids'].apply(json.dumps)
  db_data = {
    'bedded_patients_transformed': pats_t,
    'flowsheets_transformed': flowsheets_t,
    'active_procedures_transformed': active_procedures_t,
    'lab_orders_transformed': lab_orders_t,
    'lab_results_transformed': lab_results_t,
    'med_orders_transformed': med_orders_t,
    'med_admin_transformed': med_admin_t,
    'location_history_transformed': loc_history_t,
    'notes_transformed': notes_t,
    'note_texts_transformed': note_texts_t,
    'chiefcomplaint_transformed': chiefcomplaint_t,
    'treatmentteam_transformed': treatmentteam_t
  }
  return db_data


def combine_cloudwatch_data(ctxt, pats_t, flowsheets_t, active_procedures_t,
                            lab_orders_t, lab_results_t, med_orders_t,
                            med_admin_t, loc_history_t, notes_t, note_texts_t,
                            chiefcomplaint_t, treatmentteam_t):
  return {
    'bedded_pats'       : len(pats_t.index),
    'flowsheets'        : len(flowsheets_t.index),
    'lab_orders'        : len(lab_orders_t.index),
    'active_procedures' : len(active_procedures_t.index),
    'lab_results'       : len(lab_results_t.index),
    'med_orders'        : len(med_orders_t.index),
    'med_admin'         : len(med_admin_t.index),
    'loc_history'       : len(loc_history_t.index),
    'notes'             : len(notes_t.index),
    'note_texts'        : len(note_texts_t.index),
    'chiefcomplaint'    : len(chiefcomplaint_t.index),
    'treatmentteam'     : len(treatmentteam_t.index)
  }


def get_combine_tasks():
  return [{
    'name': 'combine_db_data',
    'deps': [
      'patients_combine',
      'timezone_hack_flowsheets',
      'active_procedures_transform',
      'lab_orders_transform',
      'lab_results_transform',
      'med_orders_transform',
      'timezone_hack_med_admin',
      'loc_history_transform',
      'notes_transform',
      'notes_texts_transform',
      'chiefcomplaint_transform',
      'treatmentteam_transform'
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_db_data
  }, {
    'name': 'combine_cloudwatch_data',
    'deps': [
      'patients_combine',
      'timezone_hack_flowsheets',
      'active_procedures_transform',
      'lab_orders_transform',
      'lab_results_transform',
      'med_orders_transform',
      'timezone_hack_med_admin',
      'loc_history_transform',
      'notes_transform',
      'notes_texts_transform',
      'chiefcomplaint_transform',
      'treatmentteam_transform'
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_cloudwatch_data
  }, {
    'name': 'combine_extract_data',
    'deps': [
      'bedded_patients_extract',
      'ed_patients_extract',
      'flowsheets_extract',
      'active_procedures_extract',
      'lab_orders_extract',
      'lab_results_extract',
      'med_orders_extract',
      'med_admin_extract',
      'loc_history_extract',
      'notes_extract',
      'notes_texts_extract',
      'chiefcomplaint_extract',
      'treatmentteam_extract'
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_extract_data
  }]



def push_cloudwatch_metrics(ctxt, stats, aws_region, prod_or_dev, hospital):
  boto_client = boto3.client('cloudwatch', region_name=aws_region)
  metric_data = [
    { 'MetricName': 'ExtractTime', 'Value': (dt.datetime.now() - start_time).total_seconds(), 'Unit': 'Seconds'},
    { 'MetricName': 'NumBeddedPatients', 'Value': stats['bedded_pats'], 'Unit': 'Count'},
    { 'MetricName': 'NumFlowsheets', 'Value': stats['flowsheets'], 'Unit': 'Count'},
    { 'MetricName': 'NumActiveProcedures', 'Value': stats['active_procedures'], 'Unit': 'Count'},
    { 'MetricName': 'NumLabOrders', 'Value': stats['lab_orders'], 'Unit': 'Count'},
    { 'MetricName': 'NumLabResults', 'Value': stats['lab_results'], 'Unit': 'Count'},
    { 'MetricName': 'NumLocationHistory', 'Value': stats['loc_history'], 'Unit': 'Count'},
    { 'MetricName': 'NumMedAdmin', 'Value': stats['med_admin'], 'Unit': 'Count'},
    { 'MetricName': 'NumMedOrders', 'Value': stats['med_orders'], 'Unit': 'Count'},
  ]
  for md in metric_data:
    md['MetricName'] = '{}_{}'.format(hospital, md['MetricName'])
    md['Dimensions'] = [{'Name': 'ETL', 'Value': prod_or_dev}]
    md['Timestamp'] = dt.datetime.utcnow()
  try:
    boto_client.put_metric_data(Namespace='OpsDX', MetricData=metric_data)
    ctxt.log.info('successfully pushed cloudwatch metrics')
  except botocore.exceptions.EndpointConnectionError as e:
    ctxt.log.error('unsuccessfully pushed cloudwatch metrics')
    ctxt.log.error(e)


def submit_time_to_cloudwatch(aws_region, prod_or_dev, hospital):
  boto_client = boto3.client('cloudwatch', region_name=aws_region)
  metric_data = [{
    'MetricName': '{}_TotalTime'.format(hospital),
    'Value':      (dt.datetime.now() - start_time).total_seconds(),
    'Unit':       'Seconds',
    'Dimensions': [{'Name': 'ETL', 'Value': prod_or_dev}],
    'Timestamp':  dt.datetime.utcnow(),
  }]
  try:
    boto_client.put_metric_data(Namespace='OpsDX', MetricData=metric_data)
    logging.info('successfully pushed total time to cloudwatch')
  except botocore.exceptions.EndpointConnectionError as e:
    logging.error('unsuccessfully pushed total time cloudwatch metrics')
    logging.error(e)

def build_med_admin_request_data(ctxt, med_orders):
  if not med_orders.empty:
    return med_orders[med_orders.order_mode == 'Inpatient'][['pat_id', 'visit_id', 'ids']]\
      .groupby(['pat_id', 'visit_id'])['ids']\
      .apply(list)\
      .reset_index()
  else:
    ctxt.log.warn("No medication order.")
    return None

def add_column(ctxt, df, col_name, col_data):
  df[col_name] = col_data
  return df

def tz_hack(ctxt, df):
  if not df.empty:
    df['tsp'] = df['tsp'].str.replace('-04:00', '+00:00')
    df['tsp'] = df['tsp'].str.replace('-05:00', '+00:00')
  return df

def skip_none(df, transform_function):
  if df is None or df.empty:
    return pd.DataFrame()
  try:
    start = dt.datetime.now()
    df = transform_function(df)
    logging.info("function time: {}".format(dt.datetime.now() - start))
    return df
  except TransformError as e:
    logging.error("== EXCEPTION CAUGHT ==")
    logging.error("error location:   " + e.func_name)
    logging.error("reason for error: " + e.reason)
    logging.error(e.context)
    traceback.print_exc()

def transform(ctxt, df, transform_list_name):
  if df is None:
    return pd.DataFrame()
  if type(df) == list:
    df = pd.concat(df)
  for transform_fn in getattr(jhapi_transform_lists, transform_list_name):
    df = skip_none(df, transform_fn)
  return df

def patients_combine(ctxt, df, df2):
  if df2 is not None and not df2.empty:
    df2 = df2[~df2.pat_id.isin(df.pat_id)]
    df2['patient_class'] = None
    df2['diagnosis'] = None
    df2['history'] = None
    df2['problem'] = None
    df2['problem_all'] = None
    df2['patient_class'] = 'Emergency NB'
    df2['diagnosis'] = df2['diagnosis'].apply(lambda x: {})
    df2['history'] = df2['history'].apply(lambda x: {})
    df2['problem'] = df2['problem'].apply(lambda x: {})
    df2['problem_all'] = df2['problem_all'].apply(lambda x: {})
    df = df.append(df2)
    df = df.reset_index()
  return df

def get_extraction_tasks(extractor, max_pats=None):
  return [
    {
      'name': 'bedded_patients_extract',
      'fn':   extractor.extract_bedded_patients,
      'args': [extractor.hospital, max_pats],
    },
    {
      'name': 'ed_patients_extract',
      'fn':   extractor.extract_ed_patients,
      'args': [extractor.hospital, max_pats],
    },
    { # Barrier 1
      'name': 'bedded_patients_transform',
      'deps': ['bedded_patients_extract'],
      'fn':   transform,
      'args': ['bedded_patients_transforms'],
    },
    { # Barrier 1
      'name': 'ed_patients_transform',
      'deps': ['ed_patients_extract'],
      'fn':   transform,
      'args': ['ed_patients_transforms'],
    },
    {
      'name': 'ed_patients_mrn_extract',
      'deps': ['ed_patients_transform'],
      'fn':   extractor.extract_ed_patients_mrn,
      'args': [],
    },
    {
      'name': 'patients_combine',
      'fn':   patients_combine,
      'deps': ['bedded_patients_transform', 'ed_patients_mrn_extract'],
      'args': []
    },
    { # Barrier 2
      'name': 'flowsheets_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_flowsheets,
    }, {
      'name': 'active_procedures_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_active_procedures,
    }, {
      'name': 'lab_orders_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_lab_orders,
    }, {
      'name': 'lab_results_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_lab_results,
    }, {
      'name': 'loc_history_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_loc_history,
    }, {
      'name': 'med_orders_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_med_orders,
    }, {
      'name': 'notes_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_notes,
    }, {
      'name': 'chiefcomplaint_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_chiefcomplaint,
    }, {
      'name': 'treatmentteam_extract',
      'deps': ['patients_combine'],
      'fn':   extractor.extract_treatmentteam,
    }, { # Barrier 3
      'name': 'flowsheets_transform',
      'deps': ['flowsheets_extract'],
      'fn':   transform,
      'args': ['flowsheet_transforms'],
    }, {
      'name': 'treatmentteam_transform',
      'deps': ['treatmentteam_extract'],
      'fn': transform,
      'args': ['treatmentteam_transforms']
    }, {
      'name': 'chiefcomplaint_transform',
      'deps': ['chiefcomplaint_extract'],
      'fn': transform,
      'args': ['chiefcomplaint_transforms']
    }, {
      'name': 'active_procedures_transform',
      'deps': ['active_procedures_extract'],
      'fn':   transform,
      'args': ['active_procedures_transforms'],
    }, {
      'name': 'lab_orders_transform',
      'deps': ['lab_orders_extract'],
      'fn':   transform,
      'args': ['lab_orders_transforms'],
    }, {
      'name': 'lab_results_transform',
      'deps': ['lab_results_extract'],
      'fn':   transform,
      'args': ['lab_results_transforms'],
    }, {
      'name': 'loc_history_transform',
      'deps': ['loc_history_extract'],
      'fn':   transform,
      'args': ['loc_history_transforms'],
    }, {
      'name': 'med_orders_transform',
      'deps': ['med_orders_extract'],
      'fn':   transform,
      'args': ['med_orders_transforms'],
    }, {
      'name': 'notes_transform',
      'deps': ['notes_extract'],
      'fn':   transform,
      'args': ['notes_transforms'],
    }, { # Barrier 4
      'name': 'build_med_admin_request_data',
      'deps': ['med_orders_transform'],
      'fn':   build_med_admin_request_data,
    }, {
      'name': 'notes_texts_extract',
      'deps': ['notes_extract'],
      'fn':   extractor.extract_note_texts,
    }, { # Barrier 5
      'name': 'med_admin_extract',
      'deps': ['build_med_admin_request_data'],
      'fn':   extractor.extract_med_admin,
    }, {
      'name': 'notes_texts_transform',
      'deps': ['notes_texts_extract'],
      'fn':   transform,
      'args': ['note_texts_transforms'],
    }, { # Barrier 6
      'name': 'med_admin_transform',
      'deps': ['med_admin_extract'],
      'fn':   transform,
      'args': ['med_admin_transforms'],
    }, { # Barrier 7
      'name': 'timezone_hack_flowsheets',
      'deps': ['flowsheets_transform'],
      'fn':   tz_hack,
    }, {
      'name': 'timezone_hack_med_admin',
      'deps': ['med_admin_transform'],
      'fn':   tz_hack,
    },
    # Discharge time stuff
    {
      'name': 'non_discharged_patients_extract',
      'coro':  loader.extract_non_discharged_patients,
      'args': [extractor.hospital]
    }, {
      'name': 'contacts_extract',
      'deps': ['non_discharged_patients_extract'],
      'fn':   extractor.extract_contacts,
    }, {
      'name': 'contacts_transform',
      'deps': ['contacts_extract'],
      'fn':   transform,
      'args': ['contacts_transforms']
    }
  ]



def start_repl(task_results):
  global results
  results = task_results
  print("\n\n\tStarting REPL.")
  print("\n\n\tThe dictionary 'results' holds all dataframes.")
  print("\n\n\tHere are the contents:")
  for name, df in results.items():
    if df is None:
      continue
    print("\t{:30} -- dataframe of size {}".format(name, len(df)))
  print("\n\n")
  IPython.embed()



def parse_arguments():
  parser = argparse.ArgumentParser(description='JHAPI ETL')
  parser.add_argument(
    '-m', '--max_pats',
    type = int,
    help = "The maximum number of patients to fetch from jhapi",
  )
  parser.add_argument(
    '-ho', '--hospital',
    choices = ['JHH', 'BMC', 'SMH', 'HCGH', 'SHM'],
    help    = "The hospital to fetch patients for",
  )
  parser.add_argument(
    '-l', '--lookback_hours',
    type = int,
    help = "The number of hours to search back in the flowsheets (72 max)",
  )
  parser.add_argument(
    '-d', '--db_name',
    type = str,
    help = "The name of the db to load into (defaults to environment variable)",
  )
  parser.add_argument(
    '-r', '--repl',
    choices = [True, False],
    type    = bool,
    default = False,
    help    = "Whether or not to skip loading and drop into a REPL",
  )
  return parser.parse_args()


if __name__ == '__main__':
  pd.set_option('display.width', 200)
  pd.set_option('display.max_rows', 30)
  pd.set_option('display.max_columns', 1000)
  pd.set_option('display.max_colwidth', 40)
  pd.options.mode.chained_assignment = None
  logging.getLogger().setLevel(0)
  args = parse_arguments()
  eng_ret = main(
    max_pats       = args.max_pats,
    hospital       = args.hospital,
    lookback_hours = args.lookback_hours,
    db_name        = args.db_name,
    repl           = args.repl,
  )
  if args.repl:
    start_repl(eng_ret.task_results)
