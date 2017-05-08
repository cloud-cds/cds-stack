from etl.core.exceptions import TransformError
from etl.core.config import Config
from etl.transforms.pipelines import jhapi as jhapi_transform_lists
from etl.load.pipelines.epic2op import Epic2OpLoader
import etl.load.pipelines.epic2op as epic2op_loader
from etl.load.pipelines.criteria import get_criteria_tasks
from etl.core.task import Task
from etl.core.plan import Plan
from etl.core.engine import Engine
from etl.io_config.jhapi import JHAPIConfig
import os, sys, traceback, functools
import pandas as pd
import datetime as dt
import dateparser
import logging
import asyncpg, aiohttp
import ujson as json
import boto3, botocore
from time import sleep
import asyncio

MODE = {
  1: 'real',
  2: 'test',
  3: 'real&test'
}

def main(max_num_pats=None, hospital=None, lookback_hours=None, db_name=None):
  # Create config objects
  config = Config(debug=True, db_name=db_name)
  config_dict = {
    'db_name': os.environ['db_name'],
    'db_user': os.environ['db_user'],
    'db_pass': os.environ['db_password'],
    'db_host': os.environ['db_host'],
    'db_port': os.environ['db_port'],
  }

  # Create loader
  loader = Epic2OpLoader(config)
  if 'TREWS_ETL_ARCHIVE' in os.environ:
    loader.archive = int(os.environ['TREWS_ETL_ARCHIVE'])
  notify_epic = int(os.environ['TREWS_ETL_EPIC_NOTIFICATIONS'])

  # Create jhapi_extractor
  extractor = JHAPIConfig(
    hospital =       hospital or os.environ['TREWS_ETL_HOSPITAL'],
    lookback_hours = lookback_hours or os.environ['TREWS_ETL_HOURS'],
    jhapi_server =   'prod' or os.environ['TREWS_ETL_SERVER'],
    jhapi_id =       os.environ['jhapi_client_id'],
    jhapi_secret =   os.environ['jhapi_client_secret'],
  )
  
  # Get stuff for boto client
  aws_region = os.environ['AWS_DEFAULT_REGION']
  prod_or_dev = os.environ['db_name']

  # Get mode (real, test, both)
  mode = MODE[int(os.environ.get('TREWS_ETL_MODE', 0))]

  ########################
  # Build plan
  job_id = loader.job_id
  all_tasks = []
  if 'real' in mode:
    all_tasks += get_extraction_tasks(extractor, max_num_pats)
    all_tasks += get_combine_tasks()
    all_tasks += [
      { 
        'name': 'load_task', 'deps': ['combine_data', 'build_db_extract_data'],
        'fn':   loader.run_loop, 'args': [mode]
      }, {
        'name': 'push_cloudwatch_metrics', 'deps': ['combine_cloudwatch_data'],
        'fn': push_cloudwatch_metrics, 'args': [aws_region, prod_or_dev]
      }, {
        'name': 'get_notifications', 'deps': ['load_task'],
        'coro': epic2op_loader.get_notifications_for_epic,
        'args': [job_id]
      }, {
        'name': 'push_notifications', 'deps': ['get_notifications'],
        'fn': extractor.push_notifications
      },
    ]
    if notify_epic:
      all_tasks += get_notification_tasks(loader, extractor)
  else:
    all_tasks = [
      {'name': 'load_task', 'fn': loader.run_loop, 'args': [None, None, mode]}
    ]

  criteria_tasks = get_criteria_tasks(dependency = 'load_task')
  

  ########################
  # Run plan
  # TODO: Check if empty responses from extractor break the engine
  plan = Plan(name="epic2op_plan", config=config_dict)
  for task_def in all_tasks:
    plan.add(Task(**task_def))
  for task in criteria_tasks:
    plan.add(task)
  engine = Engine(plan=plan, name="epic2op_engine", nprocs=2, loglevel=logging.DEBUG)
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  loop.run_until_complete(engine.run())
  loop.close()
  return "finished"



def combine_extract_data(ctxt, pats, flowsheets, active_procedures, lab_orders, 
                      lab_results, med_orders, med_admin, loc_history, notes, 
                      note_texts):
  return {
    'bedded_patients': pats,
    'flowsheets': flowsheets,
    'active_procedures': active_procedures,
    'lab_orders': lab_orders,
    'lab_results': lab_results,
    'med_orders': med_orders,
    'med_admin': med_admin,
    'location_history': loc_history,
    'notes': notes,
    'note_texts': note_texts,
  }


def combine_db_data(ctxt, pats_t, flowsheets_t, active_procedures_t, lab_orders_t, 
                 lab_results_t, med_orders_t, med_admin_t, loc_history_t, 
                 notes_t, note_texts_t):
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
  }
  return db_data


def combine_cloudwatch_data(ctxt, pats_t, flowsheets_t, lab_orders_t, 
                            active_procedures_t, lab_results_t, med_orders_t, 
                            med_admin_t, loc_history_t, notes_t, note_texts_t):
  return {
    # 'total_time'        : (dt.datetime.now() - self.driver_start).total_seconds(),
    # 'request_time'      : self.extract_time.total_seconds(),
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
  }


def get_combine_tasks():
  return [{
    'name': 'combine_data',
    'deps': [
      'bedded_patients_transform',
      'timezone_hack_flowsheets',
      'active_procedures_transform',
      'lab_orders_transform',
      'lab_results_transform',
      'med_orders_transform',
      'timezone_hack_med_admin',
      'loc_history_transform',
      'notes_transform',
      'notes_texts_transform',
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_db_data
  }, {
    'name': 'combine_cloudwatch_data',
    'deps': [
      'bedded_patients_transform',
      'timezone_hack_flowsheets',
      'active_procedures_transform',
      'lab_orders_transform',
      'lab_results_transform',
      'med_orders_transform',
      'timezone_hack_med_admin',
      'loc_history_transform',
      'notes_transform',
      'notes_texts_transform',
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_cloudwatch_data
  }, {
    'name': 'combine_extract_data',
    'deps': [
      'bedded_patients_extract',
      'flowsheets_extract',
      'active_procedures_extract',
      'lab_orders_extract',
      'lab_results_extract',
      'med_orders_extract',
      'med_admin_extract',
      'loc_history_extract',
      'notes_extract',
      'notes_texts_extract',
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_extract_data
  }]



def push_cloudwatch_metrics(ctxt, stats, aws_region, prod_or_dev):
  boto_client = boto3.client('cloudwatch', region_name=aws_region)
  metric_data = [
    # { 'MetricName': 'ExTrLoTime', 'Value':  etl_time.total_seconds(), 'Unit': 'Seconds'},
    # { 'MetricName': 'ExTrTime', 'Value': stats['total_time'], 'Unit': 'Seconds'},
    # { 'MetricName': 'ExTime', 'Value': stats['request_time'], 'Unit': 'Seconds'},
    { 'MetricName': 'NumBeddedPatients', 'Value': stats['bedded_pats'], 'Unit': 'Count'},
    { 'MetricName': 'NumFlowsheets', 'Value': stats['flowsheets'], 'Unit': 'Count'},
    { 'MetricName': 'NumLabOrders', 'Value': stats['lab_orders'], 'Unit': 'Count'},
    { 'MetricName': 'NumLabResults', 'Value': stats['lab_results'], 'Unit': 'Count'},
    { 'MetricName': 'NumLocationHistory', 'Value': stats['loc_history'], 'Unit': 'Count'},
    { 'MetricName': 'NumMedAdmin', 'Value': stats['med_admin'], 'Unit': 'Count'},
    { 'MetricName': 'NumMedOrders', 'Value': stats['med_orders'], 'Unit': 'Count'},
  ]
  for md in metric_data:
    md['Dimensions'] = [{'Name': 'ETL', 'Value': prod_or_dev}]
    md['Timestamp'] = dt.datetime.utcnow()
  try:
    boto_client.put_metric_data(Namespace='OpsDX', MetricData=metric_data)
  except botocore.exceptions.EndpointConnectionError as e:
    logging.error(e)



def build_med_admin_request_data(ctxt, med_orders):
  return med_orders[['pat_id', 'visit_id', 'ids']]\
    .groupby(['pat_id', 'visit_id'])['ids']\
    .apply(list)\
    .reset_index()

def add_column(ctxt, df, col_name, col_data):
  df[col_name] = col_data
  return df

def tz_hack(ctxt, df):
  est_fmt = '%Y-%m-%dT%H:%M:%S-05:00'
  five_hr = dt.timedelta(hours=5)
  if not df.empty:
    df['tsp'] = df['tsp'].apply(lambda x: (dateparser.parse(x) - five_hr).strftime(est_fmt))
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

def get_extraction_tasks(extractor, max_num_pats=None):
  return [
    {
      'name': 'bedded_patients_extract', 
      'fn':   extractor.extract_bedded_patients,
      'args': [extractor.hospital, max_num_pats],
    }, { # Barrier 1
      'name': 'bedded_patients_transform',
      'deps': ['bedded_patients_extract'],
      'fn':   transform,
      'args': ['bedded_patients_transforms'],
    }, { # Barrier 2
      'name': 'flowsheets_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_flowsheets,
    }, {
      'name': 'active_procedures_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_active_procedures,
    }, {
      'name': 'lab_orders_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_lab_orders,
    }, {
      'name': 'lab_results_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_lab_results,
    }, {
      'name': 'loc_history_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_loc_history,
    }, {
      'name': 'med_orders_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_med_orders,
    }, {
      'name': 'notes_extract',
      'deps': ['bedded_patients_transform'],
      'fn':   extractor.extract_notes,
    }, { # Barrier 3
      'name': 'flowsheets_transform',
      'deps': ['flowsheets_extract'],
      'fn':   transform,
      'args': ['flowsheet_transforms'],
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
    }
  ]



if __name__ == '__main__':
  pd.set_option('display.width', 200)
  pd.set_option('display.max_rows', 30)
  pd.set_option('display.max_columns', 1000)
  pd.set_option('display.max_colwidth', 40)
  pd.options.mode.chained_assignment = None
  logging.getLogger().setLevel(0)
  results = main(max_num_pats=20)
  print(results)
