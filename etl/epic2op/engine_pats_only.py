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


  loading_tasks  = loader.get_tasks_pat_only(job_id, 'combine_db_data', 'combine_extract_data', mode, archive, suppression=suppression)
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



def combine_extract_data(ctxt, pats, ed_pats):
  return {
    'bedded_patients': pats,
    'ed_patients': ed_pats
  }


def combine_db_data(ctxt, pats_t):
  pats_t.diagnosis = pats_t.diagnosis.apply(json.dumps)
  pats_t.history = pats_t.history.apply(json.dumps)
  pats_t.problem = pats_t.problem.apply(json.dumps)
  pats_t.problem_all = pats_t.problem_all.apply(json.dumps)
  db_data = {
    'bedded_patients_transformed': pats_t
  }
  return db_data


def combine_cloudwatch_data(ctxt, pats_t):
  return {
    'bedded_pats'       : len(pats_t.index)
  }


def get_combine_tasks():
  return [{
    'name': 'combine_db_data',
    'deps': [
      'patients_combine'
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_db_data
  }, {
    'name': 'combine_cloudwatch_data',
    'deps': [
      'patients_combine',
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_cloudwatch_data
  }, {
    'name': 'combine_extract_data',
    'deps': [
      'bedded_patients_extract',
      'ed_patients_extract',
    ], # ORDER MATTERS! - because Engine breaks if we use **kwargs'
    'fn':   combine_extract_data
  }]



def push_cloudwatch_metrics(ctxt, stats, aws_region, prod_or_dev, hospital):
  boto_client = boto3.client('cloudwatch', region_name=aws_region)
  metric_data = [
    { 'MetricName': 'ExtractTime', 'Value': (dt.datetime.now() - start_time).total_seconds(), 'Unit': 'Seconds'},
    { 'MetricName': 'NumBeddedPatients', 'Value': stats['bedded_pats'], 'Unit': 'Count'},
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
    df2 = df2[~df2.visit_id.isin(df.visit_id)]
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
