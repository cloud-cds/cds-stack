from etl.core.exceptions import TransformError
from etl.core.config import Config
# from etl.epic2op.extractor import Extractor
from etl.transforms.pipelines import jhapi as jhapi_transform_lists
from etl.load.pipelines.epic2op import Epic2OpLoader
from etl.load.pipelines.criteria import Criteria
from etl.core.task import Task
from etl.core.plan import Plan
from etl.core.engine import Engine
from etl.io_config.jhapi import JHAPIConfig
import os, sys, traceback
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

# def engine_init(hospital=None, lookback_hours=None, db_name=None, max_num_pats=None):
#   self.config = Config(debug=True, db_name=db_name)
#   mode_env = int(os.environ.get('TREWS_ETL_MODE', 0))
#   self.mode = MODE[mode_env]
#   self.loader = Epic2OpLoader(self.config)
#   if 'TREWS_ETL_ARCHIVE' in os.environ:
#     self.loader.archive = int(os.environ['TREWS_ETL_ARCHIVE'])
#   self.extractor = Extractor(
#     hospital =       hospital or os.environ['TREWS_ETL_HOSPITAL'],
#     lookback_hours = lookback_hours or os.environ['TREWS_ETL_HOURS'],
#     jhapi_server =   'prod' or os.environ['TREWS_ETL_SERVER'],
#     jhapi_id =       os.environ['jhapi_client_id'],
#     jhapi_secret =   os.environ['jhapi_client_secret'],
#   )
#   self.notify_epic = int(os.environ['TREWS_ETL_EPIC_NOTIFICATIONS'])
#   self.prod_or_dev = os.environ['db_name']
#   # Create boto client
#   aws_region = os.environ['AWS_DEFAULT_REGION']
#   self.boto_client = boto3.client('cloudwatch', region_name=aws_region)

#   self.criteria = Criteria(self.config)
#   self.extract_time = dt.timedelta(0)
#   self.transform_time = dt.timedelta(0)
#   self.max_num_pats = int(max_num_pats) if max_num_pats else max_num_pats

#   async def init(self):
#     self.pool = await asyncpg.create_pool(database=self.config.db_name, user=self.config.db_user, password=self.config.db_pass, host=self.config.db_host, port=self.config.db_port)

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


def push_cloudwatch_metrics(stats):
  etl_time = (dt.datetime.now() - self.driver_start)
  metric_data = [{
    'MetricName': 'ExTrLoTime',
    'Value':  etl_time.total_seconds(), 'Unit': 'Seconds'
  },{ 'MetricName': 'ExTrTime',
    'Value': stats['total_time'], 'Unit': 'Seconds'
  },{ 'MetricName': 'ExTime',
    'Value': stats['request_time'], 'Unit': 'Seconds'
  },{ 'MetricName': 'NumBeddedPatients',
    'Value': stats['bedded_pats'], 'Unit': 'Count'
  },{ 'MetricName': 'NumFlowsheets',
    'Value': stats['flowsheets'], 'Unit': 'Count'
  },{ 'MetricName': 'NumLabOrders',
    'Value': stats['lab_orders'], 'Unit': 'Count'
  },{ 'MetricName': 'NumLabResults',
    'Value': stats['lab_results'], 'Unit': 'Count'
  },{ 'MetricName': 'NumLocationHistory',
    'Value': stats['loc_history'], 'Unit': 'Count'
  },{ 'MetricName': 'NumMedAdmin',
    'Value': stats['med_admin'], 'Unit': 'Count'
  },{ 'MetricName': 'NumMedOrders',
    'Value': stats['med_orders'], 'Unit': 'Count'
  }]
  for md in metric_data:
    md['Dimensions'] = [{'Name': 'ETL', 'Value': self.prod_or_dev}]
    md['Timestamp'] = dt.datetime.utcnow()
  try:
    self.boto_client.put_metric_data(Namespace='OpsDX', MetricData=metric_data)
  except botocore.exceptions.EndpointConnectionError as e:
    logging.error(e)


def tz_hack(ctxt, df):
  est_fmt = '%Y-%m-%dT%H:%M:%S-05:00'
  five_hr = dt.timedelta(hours=5)
  if not df.empty:
    df['tsp'] = df['tsp'].apply(lambda x: (dateparser.parse(x) - five_hr).strftime(est_fmt))
  return df
  

def main_old(self):
  self.db_data = None
  self.db_raw_data = None
  self.driver_start = dt.datetime.now()
  if 'real' in self.mode:
    self.extract_pat_data()

  self.loader.run_loop(self.db_data, self.db_raw_data, self.mode)
  self.criteria.run_loop()

  if 'real' in self.mode:
    if self.notify_epic:
      notifications = self.loader.get_notifications_for_epic()
      self.extractor.push_notifications(notifications)

  self.push_cloudwatch_metrics(self.cloudwatch_stats)

def extract_pat_data(self):
  pats = self.extract(self.extractor.extract_bedded_patients, "bedded_patients", [self.max_num_pats])
  pats_t = self.transform(pats, jhapi.bedded_patients_transforms, "bedded_patients")
  pats_t = pats_t.assign(hospital = self.extractor.hospital)

  flowsheets = self.extract(self.extractor.extract_flowsheets, "flowsheets", [pats_t])
  flowsheets_t = self.transform(flowsheets, jhapi.flowsheet_transforms, "flowsheets")
  # Timezone hack
  if not flowsheets.empty:
    flowsheets_t['tsp'] = flowsheets_t['tsp'].apply(self.tz_hack)

  active_procedures = self.extract(self.extractor.extract_active_procedures, "active_procedures", [pats_t])
  active_procedures_t = self.transform(active_procedures, jhapi.active_procedures_transforms, "active_procedures")

  lab_orders = self.extract(self.extractor.extract_lab_orders, "lab_orders", [pats_t])
  lab_orders_t = self.transform(lab_orders, jhapi.lab_orders_transforms, "lab_orders")

  lab_results = self.extract(self.extractor.extract_lab_results, "lab_results", [pats_t])
  lab_results_t = self.transform(lab_results, jhapi.lab_results_transforms, "lab_results")

  loc_history = self.extract(self.extractor.extract_loc_history, "loc_history", [pats_t])
  loc_history_t = self.transform(loc_history, jhapi.loc_history_transforms, "loc_history")

  med_orders = self.extract(self.extractor.extract_med_orders, "med_orders", [pats_t])
  med_orders_t = self.transform(med_orders, jhapi.med_orders_transforms, "med_orders")

  if not med_orders_t.empty:
    request_data = med_orders_t[['pat_id', 'visit_id', 'ids']]\
      .groupby(['pat_id', 'visit_id'])['ids']\
      .apply(list)\
      .reset_index()
    ma_start = dt.datetime.now()
    med_admin = self.extract(self.extractor.extract_med_admin, "med_admin", [request_data])
    ma_total = dt.datetime.now() - ma_start
    med_admin_t = self.transform(med_admin, jhapi.med_admin_transforms, "med_admin")
    # Timezone hack
    if not med_admin_t.empty:
      med_admin_t['tsp'] = med_admin_t['tsp'].apply(self.tz_hack)
  else:
    # Make empty dataframes so functions down the line don't break
    med_admin   = pd.DataFrame()
    med_admin_t = pd.DataFrame()

  notes = self.extract(self.extractor.extract_notes, "notes", [pats_t])
  notes_t = self.transform(notes, jhapi.notes_transforms, "notes")

  note_texts = self.extract(self.extractor.extract_note_texts, "note_texts", [notes])
  note_texts_t = self.transform(note_texts, jhapi.note_texts_transforms, "note_texts")

  # Main finished
  logging.info("extract time: {}".format(self.extract_time))
  logging.info("transform time: {}".format(self.transform_time))
  logging.info("total time: {}".format(dt.datetime.now() - self.driver_start))

  # Create stats object
  self.cloudwatch_stats = {
    'total_time'        : (dt.datetime.now() - self.driver_start).total_seconds(),
    'request_time'      : self.extract_time.total_seconds(),
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

  # Prepare for database
  pats_t.diagnosis = pats_t.diagnosis.apply(json.dumps)
  pats_t.history = pats_t.history.apply(json.dumps)
  pats_t.problem = pats_t.problem.apply(json.dumps)
  pats_t.problem_all = pats_t.problem_all.apply(json.dumps)
  if not med_orders_t.empty:
    med_orders_t['ids'] = med_orders_t['ids'].apply(json.dumps)
  self.db_data = {
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

  self.db_raw_data = {
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

def add_column(ctxt, df, col_name, col_data):
  df[col_name] = col_data
  return df

def printer(ctxt, df):
  print(df)

def build_med_admin_request_data(ctxt, med_orders):
  return med_orders[['pat_id', 'visit_id', 'ids']]\
    .groupby(['pat_id', 'visit_id'])['ids']\
    .apply(list)\
    .reset_index()

def main(hospital=None, lookback_hours=None, db_name=None, max_num_pats=None):
  extractor = JHAPIConfig(
    hospital =       hospital or os.environ['TREWS_ETL_HOSPITAL'],
    lookback_hours = lookback_hours or os.environ['TREWS_ETL_HOURS'],
    jhapi_server =   'prod' or os.environ['TREWS_ETL_SERVER'],
    jhapi_id =       os.environ['jhapi_client_id'],
    jhapi_secret =   os.environ['jhapi_client_secret'],
  )
  import functools
  all_tasks = [
    {
      'name': 'bedded_patients_extract', 
      'fn':   extractor.extract_bedded_patients,
      'args': [extractor.hospital, max_num_pats],
    }, 
    # Barrier 1
    {
      'name': 'bedded_patients_transform',
      'deps': ['bedded_patients_extract'],
      'fn':   transform,
      'args': ['bedded_patients_transforms'],
    }, 
    # Barrier 2
    {
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
    }, 
    # Barrier 3
    {
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
    }, 
    # Barrier 4
    {
      'name': 'build_med_admin_request_data',
      'deps': ['med_orders_transform'],
      'fn':   build_med_admin_request_data,
    }, {
      'name': 'note_texts_extract',
      'deps': ['notes_extract'],
      'fn':   extractor.extract_note_texts,
    }, 
    # Barrier 5
    {
      'name': 'med_admin_extract',
      'deps': ['build_med_admin_request_data'],
      'fn':   extractor.extract_med_admin,
    }, {
      'name': 'notes_texts_transform',
      'deps': ['notes_texts_extract'],
      'fn':   transform,
      'args': ['notes_texts_transforms'],
    }, 
    # Barrier 6
    {
      'name': 'med_admin_transform',
      'deps': ['med_admin_extract'],
      'fn':   transform,
      'args': ['med_admin_transforms'],
    },
    # Barrier 7
    {
      'name': 'timezone_hack_flowsheets',
      'deps': ['flowsheets_transform'],
      'fn':   tz_hack,
    }, {
      'name': 'timezone_hack_med_admin',
      'deps': ['med_admin_transform'],
      'fn':   tz_hack,
    }, 
    # Extra stuff
    {
      'name': 'printer',
      'deps': ['med_admin_transform'],
      'fn':   printer,
    }
  ]

  # TODO: Check if empty responses from extractor break the engine
  # TODO: Push all final dataframes to the old main for cloudwatch logging

  plan = Plan(
    name="Epic to Operations plan", 
    config={
      'db_name': os.environ['db_name'],
      'db_user': os.environ['db_user'],
      'db_pass': os.environ['db_password'],
      'db_host': os.environ['db_host'],
      'db_port': os.environ['db_port'],
    }
  )
  for task_def in all_tasks:
    plan.add(Task(**task_def))
  engine = Engine(
    plan = plan,
    name = "Epic 2 Op engine",
    nprocs = 1,
    loglevel = logging.DEBUG
  )
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  loop.run_until_complete(engine.run())
  loop.close()
  return "finished"

if __name__ == '__main__':
  pd.set_option('display.width', 200)
  pd.set_option('display.max_rows', 30)
  pd.set_option('display.max_columns', 1000)
  pd.set_option('display.max_colwidth', 40)
  pd.options.mode.chained_assignment = None
  logging.getLogger().setLevel(0)
  results = main()
  print(results)
  # engine = Engine()
  # results = engine.main()
