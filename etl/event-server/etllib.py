import os
import uvloop
import asyncio
import logging
from etl.core.engine import TaskContext
import pandas as pd
from etl.core.config import Config
from etl.io_config.epic_api import EpicAPIConfig
import etl.io_config.core as core
import traceback
import etl.load.pipelines.epic2op as loader
import datetime as dt
import numpy as np

ETL_INTERVAL_SECS = int(os.environ['ETL_INTERVAL_SECS']) if 'ETL_INTERVAL_SECS' in os.environ else 30

WORKSPACE = core.get_environment_var('TREWS_ETL_WORKSPACE', 'event_workspace')

hospital = core.get_environment_var('TREWS_ETL_HOSPITAL')
# Create data for loader
lookback_hours = core.get_environment_var('TREWS_ETL_HOURS', '8')
op_lookback_days = int(core.get_environment_var('TREWS_ET_OP_DAYS', 365))
# Create jhapi_extractor
extractor = EpicAPIConfig(
  hospital       = hospital,
  lookback_hours = lookback_hours,
  jhapi_server   = core.get_environment_var('TREWS_ETL_SERVER', 'prod'),
  jhapi_id       = core.get_environment_var('jhapi_client_id'),
  jhapi_secret   = core.get_environment_var('jhapi_client_secret'),
  op_lookback_days = op_lookback_days
)

Extraction_With_Deps = [extractor.extract_med_admin, extractor.extract_note_texts]


# Get mode (real, test, both)
MODE = {
  1: 'real',
  2: 'test',
  3: 'real&test'
}

mode = MODE[int(core.get_environment_var('TREWS_ETL_MODE', '1'))]

# Get suppression alert mode
suppression = int(core.get_environment_var('TREWS_ETL_SUPPRESSION', '0'))


class CDMBuffer():
  def __init__(self, etl):
    self.buf = {}
    self.etl = etl
    logging.debug("test...............")


  def add(self, results):
    self.etl.log.info("add data to buffer: {}".format(results))
    for name in results:
      df = results[name]
      self.etl.log.info("cdm_buf: adding {}".format(name))
      if name in self.buf:
        self.buf[name] = pd.concat([self.buf[name], df]).drop_duplicates().reset_index(drop=True)
      else:
        self.buf[name] = df
      self.etl.log.info("cdm_buf: added {}".format(name))
    if self.is_full():
        asyncio.ensure_future(self.run_etl(), loop=self.loop)

  def get(self):
    buf = self.buf
    self.buf = {}
    return buf

  def is_full(self):
    '''
    return True if the buffer size is larger than a threshold
    '''
    return False

  def is_ready(self):
    '''
    TODO: buffer is ready to run etl, e.g., buffer need to have enough data to start etl
    '''
    if self.buf:
      return True
    return False

class ETL():
  def __init__(self, app):
    self.app = app
    self.db_pool = app.db_pool
    self.log = self.app.logger
    self.log.info('create ETL instance')
    self.loop = asyncio.get_event_loop()
    db_name = core.get_environment_var('db_name')
    self.config = Config(debug=True, db_name=db_name)
    extractor.log = self.config.log
    self.cdm_buf = CDMBuffer(self)
    self.load_pt_map()
    self.init_etl()
    self.ctxt = TaskContext('ETL', self.config, log=self.log)
    self.ctxt.loop = self.loop
    self.ctxt.db_pool = self.db_pool


  def init_etl(self):
    '''
    TODO: initialize attributes
    '''
    # start etl perioidically
    self.log.info('start etl perioidically (every {} seconds)'.format(ETL_INTERVAL_SECS))
    self.loop.call_later(ETL_INTERVAL_SECS, self.run_etl)

  def run_etl(self, later=ETL_INTERVAL_SECS):
    '''
    Consume data buffer and run ETL
    '''
    self.log.info("etl started")
    try:
      if self.cdm_buf.is_ready():
        # start ETL for current buffer
        self.log.info("buffer is ready for etl")
        asyncio.ensure_future(self.load_to_cdm(self.cdm_buf.get()))
      else:
        self.log.info("buffer is not ready, so skip etl.")
    except Exception as ex:
      self.log.warning(str(ex))
      traceback.print_exc()

    finally:
      # TODO: start the ETL_scheduler 5 minutes later
      self.loop.call_later(later, self.run_etl)
    self.log.info("etl end")

  async def load_to_cdm(self, buf):
    start_time = dt.datetime.now()
    job_id = "job_etl_{}_{}".format(hospital, dt.datetime.now().strftime('%Y%m%d%H%M%S')).lower()
    await loader.epic_2_workspace(self.ctxt, buf, self.config.get_db_conn_string_sqlalchemy(), job_id, 'unicode', WORKSPACE)
    num_delta = await loader.workspace_to_cdm_delta(self.ctxt, job_id, WORKSPACE, keep_delta_table=True)
    logging.info("{} num_delta = {}".format(job_id, num_delta))
    if num_delta:
      prediction_params = await loader.load_online_prediction_parameters(self.ctxt, job_id)
      num_twf_rows = await loader.workspace_fillin_delta(self.ctxt, prediction_params, job_id, WORKSPACE)
      if num_twf_rows:
        await loader.workspace_derive(self.ctxt, prediction_params, job_id, WORKSPACE)
        await loader.workspace_submit_delta(self.ctxt, job_id, WORKSPACE)
        await loader.notify_delta_ready_to_trews_alert_server(self.ctxt, job_id, WORKSPACE)
      else:
        logging.info("No new or updated rows in TWF. Skip ETL {}".format(job_id))
    else:
      logging.info("No change for {}. Skip ETL".format(job_id))
    end_time = dt.datetime.now()
    extractor.cloudwatch_logger.push(
      dimension_name = 'ETL',
      metric_name    = 'load_to_cdm_time_push',
      value          = (end_time - start_time).total_seconds(),
      unit           = 'Seconds'
    )

  def load_pt_map(self):
    '''
    TODO: load and create the ZID to EID mapping
    '''
    self.pt_map = {}

  async def lookup_zid(self, zid):
    if zid in self.pt_map:
      return self.pt_map[zid]
    else:
      pt = await self.extract_pt(zid)
      self.pt_map[zid] = pt
      return pt

  async def run_requests(self, buf):
    start_time = dt.datetime.now()
    extraction_set = {}
    pats = []
    for zid in buf:
      pt = await self.app.etl.lookup_zid(zid)
      pats.append(pt)
      for ext in buf[zid]['funcs']:
        if ext in extraction_set:
          extraction_set[ext]['pts'].append(pt)
          extraction_set[ext]['args'].append(buf[zid]['args'])
        else:
          extraction_set[ext] = {'pts': [pt], 'args': [buf[zid]['args']]}
    tasks = [asyncio.ensure_future(ext(self.ctxt, pd.DataFrame(extraction_set[ext]['pts']), extraction_set[ext]['args'])) for ext in extraction_set if ext not in Extraction_With_Deps]
    logging.info("tasks: {}".format(tasks))
    results = await asyncio.gather(*tasks)
    tasks_with_deps = [asyncio.ensure_future(ext(self.ctxt, pd.DataFrame(extraction_set[ext]['pts']), extraction_set[ext]['args'], results)) for ext in extraction_set if ext in Extraction_With_Deps]
    logging.info("tasks_with_deps: {}".format(tasks_with_deps))
    if tasks_with_deps and len(tasks_with_deps) > 0:
      results_with_deps = await asyncio.gather(*tasks_with_deps)
      results += results_with_deps
    results.append({'bedded_patients_transformed': self.gen_bedded_patients_transformed(pats)})
    results_dict = {}
    for r in results:
      if r:
        for k in r:
          if r[k] is not None and not r[k].empty:
            results_dict[k] = r[k]
    self.log.info("load data to cdm_buf")
    for k in results_dict:
      self.log.info(k)
    self.cdm_buf.add(results_dict)
    end_time = dt.datetime.now()
    extractor.cloudwatch_logger.push(
      dimension_name = 'ETL',
      metric_name    = 'run_requests_push',
      value          = (end_time - start_time).total_seconds(),
      unit           = 'Seconds'
    )

  def gen_bedded_patients_transformed(self, pats):
    df = pd.DataFrame(pats)
    # df['age'] = np.nan
    df['admittime'] = np.nan
    # df['gender'] = np.nan
    df['patient_class'] = np.nan
    df['diagnosis'] = np.nan
    df['history'] = np.nan
    df['problem_all'] = np.nan
    df['problem'] = np.nan
    df['hospital'] = hospital
    logging.info(df)
    return df

  async def extract_pt(self, zid):
    '''
    TODO: extract eid by using contact api
    '''
    pt = await extractor.extract_mrn_by_zid(self.ctxt, zid)
    # NOTE: for testing
    # pt = {'zid': 'Z14184', 'pat_id': 'E100069114', 'age': None, 'gender': None}
    contacts = await extractor.extract_contacts(self.ctxt, [pt], None, idtype='patient')
    pt['visit_id'] = contacts.iloc[0]['CSN']
    self.log.info("extract_mrn_by_zid: {}".format(pt))
    return pt