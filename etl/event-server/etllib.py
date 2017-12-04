import os
import uvloop
import asyncio
import logging
from etl.core.engine import TaskContext
import pandas as pd
from etl.core.config import Config
from etl.io_config.jhapi import JHAPIConfig
import etl.io_config.core as core

ETL_INTERVAL_SECS = int(os.environ['ETL_INTERVAL_SECS']) if 'ETL_INTERVAL_SECS' in os.environ else 30


hospital = core.get_environment_var('TREWS_ETL_HOSPITAL')
# Create data for loader
lookback_hours = core.get_environment_var('TREWS_ETL_HOURS')
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

class DataBuffer():
  def __init__(self):
    pass

  def add_to_buf(self, result):
    # TODO: add data to buffer
    if self.buffer_is_full():
        asyncio.ensure_future(self.run_etl(), loop=self.loop)

  def is_full(self):
    '''
    return True if the buffer size is larger than a threshold
    '''

  def is_ready(self):
    '''
    TODO: buffer is ready to run etl, e.g., buffer need to have enough data to start etl
    '''
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
    self.buf = DataBuffer()
    self.load_ZE_map()
    self.init_etl()

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
      if self.buf.is_ready():
          # TODO: start ETL for current buffer
          self.log.info("buffer is ready for etl")
      else:
        self.log.info("buffer is not ready, so skip etl.")
    except Exception as ex:
      self.log.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

    finally:
      # TODO: start the ETL_scheduler 5 minutes later
      self.loop.call_later(later, self.run_etl)
    self.log.info("etl end")

  def load_ZE_map(self):
    '''
    TODO: load and create the ZID to EID mapping
    '''
    self.ZE_map = {}

  def convert_zid_to_eid(self, zid):
    if zid in self.ZE_map:
      return self.ZE_map[zid]
    else:
      eid = self.extract_eid(zid)
      self.ZE_map[zid] = eid
      return eid

  def extract_eid(self, zid):
    '''
    TODO: extract eid by using contact api
    '''
    # ctxt = TaskContext('ETL', self.config, log=self.log)
    # ctxt.loop = uvloop.new_event_loop()
    # pts = extractor.extract_mrn(ctxt, pd.DataFrame({'pat_id': [zid]}), env='POC')
    # ctxt.loop.close()

    ctxt = TaskContext('ETL', self.config, log=self.log)
    ctxt.loop = self.loop
    pts = extractor.extract_mrn(ctxt, pd.DataFrame({'pat_id': [zid]}), env='POC')
    print(pts)
    return None