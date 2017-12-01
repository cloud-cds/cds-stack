import os
import asyncio
import logging

ETL_INTERVAL_SECS = int(os.environ['ETL_INTERVAL_SECS']) if 'ETL_INTERVAL_SECS' in os.environ else 3

class Buffer():
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
    self.buf = Buffer()
    self.load_ZE_map()
    self.init_etl()


  def load_ZE_map(self.):
    '''
    TODO: load and create the ZID to EID mapping
    '''
    self.ZE_map = {}

  def init_etl(self):
    '''
    TODO: initialize attributes
    '''
    # start etl perioidically
    self.log.info('start etl perioidically (every {} seconds)'.format(ETL_INTERVAL_SECS))
    self.loop.call_later(ETL_INTERVAL_SECS, self.run_etl)

  def run_etl(self):
    '''
    Consume data buffer and run ETL
    '''
    self.log.info("etl started")
    if self.buf.is_ready():
        # TODO: start ETL for current buffer
        self.log.info("buffer is ready for etl")
    else:
      self.log.info("buffer is not ready, so skip etl.")
    # TODO: start the ETL_scheduler 5 minutes later
    self.loop.call_later(ETL_INTERVAL_SECS, self.run_etl)
    self.log.info("etl end")