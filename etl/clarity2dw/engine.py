import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import asyncpg
from etl.core.config import Config
import json
from etl.clarity2dw.extractor import Extractor
import os

CONF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')

job_test_c2dw = {
  'reset_dataset': {
    'remove_pat_enc': True,
    'remove_data': True,
    'start_enc_id': '(select max(enc_id) from pat_enc)'
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      'plan': False,
      # 'fid': 'age',
    },
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive': {
    # 'fid': 'cardio_sofa'
  },
  'config': {
    'dataset_id': 1,
    'debug': True,
    # 'db_name': 'test_c2dw',
    # 'db_host': 'dev.opsdx.io',
    'conf': CONF,
  },
}


job = {
  # 'reset_dataset': {
  #   'remove_pat_enc': True,
  #   'remove_data': True,
  #   'start_enc_id': 1
  # },
  # 'transform': {
  #   # 'populate_patients': True,
  #   'populate_measured_features': {
  #     'plan': False,
  #     # 'fid': 'propofol_dose',
  #   },
  # },
  # 'fillin': {
  #   'recalculate_popmean': False,
  # },
  # 'derive': {
  #   'fid': None,
  # },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas':True,
    # 'calculate_historical_criteria':False
  },
  'config': {
    'dataset_id': 1,
    'debug': True,
    # 'db_name': 'test_c2dw',
    # 'db_host': 'dev.opsdx.io',
    'conf': CONF,
  },
}



# engine for clarity ETL
class Engine(object):
  """docstring for Engine"""
  def __init__(self, job):
    self.job = job
    self.config = Config(**self.job['config'])


  async def init(self):
    self.pool = await asyncpg.create_pool(database=self.config.db_name, user=self.config.db_user, password=self.config.db_pass, host=self.config.db_host, port=self.config.db_port)
    self.extractor = Extractor(self.pool, self.config)

  def main(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run())

  async def run(self):
    # extractors to run ETL
    await self.init()
    await self.extractor.run(self.job)

if __name__ == '__main__':
  engine = Engine(job)
  engine.main()
