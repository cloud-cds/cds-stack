import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import asyncpg
from etl.core.config import Config
import json
from extractor import Extractor
import os

job_test_c2dw = {
  'reset_dataset': {
    # 'remove_pat_enc': True,
    'remove_data': True,
    'start_enc_id': '(select max(enc_id) from pat_enc)'
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      # 'plan': False,
      # 'fid': 'propofol_dose',
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
    'db_name': 'test_c2dw',
    # 'db_host': 'dev.opsdx.io',
    'conf': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf'),
  },
}

job = {
  'reset_dataset': {
    'remove_pat_enc': True,
    'remove_data': True,
    'start_enc_id': 1
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      # 'plan': False,
      # 'fid': 'propofol_dose',
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
    'conf': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf'),
  },
}



# engine for clarity ETL
class Engine(object, job):
  """docstring for Engine"""
  def __init__(self, job):
    self.job = job
    self.config = Config(**self.job['config'])


  async def _init_(self):
    self.pool = await asyncpg.create_pool(database=self.config.db_name, user=self.config.db_user, password=self.config.db_pass, host=self.config.db_host, port=self.config.db_port)
    self.extractor = Extractor(self.pool, self.config)

  def run_loop(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run())

  async def run(self):
    # extractors to run ETL
    await self._init_()
    await self.extractor.run(self.job)

if __name__ == '__main__':
  engine = Engine(job_test_c2dw)
  engine.run_loop()
