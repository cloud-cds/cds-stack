import asyncio
import asyncpg
from etl.core.config import Config
import json
from extractor import Extractor
import os

job = {
  # 'transform': {
  #   'init': True,
  #   'populate_patients': True,
  #   'populate_measured_features': {
  #     'plan': False,
  #     #'fid': 'heart_rate',
  #   }
  # },
  # 'fillin': {
  #   'recalculate_popmean': False,
  # },
  'derive': {},
}

config_args = {
  'dataset_id': 1,
  'debug': True,
  'db_name': 'opsdx_dw_test',
  'conf': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf'),
}

# engine for clarity ETL
class Engine(object):
  """docstring for Engine"""
  def __init__(self):
    self.config = Config(**config_args)


  async def _init_(self):
    self.pool = await asyncpg.create_pool(database=self.config.db_name, user=self.config.db_user, password=self.config.db_pass, host=self.config.db_host, port=self.config.db_port)
    self.extractor = Extractor(self.pool, self.config)

  def run_loop(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run())

  async def run(self):
    # extractors to run ETL
    await self._init_()
    await self.extractor.run(job)

if __name__ == '__main__':
  engine = Engine()
  engine.run_loop()
