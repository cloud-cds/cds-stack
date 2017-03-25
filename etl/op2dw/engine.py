import asyncio
import asyncpg
import json
import logging
from extractor import Extractor

host          = os.environ['db_host']
port          = os.environ['db_port']
db            = os.environ['db_name']
user          = os.environ['db_user']
pw            = os.environ['db_password']
remote_server = os.environ['etl_remote_server']

tables_to_load = [
  'pat_status',
  'deterioration_feedback',
  'feedback_log',
]

# engine for clarity ETL
class Engine(object):
  '''
  ETL workflow for ingesting TREWS operational DB to the data warehouse.
  '''

  async def _init_(self):
    self.dbpool = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port)
    self.extractor = Extractor()

  def run_loop(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run())

  async def run(self):
    await self._init_()
    # extractors to run ETL
    logging.info("Running op2dw ETL")
    db_id = 0
    etl_id = 0
    for tbl in tables_to_load:
      e = Extractor(remote_server, db_id, etl_id, tbl)
      await e.run(pool)

if __name__ == '__main__':
  engine = Engine()
  engine.run_loop()
