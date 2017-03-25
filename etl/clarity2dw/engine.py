import asyncio
import asyncpg
from config import Config
import json
from extractor import Extractor

# engine for clarity ETL
class Engine(object):
  """docstring for Engine"""
  def __init__(self):
    self.config = Config(debug=True)


  async def _init_(self):
    self.pool = await asyncpg.create_pool(database=self.config.db_name, user=self.config.db_user, password=self.config.db_pass, host=self.config.db_host, port=self.config.db_port)
    self.extractor = Extractor(self.pool, self.config)

  def run_loop(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run())

  async def run(self):
    # extractors to run ETL
    await self._init_()
    await self.extractor.run()

if __name__ == '__main__':
  engine = Engine()
  engine.run_loop()
