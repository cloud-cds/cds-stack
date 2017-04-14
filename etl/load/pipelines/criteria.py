import os
import asyncio
import asyncpg

class Criteria:
  def __init__(self, config):
    self.config = config
    self.log = self.config.log
    self.pool = None

  async def async_init(self):
    self.pool = await asyncpg.create_pool(
      database = self.config.db_name,
      user     = self.config.db_user,
      password = self.config.db_pass,
      host     = self.config.db_host,
      port     = self.config.db_port
    )

  def run_loop(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.calculate())
    loop.close()

  async def calculate(self):
    if self.pool is None:
      await self.async_init()
    async with self.pool.acquire() as conn:
      await self.garbage_collection(conn)
      await self.advance_criteria_snapshot(conn)
      await self.notify_etl_listeners(conn)

  async def garbage_collection(self, conn):
      self.log.info("advancing criteria snapshot")
      await conn.execute("select garbage_collection();")
      self.log.info("advanced criteria snapshot")

  async def advance_criteria_snapshot(self, conn):
      self.log.info("start garbage_collection")
      await conn.execute("select advance_criteria_snapshot();")
      self.log.info("completed garbage_collection")

  async def notify_etl_listeners(self, conn):
      if 'etl_channel' in os.environ:
          await conn.execute("notify %s;" % os.environ['etl_channel'])
          self.log.info("completed etl notifications")
      else:
          self.log.info("no etl channel found in the environment, skipping etl notifications")
