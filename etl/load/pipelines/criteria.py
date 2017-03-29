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

  async def calculate(self):
    async with self.pool.acquire() as conn:
      self.log.info("start garbage_collection")
      self.garbage_collection(conn)
      self.log.info("completed garbage_collection")
      self.log.info("advancing criteria snapshot")
      self.advance_criteria_snapshot(conn)
      self.log.info("advanced criteria snapshot")

  async def garbage_collection(self, conn):
      await conn.execute("select garbage_collection();")


  async def advance_criteria_snapshot(self, conn):
      await conn.execute("select advance_criteria_snapshot();")