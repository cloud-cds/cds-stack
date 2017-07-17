import asyncio, asyncpg
import psycopg2
from etl.core.environment import Environment
import traceback


class Database:
  def __init__(self, db_user=None, db_pass=None, db_host=None, db_port=None, db_name=None):
    flags = Environment()
    self.db_user = db_user or flags.db_user
    self.db_pass = db_pass or flags.db_password
    self.db_host = db_host or flags.db_host
    self.db_port = db_port or flags.db_port
    self.db_name = db_name or flags.db_name
    self.dsn = 'postgres://{}:{}@{}:{}/{}'.format(
      self.db_user, self.db_pass, self.db_host, self.db_port, self.db_name
    )



  async def get_connection_pool(self):
    try:
      return await asyncpg.create_pool(dsn=self.dsn, timeout=2)
    except Exception:
      traceback.print_exc()
      print("Could not connect to database.")
      print("Maybe environment variables are wrong or VPN not connected.")

