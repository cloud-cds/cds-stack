import os
import asyncio
import asyncpg
from etl.core.task import Task
import etl.io_config.core as core

def get_criteria_tasks(dependency=None, lookback_hours=24*7, hospital='HCGH'):
  return [
    Task(
      name = 'garbage_collection',
      deps = [dependency] if dependency else [],
      coro = garbage_collection,
      args = None if dependency else [None]
    ),
    Task(
      name = 'advance_criteria_snapshot',
      deps = ['garbage_collection'],
      coro = advance_criteria_snapshot,
      args = [lookback_hours, hospital]
    ),
    Task(
      name = 'notify_etl_listeners',
      deps = ['get_notifications_for_epic'],
      coro = notify_etl_listeners,
    ),
  ]

async def garbage_collection(ctxt, _):
  async with ctxt.db_pool.acquire() as conn:
    await conn.execute("select garbage_collection();")


async def advance_criteria_snapshot(ctxt, _, lookback_hours, hospital):
  prod_or_dev = core.get_environment_var('db_name')
  nprocs = core.get_environment_var('TREWS_DB_NPROCS', 4)
  server = 'dev_db' if 'dev' in prod_or_dev else 'prod_db'
  async with ctxt.db_pool.acquire() as conn:
    sql = '''
    select distribute_advance_criteria_snapshot('{server}', {hours}, '{hospital}', {nprocs});
    '''.format(server=server,
               hours=lookback_hours,
               hospital=hospital,
               nprocs=nprocs)
    ctxt.log.info("start advance_criteria_snapshot: {}".format(sql))
    await conn.execute(sql)


async def notify_etl_listeners(ctxt, _):
  if 'etl_channel' in os.environ:
    async with ctxt.db_pool.acquire() as conn:
      await conn.execute("notify %s;" % os.environ['etl_channel'])
  else:
    ctxt.log.info("no etl channel found in the environment, skipping etl notifications")
