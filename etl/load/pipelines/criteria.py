import os
import asyncio
import asyncpg
from etl.core.task import Task

def get_criteria_tasks(dependency=None, lookback_hours=24*7):
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
      args = [lookback_hours]
    ),
    Task(
      name = 'notify_etl_listeners',
      deps = ['advance_criteria_snapshot'],
      coro = notify_etl_listeners,
    ),
  ]

async def garbage_collection(ctxt, _):
  async with ctxt.db_pool.acquire() as conn:
    await conn.execute("select garbage_collection();")


async def advance_criteria_snapshot(ctxt, _, lookback_hours):
  async with ctxt.db_pool.acquire() as conn:
    sql = '''
    select advance_criteria_snapshot(pat_id) from (select distinct(pat_id) from criteria_meas where now() - tsp < interval '{} hours') P;
    '''.format(lookback_hours)
    ctxt.log.info("start advance_criteria_snapshot: {}".format(sql))
    await conn.execute(sql)


async def notify_etl_listeners(ctxt, _):
  if 'etl_channel' in os.environ:
    async with ctxt.db_pool.acquire() as conn:
      await conn.execute("notify %s;" % os.environ['etl_channel'])
  else:
    ctxt.log.info("no etl channel found in the environment, skipping etl notifications")
