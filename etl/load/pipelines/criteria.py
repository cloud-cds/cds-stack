import os
import asyncio
import asyncpg
from etl.core.task import Task

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
      deps = ['advance_criteria_snapshot'],
      coro = notify_etl_listeners,
    ),
  ]

async def garbage_collection(ctxt, _):
  async with ctxt.db_pool.acquire() as conn:
    await conn.execute("select garbage_collection();")


async def advance_criteria_snapshot(ctxt, _, lookback_hours, hospital):
  async with ctxt.db_pool.acquire() as conn:
    sql = '''
    select advance_criteria_snapshot(pat_id)
    from (
    select distinct p.pat_id from pat_enc p
    inner join criteria_meas m on p.pat_id = m.pat_id
    inner join cdm_s s on s.enc_id = p.enc_id
    where now() - tsp < interval '{hours} hours' and s.fid = 'hospital' and s.value = '{hospital}'

    ) P;
    '''.format(hours=lookback_hours, hospital=hospital)
    ctxt.log.info("start advance_criteria_snapshot: {}".format(sql))
    await conn.execute(sql)


async def notify_etl_listeners(ctxt, _):
  if 'etl_channel' in os.environ:
    async with ctxt.db_pool.acquire() as conn:
      await conn.execute("notify %s;" % os.environ['etl_channel'])
  else:
    ctxt.log.info("no etl channel found in the environment, skipping etl notifications")
