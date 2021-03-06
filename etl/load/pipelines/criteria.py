import os
import asyncio
import asyncpg
from etl.core.task import Task
import etl.io_config.core as core

def get_criteria_tasks(job_id, dependency=None, lookback_hours=24*7,
                       hospital='HCGH', suppression=0):
  criteria_tasks = [
    Task(
      name = 'garbage_collection',
      deps = [dependency] if dependency else [],
      coro = garbage_collection,
      args = [hospital]
    ),
    Task(
      name = 'advance_criteria_snapshot',
      deps = ['garbage_collection'],
      coro = advance_criteria_snapshot,
      args = [lookback_hours, job_id]
    )]
  if suppression == 0:
    criteria_tasks += [
      Task(
        name = 'notify_etl_listeners',
        deps = ['get_notifications_for_epic'],
        coro = notify_etl_listeners,
      )
    ]
  return criteria_tasks

async def garbage_collection(ctxt, _, hospital):
  async with ctxt.db_pool.acquire() as conn:
    await conn.execute("select garbage_collection();")

async def advance_criteria_snapshot(ctxt, _, lookback_hours, job_id):
  prod_or_dev = core.get_environment_var('db_name')
  nprocs = core.get_environment_var('TREWS_DB_NPROCS', 4)
  server = 'dev_db' if 'dev' in prod_or_dev else 'prod_db'
  async with ctxt.db_pool.acquire() as conn:
    sql = '''
    select distribute_advance_criteria_snapshot_for_job('{server}', {hours}, '{job_id}', {nprocs});
    '''.format(server=server,
               hours=lookback_hours,
               job_id=job_id,
               nprocs=nprocs)
    ctxt.log.info("start advance_criteria_snapshot: {}".format(sql))
    await conn.execute(sql)


async def notify_etl_listeners(ctxt, _):
  if 'etl_channel' in os.environ:
    async with ctxt.db_pool.acquire() as conn:
      await conn.execute("notify %s;" % os.environ['etl_channel'])
  else:
    ctxt.log.info("no etl channel found in the environment, skipping etl notifications")