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
    # Task(
    #   name = 'notify_etl_listeners',
    #   deps = ['get_notifications_for_epic'],
    #   coro = notify_etl_listeners,
    # ),
    # Task(
    #   name = 'notify_lmc',
    #   deps = ['advance_criteria_snapshot'],
    #   coro = notify_lmc,
    #   args = [hospital]
    # ),
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

# async def notify_lmc(ctxt, _, hospital):
#   attempts = 0
#   while attempts < 3:
#     try:
#       reader, writer = await asyncio.open_connection('127.0.0.1', 40000, loop=ctxt.loop)
#       message = hospital
#       print('notify_lmc send: %r' % message)
#       writer.write(message.encode())

#       data = await reader.read(100)
#       print('notify_lmc received: %r' % data.decode())

#       print('Close the socket')
#       writer.close()
#     except Exception as e:
#       attempts += 1
#       ctxt.log.exception("notify_lmc Error: %s" % e)
#       random_secs = random.uniform(0, 1)
#       wait_time = min(((1**attempts) + random_secs), 10)
#       await asyncio.sleep(wait_time)
#       ctxt.log.info("notify_lmc {} attempts {}".format(fid or '', attempts))
#       continue

