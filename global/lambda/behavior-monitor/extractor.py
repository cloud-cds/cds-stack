import asyncio
import asyncpg
import datetime
import logging
import json
import os
import random
import sys
import time
import traceback

logging.basicConfig()

mapping = {
  'u'          : 'uid',
  'actionType' : 'action',
  'q'          : 'pat_id',
  'csn'        : 'csn',
  'loc'        : 'loc',
  'depid'      : 'dep',
  'action'     : 'action_data'
}

attr_order = ['tsp', 'uid', 'action', 'pat_id', 'csn', 'loc', 'dep', 'action_data', 'log_entry']

def extract_user_interactions(events):
  interactions = []
  for evt in events:
    evt_msg = json.loads(evt['message'])
    if 'resp' in evt_msg:
      query_params = { dst : evt_msg['resp']['body'][src] for src, dst in mapping.items() }
      query_params['tsp'] = datetime.utcfromtimestamp(evt['timestamp'] / 1000)
      query_params['log_entry'] = None
      interactions.append([query_params[i] for i in ['tsp']])

    else:
      logging.error('Invalid log event for extracting user interaction')

  logging.info('Extracted {} interactions from logs'.format(len(interactions)))
  return interactions

async def execute_with_backoff(sql, params, timeout=10, backoff=2, base=2, max_timeout=10*60, max_backoff=3*60):
  conn = await asyncpg.connect(database = os.environ['db_name'],     \
                               user     = os.environ['db_user'],     \
                               password = os.environ['db_password'], \
                               host     = os.environ['db_host'],     \
                               port     = os.environ['db_port'])

  attempts = 0
  done = False
  if timeout:
    init_timeout = timeout
  while not done:
    try:
      attempts += 1
      async with conn.transaction():
        await conn.executemany(sql, params, timeout=timeout)
        done = True
    except Exception as e:
      random_secs = random.uniform(0, 1)
      if timeout:
        timeout = min(((base**attempts) + random_secs + init_timeout), max_timeout)
      wait_time = min(((base**attempts) + random_secs), max_backoff)
      logging.warn("execute_load failed: retry %s times in %s secs with timeout %s secs" % (attempts, wait_time, timeout))
      logging.exception(e)
      await asyncio.sleep(wait_time)
      continue

  await conn.close()


def insert_interactions(db_table, interactions):
  global attr_order

  attrs = ','.join(attr_order)
  params = list(map(lambda x: '${}'.format(x+1), range(len(attr_order))))
  sql = 'insert into user_interactions ({}) values ({})'.format(attrs, params)

  loop = asyncio.new_event_loop()
  loop.run_until_complete(execute_with_backoff(sql, interactions))
  loop.close()

def log_entry(event):
  encoded_entry = base64.b64decode(event['awslogs']['data'])
  decompressed_entry = zlib.decompress(encoded_entry, 16+zlib.MAX_WBITS)
  log_events = json.loads(decompressed_entry)['logEvents']
  interactions = extract_user_interactions(log_events)
  insert_interactions(db_table, interactions)

def handler(event, context):
  if 'awslogs' in event:
    log_entry(event)
  else:
    logger.error('No awslogs found while processing log entry')
