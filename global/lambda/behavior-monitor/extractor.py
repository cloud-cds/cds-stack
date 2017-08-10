import base64
import datetime
import logging
import json
import os
import random
import sys
import time
import traceback
import urllib.parse
import zlib

import sqlalchemy
from sqlalchemy.sql import text

logging.basicConfig()

# Configuration.
extract_log_entry = os.environ['EXTRACT_LOG_ENTRY'] == 'true' if 'EXTRACT_LOG_ENTRY' in os.environ else False

# Globals.

qs_mapping = {
  'USERID'     : 'uid',
  'PATID'      : 'pat_id',
  'CSN'        : 'csn',
  'LOC'        : 'loc',
  'DEP'        : 'dep'
}

body_mapping = {
  'u'          : 'uid',
  'actionType' : 'action',
  'q'          : 'pat_id',
  'csn'        : 'csn',
  'loc'        : 'loc',
  'depid'      : 'dep',
  'action'     : 'action_data'
}

attr_order = ['tsp', 'addr', 'session', 'uid', 'action', 'pat_id', 'csn', 'loc', 'dep', 'action_data', 'render_data', 'log_entry']

def get_db_engine():
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  engine = sqlalchemy.create_engine(conn_str)
  return engine

##
# Returns a list of user_interactions records, with records represented as a list of strings.
# See the user_interactions db table.
def extract_user_interactions(events):
  global attr_order
  interactions = []
  for evt in events:
    evt_msg = json.loads(evt['message'])
    if 'resp' in evt_msg:
      if evt_msg['resp']['method'] == 'GET':
        url = urllib.parse.urlparse(evt_msg['resp']['url'])
        query_dict = urllib.parse.parse_qs(url.query)
        query_params = { dst : query_dict[src][0] if src in query_dict and len(query_dict[src]) > 0 else None for src, dst in qs_mapping.items() }
        query_params['action'] = 'page-get'
        for i in ['action_data', 'render_data']:
          query_params[i] = None

      else:
        query_params = { dst : evt_msg['resp']['body'][src] if src in evt_msg['resp']['body'] else None for src, dst in body_mapping.items() }
        query_params['action'] = 'page-load' if query_params['action'] is None else query_params['action']
        query_params['action_data'] = json.dumps(query_params['action_data']) if query_params['action_data'] is not None else None
        query_params['render_data'] = json.dumps(evt_msg['resp']['render_data']) if 'render_data' in evt_msg['resp'] else None

      query_params['tsp'] = datetime.datetime.utcfromtimestamp(evt['timestamp'] / 1000)
      query_params['addr'] = evt_msg['resp']['headers']['X-Real-Ip'] if 'X-Real-Ip' in evt_msg['resp']['headers'] else None
      query_params['log_entry'] = evt_msg['resp'] if extract_log_entry else None

      session_prefix = "route="
      session_id = evt_msg['resp']['headers']['Cookie'] if 'Cookie' in evt_msg['resp']['headers'] else None
      query_params['session'] = session_id[len(session_prefix):] if session_id and session_id.startswith(session_prefix) else session_id

      #interactions.append([query_params[i] for i in attr_order])
      interactions.append(query_params)

    else:
      logging.error('Invalid log event for extracting user interaction')

  logging.info('Extracted {} interactions from logs'.format(len(interactions)))
  return interactions

##
# Executes a sql query with fresh connection and transaction, and a randomized retry backoff.

## ASYNC VERSION
# async def execute_with_backoff(sql, params, timeout=10, backoff=2, base=2, max_timeout=10*60, max_backoff=3*60):
#   conn = await asyncpg.connect(database = os.environ['db_name'],     \
#                                user     = os.environ['db_user'],     \
#                                password = os.environ['db_password'], \
#                                host     = os.environ['db_host'],     \
#                                port     = os.environ['db_port'])
#   attempts = 0
#   done = False
#   if timeout:
#     init_timeout = timeout
#   while not done:
#     try:
#       attempts += 1
#       async with conn.transaction():
#         await conn.executemany(sql, params, timeout=timeout)
#         done = True
#     except Exception as e:
#       random_secs = random.uniform(0, 1)
#       if timeout:
#         timeout = min(((base**attempts) + random_secs + init_timeout), max_timeout)
#       wait_time = min(((base**attempts) + random_secs), max_backoff)
#       logging.warn("execute_load failed: retry %s times in %s secs with timeout %s secs" % (attempts, wait_time, timeout))
#       logging.exception(e)
#       await asyncio.sleep(wait_time)
#       continue
#   await conn.close()

def execute_with_backoff(sql, params_list, timeout=10, backoff=2, base=2, max_timeout=10*60, max_backoff=3*60):
  engine = get_db_engine()
  conn = engine.connect()
  for params in params_list:
    conn.execute(text(sql), params)
  conn.close()
  engine.dispose()

##
# Inserts log events into the user_interactions table.
# def insert_interactions(interactions):
#   global attr_order
#   attrs = ','.join(attr_order)
#   params = list(map(lambda x: '${}'.format(x+1), range(len(attr_order))))
#   sql = 'insert into user_interactions ({}) values ({})'.format(attrs, params)
#   loop = asyncio.new_event_loop()
#   loop.run_until_complete(execute_with_backoff(sql, interactions))
#   loop.close()

def insert_interactions(interactions):
  global attr_order
  attrs = ','.join(attr_order)
  params = ','.join([':' + i for i in attr_order])
  sql = 'insert into user_interactions ({}) values ({})'.format(attrs, params)
  execute_with_backoff(sql, interactions)

def log_entry(event):
  encoded_entry = base64.b64decode(event['awslogs']['data'])
  decompressed_entry = zlib.decompress(encoded_entry, 16+zlib.MAX_WBITS)
  log_events = json.loads(decompressed_entry)['logEvents']
  interactions = extract_user_interactions(log_events)
  insert_interactions(interactions)

def handler(event, context):
  if 'awslogs' in event:
    log_entry(event)
  else:
    logging.error('No awslogs found while processing log entry')
