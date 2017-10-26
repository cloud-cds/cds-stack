import os
import sys
import base64
import datetime
import logging
import json
import random
import time
import traceback
import urllib.parse
import zlib

import sqlalchemy
from sqlalchemy.sql import text

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration.
extract_log_entry = os.environ['EXTRACT_LOG_ENTRY'] == 'true' if 'EXTRACT_LOG_ENTRY' in os.environ else False

# Globals.

qs_mapping = {
  'PATID'      : 'pat_id',
  'USERID'     : 'uid',
  'TSESSID'    : 'user_session',
  'CSN'        : 'csn',
  'LOC'        : 'loc',
  'DEP'        : 'dep'
}

body_mapping = {
  'q'          : 'pat_id',
  'u'          : 'uid',
  's'          : 'user_session',
  'actionType' : 'action',
  'csn'        : 'csn',
  'loc'        : 'loc',
  'depid'      : 'dep',
  'action'     : 'action_data'
}

attr_order = ['tsp', 'addr', 'host_session', 'user_session', 'uid', 'action', 'pat_id', 'csn', 'loc', 'dep', 'action_data', 'render_data', 'log_entry']

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
    skip = False
    evt_msg = json.loads(evt['message'])
    if 'resp' in evt_msg:
      if evt_msg['resp']['method'] == 'GET':
        url = urllib.parse.urlparse(evt_msg['resp']['url'])
        query_dict = urllib.parse.parse_qs(url.query)
        query_params = { dst : query_dict[src][0] if src in query_dict and len(query_dict[src]) > 0 else None for src, dst in qs_mapping.items() }
        query_params['url'] = evt_msg['resp']['url']
        query_params['action'] = 'page-get'
        for i in ['action_data', 'render_data']:
          query_params[i] = None

      else:
        query_params = { dst : evt_msg['resp']['body'][src] if src in evt_msg['resp']['body'] else None for src, dst in body_mapping.items() }

        url = urllib.parse.urlparse(evt_msg['resp']['url'])
        logger.info('Processing url/body %s %s' % (str(url), str(evt_msg['resp']['body'])))
        if url.path == "/log" and 'session-close' in evt_msg['resp']['body']:
          query_params['action'] = 'page-close'
          query_params['user_session'] = evt_msg['resp']['body']['session-id'] if 'session-id' in evt_msg['resp']['body'] else None

        elif url.path == "/api":
          query_params['action'] = 'page-load' if query_params['action'] is None else query_params['action']

        else:
          skip = True

        if not skip:
          logger.info('Found user_session %s action %s' % (query_params['user_session'], query_params['action']))
          query_params['action_data'] = json.dumps(query_params['action_data']) if query_params['action_data'] is not None else None
          query_params['render_data'] = json.dumps(evt_msg['resp']['render_data']) if 'render_data' in evt_msg['resp'] else None

      if not skip:
        if query_params['uid'] is None:
          query_params['uid'] = 'UNKNOWN'

        query_params['tsp'] = datetime.datetime.utcfromtimestamp(evt['timestamp'] / 1000)
        query_params['addr'] = evt_msg['resp']['headers']['X-Real-Ip'] if 'X-Real-Ip' in evt_msg['resp']['headers'] else None
        query_params['log_entry'] = evt_msg['resp'] if extract_log_entry else None

        session_prefix = "route="
        session_id = evt_msg['resp']['headers']['Cookie'] if 'Cookie' in evt_msg['resp']['headers'] else None
        session_id = next((s for s in map(lambda x: x.strip(), session_id.split(';')) if s.startswith(session_prefix)), None)
        query_params['host_session'] = session_id[len(session_prefix):] if session_id and session_id.startswith(session_prefix) else session_id

        #interactions.append([query_params[i] for i in attr_order])
        interactions.append(query_params)

    else:
      logger.error('Invalid log event for extracting user interaction')

  logger.info('Extracted {} interactions from logs'.format(len(interactions)))
  return interactions


def execute_with_backoff(sql, params_list, timeout=10, backoff=2, base=2, max_timeout=10*60, max_backoff=3*60):
  if params_list:
    engine = get_db_engine()
    conn = engine.connect()
    for params in params_list:
      conn.execute(text(sql), params)
    conn.close()
    engine.dispose()


def insert_interactions(interactions):
  global attr_order
  attrs = ','.join(map(lambda x: 'enc_id' if x == 'pat_id' else x, attr_order))
  params = ','.join([('pat_id_to_enc_id(:' + i + ')') if i == 'pat_id' else (':' + i) for i in attr_order])
  sql = 'insert into user_interactions ({}) values ({})'.format(attrs, params)
  execute_with_backoff(sql, interactions)

  # Add to legacy table to support email report until this is migrated.
  old_sql = 'insert into usr_web_log (doc_id, tsp, pat_id, visit_id, loc, dep, raw_url) values (:uid, :tsp, :pat_id, :csn, :loc, :dep, :url)'
  execute_with_backoff(old_sql, filter(lambda x: 'url' in x, interactions))

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
    logger.error('No awslogs found while processing log entry')
