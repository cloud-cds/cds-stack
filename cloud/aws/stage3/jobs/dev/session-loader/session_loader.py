import os, sys, traceback
import logging
import argparse
import datetime
from pytz import timezone
import boto3
import grequests
import requests # Must be imported after grequests: https://github.com/kennethreitz/grequests/issues/103
import sqlalchemy
from sqlalchemy import text
from jhapi_io import JHAPI

logging.basicConfig(level=logging.INFO)

EPIC_SERVER   = os.environ['epic_server'] if 'epic_server' in os.environ else 'prod'
client_id     = os.environ['jhapi_client_id']
client_secret = os.environ['jhapi_client_secret']
drop_if_empty = os.environ['drop_if_empty'].lower() == 'true' if 'drop_if_empty' in os.environ else False

def get_db_engine():
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  engine = sqlalchemy.create_engine(conn_str)
  return engine

def mk_time(t):
  tz = timezone('US/Eastern')
  return str(t.astimezone(tz))

def push_sessions_to_epic(t_in):
  # Get sessions.
  t_start = mk_time(t_in)

  user_sessions_sql = \
  '''
  select pat_id,
         visit_id,
         session_start as tsp,
         username,
         session_start,
         session_end,
         not (soi is null or soi = 'No Infection') as soi_yn,
         soi,
         coalesce((select string_agg(o, '|') from jsonb_array_elements_text(final_state#>'{severe_sepsis, trews_organ_dysfunction, overridden}') R(o)),
                  (select string_agg(o, '|') from jsonb_array_elements_text(final_state#>'{severe_sepsis, organ_dysfunction, overridden}') R(o))
                  ) as orgdf,
         (case when override_shk then 2 when override_sep then 1 else 0 end) as manual_override_flag,
         (subalert_state#>>'{score}')::numeric as score,
         (subalert_state#>>'{pct_mortality}')::numeric as mortality_est,
         (subalert_state#>>'{pct_sevsep}')::numeric as sepsis_est
  from (
    select pat_id,
           visit_id,
           user_session,
           username,
           session_start,
           session_end,
           final_state,
           final_state#>>'{severe_sepsis, suspicion_of_infection, value}' as soi,
           (final_state#>>'{ui, severe_sepsis, is_met}')::bool as override_sep,
           (final_state#>>'{ui, septic_shock, is_met}')::bool as override_shk,
           (final_state#>>'{severe_sepsis, trews_subalert, value}')::jsonb as subalert_state
    from (
      select pat_id,
             visit_id,
             user_session,
             uid as username,
             min(tsp) filter (where action = 'page-get') as session_start,
             max(tsp) filter (where action = 'close_session') as session_end,
             array_agg(action order by tsp),
             last(render_data order by tsp) filter (where action in ('page-load', 'override', 'override_many')) as final_state
      from user_interactions U
      inner join pat_enc P on U.enc_id = P.enc_id
      where tsp > now () - interval '1 week' and uid <> 'CAPTUREUSER'
      group by pat_id, visit_id, user_session, uid
    ) R
    where
      session_start is not null
      and session_end is not null
      and session_end > '%(t_start)s'::timestamptz
  ) R
  ;
  ''' % {'t_start': t_start}

  logging.info(user_sessions_sql)

  engine = get_db_engine()
  conn = engine.connect()

  flowsheet_ids = {
    'username'               : ('94854', lambda x: str(x) if x else 'UNKNOWN'),
    'session_start'          : ('94855', lambda x: mk_time(x)),
    'session_end'            : ('94856', lambda x: mk_time(x)),
    'soi_yn'                 : ('94857', lambda x: '1' if x else '0'),
    'soi'                    : ('94858', lambda x: "null" if not x else str(x)),
    'orgdf'                  : ('94859', lambda x: "null" if not x else str(x)),
    'manual_override_flag'   : ('94860', lambda x: str(x) if x else '0'),
    'score'                  : ('94861', lambda x: str(round(x, 4)) if x else '-1.0'),
    'mortality_est'          : ('94862', lambda x: str(round(x, 2)) if x else '-1.0'),
    'sepsis_est'             : ('94863', lambda x: str(round(x, 2)) if x else '-1.0'),
  }

  fields = list(flowsheet_ids.keys())
  flowsheets = {}

  results = conn.execute(text(user_sessions_sql))
  num_results = 0
  for row in results:
    num_results += 1
    logging.info('Session: %s' % str(row))
    for f in fields:
      if f not in flowsheets:
        flowsheets[f] = []

      if not drop_if_empty or row[f] is not None:
        flowsheets[f].append({
          'pat_id'   : row['pat_id'],
          'visit_id' : row['visit_id'],
          'tsp'      : row['tsp'],
          'value'    : flowsheet_ids[f][1](row[f])
        })

  logging.info('# Results: %s' % num_results)

  conn.close()
  engine.dispose()

  # Push sessions data to flowsheets, and metrics to Cloudwatch
  jhapi_loader = JHAPI(EPIC_SERVER, client_id, client_secret)
  boto_cloudwatch_client = boto3.client('cloudwatch')

  for k in flowsheets:
    logging.info('Pushing flowsheet %s %s' %(k, flowsheet_ids[k][0]))
    responses = jhapi_loader.load_flowsheet(flowsheets[k], flowsheet_id=flowsheet_ids[k][0])

    successes = 0
    failures = 0
    for fs, resp in zip(flowsheets[k], responses):
      if resp is None:
        failures += 1
        logging.error('Failed to push session flowsheet %s: %s %s %s' % (k, fs['pat_id'], fs['visit_id'], fs['value']))
      elif resp.status_code != requests.codes.ok:
        failures += 1
        logging.error('Failed to push session flowsheet %s: %s %s %s HTTP %s' % (k, fs['pat_id'], fs['visit_id'], fs['value'], resp.status_code))
      elif resp.status_code == requests.codes.ok:
        successes += 1

    logging.info("Flowsheet loader stats: %s: %s successes / %s failures" % (k, successes, failures))

    cwm_status = [{
      'MetricName' : 'fs_session_push_successes',
      'Timestamp'  : datetime.datetime.utcnow(),
      'Value'      : successes,
      'Unit'       : 'Count',
      'Dimensions' : [{'Name': 'FSSessionLoaderType', 'Value': k}]
    }, {
      'MetricName' : 'fs_session_push_failures',
      'Timestamp'  : datetime.datetime.utcnow(),
      'Value'      : failures,
      'Unit'       : 'Count',
      'Dimensions' : [{'Name': 'FSSessionLoaderType', 'Value': k}]
    }]

    logging.info("FSSessionLoader pushed to CW: %s" % k)


def parse_arguments():
  parser = argparse.ArgumentParser(description='TREWS User Session Loader')
  parser.add_argument('window_in_minutes', type=int)
  return parser.parse_args()


if __name__ == '__main__':
  args = parse_arguments()
  t_start = datetime.datetime.now() - datetime.timedelta(minutes=args.window_in_minutes)
  push_sessions_to_epic(t_start)
