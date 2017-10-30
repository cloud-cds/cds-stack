import os, sys, traceback
import logging
import argparse
import datetime
from pytz import timezone
import boto3
import requests
import grequests
import sqlalchemy
from sqlalchemy import text
from jhapi_io import JHAPI

logging.basicConfig(level=logging.INFO)

EPIC_SERVER   = os.environ['epic_server'] if 'epic_server' in os.environ else 'prod'
client_id     = os.environ['jhapi_client_id']
client_secret = os.environ['jhapi_client_secret']

def get_db_engine():
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  engine = sqlalchemy.create_engine(conn_str)
  return engine

def push_sessions_to_epic(t_in):
  # Get sessions.
  out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'
  tz = timezone('US/Eastern')
  out_tsp_fmt = out_tsp_fmt_tmp.format( int(round(tz._utcoffset.total_seconds() / (60 * 60))) )
  t_start = (t_in + tz._utcoffset).strftime(out_tsp_fmt)

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
         coalesce(final_state#>>'{severe_sepsis, trews_organ_dysfunction, overridden}',
                  final_state#>>'{severe_sepsis, organ_dysfunction, overridden}'
                  ) as orgdf,
         (case when override_shk then 2 when override_sep then 1 else 0 end) as manual_override_flag,
         subalert_state#>>'{score}' as score,
         subalert_state#>>'{pct_mortality}' as mortality_est,
         subalert_state#>>'{pct_sevsep}' as sepsis_est
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
             min(tsp) as session_start,
             max(tsp) as session_end,
             array_agg(action order by tsp),
             last(render_data order by tsp) filter (where action = 'page-load') as final_state
      from user_interactions U
      inner join pat_enc P on U.enc_id = P.enc_id
      where tsp > '%(t_start)s'::timestamptz and uid <> 'CAPTUREUSER'
      group by pat_id, visit_id, user_session, uid
    ) R
  ) R;
  ''' % {'t_start': t_start}

  engine = get_db_engine()
  conn = engine.connect()

  flowsheet_ids = {
    'username'               : 1,
    'session_start'          : 2,
    'session_end'            : 3,
    'soi_yn'                 : 4,
    'soi'                    : 5,
    'orgdf'                  : 6,
    'manual_override_flag'   : 7,
    'score'                  : 8,
    'mortality_est'          : 9,
    'sepsis_est'             : 10,
  }
  fields = list(flowsheet_ids.keys())
  flowsheets = {}

  results = conn.execute(text(user_sessions_sql))
  for row in results:
    for f in fields:
      if f not in flowsheets:
        flowsheets[f] = []
      flowsheets[f].append({
        'pat_id'   : row['pat_id'],
        'visit_id' : row['visit_id'],
        'tsp'      : row['tsp'],
        'value'    : row[f]
      })

  conn.close()
  engine.dispose()

  # Push sessions data to flowsheets, and metrics to Cloudwatch
  jhapi_loader = JHAPI(EPIC_SERVER, client_id, client_secret)
  boto_cloudwatch_client = boto3.client('cloudwatch')

  for k in flowsheets:
    responses = jhapi_loader.load_flowsheet(flowsheets[k], flowsheet_id=flowsheet_ids[k])

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

    logging.info("FSSessionLoader pushed to CW: %s - %s" % (k, str(cwm_status)))


def parse_arguments():
  parser = argparse.ArgumentParser(description='TREWS User Session Loader')
  parser.add_argument('window_in_minutes', type=int)
  return parser.parse_args()


if __name__ == '__main__':
  args = parse_arguments()
  t_start = datetime.datetime.now() - datetime.timedelta(minutes=args.window_in_minutes)
  push_sessions_to_epic(t_start)
