import os
import sys
try:
  root = os.environ["LAMBDA_TASK_ROOT"]
  sys.path.insert(0, root)
  print("Lambda Root inserted into front of Path")
except:
  print("Lambda Root Not inserted Into Path")
import boto3
import base64
import json
import zlib
import sqlalchemy
from pytz import timezone
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

#############
# Parameters.

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

unique_usrs_window = timedelta(minutes=60)
reports_window = timedelta(hours=24)

state_group_list = [
  {'state':range(-50,50), 'test_out':10 , 'var':'total',         'english':'total (with state information)'},
  {'state':range(20,40),  'test_out':10 , 'var':'sev',           'english':'had severe sepsis in this interval'},
  {'state':range(30,40),  'test_out':5  , 'var':'sho',           'english':'had septic shock in this interval'},
  {'state':[10, 12],      'test_out':5  , 'var':'sev_nosus',     'english':'had severe sepsis without sus in this interval'},
  {'state':[32, 22],      'test_out':5  ,  'var':'sev_3_m',      'english':'where the severe sepsis 3 hour bundle was missed'},
  {'state':[34, 24],      'test_out':5  ,  'var':'sev_6_m',      'english':'where the severe sepsis 6 hour bundle was missed'},
  {'state':[31, 21],      'test_out':5  ,  'var':'sev_3_h',      'english':'where the severe sepsis 3 hour bundle was met'},
  {'state':[33, 23],      'test_out':5  ,  'var':'sev_6_h',      'english':'where the severe sepsis 6 hour bundle was met'},
  {'state':[36],          'test_out':5  ,  'var':'sho_3_h',      'english':'where the septic shock 6 hour bundle was missed'},
  {'state':[35],          'test_out':5  ,  'var':'sho_6_h',      'english':'where the septic shock 6 hour bundle was met'},
]

#==================================================
## Evnvironment Variables
#==================================================
def try_to_read_from_environ(var_str, default_val):
  if var_str in os.environ:
    logger.info("Selecting {} from Environment".format(var_str))
    return os.environ[var_str]
  else:
    logger.info("Selecting default value for {}".format(var_str))
    return default_val

BEHAMON_MODE = try_to_read_from_environ('BEHAMON_MODE','reports')
BEHAMON_STACK = try_to_read_from_environ('BEHAMON_STACK','dev')
BEHAMON_WEB_LOG_LISTEN = try_to_read_from_environ('BEHAMON_WEB_LOG_LISTEN','opsdx-web-logs-prod')
BEHAMON_WEB_FILT_STR = try_to_read_from_environ('BEHAMON_WEB_FILT_STR','*USERID*')
BEHAMON_WEB_LOG_STREAM_STR = try_to_read_from_environ('BEHAMON_WEB_LOG_STREAM_STR','monitoring')
BEHAMON_TS_RULE_PERIOD_MINUTES = float(try_to_read_from_environ('BEHAMON_TS_RULE_PERIOD_MINUTES','10'))
BEHAMON_REPORT_RULE_PERIOD_MINUTES = float(try_to_read_from_environ('BEHAMON_REPORT_RULE_PERIOD_MINUTES','1440'))


mode_2_period = {
  'reports': timedelta(minutes=BEHAMON_REPORT_RULE_PERIOD_MINUTES)
}

###############################
# Support Functions

def get_tz_format(tz_in_str='US/Eastern'):
  out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'
  tz = timezone(tz_in_str)
  out_tsp_fmt = out_tsp_fmt_tmp.format( int(round(tz._utcoffset.total_seconds() / (60 * 60))) )
  return out_tsp_fmt, tz

def to_tz_str(time_in,out_tsp_fmt,tz):
  try:
    str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
  except:
    str_out = None  # handles nones and nans
  return str_out


def get_db_engine():
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  engine = sqlalchemy.create_engine(conn_str)
  return engine


##########################
# Report constructors.

def get_report_intro(first_time_str, last_time_str):
  return
    '''
    <p> The following report was generated from %(stack)s data in %(db_name)s<br>
        It covers times between %(s)s and %(e)s
    </p>
    ''' % {'stack': BEHAMON_STACK, 'db_name': os.environ['db_name'], 's': first_time_str, 'e': last_time_str}

##
#
def pats_seen_by_docs(connection, first_time_str, last_time_str):
  num_pats_seen = """
      select doc_id, count(distinct pat_id) as num_pats_seen, min(tsp) as first_access, max(tsp) as last_access
      from usr_web_log
      where tsp between \'{}\'::timestamptz and \'{}\'::timestamptz group by doc_id;""".format(first_time_str, last_time_str)

  logger.debug("Metrics query: %s" % num_pats_seen)

  num_pats_seen_df = pd.read_sql(num_pats_seen, connection)

  logger.debug("Report query shape: %s" % str(num_pats_seen_df.shape))

  return num_pats_seen_df.to_html()

##
#
def get_state_counts(db_con, state_group_list, start_time_str, stop_time_str, dataset_id, source_tbl):
  state_group_out = []
  for state_group in state_group_list:
    query = sqlalchemy.text(
      """select count(DISTINCT PAT_ID) as {name}
          from {table_name}
          where window_ts between \'{start}\'::timestamptz and \'{stop}\'::timestamptz
              and dataset_id = {dataset}
              and pat_state in ({pat_states});""".
                              format(name=state_group['var'],
                                     start=start_time_str,
                                     stop=stop_time_str,
                                     pat_states=','.join([str(num) for num in state_group['state']]),
                                     dataset=dataset_id,
                                     table_name=source_tbl))

    out_df = pd.read_sql(query,db_con)
    # print(out_df)
    print("\n")
    print(query)
    # state_group['results'] = state_group['test_out']
    state_group['results'] = out_df[state_group['var']].iloc[0]
    state_group_out.append(state_group)

  return state_group_out

##
#
def sepsis_stats(connection, first_time_str, last_time_str):

  tmp_hist_state_table_name = """lambda_hist_pat_state_{now}""".format(now=datetime.utcnow().strftime("%Y%m%d%H%M%S"))

  tmp_ds_id = -1

  get_hist_states = """
    create temporary table {tmp_tbl} as
    select pat_id, {ds_id} as dataset_id,
      case when last(flag) >= 0 then last(flag) else last(flag) + 1000 END as pat_state,
      last(update_date) as window_ts
    from
    criteria_events
    where is_met = true and flag != -1
    group by pat_id, event_id
    order by pat_id, window_ts;
  """.format(ds_id=tmp_ds_id,tmp_tbl=tmp_hist_state_table_name)

  logger.debug("Hist State from Events Query: %s" % get_hist_states)

  connection.execute(sqlalchemy.text(get_hist_states))

  logger.debug("Temp Table Created")

  state_group_out = get_state_counts(connection, state_group_list, first_time_str, last_time_str, tmp_ds_id, tmp_hist_state_table_name)

  logger.debug("Got State Counts")


  def state_stats_to_html(state_group_out):
    html = '<p>'
    for state_group in state_group_out:
      html += "{num} patients {english}<br>".format(num=state_group['results'], english=state_group['english'])
    html += '</p>'
    return html

  return state_stats_to_html(state_group_out)


##
#
def notification_stats(connection, first_time_str, last_time_str):

  logger.info("inside notification stats")

  notification_q = """
  with
  flat_notifications as (
    select
      pat_id,
      to_timestamp(cast(message#>>'{{timestamp}}' as numeric)) as tsp,
      cast(message#>>'{{read}}' as boolean) as read,
      cast(message#>>'{{alert_code}}' as integer) alert_code
    from notifications
    ),
  num_notes_at_once as (
    select pat_id, tsp, count(distinct(alert_code)) as number_of_unread_notifications
    from
    flat_notifications
    where not read and tsp BETWEEN '{start}'::timestamptz and '{end}'::timestamptz
    group by pat_id, tsp
  ),
  max_notes_at_once as (
    select pat_id, max(number_of_unread_notifications) as max_unread_notes
    from num_notes_at_once
    group by pat_id
  )
  select
    max_unread_notes,
    count(distinct(pat_id)) as number_of_pats
  from max_notes_at_once
  group by max_unread_notes
  order by max_unread_notes;
  """.format(start=first_time_str,end=last_time_str)

  logger.debug(notification_q)

  notifications_df = pd.read_sql(sqlalchemy.text(notification_q), connection)

  logger.debug("Notifications query shape: %s" % str(notifications_df.shape))

  return notifications_df.to_html()


#####################################
# Behavior monitor report generation.
#
def calc_behamon_report_metrics(firstTime, lastTime, send_email=True):
  logger.info('Processing report for %s %s' % (firstTime.isoformat(), lastTime.isoformat()))

  engine = get_db_engine()
  connection = engine.connect()

  out_tsp_fmt, tz = get_tz_format(tz_in_str='US/Eastern')
  first_time_str = to_tz_str(firstTime, out_tsp_fmt, tz)
  last_time_str = to_tz_str(lastTime, out_tsp_fmt, tz)

  # Build HTML
  html_body = ''
  metric_template = '<h1>{name}</h1><p>{out}</p>'

  html_body += metric_template.format(
        name='Introduction',
        out=get_report_intro(first_time_str, last_time_str)
        )

  html_body += metric_template.format(
        name='Usage Statistics',
        out=pats_seen_by_docs(connection, first_time_str, last_time_str)
        )

  html_body += metric_template.format(
        name='Sepsis / Bundle Overview',
        out=sepsis_stats(connection, first_time_str, last_time_str)
        )

  html_body += metric_template.format(
        name='Notification Statisitcs',
        out=notification_stats(connection, first_time_str, last_time_str)
        )

  connection.close()
  engine.dispose()

  logging.info('HTML:\n' + html_body)

  # Send email
  client = boto3.client('ses')
  if send_email:
    response = client.send_email(
      Source='trews-jhu@opsdx.io',
      Destination={
        'ToAddresses': ['trews-jhu@opsdx.io'],
      },
      Message={
        'Subject': {'Data': 'Report Metrics (%s)' % BEHAMON_STACK},
        'Body': {
          'Html': {'Data': html_body},
        },
      }
    )
    logger.info('Send email status: %s' % str(response))

  else:
    logging.info('Skipping report email.')


####################
# Lambda entrypoint.
def handler(event, context):
  logger.info("Mode: %s" % BEHAMON_MODE)
  logger.info("Input event: %s" % json.dumps(event))

  if BEHAMON_MODE != 'reports':
    logger.error("Invalid mode: %s" % BEHAMON_MODE)
    return

  # assumed to be periodically driven
  execution_period_td = mode_2_period[BEHAMON_MODE]
  logger.info("Mode execution period: %s" % str(execution_period_td))

  last_execution = datetime.now() - execution_period_td
  this_execution = datetime.now()

  calc_behamon_report_metrics(last_execution, this_execution)

  return


