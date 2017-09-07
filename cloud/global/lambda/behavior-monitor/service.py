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
logging.basicConfig()

logging.debug("Import complete")

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
  # =============================
  ## Get Results
  # =============================

#==================================================
## Parameters
#==================================================
modes = ['watcher', 'metrics', 'reports']

col2key_dict = {'doc_id': 'USERID',
                'tsp': 'tsp',
                'pat_id': 'PATID',
                'visit_id': 'CSN',
                'loc': 'LOC',
                'dep': 'DEP',
                'raw_url': 'raw_url'}


col_2_dtype_dict = {'doc_id': sqlalchemy.types.String(length=50),
                    'tsp': sqlalchemy.types.DateTime(timezone=True),
                    'pat_id': sqlalchemy.types.String(length=50),
                    'visit_id': sqlalchemy.types.String(length=50),
                    'loc': sqlalchemy.types.String(length=50),
                    'dep': sqlalchemy.types.String(length=50),
                    'raw_url': sqlalchemy.types.String()}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

unique_usrs_window = timedelta(minutes=60)

reports_window = timedelta(hours=24)


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

BEHAMON_MODE = try_to_read_from_environ('BEHAMON_MODE','watcher')
BEHAMON_STACK = try_to_read_from_environ('BEHAMON_STACK','dev')
BEHAMON_WEB_LOG_LISTEN = try_to_read_from_environ('BEHAMON_WEB_LOG_LISTEN','opsdx-web-logs-prod')
BEHAMON_WEB_FILT_STR = try_to_read_from_environ('BEHAMON_WEB_FILT_STR','*USERID*')
BEHAMON_WEB_LOG_STREAM_STR = try_to_read_from_environ('BEHAMON_WEB_LOG_STREAM_STR','monitoring')
BEHAMON_TS_RULE_PERIOD_MINUTES = float(try_to_read_from_environ('BEHAMON_TS_RULE_PERIOD_MINUTES','10'))
BEHAMON_REPORT_RULE_PERIOD_MINUTES = float(try_to_read_from_environ('BEHAMON_REPORT_RULE_PERIOD_MINUTES','1440'))


mode_2_period = {
  'metrics': timedelta(minutes=BEHAMON_TS_RULE_PERIOD_MINUTES),
  'reports': timedelta(minutes=BEHAMON_REPORT_RULE_PERIOD_MINUTES)
}

#==================================================
## Support Functions
#==================================================
def time2epoch(tsp):
  e = int(((tsp - datetime(1970, 1, 1, tzinfo=tsp.tzinfo)).total_seconds()) * (10 ** 3))
  return e

def epoch2time(epoch):
  return datetime.utcfromtimestamp(epoch / 1000)

def logs2dicts(logs,allDicts=[]):

  for log in logs:
    url_str_raw = json.loads(log['message'])['req']['url']
    try:
      _, url_str = url_str_raw.split('?')  # cleans website stuff
    except:
      logger.debug('URL did not match pattern')
      logger.debug(url_str_raw)
      continue

    key_val_list = url_str.split('&')

    rawDict = {}
    for keyValStr in key_val_list:
      key, value = keyValStr.split('=')
      rawDict[key] = value

    rawDict['tsp'] = epoch2time(log['timestamp'])
    rawDict['raw_url'] = url_str_raw

    procDict = {}
    for db_col, key in col2key_dict.iteritems():
      if key in rawDict:
        procDict[db_col] = rawDict[key]
      else:
        procDict[db_col] = None

    allDicts.append(procDict)

  return allDicts

def get_tz_format(tz_in_str='US/Eastern'):
  out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'
  tz = timezone(tz_in_str)
  out_tsp_fmt = out_tsp_fmt_tmp.format( int(round(tz._utcoffset.total_seconds() / (60 * 60))) )
  return out_tsp_fmt, tz

def to_tz_str(time_in,out_tsp_fmt,tz):
  try:
    str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
    # str_out = (time_in).strftime(out_tsp_fmt)
  except:
    str_out = None  # handles nones and nans
  return str_out

def datetime_2_utc_str(df, tz_in_str='US/Eastern', column_list=None):
  """A slightly more general version of the TZ hack to write to DB"""

  out_tsp_fmt, tz = get_tz_format(tz_in_str)

  if column_list is None:
    types = df.dtypes
    column_list = [col for col in df.columns if np.issubdtype(types.loc[col], np.datetime64)]

  tz_func = lambda x: to_tz_str(x,out_tsp_fmt,tz)

  for col in column_list:
    df[col] = df[col].apply(tz_func)

  return df

def get_db_engine():


  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']

  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)

  engine = sqlalchemy.create_engine(conn_str)
  return engine

def data_2_db(sql_table_name, data_in,dtype_dict=None):
  # =============================
  # Clean Data
  # =============================
  results = pd.DataFrame(data_in)

  results = datetime_2_utc_str(results)

  pat = re.compile('test_.*')

  results = results.loc[results['doc_id'].apply(lambda x: re.match(pat, x) is None)]

  results['doc_id'] = results['doc_id'].apply(lambda x: x[1::] if x[0] == '+' else x)

  results = results[results['pat_id'].notnull()&results['tsp'].notnull()&results['doc_id'].notnull()]

  # =============================
  # Upsert to Database
  # =============================
  engine = get_db_engine()

  nrows = results.shape[0]

  temp_table_name = 'temp_2_' + sql_table_name

  logger.debug("saving data frame to {}: nrows = {}".format(temp_table_name, nrows))

  connection = engine.connect()
  connection.execute("""DROP TABLE IF EXISTS {}""".format(temp_table_name))

  # if dtype_dict is None:
  #   results.to_sql(temp_table_name, engine, if_exists='append', index=False, schema='public')
  # else:

  results.to_sql(temp_table_name, engine, if_exists='append', index=False, schema='public', dtype=dtype_dict)

  insert_visit_sql = """
      insert into {} (doc_id, tsp, pat_id, visit_id, loc, dep, raw_url)
      select distinct doc_id, tsp, pat_id, last(visit_id), last(loc), last(dep), last(raw_url)
      from {}
      group by doc_id, tsp, pat_id
      order by tsp
      on conflict (doc_id, tsp, pat_id)
      DO UPDATE SET visit_id = EXCLUDED.visit_id, loc = EXCLUDED.loc, dep = EXCLUDED.dep, raw_url = EXCLUDED.raw_url;
      """.format(sql_table_name, temp_table_name)

  # insert_visit_sql = """
  #     insert into {} (doc_id, tsp, pat_id, visit_id, loc, dep, raw_url)
  #     select distinct doc_id, tsp, pat_id, visit_id, loc, dep, raw_url from {}
  #     """.format(sql_table_name, temp_table_name)

  connection.execute(insert_visit_sql)

  connection.execute("""DROP TABLE IF EXISTS {}""".format(temp_table_name))

  connection.close()
  engine.dispose()
  print("Results Written")
  return results

def getfiltLogEvent(firstTime, lasTime, client,
                    logGroup, logStreamNames, filterPattern,
                    callLimit=None):

  logger.debug(time.strftime("%H:%M:%S") + " Started Filtered Search")

  res = client.filter_log_events(logGroupName=logGroup, logStreamNames=logStreamNames,
                                 startTime=firstTime, endTime=lasTime, filterPattern=filterPattern)

  resList = [res]

  if 'nextToken' in res:
    nt = res['nextToken']
  else:
    nt = False

  loops = 0
  while nt:
    loops += 1
    if callLimit is not None:
      if loops > callLimit:
        break

    logger.debug("We are on call {} of filter log events".format(loops))

    res = client.filter_log_events(logGroupName=logGroup, logStreamNames=logStreamNames,
                                   startTime=firstTime, endTime=lasTime, filterPattern=filterPattern,
                                   nextToken=nt)

    resList += [res]

    if 'nextToken' in res:
      nt = res['nextToken']
    else:
      nt = False

  logger.debug(time.strftime("%H:%M:%S") + " Filtered Search Complete ")

  return resList

def apply_func_over_sliding_window(firstTime,lastTime,func):
  unique_usrs_window_sec = unique_usrs_window.total_seconds()* (10 ** 3)

  intervalStart = firstTime
  intervalEnd = np.min([firstTime + unique_usrs_window_sec,lastTime])


  time_list = []
  metric_list = []

  window_cnt = 0
  while intervalEnd <= lastTime:
    window_cnt += 1

    value = func(intervalStart, intervalEnd)

    metric_list.append(value)
    time_list.append(intervalEnd)

    intervalStart = intervalStart+unique_usrs_window_sec
    intervalEnd   = intervalEnd+unique_usrs_window_sec


  return time_list, metric_list


#==================================================
## html metrics
#==================================================
def get_report_intro(first_time_str, last_time_str):
  html = '<p>'
  html += 'The following report was generated from {stack} data in {db_name}<br>'.format(stack=BEHAMON_STACK,db_name=os.environ['db_name'])
  html += 'It covers times between {s} and {e}'.format(s=first_time_str, e=last_time_str)
  html += '</p>'
  return html

def pats_seen_by_docs(connection, first_time_str, last_time_str):
  num_pats_seen = """
      select doc_id, count(distinct pat_id) as num_pats_seen, min(tsp) as first_access, max(tsp) as last_access
      from usr_web_log
      where tsp between \'{}\'::timestamptz and \'{}\'::timestamptz group by doc_id;""".format(first_time_str, last_time_str)

  logger.debug("Metrics query: %s" % num_pats_seen)

  num_pats_seen_df = pd.read_sql(num_pats_seen, connection)

  logger.debug("Report query shape: %s" % str(num_pats_seen_df.shape))

  return num_pats_seen_df.to_html()

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

#==================================================
## lambda_main
#==================================================
def get_usr_from_event(event):
  x = base64.b64decode(event['awslogs']['data'])
  y = zlib.decompress(x, 16+zlib.MAX_WBITS)
  logs = json.loads(y)['logEvents']

  allDicts = logs2dicts(logs)

  # ====================================
  ## write_to_db
  # ====================================

  data_2_db("usr_web_log", allDicts, dtype_dict=col_2_dtype_dict)

def calc_behamon_ts_metrics(firstTime, lastTime):
  # firstTime, lastTime assumed to be in local time
  logger.info('Processing metrics for %s %s' % (firstTime.isoformat(), lastTime.isoformat()))

  # ===========================
  # Get Data
  # ===========================
  engine = get_db_engine()

  out_tsp_fmt, tz = get_tz_format(tz_in_str='US/Eastern')

  query = sqlalchemy.text("""select * from usr_web_log where tsp between \'{}\'::timestamptz and \'{}\'::timestamptz""".
                          format(to_tz_str(firstTime, out_tsp_fmt, tz), to_tz_str(lastTime, out_tsp_fmt, tz)))

  engine.dispose()

  all_usr_dat = pd.read_sql(query,engine)
  # all_usr_dat.to_pickle('test.pkl')
  # all_usr_dat = pd.read_pickle('global/lambda/behavior-monitor/test.pkl')
  all_usr_dat['tsp']=all_usr_dat['tsp'].apply(time2epoch)

  # ===========================
  # Metric Definitions
  # ===========================
  metrics_dict = {}
  def num_unique_active_users(all_usr_dat,t_min,t_max):
    this_frame = all_usr_dat[(all_usr_dat['tsp'] >= t_min)&
                             (all_usr_dat['tsp'] < t_max)]
    if 'doc_id' in this_frame:
      num = len(set(this_frame['doc_id']))
    else:
      num = 0
    return num
  metrics_dict['num_unique_active_users_v2'] = lambda x, y: num_unique_active_users(all_usr_dat,x,y)

  def total_num_unique_users(all_usr_dat,t_min,t_max):
    this_frame = all_usr_dat[(all_usr_dat['tsp'] < t_max)]
    if 'doc_id' in this_frame:
      num = len(set(this_frame['doc_id']))
    else:
      num = 0
    return num
  metrics_dict['total_num_unique_users_v2'] = lambda x, y: total_num_unique_users(all_usr_dat,x,y)

  # ===========================
  # Execute and push metrics to cloudwatch
  # ===========================

  dimList = [{'Name': 'Source', 'Value': 'behavior-monitor'},
             {'Name': 'Stack', 'Value': BEHAMON_STACK}]

  client = boto3.client('cloudwatch')

  firstTime_e = time2epoch(firstTime)
  lastTime_e  = time2epoch(lastTime)

  successes = 0
  error_codes = []

  for metric,func in metrics_dict.iteritems():
    time_list, value_list = apply_func_over_sliding_window(firstTime_e,lastTime_e,func)

    for time,val in zip(time_list, value_list):
      put_status = client.put_metric_data(Namespace='OpsDX',
                                          MetricData=[{
                                            'MetricName': metric,
                                            'Dimensions': dimList,
                                            'Timestamp': epoch2time(time),
                                            'Value': val,
                                            'Unit': 'Count'}])

      if put_status['ResponseMetadata']['HTTPStatusCode'] != 200:
        error_codes.append(put_status['ResponseMetadata']['HTTPStatusCode'])
      else:
        successes += 1

  logger.info('TS put metrics: Successes %s / Errors %s ' % (successes, len(error_codes)))
  if len(error_codes) > 0:
    logger.info('Error codes: %s' % str(error_codes))

def calc_behamon_report_metrics(firstTime, lastTime, send_email=True):
  logger.info('Processing report for %s %s' % (firstTime.isoformat(), lastTime.isoformat()))

  # ===============================
  # Get data from DB
  # ===============================
  engine = get_db_engine()
  connection = engine.connect()

  out_tsp_fmt, tz = get_tz_format(tz_in_str='US/Eastern')

  first_time_str = to_tz_str(firstTime, out_tsp_fmt, tz)
  last_time_str = to_tz_str(lastTime, out_tsp_fmt, tz)


  metric_template = '<h1>{name}</h1><p>{out}</p>'

  html_body = ''

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
  #---------------------------
  ## Build HTML
  #---------------------------
  print('\n')
  print(html_body)
  print('\n')
  #---------------------------
  ## send email
  #---------------------------
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
    print('email not sent due to function option')


#==================================================
## Boto mains
#==================================================
def get_users_in_interval(firstTime, lastTime):
  lastTime = time2epoch(lastTime)
  firstTime = time2epoch(firstTime)

  client = boto3.client('logs')
  # # BEHAMON_WEB_FILT_STR = try_to_read_from_environ('BEHAMON_WEB_FILT_STR','{$.req.url=*USERID*}')

  filterExpr = "{{$.req.url={}}}".format(BEHAMON_WEB_FILT_STR)

  resList = getfiltLogEvent(firstTime, lastTime, client,
                            BEHAMON_WEB_LOG_LISTEN, [BEHAMON_WEB_LOG_STREAM_STR], filterExpr) # can we base this on the rule somehow? @peter

  allDicts = []
  for res in resList:
    logs = res['events']
    allDicts = logs2dicts(logs,allDicts)

  results = data_2_db("usr_web_log", allDicts, dtype_dict=col_2_dtype_dict)
  return results

def batch_proc_main(firstTime, lastTime):
    get_users_in_interval(firstTime, lastTime)
    # calc_behamon_ts_metrics(firstTime, lastTime)
    # calc_behamon_report_metrics(firstTime, lastTime)


#==================================================
## handler
#==================================================
def handler(event, context):
  # ====================================
  ## Extract from event
  # ====================================

  logger.info("Mode: %s" % BEHAMON_MODE)
  logger.info("Input event: %s" % json.dumps(event))

  if BEHAMON_MODE in modes:

    if BEHAMON_MODE == 'watcher':
      if 'awslogs' in event:
        get_usr_from_event(event)
      else:
        logger.error('No awslogs found while processing log entry')

    else:
      # assumed to be periodically driven
      execution_period_td = mode_2_period[BEHAMON_MODE]

      logger.info("Mode execution period: %s" % str(execution_period_td))

      last_execution = datetime.now() - execution_period_td
      this_execution = datetime.now()

      if BEHAMON_MODE == 'metrics':
        # run on all data since the last execution
        calc_behamon_ts_metrics(last_execution, this_execution)

      elif BEHAMON_MODE == 'reports':
        calc_behamon_report_metrics(last_execution, this_execution)

      else:
        logger.error("Invalid mode: %s" % BEHAMON_MODE)

  else:
    logger.error("Invalid mode: %s" % BEHAMON_MODE)

  return


if __name__ == '__main__':
  batch_proc_main(datetime.utcnow()-timedelta(days=29),datetime.utcnow()) # maxes to 30 days
