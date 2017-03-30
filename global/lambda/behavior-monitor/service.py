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
import pytz
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time

#==================================================
## Parameters
#==================================================
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


rule_2_exeuction_period = {'opsdx-dev-behamon_lambda_time_series_rule':timedelta(minutes=2),
                           'opsdx-dev-behamon_lambda_reports_rule':timedelta(minutes=2)}

unique_usrs_window = timedelta(minutes=60)

reports_window = timedelta(hours=24)


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
      print('URL did not match pattern')
      print(url_str_raw)
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

  results['doc_id'] = results['doc_id'].apply(lambda x: x[1::])

  # =============================
  # Upsert to Database
  # =============================
  engine = get_db_engine()

  nrows = results.shape[0]

  temp_table_name = 'temp' + sql_table_name

  print("saving data frame to {}: nrows = {}".format(temp_table_name, nrows))

  if dtype_dict is None:
    results.to_sql(temp_table_name, engine, if_exists='append', index=False, schema='public')
  else:
    results.to_sql(temp_table_name, engine, if_exists='append', index=False, schema='public', dtype=dtype_dict)

  make_final_sql = """
      insert into {} (doc_id, tsp, pat_id, visit_id, loc, dep, raw_url)
      select          doc_id, tsp, pat_id, visit_id, loc, dep, raw_url from {}
      on conflict (doc_id, tsp, pat_id)
      DO UPDATE SET visit_id = EXCLUDED.visit_id, loc = EXCLUDED.loc, dep = EXCLUDED.dep, raw_url = EXCLUDED.raw_url;
      """.format(sql_table_name, temp_table_name)

  connection = engine.connect()
  connection.execute(make_final_sql)
  connection.execute("""DROP TABLE {}""".format(temp_table_name))

  connection.close()
  engine.dispose()
  return results

def getfiltLogEvent(firstTime, lasTime, client,
                    logGroup, logStreamNames, filterPattern,
                    callLimit=None):
  print(time.strftime("%H:%M:%S") + " Started Filtered Search")

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

    print("We are on call {} of filter log events".format(loops))

    res = client.filter_log_events(logGroupName=logGroup, logStreamNames=logStreamNames,
                                   startTime=firstTime, endTime=lasTime, filterPattern=filterPattern,
                                   nextToken=nt)

    resList += [res]

    if 'nextToken' in res:
      nt = res['nextToken']
    else:
      nt = False

  print(time.strftime("%H:%M:%S") + " Filtered Search Complete ")

  return resList

def getLogs(logStart, logEnd, client,
            logGroup, logStreamNames):
  stack = client.get_log_events(logGroupName=logGroup, logStreamName=logStreamNames,
                                startTime=logStart, endTime=logEnd, startFromHead=True)
  stackList = [stack]

  loops = 0
  if 'nextToken' in stack:
    nt = stack['nextToken']
  else:
    nt = False

  while nt:
    loops += 1
    stack = client.get_log_events(logGroupName='opsdx-dev-k8s-logs', logStreamName='kubernetes/default/trews/etl',
                                  startTime=logStart, endTime=logEnd, nextToken=nt, startFromHead=True)
    stackList += [stack]

    if 'nextToken' in stack:
      nt = stack['nextToken']
    else:
      nt = False

  return stackList

def periodic_rule_2_td(rule_name):
  #--------------------------------------------------------------
  # I can only get this algorithum to work locally, not on aws.
  #--------------------------------------------------------------
  print("client")
  client = boto3.client('events')
  print("rule")
  rule_details = client.describe_rule(Name=rule_name)

  print(rule_details)
  # looks like this rate(2 minutes)
  print("execution")
  execution_period_str = str(rule_details['ScheduleExpression'][5:-1])
  print(execution_period_str)
  print("time stuff")
  timenum, timeunit = execution_period_str.split(' ')
  print(timenum)
  print(timeunit)
  print("execution period")
  execution_period_td = timedelta(**{timeunit: float(timenum)})

  execution_period_td = rule_2_exeuction_period[rule_name]

  return execution_period_td

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
  print("We are in TS metrics")
  # firstTime, lastTime assumed to be in local time

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

  dimList = [{'Name': 'Source', 'Value': 'database'},
             {'Name': 'Stack', 'Value': 'prod'}]

  client = boto3.client('cloudwatch')

  firstTime_e = time2epoch(firstTime)
  lastTime_e  = time2epoch(lastTime)
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

  pass

def calc_behamon_report_metrics(firstTime, lastTime):
  print("We are in reports!")
  # ===============================
  # Get data from DB
  # ===============================
  engine = get_db_engine()

  out_tsp_fmt, tz = get_tz_format(tz_in_str='US/Eastern')

  numPats_seen = """
      select doc_id, count(distinct pat_id) as num_pats_seen, min(tsp) as first_access, max(tsp) as last_access
      from usr_web_log 
      where tsp between \'{}\'::timestamptz and \'{}\'::timestamptz group by doc_id;""".format(
        to_tz_str(firstTime, out_tsp_fmt, tz), to_tz_str(lastTime, out_tsp_fmt, tz))

  num_pats_seen_df = pd.read_sql(numPats_seen,engine)

  engine.dispose()

  num_pats_seen_str = str(num_pats_seen_df)

  client = boto3.client('ses')
  client.send_email(
    Source='trews-jhu@opsdx.io',
    Destination={
      'ToAddresses': ['trews-jhu@opsdx.io'],
    },
    Message={
      'Subject': {'Data': 'Behavior Monitor Report'},
      'Body': {
        'Html': {'Data': num_pats_seen_df.to_html()},
      },
    }
  )



#==================================================
## Boto mains
#==================================================
def get_users_in_interval(firstTime, lastTime):
  lastTime = time2epoch(lastTime)
  firstTime = time2epoch(firstTime)

  client = boto3.client('logs')

  resList = getfiltLogEvent(firstTime, lastTime, client,
                               'opsdx-web-logs-prod', ['trews'], '{$.req.url=*USERID*}')

  allDicts = []
  for res in resList:
    logs = res['events']
    allDicts = logs2dicts(logs,allDicts)

  results = data_2_db("usr_web_log", allDicts, dtype_dict=col_2_dtype_dict)
  return results

def batch_proc_main(firstTime, lastTime):
    get_users_in_interval(firstTime, lastTime)
    calc_behamon_ts_metrics(firstTime, lastTime)
    calc_behamon_report_metrics(firstTime, lastTime)


#==================================================
## handler
#==================================================
def handler(event, context):
  # ====================================
  ## Extract from event
  # ====================================

  print("event")
  print(event)


  if "awslogs" in event:
    get_usr_from_event(event)
  else:
    # assumed to be periodically driven

    _, rule = event['resources'][0].split(':rule/')
    print("rule")
    print(rule)

    execution_period_td = periodic_rule_2_td(rule)

    print("presumed rule execution Period")
    print(execution_period_td)

    last_execution = datetime.now() - execution_period_td

    this_execution = datetime.now()

    if rule == 'opsdx-dev-behamon_lambda_time_series_rule':
      # run on all data since the last execution
      calc_behamon_ts_metrics(last_execution, this_execution)
    if rule == 'opsdx-dev-behamon_lambda_reports_rule':
      print("Report Type")
      calc_behamon_report_metrics(last_execution, this_execution)
    else:
      print("unrecognized event type:")
      print(event)

  return



if __name__ == '__main__':
  batch_proc_main(datetime.now()-timedelta(days=13.9),datetime.now())

