import base64
import json
import zlib
import boto3
import sqlalchemy
import os
from pytz import timezone
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time

#==================================================
## Utils to be moved out this function eventually
#==================================================


# class TREWSFeedback(object):
#   def on_post(self, req, resp):
#     try:
#       raw_json = req.stream.read()
#     except Exception as ex:
#       raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)
#
#     try:
#       result_json = json.loads(raw_json, encoding='utf-8')
#     except ValueError:
#       raise falcon.HTTPError(falcon.HTTP_400, 'Malformed JSON',
#                              'Could not decode the request body. The JSON was incorrect.')
#
#     try:
#       subject = 'Feedback - {}'.format(str(result_json['u']))
#       html_text = [
#         ("Physician", str(result_json['u'])),
#         ("Current patient in view", str(result_json['q'])),
#         ("Department", str(result_json['depid'])),
#         ("Feedback", str(result_json['feedback'])),
#       ]
#       body = "".join(["<h4>{}</h4><p>{}</p>".format(x, y) for x, y in html_text])
#       client = boto3.client('ses')
#       client.send_email(
#         Source='trews-jhu@opsdx.io',
#         Destination={
#           'ToAddresses': ['trews-jhu@opsdx.io'],
#         },
#         Message={
#           'Subject': {'Data': subject, },
#           'Body': {
#             'Html': {'Data': body, },
#           },
#         }
#       )
#       resp.status = falcon.HTTP_200
#       resp.body = json.dumps(result_json, encoding='utf-8')
#     except Exception as ex:
#       raise falcon.HTTPError(falcon.HTTP_400, 'Error sending email', ex.message)

#==================================================
## Main
#==================================================
#@peter email


#==================================================
## Support Functions
#==================================================
#==================================================
## Support Funcs which ideally should be in a different file
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


def time2epoch(tsp):
  return int(((tsp - datetime(1970, 1, 1)).total_seconds()) * (10 ** 3))

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

def datetime_2_utc_str(df, tz_in_str='US/Eastern', column_list=None):
  """A slightly more general version of the TZ hack to write to DB"""
  out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'

  tz = timezone(tz_in_str)

  out_tsp_fmt = out_tsp_fmt_tmp.format( int(round(tz._utcoffset.total_seconds() / (60 * 60))) )

  if column_list is None:
    types = df.dtypes
    column_list = [col for col in df.columns if np.issubdtype(types.loc[col], np.datetime64)]

  def to_tz_str(time_in):
    try:
      str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
      # str_out = (time_in).strftime(out_tsp_fmt)
    except:
      str_out = None  # handles nones and nans
    return str_out

  for col in column_list:
    df[col] = df[col].apply(to_tz_str)

  return df


def data_2_db(sql_table_name, data_in,dtype_dict=None):
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)

  engine = sqlalchemy.create_engine(conn_str)

  # right not data_in is a dataframe
  nrows = data_in.shape[0]
  print("saving data frame to %s: nrows = %s".format(sql_table_name, nrows))
  if dtype_dict is None:
    data_in.to_sql(sql_table_name, engine, if_exists='append', index=False, schema='public')
  else:
    data_in.to_sql(sql_table_name, engine, if_exists='append', index=False, schema='public', dtype=dtype_dict)

  engine.dispose()

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

#==================================================
## lambda main
#==================================================
def handler(event, context):
  # ====================================
  ## Extract from event
  # ====================================

  if not "awslogs" in event:
    print("Not the right event")
    return

  x = base64.b64decode(event['awslogs']['data'])
  y = zlib.decompress(x, 16+zlib.MAX_WBITS)
  logs = json.loads(y)['logEvents']

  allDicts = logs2dicts(logs)

  # ====================================
  ## write_to_db
  # ====================================
  results = pd.DataFrame(allDicts)
  results = datetime_2_utc_str(results)
  data_2_db("usr_web_log", results, dtype_dict=col_2_dtype_dict)

#==================================================
## Boto main
#==================================================
def getUsersInInterval(firstTime, lastTime):
  lastTime = time2epoch(lastTime)
  firstTime = time2epoch(firstTime)

  client = boto3.client('logs')

  resList = getfiltLogEvent(firstTime, lastTime, client,
                               'opsdx-web-logs-prod', ['trews'], '{$.req.url=*USERID*}')

  allDicts = []
  for res in resList:
    logs = res['events']
    allDicts = logs2dicts(logs,allDicts)

  results = pd.DataFrame(allDicts)
  results = datetime_2_utc_str(results)
  data_2_db("usr_web_log", results, dtype_dict=col_2_dtype_dict)

  return results

if __name__ == '__main__':
  getUsersInInterval(datetime.now()-timedelta(days=13.9),datetime.now())
