import os
from sqlalchemy import create_engine
import base64
import zlib
import json
from pytz import timezone
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

#==================================================
## Utils to be moved out this function eventually
#==================================================
def time2epoch(tsp):
    return int(((tsp - datetime(1970, 1, 1)).total_seconds() )*(10**3))

def epoch2time(epoch):
    return datetime.utcfromtimestamp(epoch / 1000)

def datetime_2_utc_str(df, tz_in_str='US/Eastern', column_list=None):
    """A slightly more general version of the TZ hack to write to DB"""
    out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'

    # the from pytz import common_timezones, ,from pytz import all_timezones

    tz = timezone(tz_in_str)

    out_tsp_fmt = out_tsp_fmt_tmp.format(round(tz._utcoffset.total_seconds() / (60 * 60)))

    if column_list is None:
        types = df.dtypes
        column_list = [col for col in df.columns if np.issubdtype(types.loc[col],np.datetime64)]


    def to_tz_str(time_in):
        try:
            if np.issubdtype(time_in,np.datetime64):
                str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
            else:
                str_out = str(time_in) #handles nones and nans

            return str_out
        except:
            print(time_in)
            type(time_in)
            raise ValueError("Unexcepted input in time columns")

    for col in column_list:
        df[col] = df[col].apply(to_tz_str)

    return df

def data_2_workspace(engine, sql_table_name, data_in):
    # right not data_in is a dataframe
    nrows = data_in.shape[0]
    print("saving data frame to %s: nrows = %s".format(sql_table_name, nrows))
    data_in.to_sql(sql_table_name, engine, if_exists='replace', index=False, schema='public')

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

def handler(event, context):

  # ====================================
  ## Extract from event
  # ====================================

  if not "awslogs" in event:
    print("Not the right event")
    return



  x = base64.b64decode(event['awslogs']['data'])
  y = zlib.decompress(x, 16+zlib.MAX_WBITS)
  log_j = json.loads(y)
  logs = json.loads(y)['logEvents']

  allDicts = []
  for log in logs:
    ts_epoch = log['timestamp']
    url_str_raw = json.loads(log['message'])['req']['url']
    try:
      _, url_str = url_str_raw.split('?')  # cleans website stuff
    except:
      print('URL did not match pattern')
      print(url_str_raw)
      continue

    key_val_list = url_str.split('&')

    doAppend = True
    thisDict = {}
    for keyValStr in key_val_list:
      key, value = keyValStr.split('=')
      thisDict[key] = value
      # Additional Filtering
      if 'key' == 'PATID':
        if value[0] != 'E':
          doAppend = False

    ts_epoch = log['timestamp']
    thisDict['tsp'] = epoch2time(ts_epoch)

    if doAppend:
      allDicts.append(thisDict)


  # ====================================
  ## write_to_db
  # ====================================
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  db_engine     = create_engine(conn_str)

  results = pd.DataFrame(allDicts)

  data_2_workspace(db_engine, "usr_monitor_test", results)


  # conn.close()

