import os
import json
import numpy as np
from datetime import datetime
import logging
from pytz import timezone
import sqlalchemy




def get_db_engine():
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  print(conn_str)

  engine = sqlalchemy.create_engine(conn_str)
  return engine



def time2epoch(tsp):
  e = int(((tsp - datetime(1970, 1, 1, tzinfo=tsp.tzinfo)).total_seconds()) * (10 ** 3))
  return e



def epoch2time(epoch):
  return datetime.utcfromtimestamp(epoch / 1000)



def get_tz_format(tz_in_str='US/Eastern'):
  out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'
  tz = timezone(tz_in_str)
  out_tsp_fmt = out_tsp_fmt_tmp.format( int(round(tz._utcoffset.total_seconds() / (60 * 60))) )
  return out_tsp_fmt, tz



def to_tz_str(time_in, out_tsp_fmt, tz):
  try:
    str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
  except:
    str_out = None  # handles nones and nans
  return str_out

def to_tz_str(time_in, out_tsp_fmt, tz):
  try:
    str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
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

