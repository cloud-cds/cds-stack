import utils as utils
import metrics as metrics
import argparse
import os
from datetime import datetime, timedelta
import logging
from sshtunnel import SSHTunnelForwarder
import sqlalchemy
import psycopg2
import pdb
import pandas as pd

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("Import complete")

def_stack_to_english_dict = {
  'opsdx-prod'    : 'Prod',
  'opsdx-jh-prod' : 'Prod',
  'opsdx-dev'     : 'Dev',
  'opsdx-jh-dev'  : 'Dev',
  'Test'          : 'Test'
}

## Specify remote machine for tunnel
controller_url = 'controller.jh.opsdx.io'
controller_ssh_pkey_path = '/home/twang70/.ssh/tf-opsdx'

## Specify database

host = os.environ['prod_db_host']
port = int(os.environ['db_port'])
db = os.environ['prod_db_name']
user = os.environ['db_user']
pw = os.environ['prod_db_password']

"""
host = os.environ['dev_db_host']
port = int(os.environ['db_port'])
db = os.environ['dev_db_name']
user = os.environ['db_user']
pw = os.environ['dev_db_password']
"""
def createConn(conn_str):
    conn = 'dbname={} user={} host=127.0.0.1 port=63336 password={}'.format(db, user, pw)
    conn = psycopg2.connect(conn)
    return conn

with SSHTunnelForwarder(
  (controller_url, 22),
  ssh_username='ubuntu',
  ssh_pkey=controller_ssh_pkey_path,
        remote_bind_address=(host, port),
  local_bind_address=('127.0.0.1', 63336)) as tunnel:

  #engine = utils.get_db_engine()
  #connection = engine.connect()
  #connection.execute(sqlalchemy.text('select max(tsp), min(tsp) from cdm_t'))
  conn_str  = 'postgresql+psycopg2://'
  engine = sqlalchemy.create_engine(conn_str, creator=createConn)
  connection = engine.connect()

  end_time = datetime.utcnow()
  out_tsp_fmt, tz = utils.get_tz_format(tz_in_str='US/Eastern')
  end_time_str = utils.to_tz_str(end_time, out_tsp_fmt, tz)
  report_metric_factory = metrics.metric_factory(connection, '', end_time_str, [metrics.ed_metrics])
  #report_metric_factory = metrics.metric_factory(connection, '', '', [metrics.alert_performance_metrics])
  report_metric_factory.calc_all_metrics()
  report_html_body = report_metric_factory.build_report_body()
  pdb.set_trace()
  print(report_html_body)
