import os
from sqlalchemy import create_engine

def handler(event, context):
  host          = os.environ['db_host']
  port          = os.environ['db_port']
  db            = os.environ['db_name']
  user          = os.environ['db_user']
  pw            = os.environ['db_password']
  conn_str      = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
  db_engine     = create_engine(conn_str)

  get_trews_threshold_sql = \
    '''
    select value from trews_parameters
    where name = 'trews_threshold'
    '''

  conn = db_engine.connect()
  result = conn.execute(get_trews_threshold_sql)
  conn.close()
  row = result.fetchone()
  print("trews threshold %s" % row['value'])

