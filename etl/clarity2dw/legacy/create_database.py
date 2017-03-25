"""
create_database.py
This is the main script to create the database for the EWS server
Usage:
create database:
  python create_database.py -c
delete database:
  python create_database.py -r

"""
import argparse
from psycopg2 import connect
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

from dashan_config import Config
import logging
import logging.config

def create_database(db_name, config=None):
  # Load dashan instance ini file
  print "create database", db_name
  if not config:
    config = Config(db_name)

  sql_createdb = "CREATE DATABASE %s;" % config.db_name

  # # Load logging configuration file
  # logging.config.fileConfig(config.LOG_CONF)
  # log = logging.getLogger('EWS server')
  # config.set_log(log)

  # create database db_name we need connect to postgres db
  # must be autocommit connection
  con = connect(dbname='postgres')
  con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
  cur = con.cursor()
  cur.execute(sql_createdb)
  cur.close()
  con.close()

  # create database schema
  con = connect(dbname=config.db_name)
  cur = con.cursor()
  sql_path = os.path.join(os.path.abspath(__file__), 'sql/sql_functions.sql')
  with open(sql_path, 'r') as sql_f:
    sql = sql_f.read()
  cur.execute(sql)
  sql_path = os.path.join(os.path.abspath(__file__), 'sql/create_dbschema.sql')
  with open(sql_path, 'r') as sql_f:
    sql = sql_f.read()
  cur.execute(sql)
  con.commit()

  # load cdm_function.csv
  sql_copy = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
  with open(config.CDM_FUNCTION_CSV, 'r') as cdm_function:
    cur.copy_expert(sql=sql_copy % 'cdm_function', file=cdm_function)
    con.commit()
  # load cdm_feature.csv
  with open(config.CDM_FEATURE_CSV, 'r') as cdm_feature:
    cur.copy_expert(sql=sql_copy % 'cdm_feature', file=cdm_feature)
    con.commit()

  # NOTE we do not create cdm_twf here;
  # instead, create it after creating dblink

  # cur.close()
  # con.close()

  # load fillin functions
  # con = connect(user=config.db_user, dbname=config.db_name)
  sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), \
    'sql/fillin.sql')
  with open(sql_path, 'r') as sql_f:
    sql = sql_f.read()
  # cur = con.cursor()
  cur.execute(sql)
  con.commit()
  cur.close()
  con.close()
  return config

def create_cdm_twf(conn=None, dashan=None):
  if not conn:
    conn = connect(dbname=dashan.config.db_name)
  cur = conn.cursor()
  # create cdm_twf table for twf features in cdm_feature
  sql_select_twf = \
  """
  SELECT fid, data_type
  FROM cdm_feature
  WHERE category = 'TWF';
  """
  sql_create_cdm_twf = """
  CREATE TABLE cdm_twf (
    enc_id  integer REFERENCES pat_enc(enc_id),
    tsp     timestamp,
    %s
    meta_data json,
    PRIMARY KEY (enc_id, tsp)
    );
  """
  cur.execute(sql_select_twf)
  sql_columns = ""
  for feature in cur.fetchall():
    # column names are case-insensitive
    sql_columns += "%s  %s," % feature
    sql_columns += "%s_c integer," % feature[0]
  cur.execute("DROP TABLE IF EXISTS cdm_twf")
  cur.execute(sql_create_cdm_twf % sql_columns)
  conn.commit()
  if dashan:
    conn.close()


def remove_database(db_name, config=None):
  # Load EWS server ini file
  if not config:
    config = Config(db_name)
  sql_removedb = "DROP DATABASE %s;" % config.db_name

  # # Load logging configuration file
  # logging.config.fileConfig(config.LOG_CONF)
  # log = logging.getLogger('EWS server')
  # config.set_log(log)

  # create database db_name we need connect to postgres db
  con = connect(dbname='postgres')
  con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
  cur = con.cursor()
  cur.execute(sql_removedb)
  cur.close()
  con.close()



def main():
  # Handle command-lie arguments
  parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("-r", "--remove",
    action="store_true",
    default=False,
    help="Remove the database on EWS server.")

  parser.add_argument("-c", "--create_database",
    action="store_true",
    default=False,
    help="Specify the config folder path.")

  parser.add_argument("-d", "--database",
    help="Specify the name of database",
    type=str)

  args = parser.parse_args()

  if args.database is None:
    print "please input database name!"
  else:
    if args.remove:
      remove_database(args.database)
    elif args.create_database:
      create_database(args.database)


if __name__ == "__main__":
  main()