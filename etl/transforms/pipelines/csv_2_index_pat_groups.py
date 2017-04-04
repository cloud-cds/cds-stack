import pickle
import pandas as pd
import sqlalchemy
import os
from datetime import datetime
from etl.transforms.primitives.df.pandas_utils import upsert_db

csv_path = '/home/ubuntu/peterm/index_pats'
# csv_path = '/Users/pmarian3/Desktop/index_patients'
csv_path += '/'



DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = 'opsdx_dw'
port = os.environ['db_port']
password = os.environ['db_password']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)


group_definitions = pd.read_csv(csv_path + 'group_definitions.csv')
group_definitions['update_tsp'] = datetime.utcnow()
index_patients = pd.read_csv(csv_path + 'index_cases.csv')
index_patients['update_tsp'] = datetime.utcnow()

engine = sqlalchemy.create_engine(DB_CONN_STR)
connection = engine.connect()
upsert_db(index_patients, 'index_patients',connection,index_patients.columns)
upsert_db(group_definitions, 'index_group_descriptions',connection,['group_id'])









