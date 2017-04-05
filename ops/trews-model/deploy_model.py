# deploy model parameters to opsdx
# please make sure the file names are correct before deployment
# please run this script on either dev or prod controller
import os, sys
from sqlalchemy import create_engine, types, text
import pandas as pd

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = os.environ['db_name']

if len(sys.argv) == 2:
  db = sys.argv[1]
print("db_name: " + db)

port = os.environ['db_port']
password = os.environ['db_password']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)

feature_weights_file = "lactateConstrFeatureWeights.csv"
feature_weights_dbtable = "trews_feature_weights"

scaler_file = "lactateConstrStdScale.csv"
scaler_dbtable = "trews_scaler"

parameters_file = "trews_parameters.csv"
parameters_dbtable = "trews_parameters"

# load parameters
feature_weights = pd.read_csv(feature_weights_file)
print(feature_weights.head())
scaler = pd.read_csv(scaler_file)
print(scaler.head())
parameters = pd.read_csv(parameters_file)
print(parameters.head())

# write to opsdx database
engine = create_engine(DB_CONN_STR)

conn = engine.connect()
conn.execute(text("delete from %s;" % feature_weights_dbtable).execution_options(autocommit=True))
conn.close()
feature_weights.to_sql(feature_weights_dbtable, engine, if_exists='append', index=False)
print("feature weights updated")

conn = engine.connect()
conn.execute(text("delete from %s;" % scaler_dbtable).execution_options(autocommit=True))
conn.close()
scaler.to_sql(scaler_dbtable, engine, if_exists='append', index=False)
print("scaler updated")

conn = engine.connect()
conn.execute(text("delete from %s;" % parameters_dbtable).execution_options(autocommit=True))
conn.close()
parameters.to_sql(parameters_dbtable, engine, if_exists='append', index=False)
print("parameters updated")