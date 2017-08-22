import pickle
import os
import pandas as pd
from sqlalchemy import create_engine, text
from etl.transforms.primitives.df.restructure import select_columns
from etl.transforms.primitives.df.pandas_utils import upsert_db
from datetime import datetime
import email
import os

# email_path = '/Users/pmarian3/Desktop/Inbox'
email_path = '/home/ubuntu/peterm/feedback_emails'
email_path += '/'
email_list = os.listdir(email_path)
dataset_id = 1



#------------------------------------------------------
## Parse Email files to Dataframe
#------------------------------------------------------
msgs_files = [_file for _file in email_list if str.lower(_file[-3:])=="eml"]

all_dicts = list()
for msg_file in msgs_files:
  # msg_file  = msgs_files[5]
  msg = email.message_from_file(open(email_path + msg_file))
  msg_list = msg.as_string().split(sep='\n')
  content_str = msg_list[-2] #get last line with content
  content_str = content_str[4:-4] #Remove first and last html tags
  key_val_list  = content_str.split('</p><h4>')

  content_dict = dict()
  for key_val_str in key_val_list:
    try:
      key, value = key_val_str.split('</h4><p>')
      content_dict[key] = value

    except:
      print("Message lost or partially lost: ")
      print(msg.as_string())

  if content_dict:
    date_line = [line for line in msg_list if line[0:4] == 'Date'][0]
    _, date_str = date_line.split(sep=':', maxsplit=1)
    date_str = date_str.strip()
    datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S +0000')

    content_dict['tsp'] = datetime.strptime(date_str,'%a, %d %b %Y %H:%M:%S +0000')
    all_dicts.append(content_dict)

feedback_df = pd.DataFrame(all_dicts)

#------------------------------------------------------
## Process Dataframe
#------------------------------------------------------

feedback_df.drop_duplicates(subset=['Physician','Feedback','Current patient in view'],inplace=True)
feedback_df = select_columns(feedback_df,{'Physician':'doc_id',
                                          'tsp':'tsp',
                                          'Current patient in view':'pat_id',
                                          'Department':'dep_id',
                                          'Feedback':'feedback'})
feedback_df['dataset_id'] = dataset_id



#------------------------------------------------------
## Upsert DF
#------------------------------------------------------

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = 'opsdx_dw'
port = os.environ['db_port']
password = os.environ['db_password']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)

engine = create_engine(DB_CONN_STR)
conn = engine.connect()

# def upsert_db(df, sql_tbl_name, connection, on_conflict_cols):
upsert_db(feedback_df,'feedback_log',conn,[])