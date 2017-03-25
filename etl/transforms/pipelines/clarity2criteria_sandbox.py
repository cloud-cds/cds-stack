import os
import pandas as pd
from sqlalchemy import create_engine, text
import  etl.mappings.lab_procedures as lp_config
from etl.transforms.primitives.df import derive
from etl.transforms.primitives.df import format_data
from etl.transforms.primitives.df import restructure
from etl.transforms.primitives.df import translate
from etl.mappings import lab_procedures as lp_config
import pytz


# clarityPath = '/Users/pmarian3/Desktop/clarityLoc'
# clarityPath += '/'
# op = pd.read_pickle(clarityPath + 'op.pkl')
# lp_map = pd.read_pickle(clarityPath + 'lp_map.pkl')

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = 'opsdx_dw'
port = os.environ['db_port']
password = os.environ['db_password']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)
engine = create_engine(DB_CONN_STR)
conn = engine.connect()
op = pd.read_sql_query(text("""select * from "OrderProcs";"""), con=conn)
lp_map = pd.read_sql_query(text("""SELECT * FROM lab_proc_dict;"""), con=conn)
conn.close()

op = restructure.select_columns(op, {'CSN_ID': 'csn_id',
                                    'proc_name':'fid',
                                    'ORDER_TIME': 'tsp',
                                    'OrderStatus':'order_status',
                                    'LabStatus':'proc_status',
                                    'PROC_START_TIME':'proc_start_tsp',
                                    'PROC_ENDING_TIME':'proc_end_tsp'})

op = derive.derive_lab_status_clarity(op)

def get_fid_name_mapping(lp_map):
    fid_map = dict()
    for fid, codes in lp_config.procedure_ids:
        nameList = list()
        for code in codes:
            rs = lp_map[lp_map['proc_id'] == code]['proc_name']
            if not rs.empty:
                nameList.append(rs.iloc[0])
        fid_map[fid] = nameList
    return fid_map

fid_map = get_fid_name_mapping(lp_map)
for fid, names in fid_map.items():
    print(fid)
    print(names)
    for name in names:
        op.loc[op['fid']==name,'fid'] = fid

op = op[ [x in fid_map.keys() for x in op['fid']] ]


