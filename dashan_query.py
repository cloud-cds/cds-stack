"""
dashan_query.py
"""
import os

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['psql_user']
host = os.environ['psql_host']
db = os.environ['psql_db']
port = os.environ['psql_port']
password = os.environ['psql_password']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)

from sqlalchemy import create_engine
import pandas as pd
def get_trews(eid):
    engine = create_engine(DB_CONN_STR)
    get_trews_sql = \
    '''
    select trews.* from trews inner join pat_enc on trews.enc_id = pat_enc.enc_id
    where pat_enc.pat_id = '%s' order by tsp
    ''' % eid
    try:
        df = pd.read_sql_query(get_trews_sql,con=engine)
    except Exception:
        df = pd.DataFrame()
    return df

def get_twf(eid):
    engine = create_engine(DB_CONN_STR)
    get_twf_sql = \
    '''
    select cdm_twf.* from cdm_twf inner join pat_enc on cdm_twf.enc_id = pat_enc.enc_id
    where pat_enc.pat_id = '%s' order by tsp
    ''' % eid
    df = pd.read_sql_query(get_twf_sql,con=engine)
    return df

def eid_exist(eid):
    engine = create_engine(DB_CONN_STR)
    connection = engine.connect()
    result = connection.execute("select * from pat_enc where pat_id = '%s'" % eid)
    connection.close()
    for row in result:
        return True 
    return False

if __name__ == '__main__':
    eid = 'E1000109xx'
    print eid_exist(eid)
    eid = 'E100010907'
    print eid_exist(eid)
    df = get_trews(eid)
    print df.head()
    df_data = df.drop(['enc_id','trewscore','tsp'],1)
    df_rank = df_data.rank(axis=1, method='max', ascending=False)
    twf = get_twf(eid)
    top1 = df_rank.as_matrix() < 1.5
    top1_cols = [df_rank.columns.values[t][0] for t in top1]
    top1_weights = df_data.as_matrix()[top1]
    top1_values = [row[top1_cols[i]] for i, row in twf.iterrows()]
    top2 = (df_rank.as_matrix() < 2.5) & (df_rank.as_matrix() > 1.5)
    top2_cols = [df_rank.columns.values[t][0] for t in top2]
    top2_weights = df_data.as_matrix()[top2]
    top3 = (df_rank.as_matrix() < 3.5) & (df_rank.as_matrix() > 2.5)
    top3_cols = [df_rank.columns.values[t][0] for t in top3]
    top3_weights = df_data.as_matrix()[top3]
    print top1_cols
    print top1_weights
    print top1_values
    print len(top1_cols)
    print top3_cols
    print top3_weights
    print len(top3_cols)
    
