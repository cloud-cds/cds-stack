"""
dashan_query.py
"""
import os
import datetime

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = os.environ['db_name']
port = os.environ['db_port']
password = os.environ['db_password']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)
import logging
logging.basicConfig(format='%(levelname)s|%(message)s', level=logging.DEBUG)
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


def get_admittime(eid):
    engine = create_engine(DB_CONN_STR)

    get_admittime_sql = \
    '''
    select value::timestamp from cdm_s inner join pat_enc on pat_enc.enc_id = cdm_s.enc_id
    where pat_id = '%s' and fid = 'admittime'
    ''' % eid
    df_admittime = pd.read_sql_query(get_admittime_sql,con=engine)
    if df_admittime is None or df_admittime.empty:
        return None 
    else:
        return df_admittime.value.values[0].astype(datetime.datetime)/1000000
    

def get_cdm(eid):
    engine = create_engine(DB_CONN_STR)

    get_twf_sql = \
    '''
    select cdm_twf.* from cdm_twf inner join pat_enc on cdm_twf.enc_id = pat_enc.enc_id
    where pat_enc.pat_id = '%s' order by tsp
    ''' % eid
    df_twf = pd.read_sql_query(get_twf_sql,con=engine)
    get_s_sql = \
    '''
    select cdm_s.* from cdm_s inner join pat_enc on cdm_s.enc_id = pat_enc.enc_id
    where pat_enc.pat_id = '%s'
    ''' % eid
    df_s = pd.read_sql_query(get_s_sql,con=engine)
    for idx, row in df_s.iterrows():
        fid = row['fid']
        value = row['value']
        df_twf[fid] = value
    return df_twf


def get_criteria(eid):
    engine = create_engine(DB_CONN_STR)
    get_criteria_sql = \
    '''
    select * from criteria 
    where pat_id = '%s'
    ''' % eid
    df = pd.read_sql_query(get_criteria_sql,con=engine)
    return df

def update_notifications():
    engine = create_engine(DB_CONN_STR)
    engine.execute("select update_notifications()")
    


def get_notifications(eid):
    engine = create_engine(DB_CONN_STR)
    get_notifications_sql = \
    '''
    select * from notifications 
    where pat_id = '%s'
    ''' % eid
    df = pd.read_sql_query(get_notifications_sql,con=engine)
    print df.head()
    notifications = []
    for idx, row in df.iterrows():
        notification = row['message']
        notification['id'] = row['notification_id']
        notifications.append(notification)

    return notifications

def override_criteria(eid, name, value, user='user'):
    engine = create_engine(DB_CONN_STR)
    if name == u'sus-edit':
        override_sql = """
        set time zone 'EST';
        update criteria set
        override_time = now(),
        update_date = now(),
        override_user = '%(user)s',
        value = '%(val)s'
        where pat_id = '%(pid)s' and name = 'suspicion_of_infection';
        """ % {'user': user, 'val':value, 'pid': eid}
        logging.debug("override_sql:" + override_sql)
        engine.execute(override_sql)


def eid_exist(eid):
    engine = create_engine(DB_CONN_STR)
    connection = engine.connect()
    result = connection.execute("select * from pat_enc where pat_id = '%s'" % eid)
    connection.close()
    for row in result:
        return True 
    return False

if __name__ == '__main__':
    # eid = 'E1000109xx'
    # print eid_exist(eid)
    eid = 'E100194473'
    print eid_exist(eid)
    df = get_trews(eid)
    print df.head()
    df_trews = df.drop(['enc_id','trewscore','tsp'],1)
    
    cdm = get_cdm(eid)

    # for each row sort by column
    # for idx, row in df_trews.iterrows():
    #     sorted_row = row.sort_values(ascending=False)
    #     print sorted_row
    #     print sorted_row.index[0]
    #     if sorted_row.index[0] in cdm.iloc[idx]:
    #         print cdm.iloc[idx][sorted_row.index[0]]  
    #     else:
    #         print 0
    sorted_trews = [row.sort_values(ascending=False) for idx, row in df_trews.iterrows()]
        
    names =  [row.index[0] for row in sorted_trews]
    vals = []
    for i, row in enumerate(sorted_trews):
        fid = row.index[0]
        if fid in cdm.iloc[i]:
            vals.append(cdm.iloc[i][fid])
        else:
            vals.append(0)
    for i, n in enumerate(names):
        print n, vals[i]
        #, sorted_row.index[1], sorted_row[1], sorted_row.index[2], sorted_row[2]

    # df_rank = df_trews.rank(axis=1, method='max', ascending=False)
    # top1 = df_rank.as_matrix() < 1.5
    # top1_cols = [df_rank.columns.values[t][0] for t in top1]
    # top1_weights = df_trews.as_matrix()[top1]
    # top1_values = [row[top1_cols[i]] for i, row in cdm.iterrows()]
    # top2 = (df_rank.as_matrix() < 2.5) & (df_rank.as_matrix() > 1.5)
    # top2_cols = [df_rank.columns.values[t][0] for t in top2]
    # top2_weights = df_trews.as_matrix()[top2]
    # top3 = (df_rank.as_matrix() < 3.5) & (df_rank.as_matrix() > 2.5)
    # top3_cols = [df_rank.columns.values[t][0] for t in top3]
    # top3_weights = df_trews.as_matrix()[top3]
    # print top1_cols
    # print top1_weights
    # print top1_values
    # print len(top1_cols)
    # print top3_cols
    # print top3_weights
    # print len(top3_cols)
    
