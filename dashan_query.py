"""
dashan_query.py
"""
import os
import json
import datetime
import logging
import pandas as pd
from inpatient_updater import load
from sqlalchemy import create_engine
from sqlalchemy import text

logging.basicConfig(format='%(levelname)s|%(message)s', level=logging.DEBUG)

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = os.environ['db_name']
port = os.environ['db_port']
password = os.environ['db_password']
epic_notifications = os.environ['epic_notifications']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)

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
    select value::timestamptz from cdm_s inner join pat_enc on pat_enc.enc_id = cdm_s.enc_id
    where pat_id = '%s' and fid = 'admittime'
    ''' % eid
    df_admittime = pd.read_sql_query(get_admittime_sql,con=engine)
    if df_admittime is None or df_admittime.empty:
        return None
    else:
        return df_admittime.value.values[0].astype(datetime.datetime)/1000000000


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
    select * from get_criteria('%s')
    ''' % eid
    df = pd.read_sql_query(get_criteria_sql,con=engine)
    return df

def get_criteria_log(eid):
    engine = create_engine(DB_CONN_STR)
    get_criteria_log_sql = \
    '''
    select log_id, pat_id, date_part('epoch', tsp) epoch, event from criteria_log
    where pat_id = '%s' order by tsp desc;
    ''' % eid
    df = pd.read_sql_query(get_criteria_log_sql,con=engine)
    auditlist = []
    print df.head()
    for idx,row in df.iterrows():
        audit = row['event']
        audit['log_id'] = row['log_id']
        audit['pat_id'] = row['pat_id']
        audit['timestamp'] = row['epoch']
        auditlist.append(audit)
    return auditlist

def get_notifications(eid):
    engine = create_engine(DB_CONN_STR)
    get_notifications_sql = \
    '''
    select * from notifications
    where pat_id = '%s'
    ''' % eid
    df = pd.read_sql_query(get_notifications_sql,con=engine)
    notifications = []
    for idx, row in df.iterrows():
        notification = row['message']
        notification['timestamp'] = long(notification['timestamp'])
        notification['id'] = row['notification_id']
        notifications.append(notification)

    return notifications

def toggle_notification_read(eid, notification_id, as_read):
    engine = create_engine(DB_CONN_STR)
    toggle_notifications_sql = \
    '''
    with update_notifications as
    (   update notifications
        set message = jsonb_set(message::jsonb, '{read}'::text[], '%(val)s'::jsonb, false)
        where pat_id = '%(pid)s' and notification_id = %(nid)s
        RETURNING *
    )
    insert into criteria_log (pat_id, tsp, event, update_date)
    select
            '%(pid)s',
            now(),
            '{"event_type": "toggle_notifications", "message": n.message}'
            now()
    from update_notifications n;
    ''' % {'pid': eid, 'nid': notification_id, 'val': str(as_read).lower()}
    logging.debug("toggle_notifications_read:" + toggle_notifications_sql)
    conn = engine.connect()
    conn.execute(toggle_notifications_sql)
    conn.close()
    push_notifications_to_epic(eid, engine)

def temp_c_to_f(c):
    return c * 1.8 + 32

def override_criteria(eid, name, value='[{}]', user='user', clear=False):
    engine = create_engine(DB_CONN_STR)
    if name == 'sirs_temp' and not clear:
        value[0]['lower'] = temp_c_to_f(float(value[0]['lower']))
        value[0]['upper'] = temp_c_to_f(float(value[0]['upper']))
    params = {
        'user': ("'" + user + "'") if not clear else 'null',
        'val': ("'" + (json.dumps(value) if isinstance(value, list) else value) + "'") if not clear else 'null',
        'name': name if name != 'sus-edit' else 'suspicion_of_infection',
        'pid': eid,
        'user_log': user,
        'val_log': (json.dumps(value) if isinstance(value, list) else value) if not clear else 'null',
        'clear_log': 'true' if clear else 'false'
    }
    override_sql = \
    '''
    update criteria set
        override_time = now(),
        update_date = now(),
        override_value = %(val)s,
        override_user = %(user)s
    where pat_id = '%(pid)s' and name = '%(name)s';
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
            '%(pid)s',
            now(),
            '{"event_type": "override", "name":"%(name)s", "uid":"%(user_log)s", "override_value":%(val_log)s, "clear":%(clear_log)s}',
            now()
        );
    select override_criteria_snapshot('%(pid)s');
    ''' % params
    logging.debug("override_criteria sql:" + override_sql)
    conn = engine.connect()
    conn.execute(override_sql)
    conn.close()
    push_notifications_to_epic(eid, engine)

def reset_patient(eid, uid='user', event_id=None):
    engine = create_engine(DB_CONN_STR)
    event_where_clause = '' if event_id is None or event_id == 'None' else 'and event_id = %(evid)s' % {'evid' : event_id }
    reset_sql = """
    update criteria_events set flag = -1
    where pat_id = '%(pid)s' %(where_clause)s;
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
            '%(pid)s',
            now(),
            '{"event_type": "reset", "uid":"%(uid)s"}',
            now()
        );
    delete from notifications where pat_id = '%(pid)s';
    select advance_criteria_snapshot('%(pid)s');
    """ % {'pid': eid, 'where_clause': event_where_clause, 'uid': uid}
    logging.debug("reset_patient:" + reset_sql)
    conn = engine.connect()
    conn.execute(reset_sql)
    conn.close()
    push_notifications_to_epic(eid, engine)

def deactivate(eid, deactivated):
    engine = create_engine(DB_CONN_STR)
    deactivate_sql = '''
    select * from deactivate('%(pid)s', %(deactivated)s);
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
            '%(pid)s',
            now(),
            '{"event_type": "deactivate", "uid":"%(uid)s", "deactivated": %(deactivated)s}',
            now()
        );
    ''' % {'pid': eid, "deactivated": 'true' if deactivated else "false"}
    logging.debug("deactivate user:" + deactivate_sql)
    conn = engine.connect()
    conn.execute(text(deactivate_sql).execution_options(autocommit=True))
    conn.close()
    push_notifications_to_epic(eid, engine)

def get_deactivated(eid):
    engine = create_engine(DB_CONN_STR)
    conn = engine.connect()
    deactivated = conn.execute("select deactivated from pat_status where pat_id = '%s'" % eid).fetchall()
    conn.close()
    if len(deactivated) == 1 or deactivated[0] is True:
        return True
    else:
        return False

def push_notifications_to_epic(eid, engine):
    if epic_notifications is not None and int(epic_notifications):
        notifications_sql = """
            select * from get_notifications_for_epic('%s');
            """ % eid
        notifications = pd.read_sql_query(notifications_sql, con=engine)
        if not notifications.empty:
            patients = [ {'pat_id': n['pat_id'], 'visit_id': n['visit_id'], 'notifications': n['count'],
                                'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")} for i, n in notifications.iterrows()]
            logging.debug("sending notifications to epic")
            client_id = os.environ['jhapi_client_id'],
            client_secret = os.environ['jhapi_client_secret']
            loader = load.Loader('prod', client_id, client_secret)
            loader.load_notifications(patients)
        else:
            logging.debug("no notifications")

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


